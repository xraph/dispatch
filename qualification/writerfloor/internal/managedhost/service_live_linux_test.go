//go:build linux

package managedhost

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/moby/moby/client"
	aid "github.com/xraph/authsome/id"
	apg "github.com/xraph/authsome/store/postgres"
	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
	cpg "github.com/xraph/ctrlplane/store/postgres"
	"github.com/xraph/grove"
	gdriver "github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
	dpg "github.com/xraph/dispatch/store/postgres"
)

// This drill qualifies the real provider-service channel, not outer floor admission.
// Only the fixed qualification administrator invokes CreationAdmission directly.
// The full controller harness can reuse this retained setup in the same test package.
type liveServiceFixture struct {
	AuthDSN, PolicyDSN, CoreDSN, NativeDSN string
	TokenKey                               []byte
	StopController                         func()
	Launcher                               *launcher.Host
	Mux                                    *http.ServeMux
	Core                                   *cpg.Store
	Native                                 *dpg.Store
	Custody                                *CreationTransport
	CreationAdmission                      *CreationAdmission
	Runtime                                *RuntimeHost
	Enrollment                             *launcher.ServiceEnrollment
	Plan                                   launcher.ServicePlan
	Startup                                launcher.ServiceStartup
	Action                                 managed.ActionSubmission
	Start                                  managed.ProviderStepAcceptance
	Container                              string
	Network                                string
	NativeIdentity                         durable.QueryRuntimeIdentity
	Auth                                   *apg.Store
	SDK                                    *docker.SDKManagedDockerTransport
	Root                                   string
	certificate                            tls.Certificate
	roots                                  *x509.CertPool
}

func liveRequired(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		if os.Getenv("WRITERFLOOR_SERVICE_TEST_REQUIRED") == "1" {
			t.Fatalf("required %s is absent", name)
		}
		t.Skip("live service drill requires " + name)
	}
	return v
}
func liveRandom(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw)
}
func liveJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func liveGrove(t *testing.T, dsn string) *grove.DB {
	t.Helper()
	// Each role has its own pool; reserve server capacity for authentication and control.
	driver := pgdriver.New()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := driver.Open(ctx, dsn, gdriver.WithPoolSize(4)); err != nil {
		t.Fatal("open dedicated PostgreSQL", err)
	}
	db, err := grove.Open(driver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}
func liveDatabase(t *testing.T, adminDSN, role string) (string, string) {
	t.Helper()
	u, err := url.Parse(adminDSN)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("live administrative DSN must be a PostgreSQL URL")
	}
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	name := "wf_service_" + role + "_" + liveRandom(t)
	if _, err = admin.ExecContext(t.Context(), `CREATE DATABASE "`+name+`"`); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, `DROP DATABASE "`+name+`" WITH (FORCE)`)
		if err := errors.Join(dropErr, admin.Close()); err != nil {
			t.Error("drop dedicated database", name, err)
		}
	})
	u.Path = "/" + name
	local := u.String()
	u.Host = liveRequired(t, "WRITERFLOOR_SERVICE_TEST_NATIVE_HOST")
	return local, u.String()
}
func livePrivate(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}
func liveTLS(t *testing.T, root, host string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	_, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixed service qualification CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(caKey)
	if err != nil {
		t.Fatal(err)
	}
	livePrivate(t, filepath.Join(root, "proxy-ca.pem"), caPEM)
	livePrivate(t, filepath.Join(root, "proxy-ca-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	livePrivate(t, filepath.Join(root, "upstream-ca.pem"), caPEM)
	_, serverKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverCert := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: host}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if ip := net.ParseIP(host); ip != nil {
		serverCert.IPAddresses = []net.IP{ip}
	} else {
		serverCert.DNSNames = []string{host}
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverCert, ca, serverKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER}))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA pool")
	}
	return pair, roots
}
func liveCoreCommand(t *testing.T, f *liveServiceFixture) managed.Command {
	t.Helper()
	target, err := f.Core.ReadManagedTarget(t.Context(), f.Plan.Target)
	if err != nil {
		t.Fatal(err)
	}
	return managed.Command{Key: f.Plan.Target, ID: cid.New(managed.PrefixOperation), Revision: target.Revision, LeaseEpoch: target.LeaseEpoch, Owner: "service-drill"}
}
func liveCreate(t *testing.T, f *liveServiceFixture, command docker.ManagedDockerCommand) string {
	t.Helper()
	service := ""
	if command.Service != nil {
		service = command.Service.Name
	}
	request := managed.ProviderStepAuthorization{Command: liveCoreCommand(t, f), Step: managed.ProviderStepKey{OperationKey: managed.OperationKey{Key: f.Plan.Target, ActionID: f.Action.ID}, ProviderOperationID: f.Plan.ProviderOperationID, Kind: command.Kind, Service: service}}
	invocation, err := f.CreationAdmission.Authorize(t.Context(), request, command)
	if err != nil {
		t.Fatal("fresh creation authorization", err)
	}
	if err = f.CreationAdmission.Verify(t.Context(), invocation); err != nil {
		t.Fatal(err)
	}
	if err = f.CreationAdmission.Dispatch(t.Context(), admission.Delegation{Invocation: invocation}); err != nil {
		t.Fatal("one-shot creation", err)
	}
	custody, err := f.Custody.ReadCreation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if command.Kind == managed.StepCreateNetwork {
		f.Network = custody.ResourceID
	} else {
		f.Container = custody.ResourceID
	}
	if err = f.CreationAdmission.Dispatch(t.Context(), admission.Delegation{Invocation: invocation}); !errors.Is(err, managed.ErrAuthority) {
		t.Fatal("creation was resendable", err)
	}
	accepted, err := f.Core.ReadManagedProviderStepAcceptance(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Core.RecordManagedProviderStepResult(t.Context(), managed.ProviderStepResult{Permission: accepted.Permission, Outcome: managed.PortTerminal, ResourceID: custody.ResourceID, Evidence: liveJSON(t, custody), ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	return custody.ResourceID
}

func newLiveService(t *testing.T) *liveServiceFixture { return newLiveServiceSibling(t, nil) }

func newLiveServiceSibling(t *testing.T, sibling *liveServiceFixture) *liveServiceFixture {
	adminDSN := liveRequired(t, "WRITERFLOOR_SERVICE_TEST_DSN")
	dockerHost := liveRequired(t, "WRITERFLOOR_SERVICE_TEST_DOCKER_HOST")
	image := liveRequired(t, "WRITERFLOOR_SERVICE_TEST_IMAGE")
	artifact := liveRequired(t, "WRITERFLOOR_SERVICE_TEST_ARTIFACT")
	hostIP := liveRequired(t, "WRITERFLOOR_SERVICE_TEST_HOST")
	if os.Geteuid() != 0 {
		t.Fatal("live service drill needs isolated Linux root for protected enrollment")
	}
	if !objectID(artifact) {
		t.Fatal("invalid measured executable digest")
	}
	root, err := os.MkdirTemp("/tmp", "wf-service-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("retained private failure journal", root)
			return
		}
		if removeErr := os.RemoveAll(root); removeErr != nil {
			t.Error(removeErr)
		}
	})
	dirs := map[string]string{}
	for _, name := range []string{"config", "host", "services", "custody"} {
		dirs[name] = filepath.Join(root, name)
		if err = os.Mkdir(dirs[name], 0o700); err != nil {
			t.Fatal(err)
		}
	}
	var cert tls.Certificate
	var roots *x509.CertPool
	if sibling == nil {
		cert, roots = liveTLS(t, dirs["config"], hostIP)
	} else {
		cert, roots = sibling.certificate, sibling.roots
	}
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	coreDSN, _ := liveDatabase(t, adminDSN, "core")
	f := &liveServiceFixture{Root: root, CoreDSN: coreDSN, Core: cpg.New(liveGrove(t, coreDSN)), certificate: cert, roots: roots}
	if err = f.Core.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	var serviceDSN string
	if sibling == nil {
		authDSN, _ := liveDatabase(t, adminDSN, "auth")
		policyDSN, _ := liveDatabase(t, adminDSN, "policy")
		var nativeDSN string
		nativeDSN, serviceDSN = liveDatabase(t, adminDSN, "native")
		f.Native, f.Auth = dpg.New(liveGrove(t, nativeDSN)), apg.New(liveGrove(t, authDSN))
		f.AuthDSN, f.PolicyDSN, f.NativeDSN = authDSN, policyDSN, nativeDSN
		if err = f.Native.Migrate(t.Context()); err != nil {
			t.Fatal(err)
		}
		tokenKey := make([]byte, 32)
		if _, err = rand.Read(tokenKey); err != nil {
			t.Fatal(err)
		}
		f.TokenKey = append([]byte(nil), tokenKey...)
		var closeHost func() error
		f.Launcher, closeHost, err = launcher.OpenQualificationServiceHost(t.Context(), launcher.QualificationServiceConfiguration{ConfigurationDirectory: dirs["config"], StateDirectory: dirs["host"], AuthDSN: authDSN, PolicyDSN: policyDSN, TokenKey: tokenKey, Tenant: "service-qualification"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if closeErr := closeHost(); closeErr != nil {
				t.Error(closeErr)
			}
		})
	} else {
		f.Native, f.Auth, f.Launcher = sibling.Native, sibling.Auth, sibling.Launcher
		f.AuthDSN, f.PolicyDSN, f.NativeDSN, f.TokenKey = sibling.AuthDSN, sibling.PolicyDSN, sibling.NativeDSN, append([]byte(nil), sibling.TokenKey...)
	}
	runtimeID, instanceID, hostID, err := launcher.NewServiceProcessIDs()
	if err != nil {
		t.Fatal(err)
	}
	target := managed.Key{TenantID: "service-qualification", InstanceID: cid.New(cid.PrefixInstance)}
	key := durable.Key{Namespace: "service-native", WorkflowID: "managed-history", RunID: "original"}
	configuration := liveJSON(t, struct {
		DSN    string          `json:"dsn"`
		Probes []RetainedProbe `json:"probes"`
	}{serviceDSN, []RetainedProbe{{Name: "saved-status", Request: drt.QueryRequest{Key: key, Selection: durable.RunExplicit, BuildID: "service-v1", Name: "status"}, ExpectedDigest: managed.Digest([]byte(`{"retained":true}`))}}})
	if sibling != nil {
		configuration = append([]byte(nil), sibling.Plan.Configuration...)
	}
	f.Plan = launcher.ServicePlan{BootstrapID: "worker", Target: target, ProviderOperationID: cid.New(managed.PrefixOperation), Service: managed.ProvisionService{Name: "worker", Image: image, Role: "main", CPUMillis: "1000", MemoryMiB: "256", PIDLimit: "128", Startup: managed.StartupBindingRef{BindingID: "service-worker", BindingGeneration: "1"}}, Endpoint: "https://" + net.JoinHostPort(hostIP, port), ServerName: hostIP, RuntimeID: runtimeID, InstanceID: instanceID, HostID: hostID, Installation: "service-installation", Namespace: key.Namespace, Build: "service-v1", Queue: "service-queue", ArtifactDigest: artifact, ConfigurationDigest: managed.Digest(configuration), ConfigurationVersion: "v1", ProbePolicyID: "saved-status", ProbePolicyVersion: "1", MaximumProofValidityNS: "60000000000", VerifierID: "service-verifier", ProducerVerifier: "service-producer", Configuration: configuration}
	if sibling != nil {
		f.Plan.BootstrapID = "survivor"
		f.Plan.Service.Startup.BindingID = "service-survivor"
	}
	digest, err := launcher.ServicePlanDigest(f.Plan)
	if err != nil {
		t.Fatal(err)
	}
	f.Plan.Service.Startup.BindingDigest = digest
	var custody CreationTransport
	f.Enrollment, err = launcher.NewServiceEnrollment(f.Launcher, dirs["services"], &custody, []launcher.ServicePlan{f.Plan})
	if err != nil {
		t.Fatal(err)
	}
	f.SDK, err = docker.NewSDKManagedDockerTransport(dockerHost, "1.44", &http.Transport{ResponseHeaderTimeout: 10 * time.Second}, f.Enrollment)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := f.SDK.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	journal, err := docker.OpenFileManagedDockerJournal(dirs["custody"])
	if err != nil {
		t.Fatal(err)
	}
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	livePrivate(t, filepath.Join(dirs["config"], "creation-signing-key"), signingKey)
	f.Custody, err = NewCreationTransport(journal, f.SDK, f.Core, "fixed-service-rootless", signingKey, managed.RecoveryProducerIdentity{ProducerID: "service-custodian", Profile: docker.ManagedDockerProfile, Version: "1", VerifierID: "service-custodian"})
	if err != nil {
		t.Fatal(err)
	}
	custody = *f.Custody
	f.CreationAdmission, err = NewCreationAdmission(f.Core, f.Custody)
	if err != nil {
		t.Fatal(err)
	}
	f.Startup, err = f.Enrollment.Prepare(t.Context(), f.Plan.BootstrapID)
	if err != nil {
		t.Fatal("real machine enrollment", err)
	}
	f.Runtime, err = NewRuntimeHost(t.Context(), f.Enrollment, []string{f.Plan.BootstrapID})
	if err != nil {
		t.Fatal(err)
	}
	f.Mux = http.NewServeMux()
	f.Mux.Handle("/managed-service/", f.Runtime.Handler())
	server := httptest.NewUnstartedServer(f.Mux)
	if err = server.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	server.Listener = listener
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	f.StopController = func() { server.CloseClientConnections(); server.Close() }
	t.Cleanup(server.Close)
	// Teardown has a separate administrator client. The execution adapter deliberately
	// refuses non-success responses, including NotFound, so it cannot prove cleanup.
	// Only exact returned IDs are removed here; none of these results enter custody.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cleanup, cleanupErr := client.New(client.WithHost(dockerHost), client.WithAPIVersion("1.44"))
		if cleanupErr != nil {
			t.Error("cleanup client", cleanupErr)
			return
		}
		defer func() {
			if closeErr := cleanup.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}()
		if f.Container != "" {
			if t.Failed() {
				f.Runtime.mu.Lock()
				if slot := f.Runtime.slots[f.Plan.BootstrapID]; slot != nil {
					t.Logf("failed fixture last channel refusal: %s", slot.refusal)
				}
				f.Runtime.mu.Unlock()
				inspection, inspectErr := cleanup.ContainerInspect(ctx, f.Container, client.ContainerInspectOptions{})
				if inspectErr == nil && inspection.Container.State != nil {
					state := inspection.Container.State
					t.Logf("failed fixture container state: status=%s exit=%d oom=%t", state.Status, state.ExitCode, state.OOMKilled)
					logs, logsErr := cleanup.ContainerLogs(ctx, f.Container, client.ContainerLogsOptions{ShowStderr: true, Tail: "8"})
					if logsErr == nil {
						raw, readErr := io.ReadAll(io.LimitReader(logs, 4096))
						closeErr := logs.Close()
						if readErr == nil && closeErr == nil {
							for _, match := range regexp.MustCompile(`managed service failure: ([a-zA-Z0-9_]+)`).FindAllSubmatch(raw, 8) {
								t.Logf("failed fixture service category: %s", match[1])
							}
						}
					}
				}
			}
			if _, cleanupErr = cleanup.ContainerRemove(ctx, f.Container, client.ContainerRemoveOptions{Force: true}); cleanupErr != nil && !cerrdefs.IsNotFound(cleanupErr) {
				t.Error("exact-ID container cleanup", f.Container, cleanupErr)
			}
		}
		if f.Network != "" {
			if _, cleanupErr = cleanup.NetworkRemove(ctx, f.Network, client.NetworkRemoveOptions{}); cleanupErr != nil && !cerrdefs.IsNotFound(cleanupErr) {
				t.Error("exact-ID network cleanup", f.Network, cleanupErr)
			}
		}
	})

	provision := liveJSON(t, managed.ProvisionSpec{Version: "1", Services: []managed.ProvisionService{f.Plan.Service}})
	spec := liveJSON(t, managed.ManagedSpec{Profile: "ctrlplane.managed.v1", RuntimeRoles: []managed.RuntimeRole{managed.Polling}, Provision: provision})
	identity := managed.Identity{Key: target, ProvisionOperationID: f.Plan.ProviderOperationID, Physical: managed.Physical{Provider: "docker", Placement: "fixed-service-rootless", Project: "cp-" + target.InstanceID.String(), Generation: target.InstanceID.String()}, Manifest: managed.Manifest{Version: 1, Spec: spec, SpecDigest: managed.Digest(spec), Members: []managed.Member{{Name: "worker", Service: "worker", OwnerID: cid.New(cid.PrefixWorkload), Installation: f.Plan.Installation, Namespace: f.Plan.Namespace, Queue: f.Plan.Queue, Build: f.Plan.Build, ArtifactDigest: artifact, ConfigurationDigest: f.Plan.ConfigurationDigest}}}}
	f.Action = managed.ActionSubmission{Command: managed.Command{Key: target, ID: cid.New(managed.PrefixOperation)}, Kind: managed.ActionDeploy, Installation: f.Plan.Installation, Authority: "service-qualification", ApplicationID: f.Startup.App, OrganizationID: "service-qualification", Actor: managed.AuditActor{Subject: "qualification-administrator"}, OriginalCommand: []byte(`{"operation":"service-enrollment-drill"}`), NewTarget: &identity, ReleaseID: cid.New(cid.PrefixRelease), OccurredAt: time.Now().UTC()}
	f.Action.OriginalDigest = managed.Digest(f.Action.OriginalCommand)
	f.Action.CanonicalDigest, err = managed.ActionDigest(f.Action)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Core.SubmitManagedAction(t.Context(), f.Action); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Core.ClaimManagedLease(t.Context(), managed.LeaseRequest{Command: liveCoreCommand(t, f), Duration: managed.MaxLease}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Core.ProjectManagedRows(t.Context(), managed.ProjectionRequest{Command: liveCoreCommand(t, f), ActionID: f.Action.ID, Stage: managed.ProjectionPending}); err != nil {
		t.Fatal(err)
	}
	network := liveCreate(t, f, docker.ManagedDockerCommand{Kind: managed.StepCreateNetwork, Name: "managed-" + target.InstanceID.String() + "-network"})
	command := docker.ManagedDockerCommand{Kind: managed.StepCreateContainer, Name: "managed-" + target.InstanceID.String() + "-worker", NetworkID: network, Service: &f.Plan.Service}
	liveCreate(t, f, command)
	if _, err = f.Enrollment.BindCreated(t.Context(), f.Plan.BootstrapID, command); err != nil {
		t.Fatal(err)
	}
	start := managed.ProviderStepAuthorization{Command: liveCoreCommand(t, f), Step: managed.ProviderStepKey{OperationKey: managed.OperationKey{Key: target, ActionID: f.Action.ID}, ProviderOperationID: f.Plan.ProviderOperationID, Kind: managed.StepStartContainer, Service: "worker"}}
	grant, err := f.Core.AuthorizeManagedProviderStep(t.Context(), start)
	if err != nil || !grant.NewlyIssued || grant.Permission.ResourceID != f.Container {
		t.Fatal("exact fresh start", err)
	}
	f.Start, err = f.Core.ReadManagedProviderStepAcceptance(t.Context(), start)
	if err != nil {
		t.Fatal(err)
	}
	livePrivate(t, filepath.Join(dirs["config"], "original-start-acceptance.json"), liveJSON(t, f.Start))
	if _, err = f.SDK.Execute(t.Context(), docker.ManagedDockerCommand{Kind: managed.StepStartContainer, ResourceID: f.Container}); err != nil {
		t.Fatal("start actual service", err)
	}
	targetRuntime := durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: f.Plan.Installation, Namespace: f.Plan.Namespace}, BuildID: f.Plan.Build}, RuntimeID: f.Plan.RuntimeID}
	deadline := time.Now().Add(30 * time.Second)
	for {
		f.NativeIdentity, err = f.Runtime.EnrolledIdentity(targetRuntime)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("authenticated service readiness timed out", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if sibling == nil {
		if _, err = f.Native.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: f.Plan.Installation, Namespace: f.Plan.Namespace, AppID: f.Startup.App, TenantID: f.Startup.Tenant, RequireAudit: true, SchemaVersion: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err = f.Native.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: targetRuntime.NamespaceTarget, RequestID: "service-retirement", SchemaVersion: 1, WriterProtocol: durable.RetirementWriterProtocol}); err != nil {
			t.Fatal(err)
		}
		if _, err = f.Native.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: targetRuntime.BuildTarget, RequestID: "service-build", ExpectedVersion: 0, Identity: &f.NativeIdentity.BuildIdentity}); err != nil {
			t.Fatal(err)
		}
		if _, err = f.Native.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "saved-history", WorkflowType: "managed-history", BuildID: f.Plan.Build, Queue: f.Plan.Queue, Input: []byte(`{"retained":true}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if status, err := f.Runtime.StartWorker(t.Context(), targetRuntime); err != nil || !status.Ready {
		t.Fatal("actual worker start", err)
	}
	t.Log("actual signed service resources", f.Network, f.Container)
	return f
}

func TestLiveProviderServiceEnrollmentAndNativeChannel(t *testing.T) {
	f := newLiveService(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	target := f.NativeIdentity.QueryRuntimeTarget
	identity, err := f.Runtime.ResolveBinding(ctx, target)
	if err != nil || identity != f.NativeIdentity {
		t.Fatal("real worker identity", err)
	}
	// The deadline intentionally retains Go's monotonic component across JSON.
	handle, err := f.Runtime.BeginDrain(ctx, target, drt.DrainRequest{OperationID: "original-service-drain", Deadline: time.Now().Add(45 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.Runtime.WaitDrain(ctx, target, handle)
	if err != nil || !result.Complete || !result.Quiescent || result.InFlight != 0 || result.UnknownClaims != 0 {
		t.Fatal("real native drain", err)
	}
	proof, err := f.Runtime.Verify(ctx, durable.QueryRuntimeBinding{QueryRuntimeIdentity: identity})
	if err != nil || proof.Validate(identity, proof.VerifiedAt) != nil {
		t.Fatal("real retained query proof", err)
	}
	if err = f.Core.RecordManagedProviderStepResult(ctx, managed.ProviderStepResult{Permission: f.Start.Permission, Outcome: managed.PortTerminal, ResourceID: f.Container, Evidence: liveJSON(t, proof), ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal("retained start outcome", err)
	}
	f.Runtime.mu.Lock()
	binding := *f.Runtime.slots[f.Plan.BootstrapID].binding
	f.Runtime.mu.Unlock()
	client, err := newServiceClient(f.Startup)
	if err != nil {
		t.Fatal(err)
	}
	client.boot = binding.BootNonce
	defer client.client.CloseIdleConnections()
	wrong := *client
	wrong.boot = strings.Repeat("f", 64)
	var out ServiceCommand
	if _, err = wrong.post(ctx, "/managed-service/next", ServiceResponse{BootstrapID: f.Plan.BootstrapID}, &out); err == nil {
		t.Fatal("wrong boot authenticated")
	}
	// End the actual process by its returned ID before the administrative negative
	// channel test, so its own pending poll cannot consume the test wake-up. This is
	// test teardown, not managed removal or guard-closure evidence.
	if _, err = f.SDK.Execute(ctx, docker.ManagedDockerCommand{Kind: managed.StepRemoveContainer, ResourceID: f.Container}); err != nil {
		t.Fatal("exact service teardown", err)
	}
	f.Container = ""
	// The real worker has drained. A separate administrative test client holds a
	// genuine authenticated empty poll while the exact channel is permanently closed.
	response := make(chan error, 1)
	go func() {
		var command ServiceCommand
		_, pollErr := client.post(ctx, "/managed-service/next", ServiceResponse{BootstrapID: f.Plan.BootstrapID}, &command)
		response <- pollErr
	}()
	// Authentication has an explicit five-second bound; wait beyond it before close.
	select {
	case err = <-response:
		t.Fatal("empty poll ended before closure", err)
	case <-time.After(6 * time.Second):
	}
	if err = f.Enrollment.Close(ctx, f.Plan.BootstrapID); err != nil {
		t.Fatal(err)
	}
	keyID, err := aid.ParseAPIKeyID(f.Startup.Key)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := f.Auth.GetAPIKey(ctx, keyID)
	if err != nil || saved == nil || !saved.Revoked {
		t.Fatal("real persisted API key was not revoked", err)
	}
	// A new command after closure must not escape through the previously admitted poll.
	callDone := make(chan error, 1)
	go func() {
		bounded, stop := context.WithTimeout(ctx, 3*time.Second)
		defer stop()
		_, callErr := f.Runtime.ResolveBinding(bounded, target)
		callDone <- callErr
	}()
	select {
	case err = <-response:
		if err == nil {
			t.Fatal("closed pending poll delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closed pending poll not refused")
	}
	if err = <-callDone; err == nil {
		t.Fatal("closed channel resolved a live worker")
	}
	if _, err = client.post(ctx, "/managed-service/next", ServiceResponse{BootstrapID: f.Plan.BootstrapID}, &out); err == nil {
		t.Fatal("revoked closed credential authenticated")
	}
}

// A Docker restart is a new process boot. It cannot silently inherit the
// enrollment of the killed process, even when its container and image are exact.
func TestLiveKilledWorkerCannotReuseBootEnrollment(t *testing.T) {
	f := newLiveService(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	identity := f.NativeIdentity
	key := durable.Key{Namespace: f.Plan.Namespace, WorkflowID: "managed-history", RunID: "original"}
	completionDeadline := time.Now().Add(10 * time.Second)
	for {
		execution, readErr := f.Native.GetExecution(ctx, key)
		if readErr == nil && execution.State == durable.StateCompleted {
			break
		}
		if time.Now().After(completionDeadline) {
			t.Fatal("workflow did not complete before crash", readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	before, err := f.Native.ReadHistory(ctx, key, 0, 1000)
	if err != nil || len(before) == 0 || len(before) >= 1000 {
		t.Fatal("history before process kill", err)
	}
	f.Runtime.mu.Lock()
	boot := *f.Runtime.slots[f.Plan.BootstrapID].binding
	f.Runtime.mu.Unlock()
	liveRestoreDocker(t, nil, 1024, "kill", "--signal", "KILL", f.Container)
	liveRestoreDocker(t, nil, 1024, "start", f.Container)
	deadline := time.Now().Add(10 * time.Second)
	for {
		state := strings.TrimSpace(string(liveRestoreDocker(t, nil, 1024, "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", f.Container)))
		if state == "exited 1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted process did not refuse retained boot enrollment", state)
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.Runtime.mu.Lock()
	unchanged := *f.Runtime.slots[f.Plan.BootstrapID].binding == boot
	f.Runtime.mu.Unlock()
	if !unchanged {
		t.Fatal("restart replaced original process custody")
	}
	after, err := f.Native.ReadHistory(ctx, key, 0, 1000)
	if err != nil || managed.Digest(liveJSON(t, before)) != managed.Digest(liveJSON(t, after)) {
		t.Fatal("rejected restart changed persisted history", err)
	}
	bounded, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err = f.Runtime.ResolveBinding(bounded, identity.QueryRuntimeTarget); err == nil {
		t.Fatal("dead original process was reported live")
	}
	t.Log("actual SIGKILL and Docker restart refused boot reuse; persisted history unchanged", f.Container)
}
