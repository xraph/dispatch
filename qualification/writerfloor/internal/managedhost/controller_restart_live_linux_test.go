//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
	cpg "github.com/xraph/ctrlplane/store/postgres"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
	dpg "github.com/xraph/dispatch/store/postgres"
)

type liveControllerBootstrap struct {
	Host                                                               launcher.QualificationReopenConfiguration
	CoreDSN, NativeDSN, DockerHost, ServiceDirectory, CustodyDirectory string
	Plan                                                               launcher.ServicePlan
	CreationKey                                                        []byte
	Certificates                                                       [][]byte
	TLSKey, CA                                                         []byte
	Binding                                                            launcher.ServiceBinding
	Identity                                                           durable.QueryRuntimeIdentity
	Drain                                                              drt.DrainHandle
	Receipt                                                            durable.LifecycleReceipt
	ResultPath                                                         string
}
type liveControllerResult struct {
	PID           int
	Binding       launcher.ServiceBinding
	Identity      durable.QueryRuntimeIdentity
	Drain         drt.DrainResult
	Proof         durable.QueryRuntimeVerification
	ReceiptDigest string
}

func readControllerPrivate(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, managed.ErrAuthority
	}
	directory, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	owner, ok := directory.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || !directory.IsDir() || directory.Mode().Perm() != 0o700 {
		return nil, managed.ErrAuthority
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	owner, ok = stat.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || !stat.Mode().IsRegular() || stat.Mode().Perm() != 0o600 || stat.Size() < 1 || stat.Size() > 256<<10 {
		return nil, managed.ErrAuthority
	}
	raw, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if err != nil || len(raw) > 256<<10 {
		return nil, managed.ErrAuthority
	}
	return raw, nil
}

// TestLiveControllerProcessChild is invoked only with a root-private bootstrap
// path. It reopens the retained authority and serves the existing worker boot.
func TestLiveControllerProcessChild(t *testing.T) {
	path := os.Getenv("WRITERFLOOR_CONTROLLER_CHILD_CONFIG")
	if path == "" {
		t.Skip("private controller subprocess entry")
	}
	if os.Geteuid() != 0 {
		t.Fatal("controller child requires isolated root")
	}
	raw, err := readControllerPrivate(path)
	if err != nil {
		t.Fatal("private controller bootstrap refused")
	}
	var c liveControllerBootstrap
	if err = json.Unmarshal(raw, &c); err != nil || !bytes.Equal(raw, liveJSON(t, c)) {
		t.Fatal("controller bootstrap encoding refused")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	host, closeHost, err := launcher.ReopenQualificationServiceHost(ctx, c.Host)
	if err != nil {
		t.Fatal("retained host refused")
	}
	defer closeHost()
	core := cpg.New(liveGrove(t, c.CoreDSN))
	native := dpg.New(liveGrove(t, c.NativeDSN))
	var custody CreationTransport
	enrollment, err := launcher.NewServiceEnrollment(host, c.ServiceDirectory, &custody, []launcher.ServicePlan{c.Plan})
	if err != nil {
		t.Fatal("retained enrollment refused")
	}
	sdk, err := docker.NewSDKManagedDockerTransport(c.DockerHost, "1.44", &http.Transport{ResponseHeaderTimeout: 10 * time.Second}, enrollment)
	if err != nil {
		t.Fatal("configured Docker reader refused")
	}
	defer sdk.Close()
	journal, err := docker.OpenFileManagedDockerJournal(c.CustodyDirectory)
	if err != nil {
		t.Fatal("retained creation journal refused")
	}
	created, err := NewCreationTransport(journal, sdk, core, "fixed-service-rootless", ed25519.PrivateKey(c.CreationKey), managed.RecoveryProducerIdentity{ProducerID: "service-custodian", Profile: docker.ManagedDockerProfile, Version: "1", VerifierID: "service-custodian"})
	if err != nil {
		t.Fatal("signed creation reader refused")
	}
	custody = *created
	binding, found, err := enrollment.RetainedBinding(ctx, c.Plan.BootstrapID)
	if err != nil || !found || binding != c.Binding {
		t.Fatal("retained boot binding refused")
	}
	runtime, err := NewRuntimeHost(ctx, enrollment, []string{c.Plan.BootstrapID})
	if err != nil {
		t.Fatal("retained runtime refused")
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(c.TLSKey)
	if err != nil {
		t.Fatal("private server key refused")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(c.CA) {
		t.Fatal("retained server CA refused")
	}
	endpoint, err := url.Parse(c.Plan.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Port() == "" {
		t.Fatal("fixed endpoint refused")
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", net.JoinHostPort("0.0.0.0", endpoint.Port()))
	if err != nil {
		t.Fatal("fixed listener unavailable")
	}
	waitingForIdle := atomic.Bool{}
	idle := make(chan struct{}, 1)
	runtimeHandler := runtime.Handler()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if waitingForIdle.Load() && r.URL.Path == "/managed-service/next" {
			if authenticated, e := enrollment.AuthenticateChannel(r.Context(), r, c.Plan.BootstrapID); e == nil && authenticated == binding {
				select {
				case idle <- struct{}{}:
				default:
				}
			}
		}
		runtimeHandler.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{{Certificate: c.Certificates, PrivateKey: privateKey}}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}}
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(tls.NewListener(listener, server.TLSConfig)) }()
	defer server.Close()
	// Each observation travels through the same live process's authenticated poll.
	identity, err := runtime.ResolveBinding(ctx, c.Identity.QueryRuntimeTarget)
	if err != nil || identity != c.Identity {
		t.Fatal("fresh restarted-controller identity challenge failed")
	}
	result, err := runtime.WaitDrain(ctx, c.Identity.QueryRuntimeTarget, c.Drain)
	if err != nil || !sameDrainHandle(result.Handle, c.Drain) || !result.Complete || !result.Quiescent || result.DeadlineExpired || result.InFlight != 0 || result.UnknownClaims != 0 {
		t.Fatal("retained process drain challenge failed")
	}
	proof, err := runtime.Verify(ctx, durable.QueryRuntimeBinding{QueryRuntimeIdentity: identity})
	if err != nil || proof.Validate(identity, time.Now().UTC()) != nil {
		t.Fatal("fresh restarted-controller query failed")
	}
	waitingForIdle.Store(true)
	select {
	case <-idle:
	case <-ctx.Done():
		t.Fatal("worker did not return to authenticated idle polling")
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: c.Receipt.NamespaceTarget, Operation: c.Receipt.Operation, RequestID: c.Receipt.RequestID, RequestDigest: c.Receipt.RequestDigest}
	receipt, err := native.LookupLifecycleReceipt(ctx, lookup)
	if err != nil || receipt.Match(lookup) != nil || !bytes.Equal(liveJSON(t, receipt), liveJSON(t, c.Receipt)) {
		t.Fatal("original persistent drain receipt changed")
	}
	output := liveControllerResult{PID: os.Getpid(), Binding: binding, Identity: identity, Drain: result, Proof: proof, ReceiptDigest: managed.Digest(liveJSON(t, receipt))}
	if filepath.Dir(c.ResultPath) != filepath.Dir(path) {
		t.Fatal("result scope refused")
	}
	file, err := os.OpenFile(c.ResultPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		t.Fatal("private result unavailable")
	}
	if _, err = file.Write(liveJSON(t, output)); err != nil {
		_ = file.Close()
		t.Fatal("private result write failed")
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal("private result sync failed")
	}
	if err = file.Close(); err != nil {
		t.Fatal("private result close failed")
	}
	select {
	case <-ctx.Done():
		t.Fatal("controller child exceeded bounded lifetime")
	case <-serving:
		t.Fatal("controller listener stopped unexpectedly")
	}
}

func startLiveController(t *testing.T, c liveControllerBootstrap) (liveControllerResult, func()) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	c.ResultPath = filepath.Join(dir, "result.json")
	config := filepath.Join(dir, "bootstrap.json")
	livePrivate(t, config, liveJSON(t, c))
	log, err := os.OpenFile(filepath.Join(dir, "child.log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestLiveControllerProcessChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), "WRITERFLOOR_CONTROLLER_CHILD_CONFIG="+config)
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal("start real controller child", err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if err := log.Close(); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := readControllerPrivate(c.ResultPath)
		if readErr == nil {
			var out liveControllerResult
			if json.Unmarshal(raw, &out) == nil && bytes.Equal(raw, liveJSON(t, out)) {
				if out.PID != cmd.Process.Pid || out.PID == os.Getpid() {
					t.Fatal("not a separate controller process")
				}
				return out, stop
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	t.Fatalf("controller child did not produce authenticated evidence; private log %s", filepath.Join(dir, "child.log"))
	return liveControllerResult{}, nil
}

func TestLiveControllerProcessRestartRetainsOriginalWorker(t *testing.T) {
	f := newLiveService(t)
	_, client := newLiveOperator(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	request := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: operator.BuildInput{Namespace: f.Plan.Namespace, BuildID: f.Plan.Build}, RuntimeID: f.Plan.RuntimeID}, RequestID: "restart-original-drain", OperationID: "restart-original-operation", Deadline: durable.Timestamp(time.Now().Add(2 * time.Minute))}
	drained, err := client.DrainWorker(ctx, request)
	if err != nil || !drained.Complete || !drained.Quiescent {
		t.Fatal("original native drain", err)
	}
	receipt := liveNativeReceipt(t, f, durable.OperationRequestWorkerDrain, request.RequestID, liveFingerprint(t, "operator.worker.drain.v1", request))
	binding, found, err := f.Enrollment.RetainedBinding(ctx, f.Plan.BootstrapID)
	if err != nil || !found {
		t.Fatal("original retained boot", err)
	}
	key, err := os.ReadFile(filepath.Join(f.Root, "config", "creation-signing-key"))
	if err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(filepath.Join(f.Root, "config", "upstream-ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	tlsKey, err := x509.MarshalPKCS8PrivateKey(f.certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	c := liveControllerBootstrap{Host: launcher.QualificationReopenConfiguration{QualificationServiceConfiguration: launcher.QualificationServiceConfiguration{ConfigurationDirectory: filepath.Join(f.Root, "config"), StateDirectory: filepath.Join(f.Root, "host"), AuthDSN: f.AuthDSN, PolicyDSN: f.PolicyDSN, TokenKey: f.TokenKey, Tenant: f.Startup.Tenant}, App: f.Startup.App, Environment: f.Startup.Environment}, CoreDSN: f.CoreDSN, NativeDSN: f.NativeDSN, DockerHost: liveRequired(t, "WRITERFLOOR_SERVICE_TEST_DOCKER_HOST"), ServiceDirectory: filepath.Join(f.Root, "services"), CustodyDirectory: filepath.Join(f.Root, "custody"), Plan: f.Plan, CreationKey: key, Certificates: f.certificate.Certificate, TLSKey: tlsKey, CA: ca, Binding: binding, Identity: f.NativeIdentity, Drain: drt.DrainHandle{RuntimeID: f.Plan.RuntimeID, DrainRequest: drt.DrainRequest{OperationID: request.OperationID, Deadline: request.Deadline}}, Receipt: receipt}
	f.StopController()
	first, stopFirst := startLiveController(t, c)
	stopFirst()
	second, stopSecond := startLiveController(t, c)
	defer stopSecond()
	if first.PID == second.PID || first.Binding != binding || second.Binding != binding || first.Identity != f.NativeIdentity || second.Identity != f.NativeIdentity || first.ReceiptDigest != second.ReceiptDigest || first.Proof.ProofID == second.Proof.ProofID || !second.Proof.VerifiedAt.After(first.Proof.VerifiedAt) {
		t.Fatal("real controller restart changed custody or reused stale evidence")
	}
	t.Log("distinct controller process IDs", first.PID, second.PID, "retained worker boot", binding.BootNonce, "resource", f.Container, "original drain", request.OperationID)
}
