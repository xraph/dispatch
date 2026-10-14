//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	cpg "github.com/xraph/ctrlplane/store/postgres"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
	admissionhost "github.com/xraph/dispatch/qualification/writerfloor/internal/host"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

type liveAdmissionPreparation struct {
	authority   string
	activation  managed.WriterFloorActivation
	observer    *admission.ProtectedObserver
	incarnation *RestoreIncarnation
	observation admission.Observation
	core        *cpg.Store
}

// Call before constructing recovery repositories or provider authority. The legacy
// handle must not be retained by the recovery drill. This does not replace the
// separately qualified database grant cutover or authorize restored deployments.
func prepareLiveAdmissionCore(t *testing.T, fixture *liveServiceFixture) *liveAdmissionPreparation {
	t.Helper()
	authority := "live-" + liveRandom(t)
	db := liveGrove(t, fixture.CoreDSN)
	admin, err := cpg.NewWriterFloorAdmin(db, authority)
	if err != nil {
		t.Fatal(err)
	}
	if err = admin.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	activation := managed.WriterFloorActivation{Schema: managed.WriterFloorSchema, AuthorityID: authority, MinimumProtocol: "1", Epoch: "1", ActivationID: cid.New(managed.PrefixOperation)}
	if _, err = admin.Activate(t.Context(), activation); err != nil {
		t.Fatal(err)
	}
	core, err := cpg.NewWithWriterFloorAdmission(db, authority)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := cpg.NewWriterFloorReader(db, authority)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(fixture.Root, "admission-incarnation")
	if err = os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	incarnation, err := OpenRestoreIncarnation(directory, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = incarnation.Rotate(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	observer, err := admission.NewProtectedObserver(authority, reader, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := observer.Observe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixture.Core = core
	return &liveAdmissionPreparation{authority, activation, observer, incarnation, observation, core}
}

// This verifier permits only first opening of one newly created ledger. The
// immutable certificate and single binding must match the actual issued channel;
// it never treats marker equality as evidence for reopening a used deployment.
type liveAdmissionPristineCutover struct {
	db          *sql.DB
	observer    *admission.ProtectedObserver
	activation  managed.WriterFloorActivation
	certificate admission.Certificate
	binding     admission.Binding
	evidence    string
}

func (v *liveAdmissionPristineCutover) VerifyOpen(ctx context.Context, state admission.State, request admission.Open) error {
	observation, err := v.observer.Observe(ctx)
	if err != nil {
		return err
	}
	if state.Mode != "draining" || state.Current != nil || state.Pending == nil || *state.Pending != v.activation || state.Revision != 4 || state.Records != 5 || state.Certificates != 1 || state.Sequence != 0 || state.Frontier != managed.Digest([]byte("writerfloor.empty.v1")) || request.Frontier != state.Frontier || request.Sequence != 0 || request.Executor != v.binding.ID || request.Evidence != v.evidence || !reflect.DeepEqual(observation, request.Observation) {
		return admission.ErrDenied
	}
	rows, err := v.db.QueryContext(ctx, `SELECT kind,id,payload FROM writerfloor_facts WHERE authority=$1 ORDER BY kind,id LIMIT 6`, state.Authority)
	if err != nil {
		return err
	}
	defer rows.Close()
	count, certificates, bindings, commands := 0, 0, 0, 0
	for rows.Next() {
		var kind, id string
		var raw []byte
		if err = rows.Scan(&kind, &id, &raw); err != nil {
			return err
		}
		count++
		switch kind {
		case "certificate":
			expected, encodeErr := json.Marshal(v.certificate)
			if encodeErr != nil || id != v.certificate.ID || !bytes.Equal(raw, expected) {
				return admission.ErrDenied
			}
			certificates++
		case "binding":
			expected, encodeErr := json.Marshal(v.binding)
			if encodeErr != nil || id != v.binding.ID || !bytes.Equal(raw, expected) {
				return admission.ErrDenied
			}
			bindings++
		case "admin":
			commands++
		default:
			return admission.ErrDenied
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if count != 5 || certificates != 1 || bindings != 1 || commands != 3 {
		return admission.ErrDenied
	}
	return ctx.Err()
}

func liveAdmissionCertificate(t *testing.T, f *liveServiceFixture, prepared *liveAdmissionPreparation) admission.Certificate {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	hasher := sha256.New()
	count, readErr := io.Copy(hasher, io.LimitReader(file, 256<<20+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || count == 0 || count > 256<<20 {
		t.Fatal("bounded controller executable identity", errors.Join(readErr, closeErr))
	}
	executableDigest := hex.EncodeToString(hasher.Sum(nil))
	info, err := buildinfo.ReadFile(executable)
	if err != nil {
		t.Fatal("controller build identity unavailable", err)
	}
	modules, err := liveAdmissionModules(info, executableDigest, os.Getenv("WRITERFLOOR_SERVICE_TEST_PROVENANCE"))
	if err != nil {
		t.Fatal("published module identity unavailable", err)
	}
	specification := managed.Digest(liveJSON(t, struct {
		Role, Authority, Route string
		PID                    int
		Service                launcher.ServicePlan
	}{"isolated-recovery-controller", prepared.authority, "/writerfloor/delegate", os.Getpid(), f.Plan}))
	_, artifact, ok := strings.Cut(f.Plan.Service.Image, "@sha256:")
	if !ok || !objectID(artifact) {
		t.Fatal("real digest-qualified provider artifact required")
	}
	// Evidence is the actual accepted creation/start and fixed native identity,
	// combined with protected floor/incarnation and exact executable/module bytes.
	evidence := managed.Digest(liveJSON(t, struct {
		Start               managed.ProviderStepAcceptance
		Plan                launcher.ServicePlan
		Observation         admission.Observation
		Executable, Modules string
	}{f.Start, f.Plan, prepared.observation, executableDigest, modules}))
	return admission.Certificate{ID: "isolated-controller-qualification", Artifact: executableDigest, Executable: executableDigest, Modules: modules, Specification: specification, Evidence: evidence, Protocol: 1}
}

type liveAdmissionClient struct {
	client   *http.Client
	endpoint string
	bearer   string
	admin    *admission.Admin
	runtime  *admission.Runtime
	host     *launcher.QualificationAdmission
	prepared *liveAdmissionPreparation
}

// Keep failed live checks diagnosable without exposing credentials or changing
// their result, deadline, or authority.
type liveAdmissionVerifier struct {
	t        *testing.T
	verifier admission.Verifier
}

func (v liveAdmissionVerifier) Verify(ctx context.Context, invocation admission.Invocation) error {
	started := time.Now()
	err := v.verifier.Verify(ctx, invocation)
	if err != nil {
		v.t.Logf("actual admission verification refused after %s: %v (context: %v)", time.Since(started), err, ctx.Err())
	}
	return err
}

// newLiveAdmission uses the exact real verifier/delegate supplied by the recovery
// composition. It never creates a positive fixture verifier or dispatch callback.
func newLiveAdmission(t *testing.T, f *liveServiceFixture, prepared *liveAdmissionPreparation, verifier admission.Verifier, delegate admission.Delegate) *liveAdmissionClient {
	t.Helper()
	if prepared == nil || prepared.core == nil {
		t.Fatal("protected core preparation required before recovery configuration")
	}
	ledger := newLiveRestoreDatabase(t, "admission")
	db := liveRestoreSQL(t, ledger.dsn)
	if _, err := db.ExecContext(t.Context(), admission.Schema); err != nil {
		t.Fatal(err)
	}
	certificate := liveAdmissionCertificate(t, f, prepared)
	cutover := &liveAdmissionPristineCutover{db: db, observer: prepared.observer, activation: prepared.activation, certificate: certificate, evidence: certificate.Evidence}
	admin, err := admission.NewAdmin(db, prepared.authority, cutover)
	if err != nil {
		t.Fatal(err)
	}
	if err = admin.Install(t.Context(), 128); err != nil {
		t.Fatal(err)
	}
	runtime, err := admission.NewRuntime(db, prepared.authority, prepared.observer, liveAdmissionVerifier{t: t, verifier: verifier}, delegate)
	if err != nil {
		t.Fatal(err)
	}
	boundary, err := f.Launcher.OpenQualificationAdmission(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := boundary.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})
	binding := admission.Binding{ID: "isolated-controller", Certificate: certificate.ID, RuntimeID: fmt.Sprintf("controller-%d-%s", os.Getpid(), liveRandom(t)), Principal: boundary.Credential.Account, PeerDigest: boundary.PeerDigest, Epoch: 1, Role: "executor"}
	cutover.binding = binding
	liveRestoreAdminCommand(t, admin, admission.Command{Action: "certify", Certificate: &certificate})
	liveRestoreAdminCommand(t, admin, admission.Command{Action: "bind", Binding: &binding})
	liveRestoreAdminCommand(t, admin, admission.Command{Action: "begin", Activation: &prepared.activation})
	state, err := admin.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	liveRestoreAdminCommand(t, admin, admission.Command{Action: "open", Open: &admission.Open{Observation: prepared.observation, Frontier: state.Frontier, Executor: binding.ID, Evidence: certificate.Evidence}})
	server := httptest.NewUnstartedServer(boundary.Handler)
	server.TLS, err = admissionhost.TLSConfig(f.certificate, f.roots)
	if err != nil {
		t.Fatal(err)
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	pair, err := tls.X509KeyPair(boundary.Credential.CertificatePEM, boundary.Credential.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(boundary.Credential.CAPEM) {
		t.Fatal("admission CA absent")
	}
	// The existing service server certificate is bound to the configured VM host.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: false, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{pair}, ServerName: f.Plan.ServerName}, TLSHandshakeTimeout: 2 * time.Second, ResponseHeaderTimeout: 30 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 35 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Log("isolated admission certificate", certificate.ID, "executable", certificate.Executable, "modules", certificate.Modules, "evidence", certificate.Evidence, "ledger", ledger.name)
	return &liveAdmissionClient{client: client, endpoint: server.URL + "/writerfloor/delegate", bearer: boundary.Credential.Bearer, admin: admin, runtime: runtime, host: boundary, prepared: prepared}
}

func (c *liveAdmissionClient) Invoke(ctx context.Context, inv admission.Invocation) error {
	raw, err := json.Marshal(inv)
	if err != nil || len(raw) > admission.MaxRecord {
		return admission.ErrDenied
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.bearer)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, admission.MaxRecord+1))
	if err != nil || len(body) > admission.MaxRecord {
		return admission.ErrUnavailable
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("outer admission HTTP %d: %w", response.StatusCode, admission.ErrDenied)
	}
	var saved admission.Delegation
	if exactDecode(bytes.TrimSpace(body), &saved) != nil || saved.Invocation != inv || saved.Sequence == 0 || saved.Workload != "isolated-controller" || saved.Executor != "isolated-controller" || saved.Floor != c.prepared.observation.Floor.Digest || saved.Incarnation != c.prepared.observation.Incarnation {
		return admission.ErrCorrupt
	}
	return nil
}

// Negative qualification deliberately uses a refusing verifier. It proves the
// real channel reaches configured authority, not that an invented permission may
// send. The assembled recovery drill supplies ProviderRemoval for positive work.
func TestLiveAdmissionAuthenticationAndRestoreRefusal(t *testing.T) {
	fixture := newLiveService(t)
	prepared := prepareLiveAdmissionCore(t, fixture)
	refused := &liveRestoreNoDispatch{}
	client := newLiveAdmission(t, fixture, prepared, refused, refused)
	invocation := admission.Invocation{ID: cid.New(managed.PrefixOperation).String(), Tenant: fixture.Plan.Target.TenantID, Target: fixture.Plan.Target.InstanceID.String(), Operation: fixture.Plan.ProviderOperationID.String(), Attempt: "unselected", Generation: "1", Permission: managed.Digest(liveJSON(t, fixture.Start)), Command: managed.Digest(liveJSON(t, fixture.Plan)), Kind: "provider", Slot: "container"}
	t.Run("real-authority-refuses-unselected", func(t *testing.T) {
		if err := client.Invoke(t.Context(), invocation); !errors.Is(err, admission.ErrDenied) {
			t.Fatal("unselected invocation accepted", err)
		}
		if refused.calls != 1 {
			t.Fatal("real authentication did not reach exact verifier once", refused.calls)
		}
		rows, err := client.runtime.Page(t.Context(), "", admission.MaxPage)
		if err != nil || len(rows) != 0 {
			t.Fatal("refused invocation retained delegation", err)
		}
	})
	t.Run("wrong-bearer", func(t *testing.T) {
		saved := client.bearer
		client.bearer = "invalid"
		defer func() { client.bearer = saved }()
		if err := client.Invoke(t.Context(), invocation); !errors.Is(err, admission.ErrDenied) {
			t.Fatal("invalid bearer accepted", err)
		}
		if refused.calls != 1 {
			t.Fatal("invalid bearer reached verifier")
		}
	})
	t.Run("same-floor-restore-quarantines", func(t *testing.T) {
		if _, err := prepared.incarnation.Rotate(t.Context(), prepared.observation.Incarnation); err != nil {
			t.Fatal(err)
		}
		if err := client.Invoke(t.Context(), invocation); !errors.Is(err, admission.ErrDenied) {
			t.Fatal("changed independent incarnation accepted", err)
		}
		state, err := client.admin.Read(t.Context())
		if err != nil || state.Mode != "quarantined" || state.Sequence != 0 || refused.calls != 1 {
			t.Fatal("restore did not quarantine before verifier", err)
		}
		request := admission.Command{ID: cid.New(managed.PrefixOperation).String(), ExpectedRevision: state.Revision, Action: "open", Open: &admission.Open{Observation: prepared.observation, Frontier: state.Frontier, Sequence: state.Sequence, Executor: state.Executor, Evidence: liveAdmissionCertificate(t, fixture, prepared).Evidence}}
		if _, err = client.admin.Apply(t.Context(), request); err == nil {
			t.Fatal("pristine-only verifier reopened restored deployment")
		}
	})
	t.Run("revoked-key", func(t *testing.T) {
		if err := client.host.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := client.Invoke(t.Context(), invocation); !errors.Is(err, admission.ErrDenied) {
			t.Fatal("closed admission accepted", err)
		}
		if refused.calls != 1 {
			t.Fatal("closed admission reached verifier")
		}
	})
}
