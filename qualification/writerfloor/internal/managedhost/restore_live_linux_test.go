//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	cpg "github.com/xraph/ctrlplane/store/postgres"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
	dpg "github.com/xraph/dispatch/store/postgres"
)

const liveRestorePGContainer = "7011b6d66d82034244c15f080ff4bf788f9d33ae90487f2bd16b106423c8bea7"
const liveRestoreDockerHost = "unix:///run/user/1000/docker.sock"
const liveRestoreSnapshotLimit = 64 << 20
const liveRestoreChildFlag = "WRITERFLOOR_RESTORE_INSPECTION_CHILD"

// These helpers use only databases created by this test invocation. They never
// restart PostgreSQL, replace a volume, or restore the independent admission DB,
// launcher incarnation, provider custody, or native preparation/intent journals.
type liveRestoreDatabase struct {
	dsn  string
	name string
	user string
}

type liveRestoreSnapshot struct {
	raw    []byte
	digest string
	source string
}

func newLiveRestoreDatabase(t *testing.T, role string) liveRestoreDatabase {
	t.Helper()

	dsn, _ := liveDatabase(t, liveRequired(t, "WRITERFLOOR_SERVICE_TEST_DSN"), "restore_"+role)
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.User == nil {
		t.Fatal("invalid private restore database configuration")
	}

	name := strings.TrimPrefix(parsed.Path, "/")
	if !strings.HasPrefix(name, "wf_service_restore_") || strings.Trim(name, "abcdefghijklmnopqrstuvwxyz0123456789_") != "" {
		t.Fatal("restore database is outside this fixture")
	}

	return liveRestoreDatabase{dsn: dsn, name: name, user: parsed.User.Username()}
}

// A write past the cap cancels the child immediately rather than buffering or
// truncating an archive that pg_restore might otherwise accept.
type liveRestoreBuffer struct {
	buffer bytes.Buffer
	limit  int
	cancel context.CancelFunc
}

func (b *liveRestoreBuffer) Len() int       { return b.buffer.Len() }
func (b *liveRestoreBuffer) Bytes() []byte  { return b.buffer.Bytes() }
func (b *liveRestoreBuffer) String() string { return b.buffer.String() }

func (b *liveRestoreBuffer) Write(value []byte) (int, error) {
	if len(value) > b.limit-b.Len() {
		b.cancel()

		return 0, managed.ErrCapacity
	}

	return b.buffer.Write(value)
}

func liveRestoreDocker(t *testing.T, stdin io.Reader, limit int, args ...string) []byte {
	t.Helper()

	if liveRequired(t, "WRITERFLOOR_SERVICE_TEST_DOCKER_HOST") != liveRestoreDockerHost {
		t.Fatal("restore drill requires the exact isolated provider Unix engine")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	stdout := &liveRestoreBuffer{limit: limit, cancel: cancel}
	stderr := &liveRestoreBuffer{limit: 64 << 10, cancel: cancel}
	command := exec.CommandContext(ctx, "docker", append([]string{"--host", liveRestoreDockerHost}, args...)...)
	command.Stdin, command.Stdout, command.Stderr = stdin, stdout, stderr
	command.WaitDelay = 2 * time.Second

	if err := command.Run(); err != nil {
		// No DSN or password is in argv. Do not publish tool stderr, which may
		// contain restored object contents or an external tool's environment.
		t.Fatalf("isolated PostgreSQL tool failed: %v (stderr bytes %d)", err, stderr.Len())
	}

	return stdout.Bytes()
}

func liveDumpDatabase(t *testing.T, source liveRestoreDatabase) liveRestoreSnapshot {
	t.Helper()

	identity := liveRestoreDocker(t, nil, 1024, "inspect", "--format", "{{.Id}}", liveRestorePGContainer)
	if strings.TrimSpace(string(identity)) != liveRestorePGContainer {
		t.Fatal("PostgreSQL container identity changed")
	}

	raw := liveRestoreDocker(t, nil, liveRestoreSnapshotLimit, "exec", liveRestorePGContainer, "pg_dump", "--no-password", "--username", source.user, "--dbname", source.name, "--format=custom", "--no-owner", "--no-acl")
	if len(raw) < 5 || !bytes.Equal(raw[:5], []byte("PGDMP")) {
		t.Fatal("PostgreSQL did not produce a custom-format archive")
	}

	digest := managed.Digest(raw)
	t.Log("private PostgreSQL snapshot", source.name, "bytes", len(raw), "sha256", digest)

	return liveRestoreSnapshot{raw: raw, digest: digest, source: source.name}
}

func liveRestoreClone(t *testing.T, snapshot liveRestoreSnapshot, role string) liveRestoreDatabase {
	t.Helper()

	if len(snapshot.raw) == 0 || len(snapshot.raw) > liveRestoreSnapshotLimit || managed.Digest(snapshot.raw) != snapshot.digest {
		t.Fatal("snapshot integrity or bound failure")
	}

	clone := newLiveRestoreDatabase(t, role)
	if clone.name == snapshot.source {
		t.Fatal("restore cannot overwrite its source")
	}

	// The clone is pristine. No --clean, --create, shared-volume operation, or
	// broad database deletion is allowed. liveDatabase registers exact cleanup.
	liveRestoreDocker(t, bytes.NewReader(snapshot.raw), 64<<10, "exec", "-i", liveRestorePGContainer, "pg_restore", "--no-password", "--username", clone.user, "--dbname", clone.name, "--no-owner", "--no-acl", "--exit-on-error", "--single-transaction")
	t.Log("restored private clone", snapshot.source, clone.name, snapshot.digest)

	return clone
}

type liveRestoreInspection struct {
	DSN       string                 `json:"dsn"`
	Key       durable.Key            `json:"key"`
	Lookup    durable.ReceiptRequest `json:"lookup"`
	Directory string                 `json:"directory"`
	Authority string                 `json:"authority"`
}

type liveRestoreObservation struct {
	PID           int             `json:"pid"`
	HistoryDigest string          `json:"history_digest"`
	Events        int             `json:"events"`
	Receipt       durable.Receipt `json:"receipt"`
	Incarnation   string          `json:"incarnation"`
}

func liveInspectRestoredStore(t *testing.T, request liveRestoreInspection) liveRestoreObservation {
	t.Helper()

	store := dpg.New(liveGrove(t, request.DSN))
	history, err := store.ReadHistory(t.Context(), request.Key, 0, 1000)
	if err != nil || len(history) == 0 || len(history) >= 1000 {
		t.Fatal("bounded complete history unavailable", err)
	}

	raw := liveJSON(t, history)
	if len(raw) > 4<<20 {
		t.Fatal("history exceeds inspection bound")
	}

	receipt, found, err := store.LookupReceipt(t.Context(), request.Lookup)
	if err != nil || !found {
		t.Fatal("exact retained receipt unavailable", err)
	}

	incarnation, err := OpenRestoreIncarnation(request.Directory, request.Authority)
	if err != nil {
		t.Fatal(err)
	}

	current, err := incarnation.ReadIncarnation(t.Context(), request.Authority)
	if err != nil {
		t.Fatal(err)
	}

	return liveRestoreObservation{PID: os.Getpid(), HistoryDigest: managed.Digest(raw), Events: len(history), Receipt: receipt, Incarnation: current}
}

// TestLiveRestoreInspectionProcess is an internal child entry, not a service
// readiness check. No credentials are passed through argv or written to logs.
func TestLiveRestoreInspectionProcess(t *testing.T) {
	if os.Getenv(liveRestoreChildFlag) != "1" {
		return
	}

	var request liveRestoreInspection
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 16<<10))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		t.Fatal("invalid private child input")
	}

	observation := liveInspectRestoredStore(t, request)
	if _, err := fmt.Fprintf(os.Stdout, "RESTORE_OBSERVATION:%s\n", liveJSON(t, observation)); err != nil {
		t.Fatal(err)
	}
}

func liveInspectInNewProcess(t *testing.T, request liveRestoreInspection) liveRestoreObservation {
	t.Helper()

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	stdout := &liveRestoreBuffer{limit: 64 << 10, cancel: cancel}
	stderr := &liveRestoreBuffer{limit: 64 << 10, cancel: cancel}
	command := exec.CommandContext(ctx, executable, "-test.run=^TestLiveRestoreInspectionProcess$", "-test.count=1", "-test.timeout=25s")
	command.Env = append(os.Environ(), liveRestoreChildFlag+"=1")
	command.Stdin = bytes.NewReader(liveJSON(t, request))
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = 2 * time.Second

	if err = command.Run(); err != nil {
		t.Fatalf("restore inspection process failed: %v (output bytes %d, stderr bytes %d)", err, stdout.Len(), stderr.Len())
	}

	var observation liveRestoreObservation
	found := false
	for _, line := range strings.Split(stdout.String(), "\n") {
		if encoded, ok := strings.CutPrefix(line, "RESTORE_OBSERVATION:"); ok {
			if found || json.Unmarshal([]byte(encoded), &observation) != nil {
				t.Fatal("invalid child observation")
			}

			found = true
		}
	}

	if !found || observation.PID == os.Getpid() || observation.PID <= 0 {
		t.Fatal("separate inspection process was not observed")
	}

	return observation
}

func TestLivePostgresRestoreHistoryReceiptAndProcessRestart(t *testing.T) {
	source := newLiveRestoreDatabase(t, "native")
	store := dpg.New(liveGrove(t, source.dsn))
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}

	start := durable.StartRequest{Key: durable.Key{Namespace: "restore-drill", WorkflowID: "retained", RunID: "original"}, RequestID: "start-before-backup", WorkflowType: "restore-drill", BuildID: "restore-v1", Queue: "restore", Input: []byte(`{"retained":true}`)}
	startReceipt, err := store.StartExecution(t.Context(), start)
	if err != nil {
		t.Fatal(err)
	}

	task, err := store.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: start.Namespace, Queue: start.Queue, Kind: durable.TaskWorkflow, Owner: "restore-fixture", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatal("claim retained workflow", err)
	}

	commit := durable.CommitRequest{Key: start.Key, RequestID: "commit-before-backup", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "workflow.progress", Payload: []byte("retained-result")}}}
	commit.IntentDigest, err = durable.Fingerprint("qualification.restore.commit", commit)
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := store.CommitTransition(t.Context(), commit)
	if err != nil {
		t.Fatal("retain exact receipt lookup proof", err)
	}

	directory := t.TempDir()
	if err = os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	incarnation, err := OpenRestoreIncarnation(directory, "restore-native")
	if err != nil {
		t.Fatal(err)
	}

	first, err := incarnation.Rotate(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}

	request := liveRestoreInspection{DSN: source.dsn, Key: start.Key, Lookup: durable.ReceiptRequest{Key: start.Key, RequestID: commit.RequestID, IntentDigest: commit.IntentDigest}, Directory: directory, Authority: "restore-native"}
	before := liveInspectRestoredStore(t, request)
	one := liveInspectInNewProcess(t, request)
	two := liveInspectInNewProcess(t, request)
	if one.PID == two.PID || one.HistoryDigest != before.HistoryDigest || two.HistoryDigest != before.HistoryDigest || !reflect.DeepEqual(one.Receipt, receipt) || !reflect.DeepEqual(two.Receipt, receipt) || one.Incarnation != first || two.Incarnation != first {
		t.Fatal("process restart changed retained history, receipt or independent incarnation")
	}

	snapshot := liveDumpDatabase(t, source)
	later := start
	later.RunID, later.WorkflowID, later.RequestID = "later", "after-snapshot", "later-start"
	if _, err = store.StartExecution(t.Context(), later); err != nil {
		t.Fatal(err)
	}

	// Rotate external state before exposing any restored authority, even when
	// database metadata and marker bytes in the backup have not changed.
	second, err := incarnation.Rotate(t.Context(), first)
	if err != nil || second == first {
		t.Fatal("restore incarnation did not advance", err)
	}

	clone := liveRestoreClone(t, snapshot, "native_clone")
	request.DSN = clone.dsn
	after := liveInspectInNewProcess(t, request)
	if after.HistoryDigest != before.HistoryDigest || after.Events != before.Events || !reflect.DeepEqual(after.Receipt, receipt) || after.Incarnation != second {
		t.Fatal("restore lost exact history/receipt or rolled back external incarnation")
	}

	restored := dpg.New(liveGrove(t, clone.dsn))
	if _, err = restored.GetExecution(t.Context(), later.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatal("clone was not restored from the earlier snapshot", err)
	}

	replayed, err := restored.StartExecution(t.Context(), start)
	if err != nil || !reflect.DeepEqual(replayed, startReceipt) {
		t.Fatal("restored exact start replay changed receipt", err)
	}

	replayedCommit, err := restored.CommitTransition(t.Context(), commit)
	if err != nil || !reflect.DeepEqual(replayedCommit, receipt) {
		t.Fatal("restored exact commit replay changed receipt", err)
	}

	stable := liveInspectRestoredStore(t, request)
	if stable.HistoryDigest != before.HistoryDigest {
		t.Fatal("receipt replay appended history")
	}

	t.Log("inspection client process restarts", one.PID, two.PID, "restored client", after.PID, "events", after.Events, "history sha256", after.HistoryDigest)
}

// This exact empty-fixture cutover policy is qualification-only. It is not an
// artifact certification, an Authsome host boundary, or deployed grant fencing.
type liveRestoreCutover struct {
	observer *admission.ProtectedObserver
	evidence string
}

func (v liveRestoreCutover) VerifyOpen(ctx context.Context, state admission.State, request admission.Open) error {
	actual, err := v.observer.Observe(ctx)
	if err != nil {
		return err
	}

	if state.Sequence != 0 || request.Sequence != 0 || state.Frontier != request.Frontier || request.Evidence != v.evidence || !reflect.DeepEqual(actual, request.Observation) {
		return admission.ErrDenied
	}

	return nil
}

type liveRestoreNoDispatch struct{ calls int }

func (v *liveRestoreNoDispatch) Verify(context.Context, admission.Invocation) error {
	v.calls++

	return admission.ErrDenied
}

func (v *liveRestoreNoDispatch) Dispatch(context.Context, admission.Delegation) error {
	v.calls++

	return admission.ErrDenied
}

func liveRestoreSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open private ledger", err)
	}

	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})

	return db
}

func liveRestoreAdminCommand(t *testing.T, admin *admission.Admin, command admission.Command) admission.Receipt {
	t.Helper()

	state, err := admin.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	command.ID = cid.New(managed.PrefixOperation).String()
	command.ExpectedRevision = state.Revision
	receipt, err := admin.Apply(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}

	return receipt
}

func TestLivePostgresSameFloorRestoreQuarantinesIndependentLedger(t *testing.T) {
	source := newLiveRestoreDatabase(t, "core")
	core := liveGrove(t, source.dsn)
	const authority = "restore-core"
	floorAdmin, err := cpg.NewWriterFloorAdmin(core, authority)
	if err != nil {
		t.Fatal(err)
	}

	if err = floorAdmin.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}

	activation := managed.WriterFloorActivation{Schema: managed.WriterFloorSchema, AuthorityID: authority, MinimumProtocol: "1", Epoch: "1", ActivationID: cid.New(managed.PrefixOperation)}
	floor, err := floorAdmin.Activate(t.Context(), activation)
	if err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	if err = os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	incarnation, err := OpenRestoreIncarnation(directory, authority)
	if err != nil {
		t.Fatal(err)
	}

	first, err := incarnation.Rotate(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := cpg.NewWriterFloorReader(core, authority)
	if err != nil {
		t.Fatal(err)
	}

	observer, err := admission.NewProtectedObserver(authority, reader, incarnation)
	if err != nil {
		t.Fatal(err)
	}

	observation, err := observer.Observe(t.Context())
	if err != nil || observation.Incarnation != first || !reflect.DeepEqual(observation.Floor, floor) {
		t.Fatal("real protected floor observation failed", err)
	}

	ledger := newLiveRestoreDatabase(t, "ledger")
	db := liveRestoreSQL(t, ledger.dsn)
	if _, err = db.ExecContext(t.Context(), admission.Schema); err != nil {
		t.Fatal(err)
	}

	evidence := managed.Digest([]byte("qualification-only-empty-restore-cutover"))
	admin, err := admission.NewAdmin(db, authority, liveRestoreCutover{observer: observer, evidence: evidence})
	if err != nil {
		t.Fatal(err)
	}

	if err = admin.Install(t.Context(), 128); err != nil {
		t.Fatal(err)
	}

	certificate := admission.Certificate{ID: "restore-fixture", Artifact: evidence, Executable: evidence, Modules: evidence, Specification: evidence, Evidence: evidence, Protocol: 1}
	liveRestoreAdminCommand(t, admin, admission.Command{Action: "certify", Certificate: &certificate})
	binding := admission.Binding{ID: "restore-executor", Certificate: certificate.ID, RuntimeID: "restore-fixture-process", Principal: "restore-fixture-principal", PeerDigest: managed.Digest([]byte("qualification-only-peer")), Epoch: 1, Role: "executor"}
	liveRestoreAdminCommand(t, admin, admission.Command{Action: "bind", Binding: &binding})
	liveRestoreAdminCommand(t, admin, admission.Command{Action: "begin", Activation: &activation})
	prepared, err := admin.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	liveRestoreAdminCommand(t, admin, admission.Command{Action: "open", Open: &admission.Open{Observation: observation, Frontier: prepared.Frontier, Sequence: prepared.Sequence, Executor: binding.ID, Evidence: evidence}})
	opened, err := admin.Read(t.Context())
	if err != nil || opened.Mode != "open" {
		t.Fatal("qualification ledger did not open", err)
	}

	snapshot := liveDumpDatabase(t, source)
	second, err := incarnation.Rotate(t.Context(), first)
	if err != nil || second == first {
		t.Fatal("independent restore identity failed", err)
	}

	clone := liveRestoreClone(t, snapshot, "core_clone")
	cloneReader, err := cpg.NewWriterFloorReader(liveGrove(t, clone.dsn), authority)
	if err != nil {
		t.Fatal(err)
	}

	cloneObserver, err := admission.NewProtectedObserver(authority, cloneReader, incarnation)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := cloneObserver.Observe(t.Context())
	if err != nil || !reflect.DeepEqual(restored.Floor, floor) || restored.Incarnation != second {
		t.Fatal("same-floor restore did not preserve actual marker with new external identity", err)
	}

	// Reopen the independent ledger connection as a new runtime composition.
	// Neither its state nor the incarnation directory was included in the dump.
	denied := &liveRestoreNoDispatch{}
	runtime, err := admission.NewRuntime(liveRestoreSQL(t, ledger.dsn), authority, cloneObserver, denied, denied)
	if err != nil {
		t.Fatal(err)
	}

	invocation := admission.Invocation{ID: cid.New(managed.PrefixOperation).String(), Tenant: "restore", Target: cid.New(cid.PrefixInstance).String(), Operation: cid.New(managed.PrefixOperation).String(), Attempt: "restore-attempt", Generation: "1", Permission: evidence, Command: evidence, Kind: "native", Slot: "restore-slot"}
	if _, err = runtime.Delegate(t.Context(), admission.Peer{Principal: binding.Principal, CertificateDigest: binding.PeerDigest}, invocation); !errors.Is(err, admission.ErrDenied) {
		t.Fatal("same-floor restore was not denied", err)
	}

	closed, err := admin.Read(t.Context())
	if err != nil || closed.Mode != "quarantined" || closed.Revision != opened.Revision+1 || closed.Sequence != opened.Sequence || closed.Frontier != opened.Frontier || closed.Records != opened.Records || closed.Incarnation != first || denied.calls != 0 {
		t.Fatal("quarantine did not persist before verification or dispatch", err)
	}

	if snapshot.source == ledger.name || clone.name == ledger.name {
		t.Fatal("independent ledger entered authority restore")
	}

	t.Log("same-floor restore quarantined independent ledger", "floor", floor.Digest, "external ledger", ledger.name, "authority clone", clone.name)
}

func TestRestoreArchiveWriterRefusesOverflow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	buffer := &liveRestoreBuffer{limit: 16, cancel: cancel}
	if n, err := io.Copy(buffer, strings.NewReader(strings.Repeat("x", 16))); err != nil || n != 16 || buffer.Len() != 16 || ctx.Err() != nil {
		t.Fatal("exact archive writer cap refused", n, err)
	}

	if n, err := io.Copy(buffer, strings.NewReader("x")); !errors.Is(err, managed.ErrCapacity) || n != 0 || buffer.Len() != 16 || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("archive writer overflow did not cancel without retaining bytes", n, err)
	}
}
