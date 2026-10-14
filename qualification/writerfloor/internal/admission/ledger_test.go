package admission

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
)

type observerFunc func(context.Context) (Observation, error)

func (f observerFunc) Observe(c context.Context) (Observation, error) { return f(c) }

type verifierFunc func(context.Context, Invocation) error

func (f verifierFunc) Verify(c context.Context, i Invocation) error { return f(c, i) }

type delegateFunc func(context.Context, Delegation) error

func (f delegateFunc) Dispatch(c context.Context, i Delegation) error { return f(c, i) }

type openFunc func(context.Context, State, Open) error

func (f openFunc) VerifyOpen(c context.Context, s State, o Open) error { return f(c, s, o) }

type fixture struct {
	db          *sql.DB
	admin       *Admin
	runtime     *Runtime
	observation Observation
	peer        Peer
	sends       atomic.Int64
}

func database(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("WRITERFLOOR_TEST_DSN")
	if dsn == "" {
		if os.Getenv("WRITERFLOOR_REQUIRED") == "1" {
			t.Fatal("WRITERFLOOR_TEST_DSN required")
		}
		t.Skip("set WRITERFLOOR_TEST_DSN to a disposable PostgreSQL server")
	}
	cfg, e := pgx.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	admin := stdlib.OpenDB(*cfg)
	schema := "wf_" + strings.ReplaceAll(id.New(managed.PrefixOperation).String(), "_", "")
	if _, e = admin.ExecContext(t.Context(), `CREATE SCHEMA `+schema); e != nil {
		_ = admin.Close()
		t.Fatal(e)
	}
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(4)
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, dropErr := admin.ExecContext(ctx, `DROP SCHEMA `+schema+` CASCADE`); dropErr != nil {
			t.Error(dropErr)
		}
		if closeErr := admin.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	if _, e = db.ExecContext(t.Context(), Schema); e != nil {
		t.Fatal(e)
	}
	return db
}
func command(t *testing.T, a *Admin, c Command) Receipt {
	t.Helper()
	s, e := a.Read(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	c.ID = id.New(managed.PrefixOperation).String()
	c.ExpectedRevision = s.Revision
	receipt, e := a.Apply(t.Context(), c)
	if e != nil {
		t.Fatal(e)
	}
	return receipt
}
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{db: database(t)}
	var e error
	f.admin, e = NewAdmin(f.db, "authority", openFunc(func(_ context.Context, _ State, o Open) error {
		if o.Evidence != managed.Digest([]byte("fixture-exact-reconciliation")) {
			return ErrDenied
		}
		return nil
	}))
	if e != nil {
		t.Fatal(e)
	}
	if e = f.admin.Install(t.Context(), 128); e != nil {
		t.Fatal(e)
	}
	h := managed.Digest([]byte("fixture-reviewed-candidate"))
	c := Certificate{ID: "candidate", Artifact: h, Executable: h, Modules: h, Specification: h, Evidence: h, Protocol: 1}
	command(t, f.admin, Command{Action: "certify", Certificate: &c})
	b := Binding{ID: "executor", Certificate: c.ID, RuntimeID: "process-executor", Principal: "sa-executor", PeerDigest: managed.Digest([]byte("executor-private-proxy")), Epoch: 1, Role: "executor"}
	command(t, f.admin, Command{Action: "bind", Binding: &b})
	b = Binding{ID: "client", Certificate: c.ID, RuntimeID: "process-client", Principal: "sa-client", PeerDigest: managed.Digest([]byte("client-private-proxy")), Epoch: 1, Role: "client"}
	command(t, f.admin, Command{Action: "bind", Binding: &b})
	f.peer = Peer{"sa-executor", managed.Digest([]byte("executor-private-proxy"))}
	activation := managed.WriterFloorActivation{Schema: managed.WriterFloorSchema, AuthorityID: "authority", MinimumProtocol: "1", Epoch: "1", ActivationID: id.New(managed.PrefixOperation)}
	command(t, f.admin, Command{Action: "begin", Activation: &activation})
	floor, e := managed.NewWriterFloor(activation, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	f.observation = Observation{floor, "restore-incarnation-1"}
	s, e := f.admin.Read(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	command(t, f.admin, Command{Action: "open", Open: &Open{Observation: f.observation, Frontier: s.Frontier, Sequence: s.Sequence, Executor: "executor", Evidence: managed.Digest([]byte("fixture-exact-reconciliation"))}})
	f.runtime, e = NewRuntime(f.db, "authority", observerFunc(func(context.Context) (Observation, error) { return f.observation, nil }), verifierFunc(func(context.Context, Invocation) error { return nil }), delegateFunc(func(context.Context, Delegation) error { f.sends.Add(1); return nil }))
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func invocation() Invocation {
	return Invocation{ID: id.New(managed.PrefixOperation).String(), Tenant: "tenant", Target: id.New(id.PrefixInstance).String(), Operation: id.New(managed.PrefixOperation).String(), Attempt: "attempt-1", Generation: "1", Permission: managed.Digest([]byte("permission")), Command: managed.Digest([]byte("original-command")), Kind: "provider", Slot: "service-1"}
}

func TestDelegationConcurrentReplayAndRestart(t *testing.T) {
	f := newFixture(t)
	i := invocation()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := f.runtime.Delegate(t.Context(), f.peer, i); errs <- e }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if f.sends.Load() != 1 {
		t.Fatalf("sends=%d", f.sends.Load())
	}
	var schema string
	if e := f.db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); e != nil {
		t.Fatal(e)
	}
	cfg, e := pgx.ParseConfig(os.Getenv("WRITERFLOOR_TEST_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	cfg.RuntimeParams["search_path"] = schema
	reopened := stdlib.OpenDB(*cfg)
	t.Cleanup(func() {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	restarted, e := NewRuntime(reopened, "authority", observerFunc(func(context.Context) (Observation, error) { return Observation{}, ErrUnavailable }), verifierFunc(func(context.Context, Invocation) error { t.Error("replay verified again"); return ErrDenied }), delegateFunc(func(context.Context, Delegation) error { t.Error("replay dispatched"); return nil }))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = restarted.Delegate(t.Context(), f.peer, i); e != nil {
		t.Fatal(e)
	}
	i.Command = managed.Digest([]byte("different"))
	if _, e = restarted.Delegate(t.Context(), f.peer, i); !errors.Is(e, ErrConflict) {
		t.Fatalf("got %v", e)
	}
	page, e := restarted.Page(t.Context(), "", MaxPage)
	if e != nil || len(page) != 1 {
		t.Fatalf("page=%v err=%v", page, e)
	}
}
func TestLostResponseRemainsDelegated(t *testing.T) {
	f := newFixture(t)
	f.runtime.delegate = delegateFunc(func(context.Context, Delegation) error {
		f.sends.Add(1)
		return errors.New("response lost after dispatch")
	})
	i := invocation()
	if _, e := f.runtime.Delegate(t.Context(), f.peer, i); e == nil {
		t.Fatal("expected response loss")
	}
	if _, e := f.runtime.Delegate(t.Context(), f.peer, i); e != nil {
		t.Fatal(e)
	}
	if f.sends.Load() != 1 {
		t.Fatal("response loss resent")
	}
	s, e := f.admin.Read(t.Context())
	if e != nil || s.Sequence != 1 || s.Mode != "delegating" {
		t.Fatalf("state=%+v err=%v", s, e)
	}
	if _, e = f.runtime.Delegate(t.Context(), f.peer, invocation()); !errors.Is(e, ErrDenied) {
		t.Fatalf("fresh after response loss: %v", e)
	}
}
func TestDrainAndSameEpochRestore(t *testing.T) {
	f := newFixture(t)
	first := invocation()
	if _, e := f.runtime.Delegate(t.Context(), f.peer, first); e != nil {
		t.Fatal(e)
	}
	f.observation.Incarnation = "restored-same-floor"
	if _, e := f.runtime.Delegate(t.Context(), f.peer, invocation()); !errors.Is(e, ErrDenied) {
		t.Fatalf("got %v", e)
	}
	s, e := f.admin.Read(t.Context())
	if e != nil || s.Mode != "quarantined" || s.Sequence != 1 {
		t.Fatalf("%+v %v", s, e)
	}
	if _, e = f.runtime.Delegate(t.Context(), f.peer, first); e != nil {
		t.Fatal(e)
	}
	// Correct floor cannot reopen without exact retained frontier and independent evidence.
	bad := Command{ID: id.New(managed.PrefixOperation).String(), ExpectedRevision: s.Revision, Action: "open", Open: &Open{Observation: f.observation, Frontier: s.Frontier, Sequence: s.Sequence, Executor: "executor", Evidence: managed.Digest([]byte("untrusted"))}}
	if _, e = f.admin.Apply(t.Context(), bad); !errors.Is(e, ErrDenied) {
		t.Fatalf("got %v", e)
	}
	command(t, f.admin, Command{Action: "open", Open: &Open{Observation: f.observation, Frontier: s.Frontier, Sequence: s.Sequence, Executor: "executor", Evidence: managed.Digest([]byte("fixture-exact-reconciliation"))}})
	activation := managed.WriterFloorActivation{Schema: managed.WriterFloorSchema, AuthorityID: "authority", MinimumProtocol: "1", Epoch: "2", ActivationID: id.New(managed.PrefixOperation), ExpectedDigest: f.observation.Floor.Digest}
	command(t, f.admin, Command{Action: "begin", Activation: &activation})
	if _, e = f.runtime.Delegate(t.Context(), f.peer, invocation()); !errors.Is(e, ErrDenied) {
		t.Fatalf("got %v", e)
	}
	if f.sends.Load() != 1 {
		t.Fatal("restore/drain sent new effect")
	}
}
func TestAuthenticationBindingRevocationAndOutage(t *testing.T) {
	f := newFixture(t)
	i := invocation()
	for _, p := range []Peer{{f.peer.Principal, managed.Digest([]byte("copied-credential-new-workload"))}, {"other-principal", f.peer.CertificateDigest}} {
		if _, e := f.runtime.Delegate(t.Context(), p, i); !errors.Is(e, ErrDenied) {
			t.Fatalf("copied peer got %v", e)
		}
	}
	f.runtime.observer = observerFunc(func(context.Context) (Observation, error) { return Observation{}, ErrUnavailable })
	if _, e := f.runtime.Delegate(t.Context(), f.peer, i); !errors.Is(e, ErrUnavailable) {
		t.Fatalf("got %v", e)
	}
	f.runtime.observer = observerFunc(func(context.Context) (Observation, error) { return f.observation, nil })
	s, e := f.admin.Read(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	command(t, f.admin, Command{Action: "open", Open: &Open{Observation: f.observation, Frontier: s.Frontier, Sequence: s.Sequence, Executor: "executor", Evidence: managed.Digest([]byte("fixture-exact-reconciliation"))}})
	command(t, f.admin, Command{Action: "revoke", Revoke: "binding:executor"})
	if _, e := f.runtime.Delegate(t.Context(), f.peer, i); !errors.Is(e, ErrDenied) {
		t.Fatalf("got %v", e)
	}
	if f.sends.Load() != 0 {
		t.Fatal("denied request dispatched")
	}
}
func TestCancellationRollbackAndImmutableHistory(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	f.runtime.verifier = verifierFunc(func(context.Context, Invocation) error { cancel(); return nil })
	if _, e := f.runtime.Delegate(ctx, f.peer, invocation()); !errors.Is(e, context.Canceled) {
		t.Fatalf("got %v", e)
	}
	s, e := f.admin.Read(t.Context())
	if e != nil || s.Sequence != 0 {
		t.Fatalf("%+v %v", s, e)
	}
	if f.sends.Load() != 0 {
		t.Fatal("cancelled effect sent")
	}
	if _, e = f.db.ExecContext(t.Context(), `UPDATE writerfloor_facts SET payload=payload WHERE kind='certificate'`); e == nil {
		t.Fatal("immutable history changed")
	}
	if e = f.admin.Install(t.Context(), 128); e == nil {
		t.Fatal("pristine install overwrote authority")
	}
}
func TestCloseReplayDoesNotReopenAndCapacityRollsBack(t *testing.T) {
	f := newFixture(t)
	s, e := f.admin.Read(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	c := Command{ID: id.New(managed.PrefixOperation).String(), ExpectedRevision: s.Revision, Action: "close"}
	one, e := f.admin.Apply(t.Context(), c)
	if e != nil {
		t.Fatal(e)
	}
	two, e := f.admin.Apply(t.Context(), c)
	if e != nil || one != two {
		t.Fatalf("%v %v", two, e)
	}
	s, e = f.admin.Read(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	before := s
	// Trusted corruption fixture reduces frozen quota to exact occupancy.
	s.Limit = s.Records
	b, e := encode(s)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.db.ExecContext(t.Context(), `UPDATE writerfloor_state SET payload=$1 WHERE authority='authority'`, b); e != nil {
		t.Fatal(e)
	}
	cert := Certificate{ID: "rollback", Artifact: managed.Digest([]byte("a")), Executable: managed.Digest([]byte("e")), Modules: managed.Digest([]byte("m")), Specification: managed.Digest([]byte("s")), Evidence: managed.Digest([]byte("proof")), Protocol: 1}
	if _, e = f.admin.Apply(t.Context(), Command{ID: id.New(managed.PrefixOperation).String(), ExpectedRevision: s.Revision, Action: "certify", Certificate: &cert}); !errors.Is(e, ErrCapacity) {
		t.Fatalf("got %v", e)
	}
	after, e := f.admin.Read(t.Context())
	if e != nil || after.Records != before.Records || after.Revision != before.Revision {
		t.Fatalf("%+v %v", after, e)
	}
}
func TestCanonicalCorruptionAndTypedNilRefusal(t *testing.T) {
	var v verifierFunc
	if _, e := NewRuntime(&sql.DB{}, "authority", observerFunc(func(context.Context) (Observation, error) { return Observation{}, nil }), v, delegateFunc(func(context.Context, Delegation) error { return nil })); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
	f := newFixture(t)
	if _, e := f.db.ExecContext(t.Context(), `UPDATE writerfloor_state SET payload=convert_to('{"authority":"authority"}', 'UTF8') WHERE authority='authority'`); e != nil {
		t.Fatal(e)
	}
	if _, e := f.runtime.Delegate(t.Context(), f.peer, invocation()); !errors.Is(e, ErrCorrupt) {
		t.Fatalf("got %v", e)
	}
	if f.sends.Load() != 0 {
		t.Fatal("corrupt authority dispatched")
	}
}

func TestCutoverSharesDelegationLock(t *testing.T) {
	f := newFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	f.runtime.verifier = verifierFunc(func(ctx context.Context, _ Invocation) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	go func() { _, e := f.runtime.Delegate(t.Context(), f.peer, invocation()); done <- e }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delegation never entered locked verifier")
	}
	s, e := f.admin.Read(t.Context())
	if e != nil {
		close(release)
		t.Fatal(e)
	}
	closed := make(chan error, 1)
	go func() {
		_, closeErr := f.admin.Apply(t.Context(), Command{ID: id.New(managed.PrefixOperation).String(), ExpectedRevision: s.Revision, Action: "close"})
		closed <- closeErr
	}()
	// Admission's held lock makes close wait. Its stale CAS must refuse after commit.
	select {
	case closeErr := <-closed:
		close(release)
		t.Fatalf("close crossed held lock: %v", closeErr)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if e = <-closed; !errors.Is(e, ErrConflict) {
		t.Fatalf("got %v", e)
	}
	command(t, f.admin, Command{Action: "close"})
	if _, e = f.runtime.Delegate(t.Context(), f.peer, invocation()); !errors.Is(e, ErrDenied) {
		t.Fatalf("got %v", e)
	}
	if f.sends.Load() != 1 {
		t.Fatal("wrong send count")
	}
}

func TestBelowFloorAndReplacementBindingRefuse(t *testing.T) {
	f := newFixture(t)
	h := managed.Digest([]byte("denied-legacy"))
	legacy := Certificate{ID: "legacy", Artifact: h, Executable: h, Modules: h, Specification: h, Evidence: h, Protocol: 0}
	command(t, f.admin, Command{Action: "certify", Certificate: &legacy})
	b := Binding{ID: "legacy-client", Certificate: legacy.ID, RuntimeID: "legacy-process", Principal: "legacy-principal", PeerDigest: managed.Digest([]byte("legacy-proxy")), Epoch: 1, Role: "executor"}
	command(t, f.admin, Command{Action: "bind", Binding: &b})
	if _, e := f.runtime.Delegate(t.Context(), Peer{b.Principal, b.PeerDigest}, invocation()); !errors.Is(e, ErrDenied) {
		t.Fatalf("got %v", e)
	}
	s, e := f.admin.Read(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	b.ID = "replacement"
	b.Principal = "new-principal"
	if _, e = f.admin.Apply(t.Context(), Command{ID: id.New(managed.PrefixOperation).String(), ExpectedRevision: s.Revision, Action: "bind", Binding: &b}); !errors.Is(e, ErrConflict) {
		t.Fatalf("got %v", e)
	}
	if f.sends.Load() != 0 {
		t.Fatal("legacy process dispatched")
	}
}

func TestFloorMonotonicityAndExactEncoding(t *testing.T) {
	initial := managed.WriterFloorActivation{Schema: managed.WriterFloorSchema, AuthorityID: "authority", MinimumProtocol: "2", Epoch: "1", ActivationID: id.New(managed.PrefixOperation)}
	floor, e := managed.NewWriterFloor(initial, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		epoch, protocol managed.Decimal
		previous        string
		allowed         bool
	}{{"2", "2", floor.Digest, true}, {"2", "1", floor.Digest, false}, {"1", "2", floor.Digest, false}, {"3", "2", floor.Digest, false}, {"2", "2", managed.Digest([]byte("other")), false}} {
		s := State{Authority: "authority", Current: &floor}
		next := managed.WriterFloorActivation{Schema: managed.WriterFloorSchema, AuthorityID: "authority", MinimumProtocol: test.protocol, Epoch: test.epoch, ActivationID: id.New(managed.PrefixOperation), ExpectedDigest: test.previous}
		if e = beginActivation(&s, next); (e == nil) != test.allowed {
			t.Fatalf("%+v err=%v", test, e)
		}
	}
	var state State
	if decode([]byte(`{"authority":"a","authority":"b"}`), &state) == nil {
		t.Fatal("duplicate fields accepted")
	}
	if _, e = encode(strings.Repeat("x", MaxRecord)); !errors.Is(e, ErrCapacity) {
		t.Fatal("encoded size not bounded")
	}
}

type floorReaderFunc func(context.Context) (managed.WriterFloor, error)

func (f floorReaderFunc) ReadManagedWriterFloor(c context.Context) (managed.WriterFloor, error) {
	return f(c)
}

type incarnationFunc func(context.Context, string) (string, error)

func (f incarnationFunc) ReadIncarnation(c context.Context, a string) (string, error) { return f(c, a) }
func TestProtectedObservationRefusesRestoreDuringRead(t *testing.T) {
	request := managed.WriterFloorActivation{Schema: managed.WriterFloorSchema, AuthorityID: "authority", MinimumProtocol: "1", Epoch: "1", ActivationID: id.New(managed.PrefixOperation)}
	floor, e := managed.NewWriterFloor(request, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	observer, e := NewProtectedObserver("authority", floorReaderFunc(func(context.Context) (managed.WriterFloor, error) { return floor, nil }), incarnationFunc(func(context.Context, string) (string, error) {
		calls++
		if calls == 1 {
			return "original", nil
		}
		return "restored", nil
	}))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = observer.Observe(t.Context()); !errors.Is(e, ErrDenied) {
		t.Fatalf("got %v", e)
	}
}

func TestCloseWinsOverLateDispatchAcknowledgement(t *testing.T) {
	f := newFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	f.runtime.delegate = delegateFunc(func(ctx context.Context, _ Delegation) error {
		f.sends.Add(1)
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	first := invocation()
	go func() { _, e := f.runtime.Delegate(t.Context(), f.peer, first); done <- e }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not enter")
	}
	command(t, f.admin, Command{Action: "close"})
	close(release)
	if e := <-done; !errors.Is(e, ErrConflict) {
		t.Fatalf("late acknowledgement: %v", e)
	}
	s, e := f.admin.Read(t.Context())
	if e != nil || s.Mode != "closed" || s.Sequence != 1 {
		t.Fatalf("%+v %v", s, e)
	}
	if _, e = f.runtime.Delegate(t.Context(), f.peer, first); e != nil {
		t.Fatal(e)
	}
	if _, e = f.runtime.Delegate(t.Context(), f.peer, invocation()); !errors.Is(e, ErrDenied) {
		t.Fatalf("fresh while closed: %v", e)
	}
	if f.sends.Load() != 1 {
		t.Fatal("close or replay resent")
	}
}
