package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

type drainGateStore struct {
	durable.Store
	kind             durable.TaskKind
	after            bool
	entered, release chan struct{}
	calls            atomic.Int64
}

func (s *drainGateStore) gate(kind durable.TaskKind, after bool) {
	if kind == s.kind && after == s.after {
		s.calls.Add(1)
		close(s.entered)
		<-s.release
	}
}
func (s *drainGateStore) ClaimTask(ctx context.Context, r durable.ClaimRequest) (*durable.Task, error) {
	s.gate(r.Kind, false)
	v, err := s.Store.ClaimTask(ctx, r)
	s.gate(r.Kind, true)
	return v, err
}
func (s *drainGateStore) ClaimTimeoutTask(ctx context.Context, r durable.TimeoutClaimRequest) (*durable.Task, error) {
	s.gate(drt.TaskTimeout, false)
	v, err := s.Store.ClaimTimeoutTask(ctx, r)
	s.gate(drt.TaskTimeout, true)
	return v, err
}
func (s *drainGateStore) ClaimChildDelivery(ctx context.Context, r durable.ChildDeliveryClaimRequest) (*durable.ChildDelivery, error) {
	s.gate(drt.TaskChildDelivery, false)
	v, err := s.Store.ClaimChildDelivery(ctx, r)
	s.gate(drt.TaskChildDelivery, true)
	return v, err
}
func (s *drainGateStore) ClaimExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutClaimRequest) (*durable.ExecutionTimeoutTask, error) {
	s.gate(drt.TaskExecutionTimeout, false)
	v, err := s.Store.ClaimExecutionTimeout(ctx, r)
	s.gate(drt.TaskExecutionTimeout, true)
	return v, err
}
func awaitDrainSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("drain barrier did not arrive")
	}
}
func drainFixture(t *testing.T, s durable.Store, kind durable.TaskKind) drt.Options {
	t.Helper()
	o := workerOptions(t)
	o.LeaseDuration, o.StoreTimeout = 5*time.Second, time.Second
	o.Activities["work"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) { return []byte("done"), nil }
	o.Workflows["child"] = func(*drt.Workflow, []byte) ([]byte, error) { return []byte("child"), nil }
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		switch kind {
		case durable.TaskActivity:
			return w.Activity("work", "work", "", nil).Get()
		case durable.TaskTimer:
			return w.Timer("timer", time.Millisecond).Get()
		case drt.TaskTimeout:
			return w.ActivityWithOptions("work", "work", "", nil, drt.ActivityOptions{ScheduleToStartTimeout: time.Millisecond}).Get()
		case drt.TaskChildDelivery:
			return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "childq"}).Get()
		default:
			return []byte("done"), nil
		}
	}
	seed := newWorker(t, s, o)
	if kind == drt.TaskExecutionTimeout {
		_, err := seed.StartExecution(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: o.BuildID, Queue: o.Queue, RunTimeout: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		startWorkerRun(t, seed, o)
		if kind != durable.TaskWorkflow {
			runTask(t, seed, durable.TaskWorkflow)
		}
	}
	if kind == drt.TaskChildDelivery {
		child := o
		child.Queue = "childq"
		runTask(t, newWorker(t, s, child), durable.TaskWorkflow)
	}
	if kind == durable.TaskTimer || kind == drt.TaskTimeout || kind == drt.TaskExecutionTimeout {
		time.Sleep(3 * time.Millisecond)
	}
	return o
}

func TestDrainCoversEveryClaimBeforeAndAfterGrant(t *testing.T) {
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity, durable.TaskTimer, drt.TaskTimeout, drt.TaskChildDelivery, drt.TaskExecutionTimeout} {
		for _, after := range []bool{false, true} {
			name := string(kind) + "/before"
			if after {
				name = string(kind) + "/after"
			}
			t.Run(name, func(t *testing.T) {
				base := memory.New()
				o := drainFixture(t, base, kind)
				s := &drainGateStore{Store: base, kind: kind, after: after, entered: make(chan struct{}), release: make(chan struct{})}
				w := newWorker(t, s, o)
				result := make(chan error, 1)
				go func() {
					worked, err := w.RunOnce(t.Context(), kind)
					if err == nil && !worked {
						err = errors.New("fixture had no committed grant")
					}
					result <- err
				}()
				awaitDrainSignal(t, s.entered)
				h, err := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(time.Second)})
				if err != nil {
					t.Fatal(err)
				}
				status := w.Status()
				if status.InFlight != 1 || !status.AdmissionClosed || status.Ready || status.State != drt.WorkerDraining {
					t.Fatalf("claim not registered: %+v", status)
				}
				for _, blockedKind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity, durable.TaskTimer, drt.TaskTimeout, drt.TaskChildDelivery, drt.TaskExecutionTimeout} {
					if worked, e := w.RunOnce(t.Context(), blockedKind); worked || !errors.Is(e, drt.ErrWorkerDraining) {
						t.Fatalf("claim bypass %s: %t %v", blockedKind, worked, e)
					}
				}
				observer, cancel := context.WithCancel(t.Context())
				cancel()
				if _, e := w.WaitDrain(observer, h); !errors.Is(e, context.Canceled) {
					t.Fatalf("observer: %v", e)
				}
				close(s.release)
				if e := <-result; e != nil {
					t.Fatal(e)
				}
				d, e := w.WaitDrain(t.Context(), h)
				if e != nil || !d.Complete || !d.Quiescent || d.InFlight != 0 || s.calls.Load() != 1 {
					t.Fatalf("drain: %+v calls=%d err=%v", d, s.calls.Load(), e)
				}
				if e = w.Run(t.Context()); !errors.Is(e, drt.ErrWorkerDraining) {
					t.Fatalf("restarted drained worker: %v", e)
				}
			})
		}
	}
}

type drainRenewStore struct {
	durable.Store
	renewals chan time.Time
}

func (s *drainRenewStore) RenewTask(ctx context.Context, k durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	until, err := s.Store.RenewTask(ctx, k, token, ttl)
	if err == nil {
		select {
		case s.renewals <- until:
		default:
		}
	}
	return until, err
}
func TestDrainPreservesRenewalAndIgnoresObserverCancellation(t *testing.T) {
	s := &drainRenewStore{Store: memory.New(), renewals: make(chan time.Time, 10)}
	o := workerOptions(t)
	o.LeaseDuration = 90 * time.Millisecond
	o.StoreTimeout = 20 * time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("work", "work", "", nil).Get() }
	o.Activities["work"] = func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
		close(entered)
		select {
		case <-release:
			return []byte("done"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	w := newWorker(t, s, o)
	startWorkerRun(t, w, o)
	runTask(t, w, durable.TaskWorkflow)
	result := make(chan error, 1)
	go func() { _, e := w.RunOnce(t.Context(), durable.TaskActivity); result <- e }()
	awaitDrainSignal(t, entered)
	accept, cancel := context.WithCancel(t.Context())
	r := drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(2 * time.Second)}
	h, e := w.BeginDrain(accept, r)
	cancel()
	if e != nil {
		t.Fatal(e)
	}
	for range 4 {
		select {
		case <-s.renewals:
		case <-time.After(time.Second):
			t.Fatal("draining activity stopped renewal")
		}
	}
	if again, duplicateErr := w.BeginDrain(t.Context(), r); duplicateErr != nil || again != h {
		t.Fatalf("duplicate: %+v %v", again, duplicateErr)
	}
	r.Deadline = r.Deadline.Add(-time.Second)
	if _, e = w.BeginDrain(t.Context(), r); !errors.Is(e, durable.ErrRequestConflict) {
		t.Fatalf("deadline replaced: %v", e)
	}
	close(release)
	if e = <-result; e != nil {
		t.Fatal(e)
	}
	if d, e := w.WaitDrain(t.Context(), h); e != nil || !d.Complete {
		t.Fatalf("drain: %+v %v", d, e)
	}
}

func TestDrainTimeoutRetainsUncooperativeHandlerAndFencesReplacement(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.LeaseDuration = 90 * time.Millisecond
	o.StoreTimeout = 20 * time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("work", "work", "", nil).Get() }
	o.Activities["work"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
		close(entered)
		<-release
		return []byte("old"), nil
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	runTask(t, w, durable.TaskWorkflow)
	result := make(chan error, 1)
	go func() { _, e := w.RunOnce(t.Context(), durable.TaskActivity); result <- e }()
	awaitDrainSignal(t, entered)
	h, e := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(15 * time.Millisecond)})
	if e != nil {
		t.Fatal(e)
	}
	d, e := w.WaitDrain(t.Context(), h)
	if !errors.Is(e, drt.ErrDrainIncomplete) || d.Complete || d.Quiescent || !d.DeadlineExpired || d.InFlight != 1 {
		t.Fatalf("false quiescence: %+v %v", d, e)
	}
	o.Owner = "replacement"
	o.Activities["work"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) { return []byte("replacement"), nil }
	replacement := newWorker(t, s, o)
	time.Sleep(100 * time.Millisecond)
	runTask(t, replacement, durable.TaskActivity)
	close(release)
	if e = <-result; e == nil {
		t.Fatal("cancelled old handler accepted result")
	}
	runTask(t, replacement, durable.TaskWorkflow)
	execution, e := s.GetExecution(t.Context(), key)
	if e != nil || string(execution.Output) != "replacement" {
		t.Fatalf("replacement: %+v %v", execution, e)
	}
	d, e = w.WaitDrain(t.Context(), h)
	if !errors.Is(e, drt.ErrDrainIncomplete) || !d.Quiescent || !d.DeadlineExpired || d.Complete {
		t.Fatalf("rewritten expired result: %+v %v", d, e)
	}
}

func TestDrainBeforeStartRetainsQueryRuntime(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetQueryHandler("status", func([]byte) ([]byte, error) { return []byte("retained"), nil })
		return []byte("done"), nil
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	if status := w.Status(); status.Ready || status.State != drt.WorkerNotStarted || status.RuntimeID == "" {
		t.Fatalf("initial status: %+v", status)
	}
	h, e := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(time.Second)})
	if e != nil {
		t.Fatal(e)
	}
	if d, waitErr := w.WaitDrain(t.Context(), h); waitErr != nil || !d.Complete {
		t.Fatalf("prestart: %+v %v", d, waitErr)
	}
	before, _ := s.GetExecution(t.Context(), key)
	q, e := w.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: o.BuildID, Name: "status", Selection: durable.RunExplicit})
	after, _ := s.GetExecution(t.Context(), key)
	if e != nil || string(q.Output) != "retained" || before.Revision != after.Revision || before.LastSequence != after.LastSequence {
		t.Fatalf("retained query: %+v %v", q, e)
	}
	other := newWorker(t, s, o)
	if other.Status().RuntimeID == w.Status().RuntimeID {
		t.Fatal("replacement reused process incarnation")
	}
	if _, e = other.WaitDrain(t.Context(), h); !errors.Is(e, durable.ErrRequestConflict) {
		t.Fatalf("foreign drain accepted: %v", e)
	}
}

type unknownDrainClaimStore struct{ durable.Store }

func (s *unknownDrainClaimStore) ClaimTask(ctx context.Context, r durable.ClaimRequest) (*durable.Task, error) {
	_, e := s.Store.ClaimTask(ctx, r)
	if e != nil {
		return nil, e
	}
	return nil, errors.New("claim response lost")
}
func TestDrainUnknownClaimNeverReportsSuccess(t *testing.T) {
	s := memory.New()
	o := drainFixture(t, s, durable.TaskWorkflow)
	w := newWorker(t, &unknownDrainClaimStore{Store: s}, o)
	if _, e := w.RunOnce(t.Context(), durable.TaskWorkflow); e == nil {
		t.Fatal("missing ambiguous claim error")
	}
	h, e := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(time.Second)})
	if e != nil {
		t.Fatal(e)
	}
	if d, e := w.WaitDrain(t.Context(), h); !errors.Is(e, drt.ErrDrainIncomplete) || d.UnknownClaims != 1 || d.Complete {
		t.Fatalf("unknown claim: %+v %v", d, e)
	}
}

func TestDrainRunStartRacesAndReadiness(t *testing.T) {
	for range 30 {
		o := workerOptions(t)
		w := newWorker(t, memory.New(), o)
		start := make(chan struct{})
		result := make(chan error, 1)
		go func() { <-start; result <- w.Run(t.Context()) }()
		close(start)
		h, e := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(time.Second)})
		if e != nil {
			t.Fatal(e)
		}
		if d, e := w.WaitDrain(t.Context(), h); e != nil || !d.Complete {
			t.Fatalf("race drain: %+v %v", d, e)
		}
		select {
		case e := <-result:
			if e != nil && !errors.Is(e, drt.ErrWorkerDraining) {
				t.Fatal(e)
			}
		case <-time.After(time.Second):
			t.Fatal("Run did not exit")
		}
		if status := w.Status(); status.Ready || status.State != drt.WorkerStopped || status.InFlight != 0 {
			t.Fatalf("race status: %+v", status)
		}
	}
	o := workerOptions(t)
	w := newWorker(t, memory.New(), o)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- w.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for !w.Status().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !w.Status().Ready {
		t.Fatal("started poller not ready")
	}
	if e := w.Run(t.Context()); !errors.Is(e, drt.ErrWorkerStarted) {
		t.Fatalf("duplicate Run: %v", e)
	}
	cancel()
	if e := <-result; e != nil {
		t.Fatal(e)
	}
	if status := w.Status(); status.Ready || status.State != drt.WorkerStopped {
		t.Fatalf("cancelled poller: %+v", status)
	}
}

func TestDrainExpiredRequestAndCancelledAcceptance(t *testing.T) {
	w := newWorker(t, memory.New(), workerOptions(t))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := drt.DrainRequest{OperationID: "expired", Deadline: time.Now().Add(-time.Second)}
	if _, e := w.BeginDrain(ctx, r); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled admission: %v", e)
	}
	if e := w.Stop(ctx); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled stop: %v", e)
	}
	if w.Status().AdmissionClosed {
		t.Fatal("cancelled acceptance changed worker")
	}
	h, e := w.BeginDrain(t.Context(), r)
	if e != nil {
		t.Fatal(e)
	}
	if d, e := w.WaitDrain(t.Context(), h); !errors.Is(e, drt.ErrDrainIncomplete) || !d.DeadlineExpired || d.Complete {
		t.Fatalf("expired request: %+v %v", d, e)
	}
}
