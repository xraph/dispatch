package engine_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// No embedding: every store method must explicitly acquire the close guard.
type strictLifecycleStore struct {
	base                                *memory.Store
	mu                                  sync.Mutex
	closed                              bool
	claimError                          error
	closes, late, wakeStarts, wakeStops int
}

func (s *strictLifecycleStore) call(method string) func() {
	s.mu.Lock()
	if s.closed {
		s.late++
		s.mu.Unlock()
		panic("store call after Close: " + method)
	}
	return s.mu.Unlock
}
func (s *strictLifecycleStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	if s.closed {
		s.late++
		return errors.New("duplicate store Close")
	}
	s.closed = true
	return s.base.Close()
}
func (s *strictLifecycleStore) StartWakeListener(ctx context.Context, wake func()) (func(), error) {
	defer s.call("StartWakeListener")()
	s.wakeStarts++
	stopped, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopped:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				wake()
			}
		}
	}()
	return func() { defer s.call("stop wake listener")(); s.wakeStops++; close(stopped); <-done }, nil
}
func (s *strictLifecycleStore) assertState(t *testing.T, closes, wakeStops int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closes != closes || s.late != 0 || s.wakeStarts != 1 || s.wakeStops != wakeStops {
		t.Fatalf("lifecycle: closes=%d late=%d wake=%d/%d", s.closes, s.late, s.wakeStarts, s.wakeStops)
	}
}
func TestLifecycleGuardCoversCompleteMemorySurface(t *testing.T) {
	want, got := reflect.TypeFor[*memory.Store](), reflect.TypeFor[*strictLifecycleStore]()
	for i := 0; i < want.NumMethod(); i++ {
		if _, ok := got.MethodByName(want.Method(i).Name); !ok {
			t.Fatalf("unguarded method %s", want.Method(i).Name)
		}
	}
}

type lifecycleSink func(context.Context, durable.Delivery) (durable.SinkReceipt, error)

func (f lifecycleSink) Accept(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	return f(ctx, d)
}
func lifecycleReceipt(d durable.Delivery) durable.SinkReceipt {
	return durable.SinkReceipt{ID: "receipt-" + d.ID, DeliveryID: d.ID, Destination: d.Destination, SchemaVersion: d.SchemaVersion, Fingerprint: d.Fingerprint}
}
func awaitLifecycle(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("lifecycle event did not arrive")
	}
}

func TestEngineStopWaitsForDurableWorkerAndRetriesPublisherDrain(t *testing.T) {
	s := &strictLifecycleStore{base: memory.New()}
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "audit", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	audit, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, "test.audit", "accepted", "installation", durable.AuditMetadata{ActorKind: "system", ActorID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.AppendSecurityAudit(t.Context(), audit); err != nil {
		t.Fatal(err)
	}
	sinkEntered, sinkRelease := make(chan struct{}), make(chan struct{})
	var sinkOnce sync.Once
	publisher, err := delivery.New(s, delivery.Config{InstallationID: n.InstallationID, Owner: "publisher", PollInterval: time.Millisecond, LeaseDuration: 50 * time.Millisecond, CallTimeout: 10 * time.Millisecond, StoreTimeout: 5 * time.Millisecond, RetryMin: time.Millisecond}, map[durable.Destination]delivery.Sink{durable.DestinationChronicle: lifecycleSink(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		sinkOnce.Do(func() { close(sinkEntered) })
		<-sinkRelease
		return lifecycleReceipt(d), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	d.BeforeStoreClose(publisher.Stop)
	workerEntered, workerRelease := make(chan struct{}), make(chan struct{})
	var workerOnce sync.Once
	eng, err := engine.Build(d, engine.WithDurableWorkflows(drt.Options{Namespace: "workflows", Queue: "work", BuildID: "v1", Owner: "worker", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"hang": func(_ *drt.Workflow, _ []byte) ([]byte, error) {
		workerOnce.Do(func() { close(workerEntered) })
		<-workerRelease
		return nil, nil
	}}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-workerRelease:
		default:
			close(workerRelease)
		}
		select {
		case <-sinkRelease:
		default:
			close(sinkRelease)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	}()
	if _, err = eng.StartDurableWorkflow(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: "workflows", WorkflowID: "hang", RunID: "run"}, RequestID: "start", WorkflowType: "hang", BuildID: "v1", Queue: "work"}); err != nil {
		t.Fatal(err)
	}
	if err = publisher.Start(); err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitLifecycle(t, workerEntered)
	awaitLifecycle(t, sinkEntered)
	var wg sync.WaitGroup
	for _, timeout := range []time.Duration{10 * time.Millisecond, 30 * time.Millisecond, 60 * time.Millisecond} {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if stopErr := eng.Stop(ctx); !errors.Is(stopErr, context.DeadlineExceeded) {
				t.Errorf("worker deadline: %v", stopErr)
			}
		})
	}
	wg.Wait()
	s.assertState(t, 0, 0)
	close(workerRelease)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = eng.Stop(ctx)
	cancel()
	if !errors.Is(err, delivery.ErrIncompleteShutdown) {
		t.Fatalf("sink deadline: %v", err)
	}
	s.assertState(t, 0, 1)
	close(sinkRelease)
	deadline := time.Now().Add(time.Second)
	for {
		status, statusErr := publisher.Status(t.Context(), durable.DestinationChronicle)
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.InFlight == 0 {
			if status.Pending != 1 {
				t.Fatal("late sink reply acknowledged", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sink call did not exit")
		}
		time.Sleep(time.Millisecond)
	}
	expired, cancelExpired := context.WithCancel(t.Context())
	cancelExpired()
	if err = eng.Stop(expired); !errors.Is(err, context.Canceled) {
		t.Fatal("expired finalization", err)
	}
	s.assertState(t, 0, 1)
	for range 4 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if stopErr := eng.Stop(ctx); stopErr != nil {
				t.Error(stopErr)
			}
		})
	}
	wg.Wait()
	s.assertState(t, 1, 1)
	if err = eng.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.assertState(t, 1, 1)
}

func TestEngineStopChecksContextAfterRequiredDrain(t *testing.T) {
	s := &strictLifecycleStore{base: memory.New()}
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d.BeforeStoreClose(func(context.Context) error { cancel(); return nil })
	if err = eng.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("expired drain finalized", err)
	}
	s.assertState(t, 0, 1)
	if err = eng.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.assertState(t, 1, 1)
}

func TestStopWorkersKeepsPublisherAndStoreLive(t *testing.T) {
	s := &strictLifecycleStore{base: memory.New()}
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "audit", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int64
	publisher, err := delivery.New(s, delivery.Config{InstallationID: n.InstallationID, Owner: "publisher", PollInterval: time.Millisecond}, map[durable.Destination]delivery.Sink{durable.DestinationChronicle: lifecycleSink(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		accepted.Add(1)
		return lifecycleReceipt(d), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(1), dispatch.WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	d.BeforeStoreClose(publisher.Stop)
	durableEntered, durableRelease := make(chan struct{}), make(chan struct{})
	eng, err := engine.Build(d, engine.WithDurableWorkflows(drt.Options{Namespace: "workers", Queue: "work", BuildID: "v1", Owner: "worker", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"hold": func(*drt.Workflow, []byte) ([]byte, error) { close(durableEntered); <-durableRelease; return nil, nil }}}))
	if err != nil {
		t.Fatal(err)
	}
	legacyEntered, legacyRelease := make(chan struct{}), make(chan struct{})
	var runs atomic.Int64
	engine.Register(eng, job.NewDefinition("hold", func(context.Context, struct{}) error { runs.Add(1); close(legacyEntered); <-legacyRelease; return nil }))
	defer func() {
		select {
		case <-legacyRelease:
		default:
			close(legacyRelease)
		}
		select {
		case <-durableRelease:
		default:
			close(durableRelease)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	}()
	if _, err = engine.Enqueue(t.Context(), eng, "hold", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if _, err = eng.StartDurableWorkflow(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: "workers", WorkflowID: "hold", RunID: "run"}, RequestID: "start", WorkflowType: "hold", BuildID: "v1", Queue: "work"}); err != nil {
		t.Fatal(err)
	}
	if err = publisher.Start(); err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitLifecycle(t, legacyEntered)
	awaitLifecycle(t, durableEntered)
	var wg sync.WaitGroup
	for _, stop := range []func(context.Context) error{eng.StopWorkers, eng.Stop, eng.StopWorkers} {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if e := stop(ctx); !errors.Is(e, context.DeadlineExceeded) {
				t.Errorf("live handler quiescence reported: %v", e)
			}
		})
	}
	wg.Wait()
	s.assertState(t, 0, 0)
	close(durableRelease)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = eng.StopWorkers(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("legacy handler did not hold quiescence: %v", err)
	}
	close(legacyRelease)
	if err = eng.StopWorkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.assertState(t, 0, 1)
	if err = eng.Start(t.Context()); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("terminal worker stop restarted: %v", err)
	}
	queued, err := engine.Enqueue(t.Context(), eng, "hold", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	audit, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, "after-workers", "accepted", "", durable.AuditMetadata{ActorKind: "system", ActorID: "host"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.AppendSecurityAudit(t.Context(), audit); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for accepted.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("publisher stopped with workers")
		}
		time.Sleep(time.Millisecond)
	}
	stored, err := s.GetJob(t.Context(), queued.ID)
	if err != nil || stored.State != job.StatePending || runs.Load() != 1 {
		t.Fatalf("stopped worker executed: %+v %v", stored, err)
	}
	for range 3 {
		wg.Go(func() {
			if e := eng.Stop(t.Context()); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	s.assertState(t, 1, 1)
	if err = eng.StopWorkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.assertState(t, 1, 1)
}

func TestStopWorkersRetainsWorkerErrorAndFinalCleanup(t *testing.T) {
	failure := errors.New("durable polling unavailable")
	s := &strictLifecycleStore{base: memory.New(), claimError: failure}
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(1), dispatch.WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(drt.Options{Namespace: "worker-error", Queue: "work", BuildID: "v1", Owner: "worker", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"unused": func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil }}}))
	if err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for eng.Health(t.Context()) == nil {
		if time.Now().After(deadline) {
			t.Fatal("worker error not visible")
		}
		time.Sleep(time.Millisecond)
	}
	if err = eng.StopWorkers(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("worker error lost: %v", err)
	}
	s.assertState(t, 0, 1)
	for range 2 {
		if err = eng.Stop(t.Context()); !errors.Is(err, failure) {
			t.Fatalf("final stop lost retained error: %v", err)
		}
	}
	s.assertState(t, 1, 1)
	if err = eng.StopWorkers(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("repeat lost retained error: %v", err)
	}
	s.assertState(t, 1, 1)
}
