package delivery_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/store/memory"
)

type sinkFunc func(context.Context, durable.Delivery) (durable.SinkReceipt, error)

func (f sinkFunc) Accept(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	return f(ctx, d)
}
func receipt(d durable.Delivery) durable.SinkReceipt {
	return durable.SinkReceipt{ID: "receipt-" + d.ID, DeliveryID: d.ID, Destination: d.Destination, SchemaVersion: d.SchemaVersion, Fingerprint: d.Fingerprint}
}
func config(owner string) delivery.Config {
	return delivery.Config{InstallationID: "installation", Owner: owner, Concurrency: 2, PollInterval: time.Millisecond, CallTimeout: 10 * time.Millisecond, StoreTimeout: 20 * time.Millisecond, LeaseDuration: 50 * time.Millisecond, RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond}
}
func fixture(t *testing.T, count int) *memory.Store {
	t.Helper()
	s := memory.New()
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "audit", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		a, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, "read", "allowed", "", durable.AuditMetadata{ActorKind: "anonymous"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.AppendSecurityAudit(t.Context(), a); err != nil {
			t.Fatal(err)
		}
	}
	return s
}
func wait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func stop(t *testing.T, p *delivery.Publisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestLostAcknowledgementConcurrentPublishers(t *testing.T) {
	s := fixture(t, 12)
	var mu sync.Mutex
	accepted := map[string]durable.SinkReceipt{}
	calls := map[string]int{}
	sink := sinkFunc(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		mu.Lock()
		defer mu.Unlock()
		calls[d.ID]++
		if r, ok := accepted[d.ID]; ok {
			return r, nil
		}
		accepted[d.ID] = receipt(d)
		return durable.SinkReceipt{}, errors.New("lost acknowledgement")
	})
	sinks := map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sink}
	p1, err := delivery.New(s, config("one"), sinks)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := delivery.New(s, config("two"), sinks)
	if err != nil {
		t.Fatal(err)
	}
	if err = p1.Start(); err != nil {
		t.Fatal(err)
	}
	if err = p2.Start(); err != nil {
		t.Fatal(err)
	}
	stop(t, p1)
	stop(t, p2)
	status, err := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "installation", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || status.Pending != 0 || len(status.Records) != 12 {
		t.Fatal(status, err)
	}
	for _, r := range status.Records {
		if r.Receipt != accepted[r.Delivery.ID] || calls[r.Delivery.ID] < 2 {
			t.Fatal("lost stable receipt", r)
		}
	}
}
func TestHungSinkBoundsCallsAndRestart(t *testing.T) {
	s := fixture(t, 6)
	release := make(chan struct{})
	var calls atomic.Int32
	sink := sinkFunc(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		calls.Add(1)
		<-release
		return receipt(d), nil
	})
	p, err := delivery.New(s, config("hung"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sink})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Start(); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { return calls.Load() == 2 })
	time.Sleep(80 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatal("replacement calls escaped concurrency bound")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err = p.Stop(ctx); !errors.Is(err, delivery.ErrIncompleteShutdown) {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), durable.DestinationChronicle)
	if err != nil || status.Pending != 6 || status.InFlight != 2 {
		t.Fatal(status, err)
	}
	close(release)
	wait(t, func() bool { st, _ := p.Status(t.Context(), durable.DestinationChronicle); return st.InFlight == 0 })
	status, _ = p.Status(t.Context(), durable.DestinationChronicle)
	if status.Pending != 6 {
		t.Fatal("canceled sink reply acknowledged")
	}
	restarted, err := delivery.New(s, config("restarted"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sinkFunc(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) { return receipt(d), nil })})
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Start(); err != nil {
		t.Fatal(err)
	}
	stop(t, restarted)
}
func TestRejectMismatchedReceipt(t *testing.T) {
	for _, change := range []func(*durable.SinkReceipt){func(r *durable.SinkReceipt) { r.DeliveryID = "wrong" }, func(r *durable.SinkReceipt) { r.Destination = durable.DestinationRelay }, func(r *durable.SinkReceipt) { r.SchemaVersion++ }, func(r *durable.SinkReceipt) { r.Fingerprint = "wrong" }} {
		s := fixture(t, 1)
		p, err := delivery.New(s, config("invalid"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sinkFunc(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
			r := receipt(d)
			change(&r)
			return r, nil
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err = p.Start(); err != nil {
			t.Fatal(err)
		}
		wait(t, func() bool {
			st, _ := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "installation", Destination: durable.DestinationChronicle}, Limit: 1})
			return len(st.Records) == 1 && st.Records[0].ErrorCategory == "invalid_receipt"
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		err = p.Stop(ctx)
		cancel()
		if !errors.Is(err, delivery.ErrIncompleteShutdown) {
			t.Fatal(err)
		}
	}
}

// The Relay record uses the normal transactional event path, not a fabricated
// publisher success. Chronicle's backlog exceeds any single claim batch.
func TestDestinationOutageIsIndependent(t *testing.T) {
	s := fixture(t, 101)
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "workflow", AppID: "app", TenantID: "tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	// StartExecution's shared fixture builder lives in the durable conformance
	// package; use its minimal valid request here.
	_, err := s.StartExecution(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: "workflow", WorkflowID: "workflow", RunID: "run"}, RequestID: "start", WorkflowType: "test", Queue: "default", BuildID: "build"})
	if err != nil {
		t.Fatal(err)
	}
	var relay atomic.Int32
	p, err := delivery.New(s, config("independent"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sinkFunc(func(context.Context, durable.Delivery) (durable.SinkReceipt, error) {
		return durable.SinkReceipt{}, errors.New("offline")
	}), durable.DestinationRelay: sinkFunc(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		relay.Add(1)
		return receipt(d), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Start(); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool {
		st, _ := p.Status(t.Context(), durable.DestinationRelay)
		return relay.Load() > 0 && st.Pending == 0
	})
	st, _ := p.Status(t.Context(), durable.DestinationChronicle)
	if st.Pending < 101 {
		t.Fatal(st)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err = p.Stop(ctx); !errors.Is(err, delivery.ErrIncompleteShutdown) {
		t.Fatal(err)
	}
}

type closingStore struct {
	*memory.Store
	mu     sync.Mutex
	closed bool
	closes int
	late   int
}

func (s *closingStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.closes++
	return nil
}
func (s *closingStore) check() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.late++
		return errors.New("store closed")
	}
	return nil
}
func (s *closingStore) ClaimDeliveries(ctx context.Context, r durable.DeliveryClaim) ([]durable.DeliveryRecord, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.Store.ClaimDeliveries(ctx, r)
}
func (s *closingStore) AcknowledgeDelivery(ctx context.Context, token durable.DeliveryToken, r durable.SinkReceipt) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.Store.AcknowledgeDelivery(ctx, token, r)
}
func (s *closingStore) DeliveryStatus(ctx context.Context, r durable.DeliveryStatusRequest) (durable.DeliveryStatus, error) {
	if err := s.check(); err != nil {
		return durable.DeliveryStatus{}, err
	}
	return s.Store.DeliveryStatus(ctx, r)
}
func (s *closingStore) RetryDelivery(ctx context.Context, r durable.DeliveryRetry) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.Store.RetryDelivery(ctx, r)
}

type drainingPool struct {
	store   *closingStore
	stopped chan struct{}
}

func (p *drainingPool) Start(context.Context) error { return nil }
func (p *drainingPool) Stop(ctx context.Context) error {
	a, err := durable.CaptureSecurityAudit("installation", "audit", "worker.stop", "accepted", "", durable.AuditMetadata{ActorKind: "worker", ActorID: "worker"})
	if err != nil {
		return err
	}
	if _, err = p.store.AppendSecurityAudit(ctx, a); err != nil {
		return err
	}
	close(p.stopped)
	return nil
}
func TestPublisherDrainsAfterWorkersBeforeStoreClose(t *testing.T) {
	s := &closingStore{Store: fixture(t, 1)}
	pool := &drainingPool{store: s, stopped: make(chan struct{})}
	sink := sinkFunc(func(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		select {
		case <-pool.stopped:
			return receipt(d), nil
		case <-ctx.Done():
			return durable.SinkReceipt{}, ctx.Err()
		}
	})
	p, err := delivery.New(s, config("closing"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sink})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.SetPool(pool)
	dispatcher.BeforeStoreClose(p.Stop)
	if err = p.Start(); err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = dispatcher.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := s.Store.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "installation", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || status.Pending != 0 || len(status.Records) != 2 {
		t.Fatal(status, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed || s.late != 0 || s.closes != 1 {
		t.Fatal("store lifecycle violated", s.closed, s.late)
	}
}

func TestIncompleteDrainRetainsStoreAndStopCanRetry(t *testing.T) {
	s := &closingStore{Store: fixture(t, 1)}
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	p, err := delivery.New(s, config("retry-stop"), map[durable.Destination]delivery.Sink{durable.DestinationChronicle: sinkFunc(func(_ context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
		once.Do(func() { close(entered) })
		<-release
		return receipt(d), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.BeforeStoreClose(p.Stop)
	if err = p.Start(); err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	err = dispatcher.Stop(ctx)
	cancel()
	if !errors.Is(err, delivery.ErrIncompleteShutdown) {
		t.Fatal(err)
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		t.Fatal("store closed during incomplete shutdown")
	}
	close(release)
	wait(t, func() bool { st, _ := p.Status(t.Context(), durable.DestinationChronicle); return st.InFlight == 0 })
	status, err := p.Status(t.Context(), durable.DestinationChronicle)
	if err != nil || status.Pending != 1 {
		t.Fatal("late response acknowledged", status, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
			defer stopCancel()
			if stopErr := dispatcher.Stop(stopCtx); stopErr != nil {
				t.Error(stopErr)
			}
		}()
	}
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed || s.late != 0 || s.closes != 1 {
		t.Fatal("retry close failed", s.closed, s.late)
	}
}
