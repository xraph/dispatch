package operatorhost

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/memory"
)

type strictHostStore struct {
	*memory.Store
	fail     string
	cause    error
	closeErr error
	cancel   context.CancelFunc
	closes   atomic.Int32
}

func (s *strictHostStore) Migrate(ctx context.Context) error {
	if s.fail == "migrate" {
		return s.cause
	}
	return s.Store.Migrate(ctx)
}
func (s *strictHostStore) RegisterNamespace(ctx context.Context, config durable.NamespaceConfig) (durable.NamespaceRecord, error) {
	if s.fail == "namespace" {
		return durable.NamespaceRecord{}, s.cause
	}
	return s.Store.RegisterNamespace(ctx, config)
}
func (s *strictHostStore) StartExecution(ctx context.Context, request durable.StartRequest) (durable.Receipt, error) {
	if s.fail == "seed" {
		s.cancel()
		return durable.Receipt{}, s.cause
	}
	return s.Store.StartExecution(ctx, request)
}
func (s *strictHostStore) Close() error {
	if s.closes.Add(1) != 1 {
		return errors.New("duplicate store close")
	}
	return s.closeErr
}

func TestConstructorFailureClosesStore(t *testing.T) {
	for _, phase := range []string{"migrate", "namespace", "seed"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cause := errors.New("construction failed at " + phase)
			store := &strictHostStore{Store: memory.New(), fail: phase, cause: cause, closeErr: errors.New("cleanup failed"), cancel: cancel}
			host, err := New(ctx, store)
			if host != nil || !errors.Is(err, cause) || errors.Is(err, store.closeErr) {
				t.Fatalf("constructor result: host=%v error=%v; want original error", host != nil, err)
			}
			if got := store.closes.Load(); got != 1 {
				t.Fatalf("store closes=%d; want exactly one", got)
			}
			if phase == "seed" && ctx.Err() == nil {
				t.Fatal("post-engine failure did not cancel the construction context")
			}
		})
	}
}

func TestConstructorTransfersStoreToEngine(t *testing.T) {
	store := &strictHostStore{Store: memory.New()}
	host, err := New(t.Context(), store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	if got := store.closes.Load(); got != 0 {
		t.Fatalf("successful constructor closed store %d times", got)
	}
	if _, err := store.GetExecution(t.Context(), durable.Key{Namespace: "production", WorkflowID: "invoice", RunID: "run-2"}); err != nil {
		t.Fatal("constructed store unavailable", err)
	}
	for range 2 {
		if err := host.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := store.closes.Load(); got != 1 {
			t.Fatalf("store closes=%d; engine must close exactly once", got)
		}
	}
}
