package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

type uncertainHandoffStore struct {
	durable.Store
	calls            atomic.Int64
	renewals         atomic.Int64
	recovered        chan struct{}
	failBeforeCommit bool
}

var errHandoffUnknown = errors.New("handoff acknowledgement unavailable")

func (s *uncertainHandoffStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	isHandoff := len(r.Events) > 0 && r.Events[0].Type == drt.EventActivityDeferred
	if !isHandoff {
		return s.Store.CommitTransition(ctx, r)
	}
	count := s.calls.Add(1)
	if s.failBeforeCommit && count <= 3 {
		return durable.Receipt{}, errHandoffUnknown
	}
	receipt, err := s.Store.CommitTransition(ctx, r)
	if err != nil {
		return receipt, err
	}
	if count <= 3 {
		return durable.Receipt{}, errHandoffUnknown
	}
	if count == 4 {
		close(s.recovered)
	}
	return receipt, nil
}

func (s *uncertainHandoffStore) RenewTask(ctx context.Context, key durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	if token.TaskID == "command:1" {
		s.renewals.Add(1)
	}
	return s.Store.RenewTask(ctx, key, token, ttl)
}

func TestAsyncUncertainHandoffRetainsRecovery(t *testing.T) {
	for _, mode := range []string{"renewal_committed", "renewal_uncommitted", "heartbeat"} {
		t.Run(mode, func(t *testing.T) {
			s := &uncertainHandoffStore{Store: memory.New(), recovered: make(chan struct{}), failBeforeCommit: mode == "renewal_uncommitted"}
			options := workerOptions(t)
			options.LeaseDuration, options.StoreTimeout = 300*time.Millisecond, 75*time.Millisecond
			if mode == "heartbeat" {
				options.LeaseDuration = 3 * time.Second
			}
			options.Workflows["order"] = retryWorkflow(asyncOptions(1))
			var handle drt.AsyncActivityHandle
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				if _, err := info.DeferCompletion(ctx); !errors.Is(err, errHandoffUnknown) {
					return nil, fmt.Errorf("test did not lose handoff response: %w", err)
				}
				if strings.HasPrefix(mode, "renewal_") {
					select {
					case <-s.recovered:
					case <-ctx.Done():
						return nil, fmt.Errorf("renewal cancelled unresolved handoff: %w", context.Cause(ctx))
					case <-time.After(2 * time.Second):
						return nil, errors.New("renewal did not reconcile handoff")
					}
				} else if err := info.Heartbeat(ctx, []byte("must not send")); !errors.Is(err, drt.ErrHandoffPending) || ctx.Err() != nil {
					return nil, errors.New("ordinary heartbeat lost pending handoff recovery")
				}
				var err error
				handle, err = info.DeferCompletion(ctx)
				return nil, err
			}
			w := newWorker(t, s, options)
			startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskActivity)
			if err := handle.Validate(); err != nil || s.calls.Load() != 4 || s.renewals.Load() != 0 {
				t.Fatalf("unresolved ownership recovery: handle=%v calls=%d renewals=%d", err, s.calls.Load(), s.renewals.Load())
			}
			if _, err := w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}); err != nil {
				t.Fatal(err)
			}
			runTask(t, w, durable.TaskWorkflow)
		})
	}
}

type cancelledHandoffStore struct {
	durable.Store
	cancel    context.CancelFunc
	once      sync.Once
	handoffs  atomic.Int64
	renewed   chan struct{}
	renewOnce sync.Once
}

func (s *cancelledHandoffStore) GetTask(ctx context.Context, key durable.Key, id string) (durable.Task, error) {
	task, err := s.Store.GetTask(ctx, key, id)
	if err == nil && s.cancel != nil {
		s.once.Do(s.cancel)
	}
	return task, err
}

func (s *cancelledHandoffStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	if len(r.Events) > 0 && r.Events[0].Type == drt.EventActivityDeferred {
		s.handoffs.Add(1)
	}
	return s.Store.CommitTransition(ctx, r)
}

func (s *cancelledHandoffStore) RenewTask(ctx context.Context, key durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	until, err := s.Store.RenewTask(ctx, key, token, ttl)
	if err == nil && token.TaskID == "command:1" {
		s.renewOnce.Do(func() { close(s.renewed) })
	}
	return until, err
}

func TestAsyncCancelledPreparedHandoffStaysUnsent(t *testing.T) {
	s := &cancelledHandoffStore{Store: memory.New(), renewed: make(chan struct{})}
	options := workerOptions(t)
	options.LeaseDuration, options.StoreTimeout = 300*time.Millisecond, 75*time.Millisecond
	options.Workflows["order"] = retryWorkflow(asyncOptions(1))
	var handle drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		cancelled, cancel := context.WithCancel(ctx)
		defer cancel()
		s.cancel = cancel
		if _, err := info.DeferCompletion(cancelled); !errors.Is(err, context.Canceled) {
			return nil, fmt.Errorf("prepared handoff cancellation: %w", err)
		}
		select {
		case <-s.renewed:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(2 * time.Second):
			return nil, errors.New("unsent handoff stopped renewal")
		}
		if s.handoffs.Load() != 0 {
			return nil, errors.New("cancelled handoff sent implicitly")
		}
		if err := info.Heartbeat(ctx, []byte("normal worker progress")); err != nil {
			return nil, err
		}
		var err error
		handle, err = info.DeferCompletion(ctx)
		return nil, err
	}
	w := newWorker(t, s, options)
	startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	if err := handle.Validate(); err != nil || handle.InitialHeartbeatSequence != 1 || s.handoffs.Load() != 1 {
		t.Fatalf("explicit handoff after cancellation: %v sequence=%d sends=%d", err, handle.InitialHeartbeatSequence, s.handoffs.Load())
	}
	if _, err := w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
}

func TestAsyncBuildIDMatchesDurableLimits(t *testing.T) {
	for _, length := range []int{200, 201, 512} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.BuildID = strings.Repeat("b", length)
			options.Workflows["order"] = retryWorkflow(asyncOptions(1))
			var handle drt.AsyncActivityHandle
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				var err error
				handle, err = info.DeferCompletion(ctx)
				return nil, err
			}
			w := newWorker(t, s, options)
			startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskActivity)
			if err := handle.Validate(); err != nil {
				t.Fatalf("valid %d-byte build produced unusable handle: %v", length, err)
			}
			if _, err := w.HeartbeatAsyncActivity(t.Context(), drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "progress", Sequence: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}); err != nil {
				t.Fatal(err)
			}
			runTask(t, w, durable.TaskWorkflow)
		})
	}
}
