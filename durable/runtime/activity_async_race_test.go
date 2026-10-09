package runtime_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

var errAsyncResponseLost = errors.New("async response lost after accepted commit")

type asyncResponseStore struct {
	durable.Store
	mu       sync.Mutex
	handoffs []durable.CommitRequest
	results  []durable.CommitRequest
	onResult func()
}

func (s *asyncResponseStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	isHandoff := len(r.Events) > 0 && r.Events[0].Type == drt.EventActivityDeferred
	isResult := r.Token.LeaseKind == durable.LeaseAsync
	if !isHandoff && !isResult {
		return s.Store.CommitTransition(ctx, r)
	}
	s.mu.Lock()
	count := 0
	if isHandoff {
		s.handoffs = append(s.handoffs, r)
		count = len(s.handoffs)
	} else {
		s.results = append(s.results, r)
		count = len(s.results)
	}
	s.mu.Unlock()
	receipt, err := s.Store.CommitTransition(ctx, r)
	if err != nil {
		return receipt, err
	}
	if isResult && count == 1 && s.onResult != nil {
		s.onResult()
	}
	if count <= 3 {
		return durable.Receipt{}, errAsyncResponseLost
	}
	return receipt, nil
}

func TestAsyncUnknownResponsesRecoverOriginalRequests(t *testing.T) {
	s := &asyncResponseStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(asyncOptions(1))
	var handle drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := info.DeferCompletion(cancelled); !errors.Is(err, context.Canceled) {
			return nil, errors.New("unsent cancellation ignored")
		}
		if _, err := info.DeferCompletion(ctx); !errors.Is(err, errAsyncResponseLost) {
			return nil, errors.New("test did not lose all handoff acknowledgements")
		}
		var err error
		handle, err = info.DeferCompletion(ctx)
		return nil, err
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	if len(s.handoffs) != 4 {
		t.Fatalf("handoff attempts: %d", len(s.handoffs))
	}
	first, err := durable.Fingerprint("commit", s.handoffs[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range s.handoffs {
		got, hashErr := durable.Fingerprint("commit", request)
		if hashErr != nil || got != first {
			t.Fatal("unknown handoff rebuilt its transaction")
		}
	}
	s.onResult = func() { runTask(t, w, durable.TaskWorkflow) }
	request := drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}
	receipt, err := w.CompleteAsyncActivity(t.Context(), request)
	if err != nil || len(s.results) != 3 {
		t.Fatalf("result receipt resolution: %+v %d %v", receipt, len(s.results), err)
	}
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted {
		t.Fatalf("test did not advance to closure: %+v %v", execution, err)
	}
	options.Owner, options.Activities = "replacement", nil
	replacement := newWorker(t, s, options)
	if got, repeatErr := replacement.CompleteAsyncActivity(t.Context(), request); repeatErr != nil || got != receipt {
		t.Fatalf("replacement callback: %+v %v", got, repeatErr)
	}
	if len(s.results) != 3 {
		t.Fatal("receipt lookup mutated state after closure")
	}
}

type asyncGateStore struct {
	durable.Store
	mode     string
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	renewals atomic.Int64
}

func (s *asyncGateStore) RenewTask(ctx context.Context, key durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	if token.TaskID == "command:1" {
		s.renewals.Add(1)
		if s.mode == "renewal" {
			var err error
			s.once.Do(func() {
				close(s.entered)
				select {
				case <-s.release:
				case <-ctx.Done():
					err = ctx.Err()
				}
			})
			if err != nil {
				return time.Time{}, err
			}
		}
	}
	return s.Store.RenewTask(ctx, key, token, ttl)
}

func (s *asyncGateStore) RecordHeartbeat(ctx context.Context, r durable.HeartbeatRequest) (durable.Receipt, error) {
	if s.mode == "heartbeat" {
		var err error
		s.once.Do(func() {
			close(s.entered)
			select {
			case <-s.release:
			case <-ctx.Done():
				err = ctx.Err()
			}
		})
		if err != nil {
			return durable.Receipt{}, err
		}
	}
	return s.Store.RecordHeartbeat(ctx, r)
}

func TestAsyncHandoffSerializesRenewalAndHeartbeat(t *testing.T) {
	for _, mode := range []string{"renewal", "heartbeat"} {
		t.Run(mode, func(t *testing.T) {
			s := &asyncGateStore{Store: memory.New(), mode: mode, entered: make(chan struct{}), release: make(chan struct{})}
			options := workerOptions(t)
			options.StoreTimeout = 250 * time.Millisecond
			options.Workflows["order"] = retryWorkflow(asyncOptions(1))
			requested := make(chan struct{})
			delivered := make(chan drt.AsyncActivityHandle, 1)
			finish := make(chan struct{})
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				hbDone := make(chan error, 1)
				if mode == "heartbeat" {
					go func() { hbDone <- info.Heartbeat(ctx, []byte("before")) }()
				}
				select {
				case <-s.entered:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				close(requested)
				handle, err := info.DeferCompletion(ctx)
				if err != nil {
					return nil, err
				}
				if mode == "heartbeat" {
					if err = <-hbDone; err != nil {
						return nil, err
					}
				}
				delivered <- handle
				select {
				case <-finish:
					return []byte("ignored"), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			w := newWorker(t, s, options)
			key := startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := w.RunOnce(ctx, durable.TaskActivity); done <- err }()
			select {
			case <-requested:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-delivered:
				t.Fatal("handoff overtook an in-flight mutation")
			case <-time.After(10 * time.Millisecond):
			}
			task, err := s.GetTask(ctx, key, "command:1")
			if err != nil || task.LeaseKind != "" {
				t.Fatalf("mutation not held before handoff: %+v %v", task, err)
			}
			close(s.release)
			var handle drt.AsyncActivityHandle
			select {
			case handle = <-delivered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if mode == "heartbeat" && handle.InitialHeartbeatSequence != 1 {
				t.Fatal("handoff omitted in-flight progress")
			}
			before := s.renewals.Load()
			time.Sleep(400 * time.Millisecond)
			if s.renewals.Load() != before {
				t.Fatal("worker renewed after handoff")
			}
			if _, err = w.CompleteAsyncActivity(ctx, drt.AsyncCompletionRequest{Handle: handle, RequestID: "done", Output: []byte("paid")}); err != nil {
				t.Fatal(err)
			}
			close(finish)
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			runTask(t, w, durable.TaskWorkflow)
		})
	}
}

type asyncLateProgressStore struct {
	durable.Store
	mode      string
	once      sync.Once
	conflicts atomic.Int64
}

func (s *asyncLateProgressStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	isHandoff := len(r.Events) > 0 && r.Events[0].Type == drt.EventActivityDeferred
	if (s.mode == "handoff" && isHandoff) || (s.mode == "callback" && r.Token.LeaseKind == durable.LeaseAsync) {
		var injected error
		s.once.Do(func() {
			request := durable.HeartbeatRequest{Key: r.Key, RequestID: "late-progress", Token: r.Token, AsyncSecret: r.AsyncSecret, Sequence: 1, Progress: []byte("late")}
			if isHandoff {
				request.LeaseDuration = time.Second
			}
			_, injected = s.RecordHeartbeat(ctx, request)
		})
		if injected != nil {
			return durable.Receipt{}, injected
		}
	}
	receipt, err := s.Store.CommitTransition(ctx, r)
	if errors.Is(err, durable.ErrTaskConflict) {
		s.conflicts.Add(1)
	}
	return receipt, err
}

func TestAsyncCheckpointGuardsLateProgress(t *testing.T) {
	for _, mode := range []string{"handoff", "callback"} {
		t.Run(mode, func(t *testing.T) {
			s := &asyncLateProgressStore{Store: memory.New(), mode: mode}
			options := workerOptions(t)
			options.Workflows["order"] = retryWorkflow(asyncOptions(1))
			var handle drt.AsyncActivityHandle
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				var err error
				handle, err = info.DeferCompletion(ctx)
				return nil, err
			}
			w := newWorker(t, s, options)
			key := startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskActivity)
			if _, err := w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}); err != nil {
				t.Fatal(err)
			}
			if s.conflicts.Load() != 1 {
				t.Fatal("test did not exercise the task observation conflict")
			}
			runTask(t, w, durable.TaskWorkflow)
			outcome := lastActivityOutcome(t, s, key)
			if outcome.Heartbeat == nil || outcome.Heartbeat.Sequence != 1 || string(outcome.Heartbeat.Details) != "late" {
				t.Fatalf("late checkpoint lost: %+v", outcome.Heartbeat)
			}
		})
	}
}

func TestAsyncConcurrentCompletion(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(asyncOptions(1))
	var handle drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		var err error
		handle, err = info.DeferCompletion(ctx)
		return nil, err
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	request := drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := w.CompleteAsyncActivity(t.Context(), request); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Type == drt.EventActivityCompleted {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("published %d results", count)
	}
	request.Handle.Secret = strings.Repeat("ee", 32)
	if _, err = w.CompleteAsyncActivity(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed credential reused receipt: %v", err)
	}
	runTask(t, w, durable.TaskWorkflow)
}

func TestAsyncConcurrentHandoffAndTerminalFailure(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(asyncOptions(3))
	var handle drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		var wg sync.WaitGroup
		handles := make(chan drt.AsyncActivityHandle, 8)
		errorsCh := make(chan error, 8)
		for range 8 {
			wg.Go(func() {
				got, err := info.DeferCompletion(ctx)
				handles <- got
				errorsCh <- err
			})
		}
		wg.Wait()
		close(handles)
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				return nil, err
			}
		}
		for got := range handles {
			if handle.Version != 0 && got != handle {
				return nil, errors.New("concurrent handoffs returned different credentials")
			}
			handle = got
		}
		return nil, nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	request := drt.AsyncCompletionRequest{Handle: handle, RequestID: "rejected",
		Failure: &drt.ApplicationError{Type: "declined", Message: "payment declined", NonRetryable: true}}
	receipt, err := w.CompleteAsyncActivity(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateFailed {
		t.Fatalf("terminal failure: %+v %v", execution, err)
	}
	if got, repeatErr := w.CompleteAsyncActivity(t.Context(), request); repeatErr != nil || got != receipt {
		t.Fatalf("failed workflow receipt: %+v %v", got, repeatErr)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	handoffs, attempts := 0, 0
	for _, event := range events {
		if event.Type == drt.EventActivityDeferred {
			handoffs++
		}
		if event.Type == drt.EventActivityAttemptStarted {
			attempts++
		}
	}
	if handoffs != 1 || attempts != 1 {
		t.Fatalf("terminal async history: handoffs=%d attempts=%d", handoffs, attempts)
	}
}

func TestAsyncHeartbeatTimeoutFencesCallbacks(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	policy := retryOptions(1)
	policy.HeartbeatTimeout = 100 * time.Millisecond
	options.Workflows["order"] = retryWorkflow(policy)
	var handle drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		var err error
		handle, err = info.DeferCompletion(ctx)
		return nil, err
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	task, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(task.DeadlineAt) + time.Millisecond)
	if _, err = w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "late", Output: []byte("paid")}); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("expired completion: %v", err)
	}
	if _, err = w.HeartbeatAsyncActivity(t.Context(), drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "late", Sequence: 1}); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("expired heartbeat: %v", err)
	}
	options.Owner, options.Activities = "timeout-worker", nil
	coordinator := newWorker(t, s, options)
	runTask(t, coordinator, drt.TaskTimeout)
	runTask(t, w, durable.TaskWorkflow)
	outcome := lastActivityOutcome(t, s, key)
	if outcome.Timeout != drt.TimeoutHeartbeat {
		t.Fatalf("wrong timeout: %+v", outcome)
	}
}
