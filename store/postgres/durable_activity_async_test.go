//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/postgres"
)

type lostRuntimeHandoffStore struct {
	durable.Store
	calls     atomic.Int64
	recovered chan struct{}
}

func (s *lostRuntimeHandoffStore) CommitTransition(ctx context.Context, request durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, request)
	if err != nil || len(request.Events) == 0 || request.Events[0].Type != drt.EventActivityDeferred {
		return receipt, err
	}
	count := s.calls.Add(1)
	if count <= 3 {
		return durable.Receipt{}, errAsyncAcknowledgementLost
	}
	if count == 4 {
		close(s.recovered)
	}
	return receipt, nil
}

// The first lookup succeeds. After a real completion commits, both the response
// and receipt lookups fail until the caller replaces this client.
type lostRuntimeCompletionStore struct {
	durable.Store
	accepted  durable.Receipt
	committed bool
}

func (s *lostRuntimeCompletionStore) CommitTransition(ctx context.Context, request durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, request)
	if err != nil || request.Token.LeaseKind != durable.LeaseAsync {
		return receipt, err
	}
	s.accepted, s.committed = receipt, true
	return durable.Receipt{}, errAsyncAcknowledgementLost
}

func (s *lostRuntimeCompletionStore) LookupReceipt(ctx context.Context, request durable.ReceiptRequest) (durable.Receipt, bool, error) {
	if s.committed {
		return durable.Receipt{}, false, errAsyncAcknowledgementLost
	}
	return s.Store.LookupReceipt(ctx, request)
}

func reopenAsyncStore(t *testing.T, s *postgres.Store, dsn string) *postgres.Store {
	t.Helper()
	if err := s.DB().Close(); err != nil {
		t.Fatal(err)
	}
	drv := pgdriver.New()
	if err := drv.Open(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return postgres.New(db)
}

func TestDurableActivityAsyncRecovery(t *testing.T) {
	for _, mode := range []string{"success", "failure", "heartbeat_timeout"} {
		t.Run(mode, func(t *testing.T) { testDurableActivityAsyncRecovery(t, mode) })
	}
}

func testDurableActivityAsyncRecovery(t *testing.T, mode string) {
	t.Helper()
	s, dsn := setupTestStoreConnection(t)
	var backend durable.Store = s
	var handoffRecovery *lostRuntimeHandoffStore
	if mode == "success" {
		handoffRecovery = &lostRuntimeHandoffStore{Store: s, recovered: make(chan struct{})}
		backend = handoffRecovery
	}
	policy := drt.ActivityOptions{StartToCloseTimeout: time.Minute,
		RetryPolicy: &drt.RetryPolicy{InitialInterval: 50 * time.Millisecond, MaximumAttempts: 2}}
	if mode == "heartbeat_timeout" {
		policy.HeartbeatTimeout = time.Second
	}
	options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "async-v1", Owner: "first",
		LeaseDuration: 2 * time.Second, StoreTimeout: 500 * time.Millisecond,
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ActivityWithOptions("charge", "charge", "", nil, policy).Get()
		}}}
	var handle drt.AsyncActivityHandle
	var identity string
	calls := 0
	options.Activities = map[string]drt.ActivityFunc{"charge": func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		calls++
		if info.Attempt == 1 {
			identity = info.IdempotencyKey()
			var err error
			handle, err = info.DeferCompletion(ctx)
			if handoffRecovery != nil {
				if !errors.Is(err, errAsyncAcknowledgementLost) {
					return nil, errors.New("test did not lose handoff acknowledgements")
				}
				select {
				case <-handoffRecovery.recovered:
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				case <-time.After(5 * time.Second):
					return nil, errors.New("handoff reconciliation did not run")
				}
				handle, err = info.DeferCompletion(ctx)
			}
			return nil, err
		}
		if info.Attempt != 2 || info.IdempotencyKey() != identity || string(info.HeartbeatDetails()) != "external" {
			return nil, errors.New("async recovery changed progress or identity")
		}
		return []byte("paid"), nil
	}}
	worker, err := drt.NewWorker(backend, options)
	if err != nil {
		t.Fatal(err)
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
	if _, err = worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity} {
		if worked, workErr := worker.RunOnce(t.Context(), kind); workErr != nil || !worked {
			t.Fatalf("run %s: %t %v", kind, worked, workErr)
		}
	}
	if handoffRecovery != nil && handoffRecovery.calls.Load() != 4 {
		t.Fatalf("handoff reconciliation calls: %d", handoffRecovery.calls.Load())
	}
	serialized, err := json.Marshal(handle)
	if err != nil {
		t.Fatal(err)
	}
	callbackOptions := drt.Options{Namespace: options.Namespace, Queue: "callbacks", BuildID: options.BuildID, Owner: "callback",
		LeaseDuration: options.LeaseDuration, StoreTimeout: options.StoreTimeout}
	lossy, err := drt.NewWorker(lostAsyncResponseStore{Store: s}, callbackOptions)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "progress", Sequence: 1, Details: []byte("external")}
	if _, err = lossy.HeartbeatAsyncActivity(t.Context(), heartbeat); !errors.Is(err, errAsyncAcknowledgementLost) {
		t.Fatalf("test did not lose heartbeat response: %v", err)
	}
	s = reopenAsyncStore(t, s, dsn)
	if err = json.Unmarshal(serialized, &handle); err != nil {
		t.Fatal(err)
	}
	callback, err := drt.NewWorker(s, callbackOptions)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat.Handle = handle
	heartbeatReceipt, err := callback.HeartbeatAsyncActivity(t.Context(), heartbeat)
	if err != nil {
		t.Fatalf("reopened heartbeat receipt: %v", err)
	}
	task, err := s.GetTask(t.Context(), key, handle.Token.TaskID)
	if err != nil || task.LeaseKind != durable.LeaseAsync || string(task.Progress) != "external" || task.HeartbeatSequence != 1 {
		t.Fatalf("reopened async grant: %+v %v", task, err)
	}
	options.Owner = "replacement"
	worker, err = drt.NewWorker(s, options)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "success" {
		lost := &lostRuntimeCompletionStore{Store: s}
		lossy, err = drt.NewWorker(lost, callbackOptions)
		if err != nil {
			t.Fatal(err)
		}
		request := drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}
		if _, err = lossy.CompleteAsyncActivity(t.Context(), request); !errors.Is(err, errAsyncAcknowledgementLost) || !lost.committed {
			t.Fatalf("test did not lose committed result: accepted=%t %v", lost.committed, err)
		}
		if worked, workErr := worker.RunOnce(t.Context(), durable.TaskWorkflow); workErr != nil || !worked {
			t.Fatalf("close workflow: %t %v", worked, workErr)
		}
		s = reopenAsyncStore(t, s, dsn)
		callback, err = drt.NewWorker(s, callbackOptions)
		if err != nil {
			t.Fatal(err)
		}
		if receipt, replayErr := callback.CompleteAsyncActivity(t.Context(), request); replayErr != nil || receipt != lost.accepted {
			t.Fatalf("reopened completion receipt after closure: %+v %v", receipt, replayErr)
		}
	} else {
		if mode == "failure" {
			if _, err = callback.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "failed", Failure: &drt.ApplicationError{Type: "temporary", Message: "retry"}}); err != nil {
				t.Fatal(err)
			}
		} else {
			waitDurableStoreTime(t, s, task.DeadlineAt)
			if worked, workErr := callback.RunOnce(t.Context(), drt.TaskTimeout); workErr != nil || !worked {
				t.Fatalf("timeout without handlers: %t %v", worked, workErr)
			}
		}
		task, err = s.GetTask(t.Context(), key, handle.Token.TaskID)
		if err != nil || task.AsyncKeyHash != "" || task.LeaseKind != "" || string(task.Progress) != "external" {
			t.Fatalf("retry did not revoke async grant or preserve progress: %+v %v", task, err)
		}
		waitDurableStoreTime(t, s, task.AvailableAt)
		for _, kind := range []durable.TaskKind{durable.TaskActivity, durable.TaskWorkflow} {
			if worked, workErr := worker.RunOnce(t.Context(), kind); workErr != nil || !worked {
				t.Fatalf("resume %s: %t %v", kind, worked, workErr)
			}
		}
	}
	if receipt, replayErr := callback.HeartbeatAsyncActivity(t.Context(), heartbeat); replayErr != nil || receipt != heartbeatReceipt {
		t.Fatalf("heartbeat receipt after closure: %+v %v", receipt, replayErr)
	}
	execution, err := s.GetExecution(t.Context(), key)
	expectedCalls := 2
	if mode == "success" {
		expectedCalls = 1
	}
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" || calls != expectedCalls {
		t.Fatalf("async recovery result: %+v calls=%d %v", execution, calls, err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	handoffs, outcomes, failures := 0, 0, 0
	for _, event := range events {
		switch event.Type {
		case drt.EventActivityDeferred:
			handoffs++
		case drt.EventActivityCompleted:
			outcomes++
		case drt.EventActivityAttemptFailed:
			failures++
			var failed drt.ActivityAttempt
			if err = json.Unmarshal(event.Payload, &failed); err != nil || failed.Heartbeat == nil || string(failed.Heartbeat.Details) != "external" || failed.Heartbeat.Sequence != 1 {
				t.Fatalf("async failure checkpoint: %+v %v", failed, err)
			}
			if mode == "heartbeat_timeout" && failed.Timeout != drt.TimeoutHeartbeat {
				t.Fatalf("wrong async timeout class: %+v", failed)
			}
		}
	}
	if handoffs != 1 || outcomes != 1 || failures != expectedCalls-1 {
		t.Fatalf("async history: handoffs=%d outcomes=%d failures=%d", handoffs, outcomes, failures)
	}
}
