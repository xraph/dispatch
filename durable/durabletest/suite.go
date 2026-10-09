// Package durabletest checks the execution store's atomicity and lease contract.
package durabletest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// Run exercises a store with unique namespaces so cases can share a database.
func Run(t *testing.T, s durable.Store) {
	t.Helper()
	t.Run("child_source_guards", func(t *testing.T) { childSourceGuards(t, s) })
	t.Run("child_concurrent_parents", func(t *testing.T) { childConcurrentParents(t, s) })
	t.Run("child_namespace_reads", func(t *testing.T) { childNamespaceAndReadValidation(t, s) })
	t.Run("child_signal_start_race", func(t *testing.T) { childSignalStartRace(t, s) })
	t.Run("child_duplicate_decision", func(t *testing.T) { childDuplicateDecision(t, s) })
	t.Run("child_creation", func(t *testing.T) { childCreation(t, s) })
	t.Run("child_atomic_conflict", func(t *testing.T) { childAtomicConflict(t, s) })
	t.Run("child_concurrent_identity", func(t *testing.T) { childConcurrentIdentity(t, s) })
	t.Run("cancellation_acceptance", func(t *testing.T) { cancellationAcceptance(t, s) })
	t.Run("cancellation_isolation", func(t *testing.T) { cancellationIsolation(t, s) })
	t.Run("cancellation_concurrent", func(t *testing.T) { cancellationConcurrent(t, s) })
	t.Run("cancellation_closure_race", func(t *testing.T) { cancellationClosureRace(t, s) })
	t.Run("cancellation_fence", func(t *testing.T) { cancellationFence(t, s) })
	t.Run("signal_acceptance", func(t *testing.T) { signalAcceptance(t, s) })
	t.Run("signal_ordering", func(t *testing.T) { signalOrdering(t, s) })
	t.Run("signal_isolation_and_limits", func(t *testing.T) { signalIsolationAndLimits(t, s) })
	t.Run("signal_with_start", func(t *testing.T) { signalWithStart(t, s) })
	t.Run("signal_rejections", func(t *testing.T) { signalRejections(t, s) })
	t.Run("signal_concurrent", func(t *testing.T) { signalConcurrent(t, s) })
	t.Run("signal_start_race", func(t *testing.T) { signalStartRace(t, s) })
	t.Run("signal_conflict_race", func(t *testing.T) { signalConflictRace(t, s) })
	t.Run("signal_closure_race", func(t *testing.T) { signalClosureRace(t, s) })
	t.Run("intent_receipts", func(t *testing.T) { intentReceipts(t, s) })
	t.Run("intent_isolation", func(t *testing.T) { intentIsolation(t, s) })
	t.Run("intent_async_retry", func(t *testing.T) { intentAsyncRetry(t, s) })
	t.Run("intent_async_timeout", func(t *testing.T) { intentAsyncTimeout(t, s) })
	t.Run("intent_rejection", func(t *testing.T) { intentRejection(t, s) })
	t.Run("intent_concurrent", func(t *testing.T) { intentConcurrent(t, s) })
	t.Run("async_handoff", func(t *testing.T) { asyncHandoff(t, s) })
	t.Run("async_heartbeat_retry", func(t *testing.T) { asyncHeartbeatRetry(t, s) })
	t.Run("async_timeout", func(t *testing.T) { asyncTimeout(t, s) })
	t.Run("async_concurrent_completion", func(t *testing.T) { asyncConcurrentCompletion(t, s) })
	t.Run("async_handoff_race", func(t *testing.T) { asyncHandoffRace(t, s) })
	t.Run("heartbeat_progress", func(t *testing.T) { heartbeatProgress(t, s) })
	t.Run("heartbeat_deadlines", func(t *testing.T) { heartbeatDeadlines(t, s) })
	t.Run("heartbeat_ordering", func(t *testing.T) { heartbeatOrdering(t, s) })
	t.Run("heartbeat_configuration", func(t *testing.T) { heartbeatConfiguration(t, s) })
	t.Run("timeout_grants", func(t *testing.T) { timeoutGrants(t, s) })
	t.Run("deadline_limits", func(t *testing.T) { deadlineLimits(t, s) })
	t.Run("retained_deadline_renewal", func(t *testing.T) { retainedDeadlineRenewal(t, s) })
	t.Run("task_control", func(t *testing.T) { taskControl(t, s) })
	t.Run("task_deadline", func(t *testing.T) { taskDeadline(t, s) })
	t.Run("task_conditions", func(t *testing.T) { taskConditions(t, s) })
	t.Run("task_control_validation", func(t *testing.T) { taskControlValidation(t, s) })
	t.Run("transition_and_receipts", func(t *testing.T) { transitions(t, s) })
	t.Run("request_conflicts", func(t *testing.T) { requestConflicts(t, s) })
	t.Run("atomic_rejection", func(t *testing.T) { atomicRejection(t, s) })
	t.Run("concurrent_claim", func(t *testing.T) { concurrentClaim(t, s) })
	t.Run("concurrent_completion", func(t *testing.T) { concurrentCompletion(t, s) })
	t.Run("expiry_and_reclaim", func(t *testing.T) { expiry(t, s) })
	t.Run("build_isolation", func(t *testing.T) { buildIsolation(t, s) })
	t.Run("namespace_isolation", func(t *testing.T) { isolation(t, s) })
	t.Run("active_workflow_identity", func(t *testing.T) { activeIdentity(t, s) })
	t.Run("durable_deadline", func(t *testing.T) { deadline(t, s) })
	t.Run("payload_isolation", func(t *testing.T) { payloadIsolation(t, s) })
	t.Run("validation", func(t *testing.T) { validation(t, s) })
}

func start(t *testing.T, s durable.Store) durable.StartRequest {
	t.Helper()
	r := durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "run-1"},
		RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders", Input: []byte("input")}
	receipt, err := s.StartExecution(t.Context(), r)
	if err != nil || receipt.Revision != 1 || receipt.FirstSequence != 1 || receipt.LastSequence != 1 {
		t.Fatalf("start: %+v, %v", receipt, err)
	}
	return r
}

func claim(t *testing.T, s durable.Store, r durable.StartRequest, ttl time.Duration) *durable.Task {
	t.Helper()
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: ttl})
	if err != nil || task == nil {
		t.Fatalf("claim: %+v, %v", task, err)
	}
	return task
}

func completion(r durable.StartRequest, task *durable.Task) durable.CommitRequest {
	return durable.CommitRequest{Key: r.Key, RequestID: "commit", ExpectedRevision: 1, Token: task.Token(),
		Events: []durable.EventInput{{Type: "workflow.progress", Payload: []byte("result")}}}
}

func transitions(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	req := completion(r, task)
	req.Tasks = []durable.TaskSpec{{ID: "next", Kind: durable.TaskWorkflow, Queue: r.Queue}}
	first, err := s.CommitTransition(t.Context(), req)
	if err != nil || first.Revision != 2 || first.FirstSequence != 2 || first.LastSequence != 2 {
		t.Fatalf("commit: %+v, %v", first, err)
	}
	next := claim(t, s, r, time.Minute)
	req2 := completion(r, next)
	req2.RequestID, req2.ExpectedRevision, req2.State = "finish", 2, durable.StateCompleted
	req2.Output = []byte("done")
	if _, err = s.CommitTransition(t.Context(), req2); err != nil {
		t.Fatal(err)
	}
	retry, err := s.CommitTransition(t.Context(), req)
	if err != nil || retry != first {
		t.Fatalf("lost-response retry after closure: %+v != %+v, %v", retry, first, err)
	}
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || execution.Revision != 3 || execution.State != durable.StateCompleted || string(execution.Output) != "done" {
		t.Fatalf("execution: %+v, %v", execution, err)
	}
	history, err := s.ReadHistory(t.Context(), r.Key, 1, 1)
	if err != nil || len(history) != 1 || history[0].Sequence != 2 || string(history[0].Payload) != "result" {
		t.Fatalf("history page: %+v, %v", history, err)
	}
	if _, err = s.StartExecution(t.Context(), r); err != nil {
		t.Fatalf("start retry after closure: %v", err)
	}
}

func requestConflicts(t *testing.T, s durable.Store) {
	r := start(t, s)
	changed := r
	changed.Input = []byte("different")
	if _, err := s.StartExecution(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed start: %v", err)
	}
	req := completion(r, claim(t, s, r, time.Minute))
	if _, err := s.CommitTransition(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	req.Events[0].Payload = []byte("changed")
	if _, err := s.CommitTransition(t.Context(), req); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed commit: %v", err)
	}
}

func atomicRejection(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	req := completion(r, task)
	req.Tasks = []durable.TaskSpec{{ID: "new-task", Kind: durable.TaskWorkflow, Queue: r.Queue},
		{ID: task.ID, Kind: durable.TaskWorkflow, Queue: r.Queue}}
	if _, err := s.CommitTransition(t.Context(), req); !errors.Is(err, durable.ErrExists) {
		t.Fatalf("reused task identity: %v", err)
	}
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || execution.Revision != 1 || execution.LastSequence != 1 {
		t.Fatalf("failed transaction changed execution: %+v, %v", execution, err)
	}
	history, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(history) != 1 {
		t.Fatalf("failed transaction changed history: %+v, %v", history, err)
	}
	req.Tasks = nil
	if _, err = s.CommitTransition(t.Context(), req); err != nil {
		t.Fatalf("failed transaction consumed task or receipt: %v", err)
	}
	got, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "other", LeaseDuration: time.Minute})
	if err != nil || got != nil {
		t.Fatalf("failed transaction leaked new task: %+v, %v", got, err)
	}
}

func concurrentClaim(t *testing.T, s durable.Store) {
	r := start(t, s)
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
				Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
			if err != nil {
				t.Error(err)
			} else if task != nil {
				winners.Add(1)
			}
		})
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("claim winners: %d", winners.Load())
	}
}

func concurrentCompletion(t *testing.T, s durable.Store) {
	r := start(t, s)
	req := completion(r, claim(t, s, r, time.Minute))
	var winners atomic.Int32
	var wg sync.WaitGroup
	for _, requestID := range []string{"one", "two"} {
		wg.Go(func() {
			own := req
			own.RequestID = requestID
			_, err := s.CommitTransition(t.Context(), own)
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, durable.ErrRevisionConflict) && !errors.Is(err, durable.ErrLeaseLost) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("completion winners: %d", winners.Load())
	}
	history, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(history) != 2 {
		t.Fatalf("concurrent history: %+v, %v", history, err)
	}
}

func expiry(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, 30*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if _, err := s.RenewTask(t.Context(), r.Key, task.Token(), time.Minute); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expired renewal: %v", err)
	}
	if _, err := s.CommitTransition(t.Context(), completion(r, task)); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expired completion: %v", err)
	}
	next := claim(t, s, r, time.Minute)
	if next.Epoch != task.Epoch+1 || next.Attempt != task.Attempt+1 || next.ID != task.ID {
		t.Fatalf("reclaim: %+v after %+v", next, task)
	}
	if _, err := s.CommitTransition(t.Context(), completion(r, task)); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("same owner old epoch: %v", err)
	}
	if until, err := s.RenewTask(t.Context(), r.Key, next.Token(), 2*time.Minute); err != nil || !until.After(next.LeaseUntil) {
		t.Fatalf("renew: %v, %v", until, err)
	}
	if _, err := s.CommitTransition(t.Context(), completion(r, next)); err != nil {
		t.Fatalf("new epoch: %v", err)
	}
}

func isolation(t *testing.T, s durable.Store) {
	r := start(t, s)
	other := r.Key
	other.Namespace += "/other"
	if _, err := s.GetExecution(t.Context(), other); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("cross-namespace get: %v", err)
	}
	if _, err := s.ReadHistory(t.Context(), other, 0, 100); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("cross-namespace history: %v", err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: other.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task != nil {
		t.Fatalf("cross-namespace claim: %+v, %v", task, err)
	}
	req := completion(r, claim(t, s, r, time.Minute))
	req.Key = other
	if _, err = s.CommitTransition(t.Context(), req); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("cross-namespace commit: %v", err)
	}
}

func activeIdentity(t *testing.T, s durable.Store) {
	r := start(t, s)
	other := r
	other.RunID, other.RequestID = "run-2", "start-2"
	if _, err := s.StartExecution(t.Context(), other); !errors.Is(err, durable.ErrExists) {
		t.Fatalf("two open runs: %v", err)
	}
	req := completion(r, claim(t, s, r, time.Minute))
	req.State = durable.StateCompleted
	if _, err := s.CommitTransition(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartExecution(t.Context(), other); err != nil {
		t.Fatalf("new run after closure: %v", err)
	}
}

func deadline(t *testing.T, s durable.Store) {
	r := start(t, s)
	req := completion(r, claim(t, s, r, time.Minute))
	due := time.Now().UTC().Add(200 * time.Millisecond)
	req.Tasks = []durable.TaskSpec{{ID: "timer", Kind: durable.TaskTimer, Queue: "timers", AvailableAt: due},
		{ID: "finish", Kind: durable.TaskWorkflow, Queue: r.Queue}}
	if _, err := s.CommitTransition(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	timerClaim := durable.ClaimRequest{Namespace: r.Namespace, Queue: "timers", Kind: durable.TaskTimer,
		Owner: "timer-worker", LeaseDuration: time.Minute}
	if task, err := s.ClaimTask(t.Context(), timerClaim); err != nil || task != nil {
		t.Fatalf("early timer: %+v, %v", task, err)
	}
	time.Sleep(time.Until(due) + 30*time.Millisecond)
	timer, err := s.ClaimTask(t.Context(), timerClaim)
	if err != nil || timer == nil || !timer.AvailableAt.Equal(due.Truncate(time.Microsecond)) {
		t.Fatalf("due timer: %+v, %v", timer, err)
	}
	finish := completion(r, claim(t, s, r, time.Minute))
	finish.RequestID, finish.ExpectedRevision, finish.State = "finish", 2, durable.StateCompleted
	if _, err = s.CommitTransition(t.Context(), finish); err != nil {
		t.Fatal(err)
	}
	late := completion(r, timer)
	late.RequestID, late.ExpectedRevision = "late-timer", 3
	if _, err = s.CommitTransition(t.Context(), late); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("closed run accepted timer: %v", err)
	}
}

func payloadIsolation(t *testing.T, s durable.Store) {
	r := start(t, s)
	r.Input[0] = 'X'
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || string(execution.Input) != "input" {
		t.Fatalf("stored input aliased: %+v, %v", execution, err)
	}
	execution.Input[0] = 'Y'
	execution, err = s.GetExecution(t.Context(), r.Key)
	if err != nil || string(execution.Input) != "input" {
		t.Fatalf("read input aliased: %+v, %v", execution, err)
	}
	req := completion(r, claim(t, s, r, time.Minute))
	if _, err = s.CommitTransition(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	req.Events[0].Payload[0] = 'Z'
	history, err := s.ReadHistory(t.Context(), r.Key, 1, 10)
	if err != nil || len(history) != 1 || string(history[0].Payload) != "result" {
		t.Fatalf("stored history aliased: %+v, %v", history, err)
	}
	history[0].Payload[0] = 'Z'
	history, err = s.ReadHistory(t.Context(), r.Key, 1, 10)
	if err != nil || string(history[0].Payload) != "result" {
		t.Fatalf("read history aliased: %+v, %v", history, err)
	}
}

func validation(t *testing.T, s durable.Store) {
	if _, err := s.StartExecution(t.Context(), durable.StartRequest{}); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("empty start: %v", err)
	}
	r := start(t, s)
	req := completion(r, claim(t, s, r, time.Minute))
	req.ExpectedRevision = 99
	if _, err := s.CommitTransition(t.Context(), req); !errors.Is(err, durable.ErrRevisionConflict) {
		t.Fatalf("revision: %v", err)
	}
	req.ExpectedRevision = 1
	req.State = durable.StateCompleted
	req.Tasks = []durable.TaskSpec{{ID: "late", Kind: durable.TaskWorkflow, Queue: r.Queue}}
	if _, err := s.CommitTransition(t.Context(), req); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("terminal scheduling: %v", err)
	}
	if _, err := s.ReadHistory(t.Context(), r.Key, 0, 0); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("unbounded history: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.GetExecution(cancelled, r.Key); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: %v", err)
	}
}

func buildIsolation(t *testing.T, s durable.Store) {
	r := start(t, s)
	request := durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute, BuildID: "v2"}
	if task, err := s.ClaimTask(t.Context(), request); err != nil || task != nil {
		t.Fatalf("wrong build claimed task: %+v, %v", task, err)
	}
	request.BuildID = r.BuildID
	if task, err := s.ClaimTask(t.Context(), request); err != nil || task == nil {
		t.Fatalf("matching build missed task: %+v, %v", task, err)
	}
}
