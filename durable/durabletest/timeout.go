package durabletest

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func timeoutGrants(t *testing.T, s durable.Store) {
	r := start(t, s)
	workflow := claim(t, s, r, time.Second)
	request := completion(r, workflow)
	request.Tasks = []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: "external", DeadlineAfter: 80 * time.Millisecond}, {ID: "timer", Kind: durable.TaskTimer, Queue: "timers", AvailableAfter: time.Microsecond, DeadlineAfter: time.Microsecond}}
	if _, err := s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	activity, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "external", BuildID: r.BuildID, Kind: durable.TaskActivity, Owner: "effect", LeaseDuration: time.Second})
	if err != nil || activity == nil {
		t.Fatalf("activity claim: %+v %v", activity, err)
	}
	poll := durable.TimeoutClaimRequest{Namespace: r.Namespace, BuildID: r.BuildID, Owner: "coordinator", LeaseDuration: 80 * time.Millisecond}
	if expired, pollErr := s.ClaimTimeoutTask(t.Context(), poll); pollErr != nil || expired != nil {
		t.Fatalf("early timeout claim: %+v %v", expired, pollErr)
	}
	time.Sleep(time.Until(activity.DeadlineAt) + 10*time.Millisecond)
	for _, wrong := range []durable.TimeoutClaimRequest{{Namespace: r.Namespace, BuildID: "other", Owner: poll.Owner, LeaseDuration: poll.LeaseDuration}, {Namespace: r.Namespace + "-other", BuildID: r.BuildID, Owner: poll.Owner, LeaseDuration: poll.LeaseDuration}} {
		if expired, pollErr := s.ClaimTimeoutTask(t.Context(), wrong); pollErr != nil || expired != nil {
			t.Fatalf("timeout scope leak: %+v %v", expired, pollErr)
		}
	}
	if _, err = s.RenewTask(t.Context(), r.Key, activity.Token(), time.Second); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("normal renewal after expiry: %v", err)
	}
	var winners atomic.Int64
	var group sync.WaitGroup
	grants := make(chan *durable.Task, 8)
	for range 8 {
		group.Go(func() {
			task, claimErr := s.ClaimTimeoutTask(t.Context(), poll)
			if claimErr != nil {
				t.Error(claimErr)
			}
			if task != nil {
				winners.Add(1)
				grants <- task
			}
		})
	}
	group.Wait()
	close(grants)
	if winners.Load() != 1 {
		t.Fatalf("concurrent timeout grants: %d", winners.Load())
	}
	timeout := <-grants
	if timeout.ID != activity.ID || timeout.Kind != durable.TaskActivity || timeout.LeaseKind != durable.LeaseTimeout || timeout.Epoch <= activity.Epoch || timeout.Attempt != activity.Attempt {
		t.Fatalf("timeout identity: %+v", timeout)
	}
	for _, token := range []durable.TaskToken{activity.Token(), {TaskID: activity.ID, Owner: activity.Owner, Epoch: activity.Epoch, LeaseKind: durable.LeaseTimeout}, {TaskID: timeout.ID, Owner: timeout.Owner, Epoch: timeout.Epoch}} {
		if _, err = s.RenewTask(t.Context(), r.Key, token, time.Second); !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("stale or forged grant renewed: %+v %v", token, err)
		}
	}
	until, err := s.RenewTask(t.Context(), r.Key, timeout.Token(), 80*time.Millisecond)
	if err != nil || !until.After(timeout.DeadlineAt) {
		t.Fatalf("timeout grant capped by expired deadline: %v %v", until, err)
	}
	invalid := durable.CommitRequest{Key: r.Key, RequestID: "retain-expired", ExpectedRevision: 2, Token: timeout.Token(), Events: []durable.EventInput{{Type: "timeout.handled"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep}}
	if _, err = s.CommitTransition(t.Context(), invalid); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("timeout became retained execution: %v", err)
	}
	time.Sleep(time.Until(until) + 10*time.Millisecond)
	next, err := s.ClaimTimeoutTask(t.Context(), poll)
	if err != nil || next == nil || next.Epoch <= timeout.Epoch {
		t.Fatalf("timeout reclaim: %+v %v", next, err)
	}
	if _, err = s.RenewTask(t.Context(), r.Key, timeout.Token(), time.Second); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("reclaimed timeout renewed: %v", err)
	}
	zero := time.Duration(0)
	retry := durable.CommitRequest{Key: r.Key, RequestID: "retry-timeout", ExpectedRevision: 2, Token: next.Token(), Events: []durable.EventInput{{Type: "activity.timed_out"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskRetry, RetryAfter: 10 * time.Millisecond, DeadlineAfter: &zero}}
	receipt, err := s.CommitTransition(t.Context(), retry)
	if err != nil {
		t.Fatal(err)
	}
	if again, retryErr := s.CommitTransition(t.Context(), retry); retryErr != nil || again != receipt {
		t.Fatalf("timeout receipt retry: %+v %v", again, retryErr)
	}
	released, err := s.GetTask(t.Context(), r.Key, activity.ID)
	if err != nil || released.Owner != "" || released.LeaseKind != "" || !released.DeadlineAt.IsZero() {
		t.Fatalf("timeout retry not released to execution: %+v %v", released, err)
	}
	time.Sleep(time.Until(released.AvailableAt) + 5*time.Millisecond)
	resumed, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: activity.Queue, BuildID: r.BuildID, Kind: durable.TaskActivity, Owner: "replacement", LeaseDuration: time.Second})
	if err != nil || resumed == nil || resumed.LeaseKind != "" || resumed.Attempt != activity.Attempt+1 {
		t.Fatalf("normal retry claim: %+v %v", resumed, err)
	}
}

func deadlineLimits(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Second)
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	limit := execution.CreatedAt.Add(100 * time.Millisecond)
	request := completion(r, task)
	request.Tasks = []durable.TaskSpec{{ID: "future", Kind: durable.TaskActivity, Queue: "absent", AvailableAfter: time.Hour, DeadlineAfter: time.Hour, DeadlineLimit: &limit}}
	if _, err = s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	future, err := s.GetTask(t.Context(), r.Key, "future")
	if err != nil || !future.DeadlineAt.Equal(limit) || !future.AvailableAt.After(limit) || future.DeadlineLimit != nil {
		t.Fatalf("deadline cap: %+v %v", future, err)
	}
	time.Sleep(time.Until(limit) + 10*time.Millisecond)
	expired, err := s.ClaimTimeoutTask(t.Context(), durable.TimeoutClaimRequest{Namespace: r.Namespace, BuildID: r.BuildID, Owner: "coordinator", LeaseDuration: time.Second})
	if err != nil || expired == nil || expired.ID != "future" {
		t.Fatalf("backoff masked overall deadline: %+v %v", expired, err)
	}
}

func retainedDeadlineRenewal(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Second)
	short := 100 * time.Millisecond
	keep := completion(r, task)
	keep.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &short}
	if _, err := s.CommitTransition(t.Context(), keep); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	longer := time.Hour
	limit := before.DeadlineAt.Add(150 * time.Millisecond)
	keep.RequestID = "start-attempt"
	keep.ExpectedRevision = 2
	keep.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &longer, DeadlineLimit: &limit, LeaseDuration: time.Second}
	if _, err = s.CommitTransition(t.Context(), keep); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || !after.DeadlineAt.Equal(limit) || !after.LeaseUntil.Equal(limit) || !after.LeaseUntil.After(before.LeaseUntil) {
		t.Fatalf("deadline replacement did not renew atomically: before=%+v after=%+v %v", before, after, err)
	}
	keep.RequestID, keep.ExpectedRevision = "renew-shorter", 3
	keep.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep, LeaseDuration: time.Microsecond}
	if _, err = s.CommitTransition(t.Context(), keep); err != nil {
		t.Fatal(err)
	}
	renewed, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || !renewed.LeaseUntil.Equal(after.LeaseUntil) {
		t.Fatalf("retained renewal shortened the grant: before=%+v after=%+v %v", after, renewed, err)
	}
	keep.RequestID = "invalid-retry-renewal"
	keep.ExpectedRevision = 4
	keep.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskRetry, RetryAfter: time.Second, LeaseDuration: time.Second}
	if _, err = s.CommitTransition(t.Context(), keep); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("released retry retained grant: %v", err)
	}
}
