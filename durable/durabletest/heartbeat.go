package durabletest

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func heartbeatActivity(t *testing.T, s durable.Store, timeout, hard time.Duration) (durable.StartRequest, durable.Task) {
	t.Helper()
	r := start(t, s)
	workflow := claim(t, s, r, time.Second)
	schedule := completion(r, workflow)
	schedule.Tasks = []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: "external"}}
	if _, err := s.CommitTransition(t.Context(), schedule); err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "external", Kind: durable.TaskActivity, Owner: "worker", LeaseDuration: time.Second})
	if err != nil || task == nil {
		t.Fatalf("claim activity: %+v %v", task, err)
	}
	request := durable.CommitRequest{Key: r.Key, RequestID: "attempt", ExpectedRevision: 2, Token: task.Token(),
		Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep,
			DeadlineAfter: &hard, LeaseDuration: time.Second, Heartbeat: &durable.HeartbeatConfig{Timeout: timeout}}}
	if _, err = s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	return r, current
}

func heartbeatRequest(r durable.StartRequest, task durable.Task, sequence int64) durable.HeartbeatRequest {
	return durable.HeartbeatRequest{Key: r.Key, RequestID: fmt.Sprintf("heartbeat:%d", sequence), Token: task.Token(),
		Sequence: sequence, Progress: []byte(fmt.Sprintf("offset:%d", sequence)), LeaseDuration: time.Second}
}

func heartbeatProgress(t *testing.T, s durable.Store) {
	r, task := heartbeatActivity(t, s, 0, 0)
	if task.HeartbeatEpoch != task.Epoch || task.HeartbeatAt.IsZero() || task.HeartbeatSequence != 0 {
		t.Fatalf("missing initial heartbeat state: %+v", task)
	}
	request := heartbeatRequest(r, task, 1)
	first, err := s.RecordHeartbeat(t.Context(), request)
	if err != nil || first.Revision != 3 || first.FirstSequence != 0 || first.LastSequence != 0 {
		t.Fatalf("heartbeat receipt: %+v %v", first, err)
	}
	request.Progress[0] = 'X'
	current, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || current.Version != task.Version+1 || current.HeartbeatSequence != 1 || current.HeartbeatAt.Before(task.HeartbeatAt) || string(current.Progress) != "offset:1" || !current.DeadlineAt.IsZero() {
		t.Fatalf("heartbeat projection: %+v %v", current, err)
	}
	current.Progress[0] = 'X'
	current, err = s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || string(current.Progress) != "offset:1" {
		t.Fatalf("heartbeat progress aliases caller: %+v %v", current, err)
	}
	if _, err = s.RecordHeartbeat(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed heartbeat receipt accepted: %v", err)
	}
	request = heartbeatRequest(r, task, 1)
	second := heartbeatRequest(r, task, 2)
	if _, err = s.RecordHeartbeat(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if receipt, replayErr := s.RecordHeartbeat(t.Context(), request); replayErr != nil || receipt != first {
		t.Fatalf("old heartbeat receipt changed: %+v %v", receipt, replayErr)
	}
	current, err = s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || current.HeartbeatSequence != 2 || string(current.Progress) != "offset:2" {
		t.Fatalf("old receipt overwrote progress: %+v %v", current, err)
	}
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || execution.Revision != 3 || execution.LastSequence != 3 {
		t.Fatalf("heartbeat changed execution history: %+v %v", execution, err)
	}
	guarded := durable.CommitRequest{Key: r.Key, RequestID: "stale-observation", ExpectedRevision: 3, Token: task.Token(),
		Events: []durable.EventInput{{Type: "observed"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep},
		Conditions: []durable.TaskCondition{{TaskID: task.ID, Version: task.Version}}}
	if _, err = s.CommitTransition(t.Context(), guarded); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("heartbeat did not invalidate task observation: %v", err)
	}
	release := durable.CommitRequest{Key: r.Key, RequestID: "retry", ExpectedRevision: 3, Token: task.Token(),
		Events: []durable.EventInput{{Type: "attempt.failed"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskRetry, RetryAfter: time.Microsecond}}
	if _, err = s.CommitTransition(t.Context(), release); err != nil {
		t.Fatal(err)
	}
	replacement, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "external", Kind: durable.TaskActivity, Owner: "replacement", LeaseDuration: time.Second})
	if err != nil || replacement == nil || replacement.HeartbeatEpoch != 0 || replacement.HeartbeatSequence != 0 || !replacement.HeartbeatAt.IsZero() || string(replacement.Progress) != "offset:2" {
		t.Fatalf("retry heartbeat reset/progress: %+v %v", replacement, err)
	}
	if receipt, replayErr := s.RecordHeartbeat(t.Context(), request); replayErr != nil || receipt != first {
		t.Fatalf("receipt lost on reclaim: %+v %v", receipt, replayErr)
	}
	if _, err = s.RecordHeartbeat(t.Context(), heartbeatRequest(r, *replacement, 3)); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("uninitialized replacement heartbeat accepted: %v", err)
	}
	hard := time.Second
	restart := durable.CommitRequest{Key: r.Key, RequestID: "restart", ExpectedRevision: 4, Token: replacement.Token(),
		Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep,
			DeadlineAfter: &hard, Heartbeat: &durable.HeartbeatConfig{Timeout: 200 * time.Millisecond}}}
	if _, err = s.CommitTransition(t.Context(), restart); err != nil {
		t.Fatal(err)
	}
	resumed := heartbeatRequest(r, *replacement, 1)
	resumed.RequestID, resumed.Progress = "resumed-heartbeat", []byte("offset:3")
	if _, err = s.RecordHeartbeat(t.Context(), resumed); err != nil {
		t.Fatal(err)
	}
	current, err = s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || current.HeartbeatEpoch != replacement.Epoch || current.HeartbeatSequence != 1 || string(current.Progress) != "offset:3" {
		t.Fatalf("replacement heartbeat not initialized: %+v %v", current, err)
	}
	finish := durable.CommitRequest{Key: r.Key, RequestID: "finish", ExpectedRevision: 5, Token: replacement.Token(),
		State: durable.StateCompleted, Events: []durable.EventInput{{Type: "completed"}}}
	if _, err = s.CommitTransition(t.Context(), finish); err != nil {
		t.Fatal(err)
	}
	if receipt, replayErr := s.RecordHeartbeat(t.Context(), request); replayErr != nil || receipt != first {
		t.Fatalf("receipt lost on closure: %+v %v", receipt, replayErr)
	}
}

func heartbeatDeadlines(t *testing.T, s durable.Store) {
	r, task := heartbeatActivity(t, s, 200*time.Millisecond, 300*time.Millisecond)
	if task.HeartbeatLimit.IsZero() || !task.DeadlineAt.Equal(task.HeartbeatAt.Add(200*time.Millisecond)) {
		t.Fatalf("initial heartbeat deadline: %+v", task)
	}
	if until, err := s.RenewTask(t.Context(), r.Key, task.Token(), time.Minute); err != nil || !until.Equal(task.DeadlineAt) {
		t.Fatalf("renewal extended heartbeat: %v %v", until, err)
	}
	time.Sleep(120 * time.Millisecond)
	request := heartbeatRequest(r, task, 1)
	if _, err := s.RecordHeartbeat(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || !current.DeadlineAt.Equal(task.HeartbeatLimit) || !current.LeaseUntil.Equal(task.HeartbeatLimit) {
		t.Fatalf("heartbeat exceeded hard deadline: %+v %v", current, err)
	}
	time.Sleep(time.Until(current.DeadlineAt) + time.Millisecond)
	if _, err = s.RecordHeartbeat(t.Context(), heartbeatRequest(r, task, 2)); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("heartbeat revived expired deadline: %v", err)
	}
	after, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || after.Version != current.Version || after.HeartbeatSequence != current.HeartbeatSequence {
		t.Fatalf("expired heartbeat mutated task: %+v %v", after, err)
	}
	grant, err := s.ClaimTimeoutTask(t.Context(), durable.TimeoutClaimRequest{Namespace: r.Namespace, Owner: "timeout", LeaseDuration: time.Second})
	if err != nil || grant == nil {
		t.Fatalf("heartbeat deadline not claimable: %+v %v", grant, err)
	}
	if _, err = s.RecordHeartbeat(t.Context(), heartbeatRequest(r, *grant, 2)); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("timeout grant could heartbeat: %v", err)
	}
}

func heartbeatOrdering(t *testing.T, s durable.Store) {
	r, task := heartbeatActivity(t, s, 0, 0)
	if _, err := s.RecordHeartbeat(t.Context(), heartbeatRequest(r, task, 2)); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("skipped heartbeat accepted: %v", err)
	}
	var winners atomic.Int64
	var group sync.WaitGroup
	for i := range 8 {
		group.Go(func() {
			request := heartbeatRequest(r, task, 1)
			request.RequestID = fmt.Sprintf("concurrent:%d", i)
			_, err := s.RecordHeartbeat(t.Context(), request)
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, durable.ErrTaskConflict) {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if winners.Load() != 1 {
		t.Fatalf("concurrent heartbeat winners: %d", winners.Load())
	}
	request := heartbeatRequest(r, task, 2)
	request.Namespace += "-other"
	if _, err := s.RecordHeartbeat(t.Context(), request); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("heartbeat namespace leak: %v", err)
	}
	request = heartbeatRequest(r, task, 2)
	request.RequestID = "start"
	if _, err := s.RecordHeartbeat(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("heartbeat overwrote another operation's receipt: %v", err)
	}
	request = heartbeatRequest(r, task, 2)
	request.Token.Epoch++
	if _, err := s.RecordHeartbeat(t.Context(), request); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("forged heartbeat grant accepted: %v", err)
	}
	request = heartbeatRequest(r, task, 2)
	request.Progress = make([]byte, (1<<20)+1)
	if _, err := s.RecordHeartbeat(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("oversized heartbeat accepted: %v", err)
	}
}

func heartbeatConfiguration(t *testing.T, s durable.Store) {
	r, task := heartbeatActivity(t, s, 0, 0)
	deadline := time.Hour
	progress := []byte("unsequenced")
	for i, update := range []*durable.TaskUpdate{
		{Action: durable.TaskKeep, DeadlineAfter: &deadline},
		{Action: durable.TaskKeep, DeadlineAfter: &deadline, Heartbeat: &durable.HeartbeatConfig{}},
		{Action: durable.TaskKeep, Progress: &progress},
	} {
		request := durable.CommitRequest{Key: r.Key, RequestID: fmt.Sprintf("change:%d", i), ExpectedRevision: 3, Token: task.Token(),
			Events: []durable.EventInput{{Type: "change"}}, TaskUpdate: update}
		if _, err := s.CommitTransition(t.Context(), request); !errors.Is(err, durable.ErrTaskConflict) {
			t.Fatalf("heartbeat configuration changed in place: %v", err)
		}
	}
	request := heartbeatRequest(r, task, 1)
	if _, err := s.RecordHeartbeat(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}
