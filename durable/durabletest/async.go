package durabletest

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

const asyncSecret = "0101010101010101010101010101010101010101010101010101010101010101"

func awaitActivity(t *testing.T, s durable.Store, timeout, hard time.Duration) (durable.StartRequest, durable.Task, durable.CommitRequest) {
	t.Helper()
	r, task := heartbeatActivity(t, s, timeout, hard)
	hash, err := durable.HashAsyncSecret(asyncSecret)
	if err != nil {
		t.Fatal(err)
	}
	handoff := durable.CommitRequest{Key: r.Key, RequestID: "await", ExpectedRevision: 3, Token: task.Token(), Events: []durable.EventInput{{Type: "activity.awaited"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskAwait, AsyncKeyHash: hash}}
	if _, err = s.CommitTransition(t.Context(), handoff); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || current.LeaseKind != durable.LeaseAsync || !current.LeaseUntil.Equal(current.DeadlineAt) || !current.DeadlineAt.Equal(task.DeadlineAt) || current.Epoch != task.Epoch || current.Version != task.Version+1 || current.AsyncKeyHash != hash || !current.HeartbeatAt.Equal(task.HeartbeatAt) {
		t.Fatalf("handoff: %+v %v", current, err)
	}
	return r, current, handoff
}

func asyncFinish(r durable.StartRequest, task durable.Task) durable.CommitRequest {
	return durable.CommitRequest{Key: r.Key, RequestID: "async-finish", ExpectedRevision: 4, Token: task.Token(), AsyncSecret: asyncSecret, Events: []durable.EventInput{{Type: "activity.completed"}}, State: durable.StateCompleted, Output: []byte("paid")}
}

func asyncProgress(r durable.StartRequest, task durable.Task, sequence int64) durable.HeartbeatRequest {
	request := heartbeatRequest(r, task, sequence)
	request.AsyncSecret, request.LeaseDuration = asyncSecret, 0
	return request
}

func asyncHandoff(t *testing.T, s durable.Store) {
	r, task, handoff := awaitActivity(t, s, 0, time.Second)
	encoded, err := json.Marshal(task)
	if err != nil || strings.Contains(string(encoded), asyncSecret) || strings.Contains(string(encoded), task.AsyncKeyHash) {
		t.Fatalf("task JSON leaks credential: %v", err)
	}
	if got, claimErr := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "external", Kind: durable.TaskActivity, Owner: "replacement", LeaseDuration: time.Second}); claimErr != nil || got != nil {
		t.Fatalf("async task reclaimed: %+v %v", got, claimErr)
	}
	if _, err = s.RenewTask(t.Context(), r.Key, handoff.Token, time.Second); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old worker renewed: %v", err)
	}
	if _, err = s.RenewTask(t.Context(), r.Key, task.Token(), time.Second); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("async worker renewal accepted: %v", err)
	}
	old := asyncFinish(r, task)
	old.Token, old.AsyncSecret = handoff.Token, ""
	if _, err = s.CommitTransition(t.Context(), old); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old worker completed: %v", err)
	}
	oldHeartbeat := heartbeatRequest(r, task, 1)
	oldHeartbeat.Token = handoff.Token
	if _, err = s.RecordHeartbeat(t.Context(), oldHeartbeat); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old worker heartbeat: %v", err)
	}
	finish := asyncFinish(r, task)
	for _, mode := range []string{"missing", "wrong", "epoch", "namespace"} {
		invalid := finish
		switch mode {
		case "missing":
			invalid.AsyncSecret = ""
		case "wrong":
			invalid.AsyncSecret = strings.Repeat("02", 32)
		case "epoch":
			invalid.Token.Epoch++
		case "namespace":
			invalid.Namespace += "other"
		}
		_, err = s.CommitTransition(t.Context(), invalid)
		expected := durable.ErrLeaseLost
		if mode == "missing" {
			expected = durable.ErrInvalid
		}
		if mode == "namespace" {
			expected = durable.ErrNotFound
		}
		if !errors.Is(err, expected) {
			t.Fatalf("invalid async %s: %v", mode, err)
		}
	}
	receipt, err := s.CommitTransition(t.Context(), finish)
	if err != nil {
		t.Fatal(err)
	}
	if repeated, repeatErr := s.CommitTransition(t.Context(), finish); repeatErr != nil || repeated != receipt {
		t.Fatalf("async receipt after closure: %+v %v", repeated, repeatErr)
	}
	if _, err = s.CommitTransition(t.Context(), handoff); err != nil {
		t.Fatalf("handoff receipt after closure: %v", err)
	}
	finish.AsyncSecret = strings.Repeat("02", 32)
	if _, err = s.CommitTransition(t.Context(), finish); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed proof reused receipt: %v", err)
	}
}

func asyncHeartbeatRetry(t *testing.T, s durable.Store) {
	r, task, _ := awaitActivity(t, s, 500*time.Millisecond, 600*time.Millisecond)
	time.Sleep(120 * time.Millisecond)
	progress := asyncProgress(r, task, 1)
	progress.Progress = []byte("offset:42")
	bad := progress
	bad.AsyncSecret = strings.Repeat("02", 32)
	if _, err := s.RecordHeartbeat(t.Context(), bad); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("wrong heartbeat proof: %v", err)
	}
	receipt, err := s.RecordHeartbeat(t.Context(), progress)
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || !current.LeaseUntil.Equal(current.DeadlineAt) || !current.DeadlineAt.Equal(task.HeartbeatLimit) || current.HeartbeatSequence != 1 || string(current.Progress) != "offset:42" {
		t.Fatalf("async heartbeat state: %+v %v", current, err)
	}
	guarded := asyncFinish(r, task)
	guarded.Conditions = []durable.TaskCondition{{TaskID: task.ID, Version: task.Version}}
	if _, err = s.CommitTransition(t.Context(), guarded); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("stale async progress observation: %v", err)
	}
	retry := asyncFinish(r, task)
	retry.RequestID, retry.State, retry.Output = "retry", "", nil
	delay := time.Second
	retry.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskRetry, RetryAfter: time.Microsecond, DeadlineAfter: &delay}
	if _, err = s.CommitTransition(t.Context(), retry); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RecordHeartbeat(t.Context(), asyncProgress(r, task, 2)); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("released callback heartbeat accepted: %v", err)
	}
	replacement, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "external", Kind: durable.TaskActivity, Owner: "replacement", LeaseDuration: time.Second})
	if err != nil || replacement == nil || replacement.AsyncKeyHash != "" || replacement.Epoch <= task.Epoch || replacement.LeaseKind != "" || string(replacement.Progress) != "offset:42" {
		t.Fatalf("retry did not clear async grant: %+v %v", replacement, err)
	}
	if repeated, repeatErr := s.RecordHeartbeat(t.Context(), progress); repeatErr != nil || repeated != receipt {
		t.Fatalf("heartbeat receipt after retry: %+v %v", repeated, repeatErr)
	}
}

func asyncTimeout(t *testing.T, s durable.Store) {
	r, task, handoff := awaitActivity(t, s, 100*time.Millisecond, time.Second)
	time.Sleep(time.Until(task.DeadlineAt) + time.Millisecond)
	finish := asyncFinish(r, task)
	if _, err := s.CommitTransition(t.Context(), finish); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("expired callback completed: %v", err)
	}
	if _, err := s.RecordHeartbeat(t.Context(), asyncProgress(r, task, 1)); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("expired callback heartbeated: %v", err)
	}
	coordinator, err := s.ClaimTimeoutTask(t.Context(), durable.TimeoutClaimRequest{Namespace: r.Namespace, Owner: "timeout", LeaseDuration: time.Second})
	if err != nil || coordinator == nil || coordinator.Epoch <= task.Epoch {
		t.Fatalf("async timeout claim: %+v %v", coordinator, err)
	}
	if _, err = s.CommitTransition(t.Context(), finish); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("timeout did not fence callback: %v", err)
	}
	if _, err = s.CommitTransition(t.Context(), handoff); err != nil {
		t.Fatalf("handoff receipt after timeout: %v", err)
	}
	finish.Token, finish.AsyncSecret, finish.RequestID = coordinator.Token(), "", "timeout"
	finish.State = durable.StateTimedOut
	if _, err = s.CommitTransition(t.Context(), finish); err != nil {
		t.Fatal(err)
	}
}

func asyncConcurrentCompletion(t *testing.T, s durable.Store) {
	r, task, _ := awaitActivity(t, s, 0, time.Second)
	var winners atomic.Int64
	var group sync.WaitGroup
	for i := range 8 {
		group.Go(func() {
			finish := asyncFinish(r, task)
			finish.RequestID = fmt.Sprintf("result:%d", i)
			if _, err := s.CommitTransition(t.Context(), finish); err == nil {
				winners.Add(1)
			} else if !errors.Is(err, durable.ErrClosed) && !errors.Is(err, durable.ErrLeaseLost) {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if winners.Load() != 1 {
		t.Fatalf("concurrent async winners: %d", winners.Load())
	}
}

func asyncHandoffRace(t *testing.T, s durable.Store) {
	r, task := heartbeatActivity(t, s, 0, time.Second)
	if _, err := s.RecordHeartbeat(t.Context(), heartbeatRequest(r, task, 1)); err != nil {
		t.Fatal(err)
	}
	task, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := durable.HashAsyncSecret(asyncSecret)
	if err != nil {
		t.Fatal(err)
	}
	handoff := durable.CommitRequest{Key: r.Key, RequestID: "handoff", ExpectedRevision: 3, Token: task.Token(), Events: []durable.EventInput{{Type: "activity.awaited"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskAwait, AsyncKeyHash: hash}}
	finish := asyncFinish(r, task)
	finish.RequestID, finish.ExpectedRevision, finish.AsyncSecret = "worker-result", 3, ""
	type result struct {
		handoff bool
		err     error
	}
	results := make(chan result, 2)
	go func() {
		_, commitErr := s.CommitTransition(t.Context(), handoff)
		results <- result{handoff: true, err: commitErr}
	}()
	go func() { _, commitErr := s.CommitTransition(t.Context(), finish); results <- result{err: commitErr} }()
	winners, asyncWon := 0, false
	for range 2 {
		outcome := <-results
		if outcome.err == nil {
			winners++
			asyncWon = outcome.handoff
		} else if !errors.Is(outcome.err, durable.ErrClosed) && !errors.Is(outcome.err, durable.ErrLeaseLost) {
			t.Error(outcome.err)
		}
	}
	if winners != 1 {
		t.Fatalf("handoff/result winners: %d", winners)
	}
	current, err := s.GetTask(t.Context(), r.Key, task.ID)
	if err != nil || string(current.Progress) != "offset:1" || current.HeartbeatSequence != 1 || !current.DeadlineAt.Equal(task.DeadlineAt) {
		t.Fatalf("racing handoff changed progress/deadline: %+v %v", current, err)
	}
	if asyncWon {
		if _, err = s.CommitTransition(t.Context(), asyncFinish(r, current)); err != nil {
			t.Fatal(err)
		}
	}
}
