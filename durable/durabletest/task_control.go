package durabletest

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func taskControl(t *testing.T, s durable.Store) {
	r := start(t, s)
	claimed := claim(t, s, r, time.Minute)
	if claimed.Version < 1 {
		t.Fatal("claim did not expose task version")
	}
	progress := []byte("offset:12")
	req := completion(r, claimed)
	req.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep, Progress: &progress}
	first, err := s.CommitTransition(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	progress[0] = 'X'
	retained, err := s.GetTask(t.Context(), r.Key, claimed.ID)
	if err != nil || retained.Done || retained.Token() != claimed.Token() || retained.Version != claimed.Version+1 || string(retained.Progress) != "offset:12" {
		t.Fatalf("retained grant/progress: %+v, %v", retained, err)
	}
	retained.Progress[0] = 'X'
	again, err := s.GetTask(t.Context(), r.Key, claimed.ID)
	if err != nil || string(again.Progress) != "offset:12" {
		t.Fatalf("read aliases progress: %+v, %v", again, err)
	}
	// Release the same task for a future retry, atomically with its history.
	retry := completion(r, claimed)
	retry.RequestID, retry.ExpectedRevision = "retry", first.Revision
	retry.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskRetry, RetryAfter: 200 * time.Millisecond}
	second, err := s.CommitTransition(t.Context(), retry)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, retryErr := s.CommitTransition(t.Context(), retry); retryErr != nil || receipt != second {
		t.Fatalf("retry receipt: %+v, %v", receipt, retryErr)
	}
	if _, err = s.RenewTask(t.Context(), r.Key, claimed.Token(), time.Minute); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("released grant could renew: %v", err)
	}
	poll := durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "replacement", LeaseDuration: time.Minute}
	if task, claimErr := s.ClaimTask(t.Context(), poll); claimErr != nil || task != nil {
		t.Fatalf("retry became available early: %+v, %v", task, claimErr)
	}
	time.Sleep(215 * time.Millisecond)
	next, err := s.ClaimTask(t.Context(), poll)
	if err != nil || next == nil || next.Epoch <= claimed.Epoch || next.Attempt != claimed.Attempt+1 || string(next.Progress) != "offset:12" {
		t.Fatalf("retry grant: %+v, %v", next, err)
	}
	late := completion(r, claimed)
	late.ExpectedRevision, late.RequestID = second.Revision, "late"
	if _, err = s.CommitTransition(t.Context(), late); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old retry token accepted: %v", err)
	}
}

func taskDeadline(t *testing.T, s durable.Store) {
	r := start(t, s)
	claimed := claim(t, s, r, time.Minute)
	timeout := 200 * time.Millisecond
	req := completion(r, claimed)
	req.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &timeout}
	req.Tasks = []durable.TaskSpec{{ID: "watchdog", Kind: durable.TaskTimer, Queue: "timeouts", AvailableAfter: timeout}}
	receipt, err := s.CommitTransition(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.GetTask(t.Context(), r.Key, claimed.ID)
	if err != nil {
		t.Fatal(err)
	}
	watchdog, err := s.GetTask(t.Context(), r.Key, "watchdog")
	if err != nil || !watchdog.AvailableAt.Equal(target.DeadlineAt) || target.LeaseUntil.After(target.DeadlineAt) {
		t.Fatalf("different clocks for deadline and watchdog: target=%+v timer=%+v %v", target, watchdog, err)
	}
	early := completion(r, claimed)
	early.RequestID, early.ExpectedRevision = "too-early", receipt.Revision
	early.Conditions = []durable.TaskCondition{{TaskID: target.ID, Version: target.Version, DeadlineElapsed: true}}
	if _, err = s.CommitTransition(t.Context(), early); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("unexpired deadline condition accepted: %v", err)
	}
	if until, renewErr := s.RenewTask(t.Context(), r.Key, claimed.Token(), time.Hour); renewErr != nil || until.After(target.DeadlineAt) {
		t.Fatalf("renewal extended activity deadline: %v, %v", until, renewErr)
	}
	time.Sleep(215 * time.Millisecond)
	if _, err = s.RenewTask(t.Context(), r.Key, claimed.Token(), time.Minute); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("expired deadline renewed: %v", err)
	}
	late := completion(r, claimed)
	late.RequestID, late.ExpectedRevision = "late", receipt.Revision
	if _, err = s.CommitTransition(t.Context(), late); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("late result accepted: %v", err)
	}
	if task, pollErr := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue,
		Kind: durable.TaskWorkflow, Owner: "replacement", LeaseDuration: time.Minute}); pollErr != nil || task != nil {
		t.Fatalf("deadline-expired task reclaimed: %+v, %v", task, pollErr)
	}
	timer, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "timeouts", Kind: durable.TaskTimer,
		Owner: "timeout-worker", LeaseDuration: time.Minute})
	if err != nil || timer == nil {
		t.Fatalf("claim watchdog: %+v, %v", timer, err)
	}
	cancel := completion(r, timer)
	cancel.RequestID, cancel.ExpectedRevision = "timeout", receipt.Revision
	cancel.Conditions = []durable.TaskCondition{{TaskID: target.ID, Version: target.Version, DeadlineElapsed: true}}
	cancel.CancelTasks = []string{target.ID}
	if _, err = s.CommitTransition(t.Context(), cancel); err != nil {
		t.Fatal(err)
	}
	finished, err := s.GetTask(t.Context(), r.Key, target.ID)
	if err != nil || !finished.Done || finished.Version != target.Version+1 {
		t.Fatalf("conditional timeout cancellation: %+v, %v", finished, err)
	}
}

func taskConditions(t *testing.T, s durable.Store) {
	r := start(t, s)
	claimed := claim(t, s, r, time.Minute)
	req := completion(r, claimed)
	req.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep}
	req.Tasks = []durable.TaskSpec{{ID: "target", Kind: durable.TaskActivity, Queue: "effects"}}
	receipt, err := s.CommitTransition(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.GetTask(t.Context(), r.Key, "target")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "effects", Kind: durable.TaskActivity,
		Owner: "activity", LeaseDuration: time.Minute})
	if err != nil || other == nil {
		t.Fatalf("claim effect: %+v, %v", other, err)
	}
	stale := completion(r, claimed)
	stale.RequestID, stale.ExpectedRevision = "cancel", receipt.Revision
	stale.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep}
	stale.Conditions = []durable.TaskCondition{{TaskID: target.ID, Version: target.Version}}
	stale.CancelTasks = []string{target.ID}
	if _, err = s.CommitTransition(t.Context(), stale); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("stale observed version cancelled a newer grant: %v", err)
	}
	// A current version still cannot pretend an unset deadline has expired.
	stale.Conditions[0].Version, stale.Conditions[0].DeadlineElapsed = other.Version, true
	if _, err = s.CommitTransition(t.Context(), stale); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("unset deadline counted as expired: %v", err)
	}
	stale.Conditions[0].DeadlineElapsed = false
	// Late task collision must roll back cancellation, progress and history.
	stale.Tasks = []durable.TaskSpec{{ID: "temporary", Kind: durable.TaskActivity, Queue: "effects"}, {ID: target.ID, Kind: durable.TaskActivity, Queue: "effects"}}
	progress := []byte("should-not-commit")
	stale.TaskUpdate.Progress = &progress
	if _, err = s.CommitTransition(t.Context(), stale); !errors.Is(err, durable.ErrExists) {
		t.Fatalf("expected late insertion rejection: %v", err)
	}
	current, err := s.GetTask(t.Context(), r.Key, target.ID)
	if err != nil || current.Done || current.Version != other.Version {
		t.Fatalf("failed transition cancelled task: %+v, %v", current, err)
	}
	source, err := s.GetTask(t.Context(), r.Key, claimed.ID)
	if err != nil || len(source.Progress) != 0 || source.Done {
		t.Fatalf("failed transition changed source: %+v, %v", source, err)
	}
	if _, err = s.GetTask(t.Context(), r.Key, "temporary"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("failed transition inserted task: %v", err)
	}
	execution, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || execution.Revision != receipt.Revision {
		t.Fatalf("failed transition advanced history: %+v, %v", execution, err)
	}
	stale.Tasks = nil
	if _, err = s.CommitTransition(t.Context(), stale); err != nil {
		t.Fatalf("rejected request stored a receipt: %v", err)
	}
	if _, err = s.RenewTask(t.Context(), r.Key, other.Token(), time.Minute); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("cancelled task owner renewed: %v", err)
	}
}

func taskControlValidation(t *testing.T, s durable.Store) {
	r := start(t, s)
	claimed := claim(t, s, r, time.Minute)
	negative := -time.Second
	cases := []durable.CommitRequest{completion(r, claimed), completion(r, claimed), completion(r, claimed), completion(r, claimed), completion(r, claimed)}
	cases[0].TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &negative}
	cases[1].TaskUpdate = &durable.TaskUpdate{Action: durable.TaskRetry}
	cases[2].TaskUpdate, cases[2].State = &durable.TaskUpdate{Action: durable.TaskKeep}, durable.StateCompleted
	cases[3].CancelTasks = []string{"unguarded"}
	cases[4].Tasks = []durable.TaskSpec{{ID: "x", Queue: "q", Kind: durable.TaskTimer, AvailableAt: time.Now(), AvailableAfter: time.Second}}
	for i, request := range cases {
		if _, err := s.CommitTransition(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("case %d accepted invalid task control: %v", i, err)
		}
	}
	foreign := r.Key
	foreign.Namespace += "-other"
	if _, err := s.GetTask(t.Context(), foreign, claimed.ID); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("cross-namespace task read: %v", err)
	}
	if !bytes.Equal(claimed.Payload, nil) {
		t.Fatal("unexpected initial task payload")
	}
}
