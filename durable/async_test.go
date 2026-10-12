package durable_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestAsyncHandoffValidation(t *testing.T) {
	now := durable.Timestamp(time.Now())
	secret := strings.Repeat("01", 32)
	hash, err := durable.HashAsyncSecret(secret)
	if err != nil {
		t.Fatal(err)
	}
	task := durable.Task{Key: durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"}, TaskSpec: durable.TaskSpec{ID: "activity", Kind: durable.TaskActivity}, Owner: "worker", Epoch: 1, Version: 1, LeaseUntil: now.Add(time.Second), DeadlineAt: now.Add(time.Minute), HeartbeatEpoch: 1, HeartbeatAt: now}
	for _, mode := range []string{"valid", "no_deadline", "no_activation", "workflow", "timeout"} {
		copyTask := task
		switch mode {
		case "no_deadline":
			copyTask.DeadlineAt = time.Time{}
		case "no_activation":
			copyTask.HeartbeatEpoch = 0
		case "workflow":
			copyTask.Kind = durable.TaskWorkflow
		case "timeout":
			copyTask.LeaseKind = durable.LeaseTimeout
		}
		updated, updateErr := durable.UpdateTask(copyTask, &durable.TaskUpdate{Action: durable.TaskAwait, AsyncKeyHash: hash}, now)
		if mode == "valid" {
			if updateErr != nil || updated.LeaseKind != durable.LeaseAsync {
				t.Fatalf("valid async handoff: %+v %v", updated, updateErr)
			}
		} else if !errors.Is(updateErr, durable.ErrInvalid) {
			t.Fatalf("invalid handoff %s: %v", mode, updateErr)
		}
	}
	for _, value := range []string{"", "not-secret", strings.Repeat("AB", 32), strings.Repeat("ff", 31)} {
		if _, err = durable.HashAsyncSecret(value); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid secret shape accepted: %v", err)
		}
	}
	for _, update := range []*durable.TaskUpdate{
		{Action: durable.TaskAwait},
		{Action: durable.TaskAwait, AsyncKeyHash: hash, LeaseDuration: time.Second},
		{Action: durable.TaskAwait, AsyncKeyHash: hash, Progress: new([]byte)},
		{Action: durable.TaskKeep, AsyncKeyHash: hash},
	} {
		request := durable.CommitRequest{Key: task.Key, RequestID: "handoff", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "await"}}, TaskUpdate: update}
		if err = request.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid async fields accepted: %v", err)
		}
	}
	task.LeaseKind, task.LeaseUntil, task.AsyncKeyHash = durable.LeaseAsync, time.Time{}, hash
	if err = durable.CheckLease(task, task.Token(), now); err != nil {
		t.Fatalf("async grant depends on worker lease: %v", err)
	}
	request := durable.CommitRequest{Key: task.Key, RequestID: "keep", ExpectedRevision: 1, Token: task.Token(), AsyncSecret: secret, Events: []durable.EventInput{{Type: "keep"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep}}
	if err = request.Validate(); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("async keep accepted: %v", err)
	}
	if err = durable.CheckLease(task, task.Token(), task.DeadlineAt); !errors.Is(err, durable.ErrTaskDeadline) {
		t.Fatalf("async grant exceeds deadline: %v", err)
	}
}
