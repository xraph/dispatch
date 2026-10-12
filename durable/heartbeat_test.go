package durable_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestHeartbeatRejectsExpiredLeaseAndReversedClock(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	task := durable.Task{Key: durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"},
		TaskSpec: durable.TaskSpec{ID: "activity", Kind: durable.TaskActivity}, Version: 2, Owner: "worker", Epoch: 1,
		LeaseUntil: now, HeartbeatEpoch: 1, HeartbeatAt: now.Add(-time.Second)}
	request := durable.HeartbeatRequest{Key: task.Key, RequestID: "hb", Token: task.Token(), Sequence: 1, LeaseDuration: time.Minute}
	if _, err := durable.ApplyHeartbeat(task, request, now); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("heartbeat revived expired grant: %v", err)
	}
	task.LeaseUntil = now.Add(time.Minute)
	task.HeartbeatAt = now.Add(time.Second)
	if _, err := durable.ApplyHeartbeat(task, request, now); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("heartbeat moved store time backwards: %v", err)
	}
	task.HeartbeatAt = now
	task.HeartbeatSequence = math.MaxInt64
	if _, err := durable.ApplyHeartbeat(task, request, now); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("heartbeat sequence overflow accepted: %v", err)
	}
}
