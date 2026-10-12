package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestAsyncHandoffReplay(t *testing.T) {
	for _, mode := range []string{"valid", "duplicate", "wrong_command", "wrong_attempt", "wrong_epoch", "wrong_version", "no_deadline", "late", "missing_checkpoint", "future_checkpoint", "final_sequence", "final_time", "final_details", "missing_start", "after_failure", "after_outcome"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			policy := asyncOptions(1)
			if mode == "no_deadline" {
				policy.StartToCloseTimeout = 0
			}
			handler := retryWorkflow(policy)
			decision, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, decision.Commands)
			at := f.execution.CreatedAt
			if mode != "missing_start" {
				f.append(drt.EventActivityAttemptStarted, encode(t, drt.ActivityAttempt{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1, HeartbeatEnabled: true}), at)
			}
			cp := &drt.HeartbeatCheckpoint{At: at.Add(time.Second), Epoch: 1, Sequence: 1, Details: []byte("before")}
			handoff := drt.ActivityHandoff{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1, Heartbeat: cp}
			deferredAt := at.Add(2 * time.Second)
			switch mode {
			case "wrong_command":
				handoff.CommandID = "other"
			case "wrong_attempt":
				handoff.Attempt++
			case "wrong_epoch":
				handoff.Epoch++
			case "wrong_version":
				handoff.Version++
			case "late":
				deferredAt = at.Add(2 * time.Minute)
			case "missing_checkpoint":
				handoff.Heartbeat = nil
			case "future_checkpoint":
				handoff.Heartbeat = &drt.HeartbeatCheckpoint{At: deferredAt.Add(time.Second), Epoch: 1, Sequence: 1}
			case "after_failure":
				f.append(drt.EventActivityAttemptFailed, encode(t, drt.ActivityAttempt{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1, HeartbeatEnabled: true, Heartbeat: cp, Failure: &drt.ApplicationError{Type: "failed", Message: "finished"}}), deferredAt)
			case "after_outcome":
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "charge", Attempt: 1, Output: []byte("paid"), Heartbeat: cp}), deferredAt)
			}
			f.append(drt.EventActivityDeferred, encode(t, handoff), deferredAt)
			if mode == "duplicate" {
				f.append(drt.EventActivityDeferred, encode(t, handoff), deferredAt)
			}
			final := *cp
			switch mode {
			case "final_sequence":
				final.Sequence = 0
				final.At = at
				final.Details = nil
			case "final_time":
				final.Sequence = 2
				final.At = at.Add(500 * time.Millisecond)
			case "final_details":
				final.Details = []byte("changed without heartbeat")
			}
			if mode != "after_outcome" {
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "charge", Attempt: 1, Output: []byte("paid"), Heartbeat: &final}), at.Add(3*time.Second))
			}
			result, err := drt.Evaluate(f.execution, f.events, handler)
			if mode == "valid" {
				if err != nil || result.State != durable.StateCompleted || string(result.Output) != "paid" {
					t.Fatalf("handoff replay: %+v %v", result, err)
				}
			} else if !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("invalid %s accepted: %+v %v", mode, result, err)
			}
		})
	}
}
