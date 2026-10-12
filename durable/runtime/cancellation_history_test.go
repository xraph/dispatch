package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestCancelHistoryRejectsCorruption(t *testing.T) {
	for _, mode := range []string{"unknown_target", "self", "unknown_cancel", "wrong_kind", "version", "command_version", "metadata", "missing_ack", "duplicate_ack", "overwritten_result", "noop_pending", "noop_metadata", "late_outcome", "extra_field", "command_input"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			at := f.execution.CreatedAt
			f.append(drt.EventCommandScheduled, encode(t, drt.Command{Version: 1, Index: 1, ID: "target", Kind: durable.TaskActivity, Name: "work"}), at)
			c := drt.Command{Version: 1, Index: 2, ID: "stop", Kind: drt.CommandCancel, TargetID: "target"}
			if mode == "unknown_target" {
				c.TargetID = "missing"
			}
			if mode == "self" {
				c.TargetID = "stop"
			}
			if mode == "command_version" {
				c.Version = 2
			}
			if mode == "command_input" {
				c.Input = []byte("bad")
			}
			f.append(drt.EventCommandScheduled, encode(t, c), at)
			event := drt.Cancellation{Version: 1, CommandID: "stop", TargetID: "target", Cancelled: true}
			switch mode {
			case "unknown_cancel":
				event.CommandID = "missing"
			case "wrong_kind":
				event.CommandID = "target"
			case "version":
				event.Version = 2
			case "metadata":
				event.Attempt = 1
			case "noop_pending":
				event.Cancelled = false
			case "overwritten_result", "noop_metadata":
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "target", Output: []byte("done")}), at)
				if mode == "noop_metadata" {
					event.Cancelled = false
					event.Attempt = 1
				}
			}
			payload := encode(t, event)
			if mode == "extra_field" {
				payload = []byte(`{"version":1,"command_id":"stop","target_id":"target","cancelled":true,"unknown":1}`)
			}
			if mode != "missing_ack" {
				f.append(drt.EventFutureCancelled, payload, at)
			}
			if mode == "duplicate_ack" {
				f.append(drt.EventFutureCancelled, payload, at)
			}
			if mode == "late_outcome" {
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "target"}), at)
			}
			_, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil })
			if !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("corrupt cancellation accepted: %v", err)
			}
		})
	}
}

func TestCancelChangedTargetFailsReplay(t *testing.T) {
	f := newHistory()
	original := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.Activity("a", "work", "", nil)
		w.Activity("b", "work", "", nil)
		return w.Cancel("stop", a).Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, original)
	if err != nil {
		t.Fatal(err)
	}
	appendCancellation(t, f, d, drt.Cancellation{Version: 1, CommandID: "stop", TargetID: "a", Cancelled: true}, f.execution.CreatedAt)
	changed := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.Activity("a", "work", "", nil)
		b := w.Activity("b", "work", "", nil)
		return w.Cancel("stop", b).Get()
	}
	if _, err = drt.Evaluate(f.execution, f.events, changed); !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("changed target: %v", err)
	}
}

func TestCancelActivityCheckpointReplay(t *testing.T) {
	for _, retrying := range []bool{false, true} {
		for _, bad := range []bool{false, true} {
			f := newHistory()
			at := f.execution.CreatedAt
			options := drt.ActivityOptions{RetryPolicy: &drt.RetryPolicy{MaximumAttempts: 2}}
			first, err := drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
				return w.ActivityWithOptions("target", "work", "", nil, options).Get()
			})
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, first.Commands)
			f.append(drt.EventActivityAttemptStarted, encode(t, drt.ActivityAttempt{Version: 1, CommandID: "target", Attempt: 1, Epoch: 1, HeartbeatEnabled: true}), at)
			checkpoint := &drt.HeartbeatCheckpoint{At: at.Add(time.Millisecond), Epoch: 1, Sequence: 1, Details: []byte("offset:42")}
			if retrying {
				f.append(drt.EventActivityAttemptFailed, encode(t, drt.ActivityAttempt{Version: 1, CommandID: "target", Attempt: 1, Epoch: 1, HeartbeatEnabled: true, Heartbeat: checkpoint, Failure: &drt.ApplicationError{Type: "retry", Message: "retry"}, RetryAfter: time.Second}), checkpoint.At)
			}
			f.append(drt.EventCommandScheduled, encode(t, drt.Command{Version: 1, Index: 2, ID: "stop", Kind: drt.CommandCancel, TargetID: "target"}), at.Add(2*time.Millisecond))
			if bad {
				checkpoint.Epoch = 2
			}
			f.append(drt.EventFutureCancelled, encode(t, drt.Cancellation{Version: 1, CommandID: "stop", TargetID: "target", Cancelled: true, Attempt: 1, Heartbeat: checkpoint}), at.Add(2*time.Millisecond))
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				target := w.ActivityWithOptions("target", "work", "", nil, options)
				if _, getErr := w.Cancel("stop", target).Get(); getErr != nil {
					return nil, getErr
				}
				_, getErr := target.Get()
				var cancelled *drt.CancelledError
				if !errors.As(getErr, &cancelled) || cancelled.Attempt != 1 || cancelled.Heartbeat == nil || string(cancelled.Heartbeat.Details) != "offset:42" {
					t.Fatalf("checkpoint missing: %v", getErr)
				}
				cancelled.Heartbeat.Details[0] = 'X'
				_, getErr = target.Get()
				if !errors.As(getErr, &cancelled) || string(cancelled.Heartbeat.Details) != "offset:42" {
					t.Fatal("cancellation checkpoint aliased")
				}
				return []byte("handled"), nil
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if bad {
				if !errors.Is(err, drt.ErrHistory) {
					t.Fatalf("bad checkpoint accepted retry=%t: %v", retrying, err)
				}
			} else if err != nil || string(d.Output) != "handled" {
				t.Fatalf("checkpoint replay retry=%t: %+v %v", retrying, d, err)
			}
		}
	}
}
