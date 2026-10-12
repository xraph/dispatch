package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestHeartbeatCheckpointReplay(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "before_start", "future", "negative_sequence", "wrong_epoch", "zero_sequence_details", "precision", "early", "wrong_class", "final_mismatch", "not_enabled"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			policy := retryOptions(1)
			policy.HeartbeatTimeout = time.Second
			policy.StartToCloseTimeout = 10 * time.Second
			handler := retryWorkflow(policy)
			first, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, first.Commands)
			at := f.execution.CreatedAt
			started := drt.ActivityAttempt{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1, HeartbeatEnabled: mode != "not_enabled"}
			f.append(drt.EventActivityAttemptStarted, encode(t, started), at)
			checkpoint := &drt.HeartbeatCheckpoint{At: at.Add(500 * time.Millisecond), Epoch: 1, Sequence: 1, Details: []byte("offset:42")}
			ended := checkpoint.At.Add(time.Second)
			kind := drt.TimeoutHeartbeat
			switch mode {
			case "missing":
				checkpoint = nil
			case "before_start":
				checkpoint.At = at.Add(-time.Second)
			case "future":
				checkpoint.At = ended.Add(time.Second)
			case "negative_sequence":
				checkpoint.Sequence = -1
			case "wrong_epoch":
				checkpoint.Epoch = 2
			case "zero_sequence_details":
				checkpoint.Sequence, checkpoint.At = 0, at
			case "precision":
				checkpoint.At = checkpoint.At.Add(time.Nanosecond)
			case "early":
				ended = ended.Add(-time.Microsecond)
			case "wrong_class":
				kind = drt.TimeoutStartToClose
			}
			failure := &drt.ApplicationError{Type: "activity_" + string(kind) + "_timeout", Message: "expired"}
			failed := drt.ActivityAttempt{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1,
				HeartbeatEnabled: true, Heartbeat: checkpoint, Timeout: kind, Failure: failure}
			f.append(drt.EventActivityAttemptFailed, encode(t, failed), ended)
			outcome := drt.Outcome{Version: 2, CommandID: "charge", Attempt: 1, Timeout: kind, Failure: failure, Heartbeat: checkpoint}
			if mode == "final_mismatch" {
				outcome.Heartbeat = &drt.HeartbeatCheckpoint{At: checkpoint.At, Epoch: checkpoint.Epoch, Sequence: checkpoint.Sequence, Details: []byte("different")}
			}
			f.append(drt.EventActivityCompleted, encode(t, outcome), ended)
			result, err := drt.Evaluate(f.execution, f.events, handler)
			if mode != "valid" {
				if !errors.Is(err, drt.ErrHistory) {
					t.Fatalf("invalid heartbeat %s accepted: %+v %v", mode, result, err)
				}
				return
			}
			if err != nil || result.State != durable.StateFailed || result.Failure.Type != "activity_heartbeat_timeout" {
				t.Fatalf("heartbeat timeout replay: %+v %v", result, err)
			}
			_, err = drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
				_, activityErr := w.ActivityWithOptions("charge", "charge", "", nil, policy).Get()
				var details *drt.ActivityError
				var application *drt.ApplicationError
				if !errors.As(activityErr, &details) || !errors.As(activityErr, &application) || details.Timeout != drt.TimeoutHeartbeat || string(details.Heartbeat.Details) != "offset:42" {
					t.Fatalf("missing typed checkpoint: %v", activityErr)
				}
				details.Heartbeat.Details[0] = 'X'
				return nil, activityErr
			})
			if err != nil {
				t.Fatal(err)
			}
			changed := policy
			changed.HeartbeatTimeout *= 2
			if _, err = drt.Evaluate(f.execution, f.events, retryWorkflow(changed)); !errors.Is(err, drt.ErrNondeterministic) {
				t.Fatalf("changed heartbeat policy accepted: %v", err)
			}
		})
	}
	invalid := retryOptions(1)
	invalid.HeartbeatTimeout = -1
	f := newHistory()
	if _, err := drt.Evaluate(f.execution, f.events, retryWorkflow(invalid)); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("negative heartbeat timeout accepted: %v", err)
	}
}

func TestHeartbeatReplayChecksInheritedProgress(t *testing.T) {
	for _, changed := range []bool{false, true} {
		f := newHistory()
		policy := retryOptions(2)
		policy.HeartbeatTimeout = time.Second
		handler := retryWorkflow(policy)
		decision, err := drt.Evaluate(f.execution, f.events, handler)
		if err != nil {
			t.Fatal(err)
		}
		f.commands(t, decision.Commands)
		at := f.execution.CreatedAt
		started := drt.ActivityAttempt{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1, HeartbeatEnabled: true}
		f.append(drt.EventActivityAttemptStarted, encode(t, started), at)
		failed := started
		failed.Heartbeat = &drt.HeartbeatCheckpoint{At: at.Add(100 * time.Millisecond), Epoch: 1, Sequence: 1, Details: []byte("saved")}
		failed.Failure, failed.RetryAfter = &drt.ApplicationError{Type: "application", Message: "retry"}, 30*time.Millisecond
		f.append(drt.EventActivityAttemptFailed, encode(t, failed), at.Add(200*time.Millisecond))
		started.Attempt, started.Epoch, started.Progress = 2, 2, []byte("saved")
		if changed {
			started.Progress = []byte("changed")
		}
		at = at.Add(230 * time.Millisecond)
		f.append(drt.EventActivityAttemptStarted, encode(t, started), at)
		f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "charge", Attempt: 2, Output: []byte("paid"),
			Heartbeat: &drt.HeartbeatCheckpoint{At: at, Epoch: 2, Sequence: 0, Details: started.Progress}}), at.Add(time.Millisecond))
		result, err := drt.Evaluate(f.execution, f.events, handler)
		if changed {
			if !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("changed inherited progress accepted: %+v %v", result, err)
			}
		} else if err != nil || string(result.Output) != "paid" {
			t.Fatalf("inherited progress rejected: %+v %v", result, err)
		}
	}
}
