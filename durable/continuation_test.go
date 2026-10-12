package durable_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestContinuationRejectsInvalidTransition(t *testing.T) {
	for _, mode := range []string{"activity", "timeout_grant", "output", "retained_task", "terminal_event", "expired", "oversized_input", "invalid_timeout", "missing_history"} {
		t.Run(mode, func(t *testing.T) {
			now := durable.Timestamp(time.Now())
			start := rootRequest()
			e, err := durable.NewExecution(start, now)
			if err != nil {
				t.Fatal(err)
			}
			task := durable.Task{Key: e.Key, TaskSpec: durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: "orders"}, Owner: "worker", Epoch: 1, LeaseUntil: now.Add(time.Minute)}
			r := durable.CommitRequest{Key: e.Key, RequestID: "continue", Token: task.Token(), ExpectedRevision: 1, State: durable.StateContinuedAsNew, Events: []durable.EventInput{{Type: "workflow.waiting"}}, Continuation: &durable.ContinueSpec{RunID: "next", WorkflowType: "order", BuildID: "v1", Queue: "orders"}}
			history := []durable.Event{{EventInput: durable.EventInput{Type: "execution.started", Payload: start.Input}, Sequence: 1, Time: now}}
			want := durable.ErrInvalid
			switch mode {
			case "activity":
				task.Kind = durable.TaskActivity
			case "timeout_grant":
				task.LeaseKind = durable.LeaseTimeout
			case "output":
				r.Output = []byte("invalid")
			case "retained_task":
				r.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep}
			case "terminal_event":
				r.Events[0].Type = "workflow.completed"
			case "expired":
				now = e.ExecutionDeadlineAt
				want = durable.ErrExecutionDeadline
			case "oversized_input":
				r.Continuation.Input = make([]byte, (1<<20)+1)
			case "invalid_timeout":
				r.Continuation.RunTimeout = time.Nanosecond
			case "missing_history":
				history = nil
			}
			if _, err = durable.PrepareContinuation(e, task, r, history, now); !errors.Is(err, want) {
				t.Fatalf("accepted invalid %s: %v", mode, err)
			}
		})
	}
}

func TestContinuationCarryCountLimit(t *testing.T) {
	now := durable.Timestamp(time.Now())
	start := rootRequest()
	e, err := durable.NewExecution(start, now)
	if err != nil {
		t.Fatal(err)
	}
	task := durable.Task{Key: e.Key, TaskSpec: durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: "orders"}}
	r := durable.CommitRequest{Key: e.Key, State: durable.StateContinuedAsNew, Events: []durable.EventInput{{Type: "workflow.waiting"}}, Continuation: &durable.ContinueSpec{RunID: "next", WorkflowType: "order", BuildID: "v1", Queue: "orders"}}
	history := make([]durable.Event, 1, 1000)
	history[0] = durable.Event{EventInput: durable.EventInput{Type: "execution.started"}, Sequence: 1, Time: now}
	for i := range 999 {
		payload, encodeErr := json.Marshal(durable.Signal{Version: 1, ID: fmt.Sprint(i), Name: "message"})
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		history = append(history, durable.Event{EventInput: durable.EventInput{Type: durable.EventSignalReceived, Payload: payload}, Sequence: int64(i + 2), Time: now})
	}
	e.LastSequence = 1000
	if _, err = durable.PrepareContinuation(e, task, r, history, now); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("oversized carry accepted: %v", err)
	}
	r.Events = []durable.EventInput{{Type: "workflow.command_scheduled", Payload: []byte(`{"version":1,"id":"receive","kind":"signal","name":"message"}`)}, {Type: durable.EventSignalConsumed, Payload: []byte(`{"version":1,"command_id":"receive","signal_id":"0"}`)}}
	b, err := durable.PrepareContinuation(e, task, r, history, now)
	if err != nil || len(b.History) != 1000 {
		t.Fatalf("drained carry rejected: %+v %v", b, err)
	}
	var carried durable.CarriedSignal
	if err = json.Unmarshal(b.History[2].Payload, &carried); err != nil || carried.Signal.ID != "1" {
		t.Fatalf("consumed signal carried again: %+v %v", carried, err)
	}
}
