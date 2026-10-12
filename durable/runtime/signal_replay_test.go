package runtime_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func appendSignal(t *testing.T, f *historyFixture, id, name, input string, at time.Time) {
	t.Helper()
	f.append(drt.EventSignalReceived, encode(t, durable.Signal{Version: 1, ID: id, Name: name, Input: []byte(input)}), at)
}
func appendSignalDecision(t *testing.T, f *historyFixture, d drt.Decision, at time.Time) {
	t.Helper()
	for _, command := range d.Commands {
		f.append(drt.EventCommandScheduled, encode(t, command), at)
	}
	for _, consumed := range d.Signals {
		f.append(drt.EventSignalConsumed, encode(t, consumed), at)
	}
	f.append(drt.EventWorkflowWaiting, nil, at)
}

func TestSignalReplayFIFOAndAssignment(t *testing.T) {
	f := newHistory()
	arrival := f.execution.CreatedAt.Add(time.Minute)
	appendSignal(t, f, "first", "approve", "A", arrival)
	appendSignal(t, f, "other", "comment", "C", arrival)
	appendSignal(t, f, "second", "approve", "B", arrival)
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.ReceiveSignal("a", "approve")
		b := w.ReceiveSignal("b", "approve")
		first, err := b.Get()
		if err != nil {
			return nil, err
		}
		if _, err = w.Activity("wait", "wait", "", nil).Get(); err != nil {
			return nil, err
		}
		second, err := a.Get()
		if err != nil {
			return nil, err
		}
		comment, err := w.ReceiveSignal("c", "comment").Get()
		return append(append(first, second...), comment...), err
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(first.Signals) != 1 || first.Signals[0].CommandID != "b" || first.Signals[0].SignalID != "first" {
		t.Fatalf("first assignment: %+v %v", first, err)
	}
	appendSignalDecision(t, f, first, arrival.Add(time.Minute))
	appendSignal(t, f, "third", "approve", "D", arrival.Add(2*time.Minute))
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "wait"}), arrival.Add(3*time.Minute))
	next, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(next.Output) != "ABC" || len(next.Signals) != 2 || next.Signals[0].SignalID != "second" || next.Signals[1].SignalID != "other" {
		t.Fatalf("reserved assignment: %+v %v", next, err)
	}
}

func TestSignalReplayLogicalTime(t *testing.T) {
	f := newHistory()
	arrival := f.execution.CreatedAt.Add(time.Minute)
	appendSignal(t, f, "signal", "approve", "yes", arrival)
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		if _, err := w.ReceiveSignal("approval", "approve").Get(); err != nil {
			return nil, err
		}
		return w.Timer("cooldown", time.Minute).Get()
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(first.Commands) != 2 || !first.Commands[1].Deadline.Equal(arrival.Add(time.Minute)) {
		t.Fatalf("timer origin: %+v %v", first, err)
	}
	appendSignalDecision(t, f, first, arrival.Add(time.Hour))
	next, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(next.Commands) != 0 || len(next.Signals) != 0 || next.State != durable.StateRunning {
		t.Fatalf("consumption time changed timer: %+v %v", next, err)
	}
	changed := func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.ReceiveSignal("approval", "different").Get() }
	if _, err = drt.Evaluate(f.execution, f.events, changed); !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("changed receive replay: %v", err)
	}
}

func TestSignalDecisionEventBound(t *testing.T) {
	for _, count := range []int{499, 500} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newHistory()
			for i := range count {
				appendSignal(t, f, fmt.Sprint(i), "many", "input", f.execution.CreatedAt)
			}
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				for i := range count {
					if _, err := w.ReceiveSignal(fmt.Sprint(i), "many").Get(); err != nil {
						return nil, err
					}
				}
				return w.ReceiveSignal("pending", "missing").Get()
			}
			decision, err := drt.Evaluate(f.execution, f.events, handler)
			if count == 499 {
				if err != nil || len(decision.Commands) != count+1 || len(decision.Signals) != count {
					t.Fatalf("valid batch: %+v %v", decision, err)
				}
			} else if !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("oversized decision accepted: %v", err)
			}
		})
	}
}

func TestSignalHistoryRejectsCorruption(t *testing.T) {
	for _, mode := range []string{"version", "duplicate_message", "missing_message", "missing_command", "wrong_kind", "wrong_name", "future_message", "duplicate_message_use", "duplicate_command_use", "consumption_version", "out_of_order", "extra_field"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			at := f.execution.CreatedAt
			message := durable.Signal{Version: 1, ID: "message", Name: "approve", Input: []byte("yes")}
			if mode == "version" {
				message.Version = 2
			}
			if mode != "future_message" {
				f.append(drt.EventSignalReceived, encode(t, message), at)
			}
			if mode == "duplicate_message" {
				f.append(drt.EventSignalReceived, encode(t, message), at)
			}
			if mode == "out_of_order" {
				appendSignal(t, f, "second", "approve", "two", at)
			}
			command := drt.Command{Version: 1, Index: 1, ID: "approval", Kind: drt.CommandSignal, Name: "approve"}
			if mode == "wrong_kind" {
				command.Kind = durable.TaskActivity
			}
			if mode == "wrong_name" {
				command.Name = "other"
			}
			if mode != "missing_command" {
				f.append(drt.EventCommandScheduled, encode(t, command), at)
			}
			consumed := drt.SignalConsumption{Version: 1, CommandID: "approval", SignalID: "message"}
			if mode == "missing_message" {
				consumed.SignalID = "absent"
			}
			if mode == "consumption_version" {
				consumed.Version = 2
			}
			if mode == "out_of_order" {
				consumed.SignalID = "second"
			}
			payload := encode(t, consumed)
			if mode == "extra_field" {
				payload = []byte(`{"version":1,"command_id":"approval","signal_id":"message","unknown":true}`)
			}
			f.append(drt.EventSignalConsumed, payload, at)
			if mode == "future_message" {
				f.append(drt.EventSignalReceived, encode(t, message), at)
			}
			if mode == "duplicate_message_use" {
				command.Index = 2
				command.ID = "another"
				f.append(drt.EventCommandScheduled, encode(t, command), at)
				consumed.CommandID = "another"
				f.append(drt.EventSignalConsumed, encode(t, consumed), at)
			}
			if mode == "duplicate_command_use" {
				appendSignal(t, f, "second", "approve", "two", at)
				consumed.SignalID = "second"
				f.append(drt.EventSignalConsumed, encode(t, consumed), at)
			}
			_, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil })
			if !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("corrupt history accepted: %v", err)
			}
		})
	}
}

func TestSignalTerminalReplayRetainsUnreadMessages(t *testing.T) {
	f := newHistory()
	at := f.execution.CreatedAt
	appendSignal(t, f, "first", "approve", "A", at)
	appendSignal(t, f, "unread", "approve", "B", at)
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.ReceiveSignal("approval", "approve").Get() }
	decision, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(decision.Output) != "A" {
		t.Fatalf("first result: %+v %v", decision, err)
	}
	appendSignalDecision(t, f, decision, at.Add(time.Minute))
	f.append(drt.EventWorkflowCompleted, decision.Output, at.Add(time.Minute))
	f.execution.State = durable.StateCompleted
	f.execution.Output = decision.Output
	replay, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(replay.Output) != "A" || len(replay.Signals) != 0 || len(replay.Commands) != 0 {
		t.Fatalf("terminal replay: %+v %v", replay, err)
	}
	// Dropping the consumption would invent a new assignment on a closed run.
	for i, event := range f.events {
		if event.Type == drt.EventSignalConsumed {
			f.events = append(f.events[:i], f.events[i+1:]...)
			break
		}
	}
	for i := range f.events {
		f.events[i].Sequence = int64(i + 1)
	}
	f.execution.LastSequence = int64(len(f.events))
	if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("closed run acquired unsaved consumption: %v", err)
	}
}
