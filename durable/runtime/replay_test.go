package runtime_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type historyFixture struct {
	execution durable.Execution
	events    []durable.Event
}

func newHistory() *historyFixture {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	f := &historyFixture{execution: durable.Execution{
		Key:          durable.Key{Namespace: "test", WorkflowID: "order", RunID: "run"},
		WorkflowType: "order", BuildID: "v1", State: durable.StateRunning,
		Revision: 1, CreatedAt: now, Input: []byte("order-input"),
	}}
	f.append(drt.EventStarted, f.execution.Input, now)
	return f
}

func (f *historyFixture) append(kind string, payload []byte, at time.Time) {
	f.events = append(f.events, durable.Event{EventInput: durable.EventInput{Type: kind, Payload: payload},
		Sequence: int64(len(f.events) + 1), Time: at})
	f.execution.LastSequence = int64(len(f.events))
	f.execution.UpdatedAt = at
}

func (f *historyFixture) commands(t *testing.T, commands []drt.Command) {
	t.Helper()
	for _, command := range commands {
		f.append(drt.EventCommandScheduled, encode(t, command), f.execution.UpdatedAt)
	}
	f.append(drt.EventWorkflowWaiting, nil, f.execution.UpdatedAt)
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReplayActivityThenTimer(t *testing.T) {
	f := newHistory()
	var clocks []time.Time
	handler := func(w *drt.Workflow, input []byte) ([]byte, error) {
		if !bytes.Equal(input, []byte("order-input")) {
			t.Fatalf("input changed: %q", input)
		}
		clocks = append(clocks, w.Now())
		value, err := w.Activity("charge", "charge-card", "billing", input).Get()
		if err != nil {
			return nil, err
		}
		clocks = append(clocks, w.Now())
		if _, err = w.Timer("cooldown", 5*time.Minute).Get(); err != nil {
			return nil, err
		}
		return value, nil
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || first.State != durable.StateRunning || len(first.Commands) != 1 || first.Commands[0].ID != "charge" {
		t.Fatalf("first decision: %+v, %v", first, err)
	}
	f.commands(t, first.Commands)
	blocked, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || blocked.State != durable.StateRunning || len(blocked.Commands) != 0 {
		t.Fatalf("pending replay duplicated scheduling: %+v, %v", blocked, err)
	}
	completedAt := f.execution.CreatedAt.Add(time.Minute)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "charge", Output: []byte("receipt")}), completedAt)
	next, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(next.Commands) != 1 || !next.Commands[0].Deadline.Equal(completedAt.Add(5*time.Minute)) {
		t.Fatalf("timer after saved activity: %+v, %v", next, err)
	}
	f.commands(t, next.Commands)
	// Replaying while the timer is pending must retain its first deadline.
	pending, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(pending.Commands) != 0 || pending.State != durable.StateRunning {
		t.Fatalf("timer replay: %+v, %v", pending, err)
	}
	f.append(drt.EventTimerFired, encode(t, drt.Outcome{Version: 1, CommandID: "cooldown"}), completedAt.Add(6*time.Minute))
	done, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || done.State != durable.StateCompleted || string(done.Output) != "receipt" || len(done.Commands) != 0 {
		t.Fatalf("completed replay: %+v, %v", done, err)
	}
	if !clocks[0].Equal(f.execution.CreatedAt) || !clocks[1].Equal(clocks[0]) || !clocks[2].Equal(clocks[0]) {
		t.Fatalf("logical start time changed: %v", clocks)
	}
}

func TestParallelFuturesUseSavedResults(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.Activity("a", "lookup", "", []byte("a"))
		b := w.Activity("b", "lookup", "", []byte("b"))
		x, err := a.Get()
		if err != nil {
			return nil, err
		}
		y, err := b.Get()
		return append(x, y...), err
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(first.Commands) != 2 {
		t.Fatalf("parallel schedule: %+v, %v", first, err)
	}
	f.commands(t, first.Commands)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "b", Output: []byte("B")}), f.execution.CreatedAt.Add(time.Second))
	waiting, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || waiting.State != durable.StateRunning || len(waiting.Commands) != 0 {
		t.Fatalf("out-of-order completion: %+v, %v", waiting, err)
	}
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "a", Output: []byte("A")}), f.execution.CreatedAt.Add(2*time.Second))
	done, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(done.Output) != "AB" {
		t.Fatalf("parallel result: %+v, %v", done, err)
	}
}

func TestReplayRejectsChangedOrOmittedCommands(t *testing.T) {
	f := newHistory()
	original := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.Activity("charge", "billing", "q", []byte("100")).Get()
	}
	first, err := drt.Evaluate(f.execution, f.events, original)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, first.Commands)
	variants := map[string]drt.WorkflowFunc{
		"input": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.Activity("charge", "billing", "q", []byte("200")).Get()
		},
		"type": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.Activity("charge", "refund", "q", []byte("100")).Get()
		},
		"queue": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.Activity("charge", "billing", "other", []byte("100")).Get()
		},
		"id": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.Activity("other", "billing", "q", []byte("100")).Get()
		},
		"kind":    func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Timer("charge", time.Second).Get() },
		"omitted": func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil },
	}
	for name, handler := range variants {
		t.Run(name, func(t *testing.T) {
			decision, replayErr := drt.Evaluate(f.execution, f.events, handler)
			if !errors.Is(replayErr, drt.ErrNondeterministic) || len(decision.Commands) != 0 {
				t.Fatalf("accepted changed command: %+v, %v", decision, replayErr)
			}
		})
	}
}

func TestWorkflowFailuresAndPanics(t *testing.T) {
	f := newHistory()
	failed, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, errors.New("declined")
	})
	if err != nil || failed.State != durable.StateFailed || failed.Failure == nil || failed.Failure.Message != "declined" {
		t.Fatalf("application failure: %+v, %v", failed, err)
	}
	_, err = drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) {
		panic("bug")
	})
	if !errors.Is(err, drt.ErrWorkflowPanic) {
		t.Fatalf("panic became successful decision: %v", err)
	}
	_, err = drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.Activity("same", "one", "", nil)
		w.Activity("same", "two", "", nil)
		return nil, nil
	})
	if !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("duplicate command IDs accepted: %v", err)
	}
}

func TestActivityFailureIsRecordedAndCatchable(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		_, err := w.Activity("lookup", "lookup", "", nil).Get()
		var failure *drt.ApplicationError
		if errors.As(err, &failure) && failure.Type == "not_found" {
			return []byte("fallback"), nil
		}
		return nil, err
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, first.Commands)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "lookup", Failure: &drt.ApplicationError{Type: "not_found", Message: "missing"}}), f.execution.CreatedAt.Add(time.Second))
	done, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(done.Output) != "fallback" {
		t.Fatalf("stored failure: %+v, %v", done, err)
	}
}

func TestMalformedHistoryRejected(t *testing.T) {
	cases := map[string]func(*historyFixture){
		"gap":           func(f *historyFixture) { f.events[0].Sequence = 2 },
		"missing":       func(f *historyFixture) { f.execution.LastSequence++ },
		"input":         func(f *historyFixture) { f.execution.Input = []byte("different") },
		"unknown_event": func(f *historyFixture) { f.append("unknown", nil, f.execution.CreatedAt) },
		"unmatched_result": func(f *historyFixture) {
			f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "missing"}), f.execution.CreatedAt)
		},
		"json": func(f *historyFixture) { f.append(drt.EventCommandScheduled, []byte("{"), f.execution.CreatedAt) },
		"unknown_field": func(f *historyFixture) {
			f.append(drt.EventCommandScheduled, []byte(`{"version":1,"index":1,"id":"a","kind":"activity","name":"a","surprise":true}`), f.execution.CreatedAt)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			f := newHistory()
			corrupt(f)
			_, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil })
			if !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("corrupt history accepted: %v", err)
			}
		})
	}
}

func TestCompletedHistoryChecksOutput(t *testing.T) {
	f := newHistory()
	f.execution.State, f.execution.Output = durable.StateCompleted, []byte("original")
	f.append(drt.EventWorkflowCompleted, []byte("original"), f.execution.CreatedAt.Add(time.Second))
	_, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) { return []byte("changed"), nil })
	if !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("changed terminal output accepted: %v", err)
	}
}

func TestReplayRejectsInvalidOutcomes(t *testing.T) {
	cases := map[string]func(*historyFixture, []drt.Command){
		"duplicate": func(f *historyFixture, commands []drt.Command) {
			data := encode(t, drt.Outcome{Version: 1, CommandID: commands[0].ID})
			f.append(drt.EventActivityCompleted, data, f.execution.CreatedAt)
			f.append(drt.EventActivityCompleted, data, f.execution.CreatedAt)
		},
		"wrong_kind": func(f *historyFixture, commands []drt.Command) {
			f.append(drt.EventTimerFired, encode(t, drt.Outcome{Version: 1, CommandID: commands[0].ID}), f.execution.CreatedAt)
		},
		"unsupported_version": func(f *historyFixture, commands []drt.Command) {
			f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: commands[0].ID}), f.execution.CreatedAt)
		},
		"output_and_failure": func(f *historyFixture, commands []drt.Command) {
			f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: commands[0].ID, Output: []byte("result"), Failure: &drt.ApplicationError{Type: "error"}}), f.execution.CreatedAt)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			f := newHistory()
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("a", "lookup", "", nil).Get() }
			decision, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, decision.Commands)
			corrupt(f, decision.Commands)
			if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("invalid result accepted: %v", err)
			}
		})
	}
}

func TestTimerDeadlineAndChangedDelay(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Timer("timer", time.Nanosecond).Get() }
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(first.Commands) != 1 || !first.Commands[0].Deadline.Equal(f.execution.CreatedAt.Add(time.Microsecond)) {
		t.Fatalf("timer should round up: %+v, %v", first, err)
	}
	f.commands(t, first.Commands)
	_, err = drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Timer("timer", time.Second).Get() })
	if !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("changed timer accepted: %v", err)
	}
	f.append(drt.EventTimerFired, encode(t, drt.Outcome{Version: 1, CommandID: "timer"}), f.execution.CreatedAt)
	if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrHistory) {
		t.Fatalf("early timer accepted: %v", err)
	}
}

func TestEvaluationDoesNotAliasHistoryPayloads(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, input []byte) ([]byte, error) {
		input[0] = 'X'
		value, err := w.Activity("a", "lookup", "", []byte("payload")).Get()
		if err != nil {
			return nil, err
		}
		value[0] = 'X'
		return value, nil
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, first.Commands)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "a", Output: []byte("saved")}), f.execution.CreatedAt)
	before := encode(t, f.events)
	done, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(done.Output) != "Xaved" || string(f.execution.Input) != "order-input" || !bytes.Equal(before, encode(t, f.events)) {
		t.Fatalf("caller data changed: %+v, %v", done, err)
	}
}

func TestInvalidFailureCannotEnterHistory(t *testing.T) {
	f := newHistory()
	_, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, &drt.ApplicationError{Message: "missing type"}
	})
	if !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("invalid failure accepted: %v", err)
	}
}

type brokenError struct{}

func (brokenError) Error() string { panic("error formatter bug") }

func TestBrokenApplicationErrorsFailTheTask(t *testing.T) {
	f := newHistory()
	_, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, brokenError{}
	})
	if !errors.Is(err, drt.ErrWorkflowPanic) {
		t.Fatalf("error formatter panic escaped task failure: %v", err)
	}
	_, err = drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) {
		var failure *drt.ApplicationError
		return nil, failure
	})
	if !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("typed nil failure accepted: %v", err)
	}
}
