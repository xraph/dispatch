package runtime_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestExecutionTimeoutFrozenReplay(t *testing.T) {
	for _, phase := range []string{"initial", "normal", "accepted", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			f := newHistory()
			f.execution.RunDeadlineAt = f.execution.CreatedAt.Add(10 * time.Second)
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				state := "normal"
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return []byte(state), nil })
				w.SetCancellationHandler(func(cleanup *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
					state = "cleanup"
					return cleanup.Timer("cleanup", time.Hour).Get()
				})
				return w.Timer("wait", time.Hour).Get()
			}
			if phase != "initial" {
				d, err := drt.Evaluate(f.execution, f.events, handler)
				if err != nil {
					t.Fatal(err)
				}
				f.commands(t, d.Commands)
			}
			if phase == "accepted" || phase == "cleanup" {
				appendWorkflowCancel(t, f, f.execution.CreatedAt.Add(time.Second))
			}
			if phase == "cleanup" {
				d, err := drt.Evaluate(f.execution, f.events, handler)
				if err != nil {
					t.Fatal(err)
				}
				f.append(drt.EventCancellationStarted, encode(t, *d.CancellationStart), f.execution.UpdatedAt)
				d, err = drt.Evaluate(f.execution, f.events, handler)
				if err != nil {
					t.Fatal(err)
				}
				f.commands(t, d.Commands)
			}
			timeout := durable.ExecutionTimeout{Version: 1, Kind: durable.TimeoutRun, DeadlineAt: f.execution.RunDeadlineAt}
			f.append(durable.EventWorkflowTimedOut, encode(t, timeout), timeout.DeadlineAt)
			f.execution.State = durable.StateTimedOut
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil || d.State != durable.StateTimedOut || len(d.Commands) != 0 || len(d.Signals) != 0 || len(d.Selections) != 0 || d.CancellationStart != nil {
				t.Fatalf("frozen replay: %+v %v", d, err)
			}
			query, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
			want := "normal"
			if phase == "cleanup" {
				want = "cleanup"
			}
			if err != nil || query.State != durable.StateTimedOut || string(query.Output) != want {
				t.Fatalf("timeout query: %+v %v", query, err)
			}
		})
	}
}

func TestExecutionTimeoutMalformedReplay(t *testing.T) {
	for _, mode := range []string{"version", "kind", "deadline", "missing_limit", "tie", "early", "output", "unknown_field", "changed_command"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			f.execution.RunDeadlineAt = f.execution.CreatedAt.Add(time.Second)
			id := "wait"
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Timer(id, time.Hour).Get() }
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			payload := durable.ExecutionTimeout{Version: 1, Kind: durable.TimeoutRun, DeadlineAt: f.execution.RunDeadlineAt}
			at := payload.DeadlineAt
			switch mode {
			case "version":
				payload.Version = 2
			case "kind":
				payload.Kind = "unknown"
			case "deadline":
				payload.DeadlineAt = at.Add(time.Second)
			case "missing_limit":
				f.execution.RunDeadlineAt = time.Time{}
			case "tie":
				f.execution.ExecutionDeadlineAt = f.execution.RunDeadlineAt
			case "early":
				at = at.Add(-time.Microsecond)
			case "output":
				f.execution.Output = []byte("late result")
			case "changed_command":
				id = "different"
			}
			raw := encode(t, payload)
			if mode == "unknown_field" {
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"extra":true`), 1)
			}
			f.append(durable.EventWorkflowTimedOut, raw, at)
			f.execution.State = durable.StateTimedOut
			_, err = drt.Evaluate(f.execution, f.events, handler)
			want := drt.ErrHistory
			if mode == "changed_command" {
				want = drt.ErrNondeterministic
			}
			if !errors.Is(err, want) {
				t.Fatalf("accepted malformed timeout: %v", err)
			}
		})
	}
}

func TestChildExecutionTimeoutReplay(t *testing.T) {
	for _, mode := range []string{"valid", "kind", "deadline", "early", "no_limit", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			options := drt.ChildOptions{RunTimeout: time.Second, ExecutionTimeout: 2 * time.Second}
			if mode == "no_limit" {
				options = drt.ChildOptions{}
			}
			var observed *drt.ChildWorkflowError
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				child := w.ChildWorkflow("child", "child", nil, options)
				_, err := child.Get()
				if !errors.As(err, &observed) || !errors.Is(err, drt.ErrChildTimedOut) || observed.Timeout == nil {
					t.Fatalf("child timeout lost: %v", err)
				}
				// A caller cannot corrupt another Get's saved timeout metadata.
				observed.Timeout.Kind = "mutated"
				_, err = child.Get()
				if !errors.As(err, &observed) {
					t.Fatal(err)
				}
				return []byte("handled"), nil
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			started := appendChildStarted(t, f, d.Commands[0])
			timeout := durable.ExecutionTimeout{Version: 1, Kind: durable.TimeoutRun, DeadlineAt: f.execution.CreatedAt.Add(time.Second)}
			at := timeout.DeadlineAt
			switch mode {
			case "kind":
				timeout.Kind = durable.TimeoutExecution
			case "deadline":
				timeout.DeadlineAt = at.Add(time.Second)
			case "early":
				at = at.Add(-time.Microsecond)
			}
			raw := encode(t, timeout)
			if mode == "mixed" {
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"type":"timeout","message":"legacy"`), 1)
			}
			msg := durable.ChildMessage{Version: 1, CommandID: "child", Parent: f.execution.Key, Child: started.Child, Policy: started.ParentClosePolicy, State: durable.StateTimedOut, CloseEvent: durable.EventInput{Type: durable.EventWorkflowTimedOut, Payload: raw}}
			f.append(durable.EventChildCompleted, encode(t, msg), at)
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if mode != "valid" {
				if !errors.Is(err, drt.ErrHistory) {
					t.Fatalf("malformed child timeout: %v", err)
				}
				return
			}
			if err != nil || d.State != durable.StateCompleted || observed == nil || observed.Timeout == nil || observed.Timeout.Kind != durable.TimeoutRun {
				t.Fatalf("typed child timeout: %+v %+v %v", d, observed, err)
			}
		})
	}
}

func TestChildExecutionTimeoutOptions(t *testing.T) {
	for _, mode := range []string{"run", "execution", "negative", "submicrosecond"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			options := drt.ChildOptions{RunTimeout: time.Second, ExecutionTimeout: 2 * time.Second}
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				return w.ChildWorkflow("child", "child", nil, options).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			appendChildStarted(t, f, d.Commands[0])
			want := drt.ErrNondeterministic
			switch mode {
			case "run":
				options.RunTimeout = 3 * time.Second
			case "execution":
				options.ExecutionTimeout = 3 * time.Second
			case "negative":
				options.RunTimeout = -1
				want = durable.ErrInvalid
			case "submicrosecond":
				options.ExecutionTimeout = 1
				want = durable.ErrInvalid
			}
			if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, want) {
				t.Fatalf("changed deadline policy: %v", err)
			}
		})
	}
}
