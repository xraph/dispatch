package runtime_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func appendChildStarted(t *testing.T, f *historyFixture, c drt.Command) durable.ChildStarted {
	t.Helper()
	queue := c.Queue
	if queue == "" {
		queue = "orders"
	}
	started := durable.ChildStarted{Version: 1, CommandID: c.ID, Child: c.Child.Key, WorkflowType: c.Name, BuildID: c.Child.BuildID, Queue: queue, ParentClosePolicy: c.Child.ParentClosePolicy}
	f.append(durable.EventChildStarted, encode(t, started), f.execution.UpdatedAt)
	return started
}

func appendChildOutcome(t *testing.T, f *historyFixture, started durable.ChildStarted, state durable.State) {
	t.Helper()
	message := durable.ChildMessage{Version: 1, CommandID: started.CommandID, Parent: f.execution.Key, Child: started.Child, Policy: started.ParentClosePolicy, State: state}
	switch state {
	case durable.StateCompleted:
		message.Output = []byte("child result")
		message.CloseEvent = durable.EventInput{Type: drt.EventWorkflowCompleted, Payload: message.Output}
	case durable.StateFailed:
		message.CloseEvent = durable.EventInput{Type: drt.EventWorkflowFailed, Payload: encode(t, drt.ApplicationError{Type: "declined", Message: "child failed", NonRetryable: true})}
	case durable.StateCancelled:
		message.CloseEvent = durable.EventInput{Type: drt.EventWorkflowCancelled, Payload: encode(t, durable.ExecutionCancellation{Version: 1, RequestID: "cancel", Reason: "requested"})}
	case durable.StateTerminated:
		message.CloseEvent = durable.EventInput{Type: durable.EventWorkflowTerminated, Payload: encode(t, durable.ExecutionTermination{Version: 1, RequestID: "terminate", Reason: "parent closed"})}
	case durable.StateTimedOut:
		message.CloseEvent = durable.EventInput{Type: drt.EventWorkflowTimedOut, Payload: encode(t, drt.ApplicationError{Type: "timeout", Message: "workflow deadline"})}
	}
	f.append(durable.EventChildCompleted, encode(t, message), f.execution.UpdatedAt.Add(time.Second))
}

func TestChildIdentityAndStartedReplay(t *testing.T) {
	f := newHistory()
	var observed durable.Key
	handler := func(w *drt.Workflow, input []byte) ([]byte, error) {
		child := w.ChildWorkflow("shipment", "ship", input, drt.ChildOptions{})
		key, err := child.Started()
		if err != nil {
			return nil, err
		}
		observed = key
		return child.Get()
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || first.State != durable.StateRunning || len(first.Commands) != 1 || observed != (durable.Key{}) {
		t.Fatalf("uncommitted start acknowledged: %+v %+v %v", first, observed, err)
	}
	command := first.Commands[0]
	if command.Child == nil || command.Child.Key.Namespace != f.execution.Namespace || command.Child.Key.RunID == "" || command.Child.Key.WorkflowID == f.execution.WorkflowID || command.Child.BuildID != f.execution.BuildID || command.Child.ParentClosePolicy != durable.ParentCloseTerminate {
		t.Fatalf("child identity: %+v", command)
	}
	again, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || !reflect.DeepEqual(again, first) {
		t.Fatalf("unstable child command: %+v %v", again, err)
	}
	other := newHistory()
	other.execution.RunID = "another-run"
	otherDecision, err := drt.Evaluate(other.execution, other.events, handler)
	if err != nil || otherDecision.Commands[0].Child.Key == command.Child.Key {
		t.Fatalf("run collision: %+v %v", otherDecision, err)
	}
	f.commands(t, first.Commands)
	started := appendChildStarted(t, f, command)
	pending, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || pending.State != durable.StateRunning || len(pending.Commands) != 0 || observed != started.Child {
		t.Fatalf("saved start: %+v %+v %v", pending, observed, err)
	}
	appendChildOutcome(t, f, started, durable.StateCompleted)
	done, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || done.State != durable.StateCompleted || string(done.Output) != "child result" {
		t.Fatalf("child result: %+v %v", done, err)
	}
}

func TestChildTerminalResults(t *testing.T) {
	for _, state := range []durable.State{durable.StateCompleted, durable.StateFailed, durable.StateCancelled, durable.StateTerminated, durable.StateTimedOut} {
		t.Run(string(state), func(t *testing.T) {
			f := newHistory()
			var childErr error
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				output, err := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{}).Get()
				childErr = err
				return output, nil
			}
			decision, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, decision.Commands)
			started := appendChildStarted(t, f, decision.Commands[0])
			appendChildOutcome(t, f, started, state)
			decision, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || decision.State != durable.StateCompleted {
				t.Fatalf("result replay: %+v %v", decision, err)
			}
			if state == durable.StateCompleted {
				if childErr != nil || string(decision.Output) != "child result" {
					t.Fatalf("success: %+v %v", decision, childErr)
				}
				return
			}
			var failure *drt.ChildWorkflowError
			if !errors.As(childErr, &failure) || failure.Child != started.Child || failure.State != state {
				t.Fatalf("lost lifecycle cause: %+v", childErr)
			}
			if state == durable.StateFailed {
				var application *drt.ApplicationError
				if !errors.As(childErr, &application) || application.Type != "declined" {
					t.Fatalf("lost application cause: %v", childErr)
				}
			}
		})
	}
}

func TestChildChangedReplay(t *testing.T) {
	for _, mode := range []string{"identity", "build", "queue", "policy", "name", "input", "omitted"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			options := drt.ChildOptions{WorkflowID: "business-child", BuildID: "child-v1", Queue: "children", ParentClosePolicy: durable.ParentCloseAbandon}
			name, input := "ship", []byte("input")
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				return w.ChildWorkflow("child", name, input, options).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			appendChildStarted(t, f, d.Commands[0])
			switch mode {
			case "identity":
				options.WorkflowID = "other"
			case "build":
				options.BuildID = "other"
			case "queue":
				options.Queue = "other"
			case "policy":
				options.ParentClosePolicy = durable.ParentCloseTerminate
			case "name":
				name = "other"
			case "input":
				input = []byte("other")
			case "omitted":
				handler = func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil }
			}
			if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrNondeterministic) {
				t.Fatalf("changed %s accepted: %v", mode, err)
			}
		})
	}
}

func TestChildSelectorAndQueryGuards(t *testing.T) {
	for _, operation := range []string{"schedule", "started", "get", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			f := newHistory()
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				child := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{})
				w.SetQueryHandler("mutate", func([]byte) ([]byte, error) {
					switch operation {
					case "schedule":
						w.ChildWorkflow("extra", "child", nil, drt.ChildOptions{})
					case "started":
						_, _ = child.Started()
					case "get":
						_, _ = child.Get()
					case "cancel":
						w.CancelChild("cancel", child)
					}
					return nil, nil
				})
				return w.Select("choose", child, w.Timer("timer", time.Hour)).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			started := appendChildStarted(t, f, d.Commands[0])
			appendChildOutcome(t, f, started, durable.StateCompleted)
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || d.State != durable.StateCompleted || len(d.Selections) != 1 || d.Selections[0].FutureID != "child" {
				t.Fatalf("child selector: %+v %v", d, err)
			}
			if _, err = drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "mutate")); !errors.Is(err, drt.ErrQueryMutation) {
				t.Fatalf("query %s mutated: %v", operation, err)
			}
		})
	}
}

func TestChildCancellationWaitsForRealResult(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		var child *drt.Future
		w.SetCancellationHandler(func(*drt.Workflow, durable.ExecutionCancellation) ([]byte, error) {
			_, err := child.Get()
			if err != nil {
				return nil, err
			}
			return nil, drt.ErrWorkflowCancelled
		})
		child = w.ChildWorkflow("child", "child", nil, drt.ChildOptions{})
		return child.Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, d.Commands)
	started := appendChildStarted(t, f, d.Commands[0])
	appendWorkflowCancel(t, f, f.execution.UpdatedAt.Add(time.Second))
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.CancellationStart == nil {
		t.Fatalf("fence: %+v %v", d, err)
	}
	f.append(drt.EventCancellationStarted, encode(t, *d.CancellationStart), f.execution.UpdatedAt)
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.State != durable.StateRunning {
		t.Fatalf("child cancellation fabricated result: %+v %v", d, err)
	}
	appendChildOutcome(t, f, started, durable.StateCompleted)
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.State != durable.StateCancelled {
		t.Fatalf("cleanup result: %+v %v", d, err)
	}
}

func TestChildForcedTerminationQuery(t *testing.T) {
	for _, phase := range []string{"normal", "accepted", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			f := newHistory()
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				state := "normal"
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return []byte(state), nil })
				w.SetCancellationHandler(func(cleanup *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
					state = "cleanup"
					return cleanup.Timer("cleanup", time.Hour).Get()
				})
				return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{}).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			appendChildStarted(t, f, d.Commands[0])
			if phase != "normal" {
				appendWorkflowCancel(t, f, f.execution.UpdatedAt.Add(time.Second))
			}
			if phase == "cleanup" {
				d, err = drt.Evaluate(f.execution, f.events, handler)
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
			f.append(durable.EventWorkflowTerminated, encode(t, durable.ExecutionTermination{Version: 1, RequestID: "terminate", Reason: "parent closed"}), f.execution.UpdatedAt.Add(time.Second))
			f.execution.State = durable.StateTerminated
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || d.State != durable.StateTerminated || len(d.Commands) != 0 || d.CancellationStart != nil {
				t.Fatalf("forced termination replay: %+v %v", d, err)
			}
			result, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
			want := "normal"
			if phase == "cleanup" {
				want = "cleanup"
			}
			if err != nil || result.State != durable.StateTerminated || string(result.Output) != want {
				t.Fatalf("terminated query: %+v %v", result, err)
			}
		})
	}
}

func TestChildMalformedHistory(t *testing.T) {
	for _, mode := range []string{"missing_start", "duplicate_start", "wrong_child", "wrong_build", "wrong_policy", "wrong_output", "duplicate_result", "result_before_start", "unknown_cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{}).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			if mode == "missing_start" {
				if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrHistory) {
					t.Fatalf("uncreated pending child accepted: %v", err)
				}
				return
			}
			started := appendChildStarted(t, f, d.Commands[0])
			switch mode {
			case "duplicate_start":
				appendChildStarted(t, f, d.Commands[0])
			case "wrong_child":
				started.Child.RunID = "other"
				f.events[len(f.events)-1].Payload = encode(t, started)
			case "wrong_build":
				started.BuildID = "other"
				f.events[len(f.events)-1].Payload = encode(t, started)
			case "wrong_policy":
				started.ParentClosePolicy = durable.ParentCloseAbandon
				f.events[len(f.events)-1].Payload = encode(t, started)
			default:
				if mode == "result_before_start" {
					f.events = f.events[:len(f.events)-1]
					f.execution.LastSequence--
				}
				appendChildOutcome(t, f, started, durable.StateCompleted)
				if mode == "duplicate_result" {
					appendChildOutcome(t, f, started, durable.StateCompleted)
				}
				if mode == "wrong_output" {
					event := &f.events[len(f.events)-1]
					var message durable.ChildMessage
					if err = json.Unmarshal(event.Payload, &message); err != nil {
						t.Fatal(err)
					}
					message.Output = []byte("changed")
					event.Payload = encode(t, message)
				}
				if mode == "unknown_cancel" {
					message := durable.ChildMessage{Version: 1, CommandID: "child", CancellationID: "unknown", Parent: f.execution.Key, Child: started.Child, Policy: started.ParentClosePolicy, Disposition: durable.ChildDeliveryApplied}
					f.append(durable.EventChildCancellationAcknowledged, encode(t, message), f.execution.UpdatedAt)
				}
			}
			if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("malformed %s accepted: %v", mode, err)
			}
		})
	}
}

func TestChildInvalidOptionsAndDecisionCapacity(t *testing.T) {
	for _, mode := range []string{"parent", "self", "build", "queue", "policy", "input", "capacity", "foreign_start"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			if mode == "parent" {
				f.execution.RunID = ""
			}
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				options := drt.ChildOptions{}
				input := []byte(nil)
				switch mode {
				case "self":
					options.WorkflowID = f.execution.WorkflowID
				case "build":
					options.BuildID = "\x00"
				case "queue":
					options.Queue = "\xff"
				case "policy":
					options.ParentClosePolicy = "unknown"
				case "input":
					input = make([]byte, (1<<20)+1)
				case "capacity":
					for i := range 500 {
						w.ChildWorkflow(fmt.Sprintf("child-%d", i), "child", nil, options)
					}
				case "foreign_start":
					_, _ = w.Timer("timer", time.Second).Started()
				}
				return w.ChildWorkflow("child", "child", input, options).Get()
			}
			if _, err := drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("invalid %s accepted: %v", mode, err)
			}
		})
	}
}
