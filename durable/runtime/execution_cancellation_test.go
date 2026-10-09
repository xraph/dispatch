package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func appendWorkflowCancel(t *testing.T, f *historyFixture, at time.Time) durable.ExecutionCancellation {
	t.Helper()
	request := durable.ExecutionCancellation{Version: 1, RequestID: "cancel", Reason: "requested"}
	f.append(durable.EventCancellationRequested, encode(t, request), at)
	return request
}

func TestWorkflowCancelReplayPhases(t *testing.T) {
	f := newHistory()
	state := ""
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state = "waiting"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		var target *drt.Future
		w.SetCancellationHandler(func(cleanup *drt.Workflow, request durable.ExecutionCancellation) ([]byte, error) {
			if request.Reason != "requested" {
				t.Fatal("reason changed")
			}
			if _, err := target.Get(); !errors.Is(err, drt.ErrWorkflowCancelled) {
				t.Fatalf("root not canceled: %v", err)
			}
			state = "cleaning"
			if _, err := cleanup.Timer("cleanup", time.Second).Get(); err != nil {
				return nil, err
			}
			state = "cleaned"
			return nil, drt.ErrWorkflowCancelled
		})
		target = w.Timer("target", time.Hour)
		return target.Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, d.Commands)
	request := appendWorkflowCancel(t, f, f.execution.CreatedAt.Add(time.Second))
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.CancellationStart == nil || len(d.Commands) != 0 || state != "waiting" {
		t.Fatalf("speculative cleanup: %+v %s %v", d, state, err)
	}
	startedAt := f.execution.CreatedAt.Add(time.Minute)
	f.append(drt.EventCancellationStarted, encode(t, *d.CancellationStart), startedAt)
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.State != durable.StateRunning || len(d.Commands) != 1 || !d.Commands[0].Deadline.Equal(startedAt.Add(time.Second)) || state != "cleaning" {
		t.Fatalf("cleanup clock: %+v %s %v", d, state, err)
	}
	f.commands(t, d.Commands)
	f.append(drt.EventTimerFired, encode(t, drt.Outcome{Version: 1, CommandID: "cleanup"}), startedAt.Add(time.Second))
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.State != durable.StateCancelled || len(d.Output) != 0 || state != "cleaned" {
		t.Fatalf("cancelled state: %+v %s %v", d, state, err)
	}
	f.append(drt.EventWorkflowCancelled, encode(t, request), startedAt.Add(time.Second))
	f.execution.State = durable.StateCancelled
	q, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
	if err != nil || string(q.Output) != "cleaned" || q.State != durable.StateCancelled {
		t.Fatalf("terminal query: %+v %v", q, err)
	}
}

func TestWorkflowCancelFrozenNormalReplay(t *testing.T) {
	for _, before := range []bool{false, true} {
		f := newHistory()
		handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
			state := "waiting"
			var work *drt.Future
			w.SetCancellationHandler(func(_ *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
				result, err := work.Get()
				if err != nil {
					return nil, err
				}
				return []byte(state + ":" + string(result)), nil
			})
			work = w.Activity("work", "work", "", nil)
			result, err := work.Get()
			if err != nil {
				return nil, err
			}
			state = string(result)
			return w.Timer("later", time.Hour).Get()
		}
		d, err := drt.Evaluate(f.execution, f.events, handler)
		if err != nil {
			t.Fatal(err)
		}
		f.commands(t, d.Commands)
		at := f.execution.CreatedAt.Add(time.Second)
		outcome := encode(t, drt.Outcome{Version: 1, CommandID: "work", Output: []byte("done")})
		if before {
			f.append(drt.EventActivityCompleted, outcome, at)
		}
		appendWorkflowCancel(t, f, at)
		if !before {
			f.append(drt.EventActivityCompleted, outcome, at)
		}
		d, err = drt.Evaluate(f.execution, f.events, handler)
		if err != nil || d.CancellationStart == nil {
			t.Fatalf("fence: %+v %v", d, err)
		}
		f.append(drt.EventCancellationStarted, encode(t, *d.CancellationStart), at)
		d, err = drt.Evaluate(f.execution, f.events, handler)
		want := "waiting:done"
		if before {
			want = "done:done"
		}
		if err != nil || d.State != durable.StateCompleted || string(d.Output) != want || len(d.Commands) != 0 {
			t.Fatalf("frozen before=%t: %+v %v", before, d, err)
		}
	}
}

func TestWorkflowCancelInterruptedSelector(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetCancellationHandler(func(cleanup *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
			return cleanup.ReceiveSignal("saved", "approve").Get()
		})
		a := w.ReceiveSignal("approval", "approve")
		timer := w.Timer("timeout", time.Hour)
		return w.Select("race", a, timer).Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, d.Commands)
	at := f.execution.CreatedAt.Add(time.Second)
	appendWorkflowCancel(t, f, at)
	appendSignal(t, f, "message", "approve", "saved", at)
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.CancellationStart == nil {
		t.Fatalf("fence: %+v %v", d, err)
	}
	f.append(drt.EventCancellationStarted, encode(t, *d.CancellationStart), at)
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(d.Output) != "saved" || len(d.Signals) != 1 {
		t.Fatalf("buffered cleanup: %+v %v", d, err)
	}
	f.commands(t, d.Commands)
	for _, consumed := range d.Signals {
		f.append(drt.EventSignalConsumed, encode(t, consumed), at)
	}
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(d.Output) != "saved" || len(d.Signals) != 0 {
		t.Fatalf("selector cleanup replay: %+v %v", d, err)
	}
}

func TestWorkflowCancelRegistrationGuards(t *testing.T) {
	for _, mode := range []string{"nil", "duplicate", "query", "recovered_query", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				cleanup := func(_ *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
					return nil, drt.ErrWorkflowCancelled
				}
				switch mode {
				case "nil":
					w.SetCancellationHandler(nil)
				case "duplicate":
					w.SetCancellationHandler(cleanup)
					w.SetCancellationHandler(cleanup)
				case "cleanup":
					w.SetCancellationHandler(func(c *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
						c.SetCancellationHandler(cleanup)
						return nil, nil
					})
				default:
					w.SetQueryHandler("status", func(_ []byte) ([]byte, error) {
						if mode == "recovered_query" {
							defer func() { _ = recover() }()
						}
						w.SetCancellationHandler(cleanup)
						return nil, nil
					})
				}
				return w.Timer("wait", time.Hour).Get()
			}
			if mode == "query" || mode == "recovered_query" {
				if _, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status")); !errors.Is(err, drt.ErrQueryMutation) {
					t.Fatalf("query guard: %v", err)
				}
			} else if mode == "cleanup" {
				appendWorkflowCancel(t, f, f.execution.CreatedAt)
				d, err := drt.Evaluate(f.execution, f.events, handler)
				if err != nil || d.CancellationStart == nil {
					t.Fatalf("fence: %+v %v", d, err)
				}
				f.append(drt.EventCancellationStarted, encode(t, *d.CancellationStart), f.execution.CreatedAt)
				if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, durable.ErrInvalid) {
					t.Fatalf("cleanup registration accepted: %v", err)
				}
			} else if _, err := drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("registration accepted: %v", err)
			}
		})
	}
}

func TestWorkflowCancelRejectsMalformedPhases(t *testing.T) {
	for _, mode := range []string{"request_version", "request_duplicate", "request_reason", "start_without_request", "start_version", "start_request", "start_count", "start_duplicate", "command_between", "waiting_between", "terminal_without_start", "terminal_request", "terminal_output", "late_outcome", "partial_failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			at := f.execution.CreatedAt
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				return w.ActivityWithOptions("work", "work", "", nil, retryOptions(1)).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			request := durable.ExecutionCancellation{Version: 1, RequestID: "cancel", Reason: "requested"}
			if mode == "request_version" {
				request.Version = 2
			}
			if mode == "request_reason" {
				request.Reason = "a\x00b"
			}
			if mode != "start_without_request" {
				f.append(durable.EventCancellationRequested, encode(t, request), at)
			}
			if mode == "request_duplicate" {
				f.append(durable.EventCancellationRequested, encode(t, request), at)
			}
			if mode == "command_between" {
				f.append(drt.EventCommandScheduled, encode(t, drt.Command{Version: 1, Index: 2, ID: "extra", Kind: durable.TaskActivity, Name: "work"}), at)
			}
			if mode == "waiting_between" {
				f.append(drt.EventWorkflowWaiting, nil, at)
			}
			if mode == "partial_failure" {
				f.append(drt.EventActivityAttemptStarted, encode(t, drt.ActivityAttempt{Version: 1, CommandID: "work", Attempt: 1, Epoch: 1}), at)
				f.append(drt.EventActivityAttemptFailed, encode(t, drt.ActivityAttempt{Version: 1, CommandID: "work", Attempt: 1, Epoch: 1, Failure: &drt.ApplicationError{Type: "failed"}}), at)
			}
			started := drt.CancellationStart{Version: 1, RequestID: "cancel", CommandCount: 1}
			switch mode {
			case "start_version":
				started.Version = 2
			case "start_request":
				started.RequestID = "other"
			case "start_count":
				started.CommandCount = 2
			}
			if mode != "terminal_without_start" {
				f.append(drt.EventCancellationStarted, encode(t, started), at)
			}
			if mode == "start_duplicate" {
				f.append(drt.EventCancellationStarted, encode(t, started), at)
			}
			if mode == "late_outcome" {
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "work", Attempt: 1, Output: []byte("stale")}), at)
			}
			if mode == "terminal_without_start" || mode == "terminal_request" || mode == "terminal_output" {
				if mode == "terminal_request" {
					request.RequestID = "changed"
				}
				f.append(drt.EventWorkflowCancelled, encode(t, request), at)
				f.execution.State = durable.StateCancelled
				if mode == "terminal_output" {
					f.execution.Output = []byte("bad")
				}
			}
			if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("invalid phase accepted: %v", err)
			}
		})
	}
}

func TestWorkflowCancelRejectsChangedReplay(t *testing.T) {
	for _, changed := range []string{"normal", "cleanup", "removed", "panic", "recovered"} {
		t.Run(changed, func(t *testing.T) {
			f := newHistory()
			mode := "original"
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				if mode != "removed" {
					w.SetCancellationHandler(func(cleanup *drt.Workflow, _ durable.ExecutionCancellation) ([]byte, error) {
						if mode == "panic" {
							panic("cleanup failure")
						}
						id := "cleanup"
						if mode == "cleanup" || mode == "recovered" {
							id = "changed"
						}
						if mode == "recovered" {
							func() { defer func() { _ = recover() }(); cleanup.Timer(id, time.Second) }()
							return nil, nil
						}
						return cleanup.Timer(id, time.Second).Get()
					})
				}
				id := "normal"
				if mode == "normal" {
					id = "changed"
				}
				return w.Timer(id, time.Hour).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			appendWorkflowCancel(t, f, f.execution.CreatedAt)
			f.append(drt.EventCancellationStarted, encode(t, drt.CancellationStart{Version: 1, RequestID: "cancel", CommandCount: 1}), f.execution.CreatedAt)
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, d.Commands)
			mode = changed
			_, err = drt.Evaluate(f.execution, f.events, handler)
			want := drt.ErrNondeterministic
			if changed == "panic" {
				want = drt.ErrWorkflowPanic
			}
			if !errors.Is(err, want) {
				t.Fatalf("changed replay accepted: %v want %v", err, want)
			}
		})
	}
}
