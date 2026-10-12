package runtime_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func appendCancellation(t *testing.T, f *historyFixture, d drt.Decision, c drt.Cancellation, at time.Time) {
	t.Helper()
	for _, command := range d.Commands {
		f.append(drt.EventCommandScheduled, encode(t, command), at)
	}
	for _, consumed := range d.Signals {
		f.append(drt.EventSignalConsumed, encode(t, consumed), at)
	}
	f.append(drt.EventFutureCancelled, encode(t, c), at)
	f.append(drt.EventWorkflowWaiting, nil, at)
}

func TestCancelWaitsForSavedAcknowledgment(t *testing.T) {
	for _, kind := range []string{"activity", "timer", "signal"} {
		t.Run(kind, func(t *testing.T) {
			f := newHistory()
			var clock time.Time
			resolved := false
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				var target *drt.Future
				switch kind {
				case "activity":
					target = w.Activity("target", "work", "", nil)
				case "timer":
					target = w.Timer("target", time.Hour)
				default:
					target = w.ReceiveSignal("target", "approve")
				}
				ack := w.Cancel("stop", target)
				if _, err := ack.Get(); err != nil {
					return nil, err
				}
				resolved = true
				clock = w.Now()
				if winner := w.Select("ack-first", ack, target); winner != ack {
					t.Fatal("shared cancellation sequence did not use candidate order")
				}
				_, err := target.Get()
				var cancelled *drt.CancelledError
				if !errors.Is(err, drt.ErrCancelled) || !errors.As(err, &cancelled) || cancelled.CommandID != "target" {
					t.Fatalf("cancellation error: %v", err)
				}
				return w.Timer("after", time.Second).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil || resolved || d.State != durable.StateRunning || len(d.Commands) != 2 {
				t.Fatalf("speculative cancellation: %+v %v resolved=%t", d, err, resolved)
			}
			at := f.execution.CreatedAt.Add(time.Minute)
			appendCancellation(t, f, d, drt.Cancellation{Version: 1, CommandID: "stop", TargetID: "target", Cancelled: true}, at)
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || !resolved || !clock.Equal(at) || len(d.Commands) != 2 || !d.Commands[1].Deadline.Equal(at.Add(time.Second)) {
				t.Fatalf("saved cancellation time: %+v %v %s", d, err, clock)
			}
			appendSelectionDecision(t, f, d, at.Add(time.Minute))
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || len(d.Commands) != 0 || len(d.Selections) != 0 || !clock.Equal(at) {
				t.Fatalf("replay changed clock: %+v %v", d, err)
			}
		})
	}
}

func TestCancelReceiveLeavesBufferedSignal(t *testing.T) {
	f := newHistory()
	appendSignal(t, f, "message", "approve", "approved", f.execution.CreatedAt)
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		old := w.ReceiveSignal("old", "approve")
		if _, err := w.Cancel("stop", old).Get(); err != nil {
			return nil, err
		}
		if _, err := old.Get(); !errors.Is(err, drt.ErrCancelled) {
			return nil, errors.New("receive was not canceled")
		}
		return w.ReceiveSignal("new", "approve").Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(d.Signals) != 0 {
		t.Fatalf("cancel consumed message: %+v %v", d, err)
	}
	appendCancellation(t, f, d, drt.Cancellation{Version: 1, CommandID: "stop", TargetID: "old", Cancelled: true}, f.execution.CreatedAt.Add(time.Second))
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(d.Output) != "approved" || len(d.Signals) != 1 || d.Signals[0].CommandID != "new" {
		t.Fatalf("buffer lost: %+v %v", d, err)
	}
}

func TestCancelPreservesCompletedTargetAndRepeatedRequests(t *testing.T) {
	f := newHistory()
	at := f.execution.CreatedAt
	f.append(drt.EventCommandScheduled, encode(t, drt.Command{Version: 1, Index: 1, ID: "target", Kind: durable.TaskActivity, Name: "work"}), at)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "target", Output: []byte("done")}), at)
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		target := w.Activity("target", "work", "", nil)
		if _, err := w.Cancel("first", target).Get(); err != nil {
			return nil, err
		}
		if _, err := w.Cancel("second", target).Get(); err != nil {
			return nil, err
		}
		return target.Get()
	}
	for _, id := range []string{"first", "second"} {
		d, err := drt.Evaluate(f.execution, f.events, handler)
		if err != nil || d.State != durable.StateRunning {
			t.Fatalf("request: %+v %v", d, err)
		}
		appendCancellation(t, f, d, drt.Cancellation{Version: 1, CommandID: id, TargetID: "target"}, at.Add(time.Minute))
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(d.Output) != "done" {
		t.Fatalf("completion overwritten: %+v %v", d, err)
	}
}

func TestCancelRejectsInvalidOwnershipAndQueries(t *testing.T) {
	f := newHistory()
	var foreign *drt.Future
	_, err := drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		foreign = w.Activity("foreign", "work", "", nil)
		return foreign.Get()
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"nil", "zero", "foreign", "ack", "bad_id", "query", "recovered_query"} {
		t.Run(mode, func(t *testing.T) {
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				target := w.Activity("target", "work", "", nil)
				switch mode {
				case "nil":
					w.Cancel("stop", nil)
				case "zero":
					w.Cancel("stop", &drt.Future{})
				case "foreign":
					w.Cancel("stop", foreign)
				case "ack":
					w.Cancel("stop-again", w.Cancel("stop", target))
				case "bad_id":
					w.Cancel(" ", target)
				default:
					w.SetQueryHandler("state", func(_ []byte) ([]byte, error) {
						if mode == "recovered_query" {
							defer func() { _ = recover() }()
						}
						w.Cancel("stop", nil)
						return nil, nil
					})
				}
				return target.Get()
			}
			if mode == "query" || mode == "recovered_query" {
				_, queryErr := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "state"))
				if !errors.Is(queryErr, drt.ErrQueryMutation) {
					t.Fatalf("query cancellation: %v", queryErr)
				}
			} else if _, err := drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("invalid target accepted: %v", err)
			}
		})
	}
}

func TestCancelReservesAcknowledgmentBudget(t *testing.T) {
	for _, count := range []int{499, 500} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newHistory()
			d, err := drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
				target := w.Activity("target", "work", "", nil)
				var ack *drt.Future
				for i := range count {
					ack = w.Cancel(fmt.Sprint(i), target)
				}
				return ack.Get()
			})
			if count == 499 {
				if err != nil || len(d.Commands) != 500 {
					t.Fatalf("valid decision: %+v %v", d, err)
				}
			} else if !errors.Is(err, durable.ErrInvalid) || len(d.Commands) != 0 {
				t.Fatalf("oversize decision returned: %+v %v", d, err)
			}
		})
	}
}
