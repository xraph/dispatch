package runtime_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func appendSelectionDecision(t *testing.T, f *historyFixture, d drt.Decision, at time.Time) {
	t.Helper()
	for _, command := range d.Commands {
		f.append(drt.EventCommandScheduled, encode(t, command), at)
	}
	for _, consumed := range d.Signals {
		f.append(drt.EventSignalConsumed, encode(t, consumed), at)
	}
	for _, selected := range d.Selections {
		f.append(drt.EventSelected, encode(t, selected), at)
	}
	f.append(drt.EventWorkflowWaiting, nil, at)
}

func TestSelectPreservesWinnerAfterLaterResults(t *testing.T) {
	for _, signalFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(signalFirst), func(t *testing.T) {
			f := newHistory()
			var selectedAt time.Time
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				approval := w.ReceiveSignal("approval", "approve")
				timer := w.Timer("deadline", time.Second)
				winner := w.Select("race", approval, timer)
				selectedAt = w.Now()
				result := "timeout"
				if winner == approval {
					value, err := winner.Get()
					if err != nil {
						return nil, err
					}
					result = string(value)
				}
				if _, err := w.ReceiveSignal("finish", "finish").Get(); err != nil {
					return nil, err
				}
				return []byte(result), nil
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil || d.State != durable.StateRunning || len(d.Commands) != 3 || len(d.Selections) != 0 {
				t.Fatalf("initial: %+v %v", d, err)
			}
			appendSelectionDecision(t, f, d, f.execution.CreatedAt)
			at := f.execution.CreatedAt.Add(time.Second)
			addSignal := func() { appendSignal(t, f, "approved", "approve", "approved", at) }
			addTimer := func() { f.append(drt.EventTimerFired, encode(t, drt.Outcome{Version: 1, CommandID: "deadline"}), at) }
			want, winnerID := "timeout", "deadline"
			if signalFirst {
				addSignal()
				want, winnerID = "approved", "approval"
			} else {
				addTimer()
			}
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || len(d.Selections) != 1 || d.Selections[0].FutureID != winnerID || !selectedAt.Equal(at) {
				t.Fatalf("winner: %+v %v %s", d, err, selectedAt)
			}
			appendSelectionDecision(t, f, d, at.Add(time.Hour))
			if signalFirst {
				addTimer()
			} else {
				addSignal()
			}
			appendSignal(t, f, "finish", "finish", "", at.Add(2*time.Hour))
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || string(d.Output) != want || len(d.Selections) != 0 || !selectedAt.Equal(at) {
				t.Fatalf("changed winner: %+v %v %s", d, err, selectedAt)
			}
			appendSelectionDecision(t, f, d, at.Add(2*time.Hour))
			f.append(drt.EventWorkflowCompleted, d.Output, at.Add(2*time.Hour))
			f.execution.State = durable.StateCompleted
			f.execution.Output = d.Output
			d, err = drt.Evaluate(f.execution, f.events, handler)
			if err != nil || string(d.Output) != want || len(d.Selections) != 0 || len(d.Signals) != 0 {
				t.Fatalf("closed replay: %+v %v", d, err)
			}
		})
	}
}

func TestSelectUsesHistorySequenceAndReturnsFailures(t *testing.T) {
	f := newHistory()
	var clock time.Time
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.Activity("a", "work", "", nil)
		b := w.Activity("b", "work", "", nil)
		selected := w.Select("race", a, b)
		clock = w.Now()
		return selected.Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	appendSelectionDecision(t, f, d, f.execution.CreatedAt)
	failure := &drt.ApplicationError{Type: "declined", Message: "declined"}
	firstAt := f.execution.CreatedAt.Add(2 * time.Second)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "b", Failure: failure}), firstAt)
	// The later sequence deliberately has an earlier timestamp. Database history order wins.
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "a", Output: []byte("late")}), f.execution.CreatedAt.Add(time.Second))
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || d.State != durable.StateFailed || d.Failure == nil || d.Failure.Type != "declined" || len(d.Selections) != 1 || d.Selections[0].FutureID != "b" || !clock.Equal(firstAt) {
		t.Fatalf("sequence/failure: %+v %v %s", d, err, clock)
	}
}

func TestSelectSignalTiesConsumptionAndReuse(t *testing.T) {
	f := newHistory()
	at := f.execution.CreatedAt.Add(time.Minute)
	appendSignal(t, f, "first", "approve", "A", at)
	appendSignal(t, f, "second", "approve", "B", at)
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.ReceiveSignal("a", "approve")
		b := w.ReceiveSignal("b", "approve")
		winner := w.Select("first", b, a)
		if winner != b || !w.Now().Equal(at) {
			t.Fatal("tie or selection clock changed")
		}
		first, err := winner.Get()
		if err != nil {
			return nil, err
		}
		first[0] = 'X'
		first, err = winner.Get()
		if err != nil {
			return nil, err
		}
		reused := w.Select("reuse", b, a)
		if reused != b {
			t.Fatal("ready future was not reusable")
		}
		if _, err = reused.Get(); err != nil {
			return nil, err
		}
		second, err := w.Select("second", a).Get()
		return append(first, second...), err
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(d.Output) != "AB" || len(d.Signals) != 2 || len(d.Selections) != 3 || d.Signals[0].CommandID != "b" {
		t.Fatalf("signal selection: %+v %v", d, err)
	}
	appendSelectionDecision(t, f, d, at.Add(time.Hour))
	d, err = drt.Evaluate(f.execution, f.events, handler)
	if err != nil || string(d.Output) != "AB" || len(d.Signals) != 0 || len(d.Selections) != 0 {
		t.Fatalf("reserved signals: %+v %v", d, err)
	}
}

func TestSelectRejectsInvalidFuturesAndQueryMutation(t *testing.T) {
	var foreign *drt.Future
	f := newHistory()
	_, err := drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		foreign = w.Activity("foreign", "work", "", nil)
		return foreign.Get()
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"empty", "nil", "duplicate", "foreign", "zero", "bad_id", "query", "recovered_query"} {
		t.Run(mode, func(t *testing.T) {
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				a := w.Activity("a", "work", "", nil)
				switch mode {
				case "empty":
					w.Select("race")
				case "nil":
					w.Select("race", nil)
				case "duplicate":
					w.Select("race", a, a)
				case "foreign":
					w.Select("race", foreign)
				case "zero":
					w.Select("race", &drt.Future{})
				case "bad_id":
					w.Select(" ", a)
				default:
					w.SetQueryHandler("status", func(_ []byte) ([]byte, error) {
						if mode == "recovered_query" {
							defer func() { _ = recover() }()
						}
						w.Select("race", nil)
						return []byte("bad"), nil
					})
				}
				return a.Get()
			}
			if mode == "query" || mode == "recovered_query" {
				result, queryErr := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
				if !errors.Is(queryErr, drt.ErrQueryMutation) || len(result.Output) != 0 {
					t.Fatalf("query select: %+v %v", result, queryErr)
				}
			} else if _, evalErr := drt.Evaluate(f.execution, f.events, handler); !errors.Is(evalErr, durable.ErrInvalid) {
				t.Fatalf("invalid future accepted: %v", evalErr)
			}
		})
	}
}

func TestSelectCandidateAndDecisionBounds(t *testing.T) {
	for _, count := range []int{1000, 1001} {
		t.Run(fmt.Sprintf("candidates_%d", count), func(t *testing.T) {
			f := newHistory()
			for i := range count {
				f.append(drt.EventCommandScheduled, encode(t, drt.Command{Version: 1, Index: int64(i + 1), ID: fmt.Sprint(i), Kind: durable.TaskActivity, Name: "work"}), f.execution.CreatedAt)
			}
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				candidates := make([]*drt.Future, count)
				for i := range count {
					candidates[i] = w.Activity(fmt.Sprint(i), "work", "", nil)
				}
				return w.Select("race", candidates...).Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if count == 1000 {
				if err != nil || d.State != durable.StateRunning || len(d.Commands) != 1 {
					t.Fatalf("candidate boundary: %+v %v", d, err)
				}
			} else if !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("too many candidates: %v", err)
			}
		})
	}
	for _, count := range []int{499, 500} {
		t.Run(fmt.Sprintf("events_%d", count), func(t *testing.T) {
			f := newHistory()
			f.append(drt.EventCommandScheduled, encode(t, drt.Command{Version: 1, Index: 1, ID: "ready", Kind: durable.TaskActivity, Name: "work"}), f.execution.CreatedAt)
			f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "ready"}), f.execution.CreatedAt)
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				ready := w.Activity("ready", "work", "", nil)
				for i := range count {
					w.Select(fmt.Sprint(i), ready)
				}
				return w.ReceiveSignal("pending", "pending").Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if count == 499 {
				if err != nil || len(d.Commands)+len(d.Selections)+len(d.Signals) != 999 {
					t.Fatalf("event bound: %+v %v", d, err)
				}
			} else if !errors.Is(err, durable.ErrInvalid) || len(d.Commands)+len(d.Selections) > 0 {
				t.Fatalf("overflow returned decision: %+v %v", d, err)
			}
		})
	}
}
