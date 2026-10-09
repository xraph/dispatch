package runtime_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestSelectRejectsChangedCandidates(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		a := w.Activity("a", "work", "", nil)
		b := w.Activity("b", "work", "", nil)
		return w.Select("race", a, b).Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	appendSelectionDecision(t, f, d, f.execution.CreatedAt)
	for _, mode := range []string{"order", "membership", "id", "omitted"} {
		t.Run(mode, func(t *testing.T) {
			changed := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				a := w.Activity("a", "work", "", nil)
				b := w.Activity("b", "work", "", nil)
				switch mode {
				case "order":
					return w.Select("race", b, a).Get()
				case "membership":
					return w.Select("race", a).Get()
				case "id":
					return w.Select("other", a, b).Get()
				default:
					return nil, nil
				}
			}
			if _, err := drt.Evaluate(f.execution, f.events, changed); !errors.Is(err, drt.ErrNondeterministic) {
				t.Fatalf("changed selector accepted: %v", err)
			}
		})
	}
}

func TestSelectHistoryRejectsCorruption(t *testing.T) {
	modes := []string{"empty_candidates", "duplicate_candidates", "unknown_candidate", "self_candidate", "selection_candidate", "wrong_command_version", "command_input", "activity_candidates", "missing_command", "wrong_command_kind", "winner_outside", "missing_outcome", "future_outcome", "duplicate_winner", "winner_version", "extra_field", "missing_winner_before_command"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			at := f.execution.CreatedAt
			a := drt.Command{Version: 1, Index: 1, ID: "a", Kind: durable.TaskActivity, Name: "work"}
			if mode == "activity_candidates" {
				a.Candidates = []string{"b"}
			}
			f.append(drt.EventCommandScheduled, encode(t, a), at)
			b := drt.Command{Version: 1, Index: 2, ID: "b", Kind: durable.TaskActivity, Name: "work"}
			f.append(drt.EventCommandScheduled, encode(t, b), at)
			command := drt.Command{Version: 1, Index: 3, ID: "race", Kind: drt.CommandSelect, Candidates: []string{"a", "b"}}
			switch mode {
			case "empty_candidates":
				command.Candidates = nil
			case "duplicate_candidates":
				command.Candidates = []string{"a", "a"}
			case "unknown_candidate":
				command.Candidates = []string{"absent"}
			case "self_candidate":
				command.Candidates = []string{"race"}
			case "selection_candidate":
				prior := drt.Command{Version: 1, Index: 3, ID: "prior", Kind: drt.CommandSelect, Candidates: []string{"a"}}
				f.append(drt.EventCommandScheduled, encode(t, prior), at)
				command.Index = 4
				command.Candidates = []string{"prior"}
			case "wrong_command_version":
				command.Version = 2
			case "command_input":
				command.Input = []byte("bad")
			case "winner_outside":
				command.Candidates = []string{"a"}
			}
			if mode != "missing_command" {
				f.append(drt.EventCommandScheduled, encode(t, command), at)
			}
			outcome := drt.Outcome{Version: 1, CommandID: "b"}
			if mode != "missing_outcome" && mode != "future_outcome" {
				f.append(drt.EventActivityCompleted, encode(t, outcome), at)
			}
			winner := drt.Selection{Version: 1, CommandID: "race", FutureID: "b"}
			if mode == "wrong_command_kind" {
				winner.CommandID = "a"
			}
			if mode == "winner_version" {
				winner.Version = 2
			}
			payload := encode(t, winner)
			if mode == "extra_field" {
				payload = []byte(`{"version":1,"command_id":"race","future_id":"b","other":true}`)
			}
			if mode != "missing_winner_before_command" {
				f.append(drt.EventSelected, payload, at)
			} else {
				f.append(drt.EventCommandScheduled, encode(t, drt.Command{Version: 1, Index: 4, ID: "later", Kind: durable.TaskActivity, Name: "work"}), at)
			}
			if mode == "duplicate_winner" {
				f.append(drt.EventSelected, payload, at)
			}
			if mode == "future_outcome" {
				f.append(drt.EventActivityCompleted, encode(t, outcome), at)
			}
			_, err := drt.Evaluate(f.execution, f.events, func(_ *drt.Workflow, _ []byte) ([]byte, error) { return nil, nil })
			if !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("corruption accepted: %v", err)
			}
		})
	}
}

func TestSelectTerminalCannotInventWinner(t *testing.T) {
	f := newHistory()
	at := f.execution.CreatedAt
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.Select("race", w.Activity("a", "work", "", nil)).Get()
	}
	d, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	appendSelectionDecision(t, f, d, at)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "a", Output: []byte("done")}), at)
	f.append(drt.EventWorkflowCompleted, []byte("done"), at)
	f.execution.State = durable.StateCompleted
	f.execution.Output = []byte("done")
	if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("terminal invented winner: %v", err)
	}
}

func TestSelectSharesDecisionBudgetWithSignals(t *testing.T) {
	for _, count := range []int{249, 250} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newHistory()
			for i := range count {
				appendSignal(t, f, fmt.Sprint(i), "next", "data", f.execution.CreatedAt)
			}
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				for i := range count {
					w.Select("select-"+fmt.Sprint(i), w.ReceiveSignal("receive-"+fmt.Sprint(i), "next"))
				}
				return w.ReceiveSignal("pending", "missing").Get()
			}
			d, err := drt.Evaluate(f.execution, f.events, handler)
			if count == 249 {
				if err != nil || len(d.Commands)+len(d.Signals)+len(d.Selections) != 997 {
					t.Fatalf("mixed budget: %+v %v", d, err)
				}
			} else if !errors.Is(err, durable.ErrInvalid) || len(d.Commands)+len(d.Selections)+len(d.Signals) != 0 {
				t.Fatalf("overflow: %+v %v", d, err)
			}
		})
	}
}
