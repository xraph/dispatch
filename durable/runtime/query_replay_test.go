package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func queryRequest(f *historyFixture, name string) drt.QueryRequest {
	return drt.QueryRequest{Key: f.execution.Key, BuildID: f.execution.BuildID, Name: name}
}

func TestQueryReconstructsWorkflowState(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "pending"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		w.SetQueryHandler("clock", func(_ []byte) ([]byte, error) { return []byte(w.Now().Format(time.RFC3339Nano)), nil })
		approved, err := w.ReceiveSignal("approval", "approve").Get()
		if err != nil {
			return nil, err
		}
		state = string(approved)
		paid, err := w.Activity("charge", "charge", "", approved).Get()
		if err != nil {
			return nil, err
		}
		state = string(paid)
		return paid, nil
	}
	check := func(want string, at time.Time) {
		t.Helper()
		before, _ := durable.Fingerprint("history", f.events)
		result, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
		if err != nil || string(result.Output) != want || result.Key != f.execution.Key || result.Revision != f.execution.Revision || result.LastSequence != f.execution.LastSequence || result.State != f.execution.State {
			t.Fatalf("query: %+v %v", result, err)
		}
		clock, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "clock"))
		if err != nil || string(clock.Output) != at.Format(time.RFC3339Nano) {
			t.Fatalf("query logical time: %+v %v", clock, err)
		}
		after, _ := durable.Fingerprint("history", f.events)
		if before != after {
			t.Fatal("query mutated caller history")
		}
	}
	check("pending", f.execution.CreatedAt)
	arrival := f.execution.CreatedAt.Add(time.Minute)
	appendSignal(t, f, "approved", "approve", "approved", arrival)
	check("approved", arrival) // No decision has consumed the persisted message yet.
	waiting, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	appendSignalDecision(t, f, waiting, arrival.Add(time.Minute))
	check("approved", arrival)
	paidAt := arrival.Add(time.Hour)
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "charge", Output: []byte("paid")}), paidAt)
	check("paid", paidAt)
	done, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.append(drt.EventWorkflowCompleted, done.Output, paidAt)
	f.execution.State = durable.StateCompleted
	f.execution.Output = done.Output
	check("paid", paidAt)
}

func TestQueryReadsFailedWorkflow(t *testing.T) {
	f := newHistory()
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "waiting"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		value, err := w.Activity("charge", "charge", "", nil).Get()
		if err != nil {
			state = err.Error()
		}
		return value, err
	}
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil {
		t.Fatal(err)
	}
	f.commands(t, first.Commands)
	failure := &drt.ApplicationError{Type: "declined", Message: "declined"}
	f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 1, CommandID: "charge", Failure: failure}), f.execution.CreatedAt)
	f.append(drt.EventWorkflowFailed, encode(t, failure), f.execution.CreatedAt)
	f.execution.State = durable.StateFailed
	got, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
	if err != nil || got.State != durable.StateFailed || string(got.Output) != "declined" {
		t.Fatalf("failed query: %+v %v", got, err)
	}
	changed := func(w *drt.Workflow, input []byte) ([]byte, error) { value, _ := handler(w, input); return value, nil }
	if _, err = drt.EvaluateQuery(f.execution, f.events, changed, queryRequest(f, "status")); !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("changed terminal result accepted: %v", err)
	}
}

func TestQueryRejectsBadReplay(t *testing.T) {
	for _, mode := range []string{"corrupt", "changed", "panic", "missing_handler"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			called := false
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { called = true; return nil, nil })
				return w.Activity("work", "work", "", nil).Get()
			}
			first, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, first.Commands)
			want := drt.ErrHistory
			switch mode {
			case "corrupt":
				f.events[1].Sequence = 99
			case "changed":
				handler = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("different", "work", "", nil).Get() }
				want = drt.ErrNondeterministic
			case "panic":
				handler = func(_ *drt.Workflow, _ []byte) ([]byte, error) { panic("workflow panic") }
				want = drt.ErrWorkflowPanic
			case "missing_handler":
				handler = nil
				want = durable.ErrInvalid
			}
			if _, err = drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status")); !errors.Is(err, want) || called {
				t.Fatalf("invalid replay queried: %v called=%t", err, called)
			}
		})
	}
}
