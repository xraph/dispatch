package durabletest

import (
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func runChainRoots(t *testing.T, s durable.Store) {
	for _, mode := range []string{"ordinary", "signal", "child"} {
		t.Run(mode, func(t *testing.T) {
			r := deadlineStart(t, time.Minute+1234*time.Nanosecond)
			r.Input = []byte("input")
			var receipt durable.Receipt
			switch mode {
			case "ordinary":
				saved, err := s.StartExecution(t.Context(), r)
				if err != nil {
					t.Fatal(err)
				}
				receipt = saved
			case "signal":
				saved, err := s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: r, Name: "go"})
				if err != nil {
					t.Fatal(err)
				}
				receipt = saved.Receipt
			case "child":
				parent := start(t, s)
				r.WorkflowID = "child"
				request := completion(parent, claim(t, s, parent, time.Minute))
				request.Children = []durable.ChildStartSpec{{CommandID: "child", Start: r, ParentQueue: parent.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}
				if _, err := s.CommitTransition(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				receipt = durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 1}
			}
			e, err := s.GetExecution(t.Context(), r.Key)
			if err != nil {
				t.Fatal(err)
			}
			if err = durable.ValidateRunMetadata(e); err != nil || e.FirstRunID != r.RunID || e.RunNumber != 1 || e.PreviousRunID != "" || e.NextRunID != "" || e.RunTimeout != r.RunTimeout || !e.FirstStartedAt.Equal(e.CreatedAt) {
				t.Fatalf("root metadata: %+v %v", e, err)
			}
			snapshot := e
			snapshot.Input = append([]byte(nil), e.Input...)
			e.Input[0] = 'X'
			e.FirstRunID = "changed"
			for _, selection := range []durable.RunSelection{durable.RunExplicit, durable.RunCurrent, durable.RunLatest} {
				key := r.Key
				if selection != durable.RunExplicit {
					key.RunID = ""
				}
				got, readErr := s.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: key, Selection: selection})
				if readErr != nil || !reflect.DeepEqual(got, snapshot) {
					t.Fatalf("lineage snapshot: %+v %v", got, readErr)
				}
			}
			if mode == "signal" {
				got, retryErr := s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: r, Name: "go"})
				if retryErr != nil || got.Receipt != receipt {
					t.Fatalf("signal retry: %+v %v", got, retryErr)
				}
			} else if got, retryErr := s.StartExecution(t.Context(), r); retryErr != nil || got != receipt {
				t.Fatalf("start retry: %+v %v", got, retryErr)
			}
		})
	}
}
