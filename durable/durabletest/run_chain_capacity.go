package durabletest

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

// fillRunHistory uses real decisions to reach a boundary without bypassing fences.
func fillRunHistory(t *testing.T, s durable.Store, r durable.StartRequest, count int64) {
	t.Helper()
	for {
		e, err := s.GetExecution(t.Context(), r.Key)
		if err != nil {
			t.Fatal(err)
		}
		if e.LastSequence >= count {
			return
		}
		req := completion(r, claim(t, s, r, time.Minute))
		req.RequestID, req.ExpectedRevision = fmt.Sprintf("fill:%d", e.Revision), e.Revision
		req.Events = make([]durable.EventInput, min(1000, count-e.LastSequence))
		for i := range req.Events {
			req.Events[i].Type = drt.EventWorkflowWaiting
		}
		req.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskRetry, RetryAt: time.Now()}
		if _, err := s.CommitTransition(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
}

func runHistory(t *testing.T, s durable.Store, key durable.Key) (durable.Execution, []durable.Event) {
	t.Helper()
	e, err := s.GetExecution(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	var events []durable.Event
	for int64(len(events)) < e.LastSequence {
		page, err := s.ReadHistory(t.Context(), key, int64(len(events)), 1000)
		if err != nil || len(page) == 0 {
			t.Fatalf("history page: %v", err)
		}
		events = append(events, page...)
	}
	return e, events
}

// RunRunChainHistoryBoundary proves accepted closing batches remain replayable.
func RunRunChainHistoryBoundary(t *testing.T, s durable.Store) {
	for _, mode := range []string{"continue", "failure", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			o := retryRuntimeOptions(t)
			r := durable.StartRequest{Key: durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "root"}, RequestID: "start", WorkflowType: "order", BuildID: o.BuildID, Queue: o.Queue}
			if mode != "continue" {
				r.RetryPolicy = &durable.WorkflowRetryPolicy{InitialInterval: time.Second, MaximumAttempts: 2}
			}
			if mode == "timeout" {
				r.RunTimeout = 45 * time.Second
			}
			if _, err := s.StartExecution(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			fillRunHistory(t, s, r, 100000)
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				w.SetQueryHandler("value", func([]byte) ([]byte, error) { return []byte("retained"), nil })
				if mode != "timeout" {
					for i := range 998 {
						w.Activity(fmt.Sprint(i), "unused", "", nil)
					}
				}
				if mode == "continue" {
					return nil, w.ContinueAsNew(nil, drt.ContinueOptions{})
				}
				if mode == "timeout" {
					return w.ReceiveSignal("wait", "never").Get()
				}
				return nil, &drt.ApplicationError{Type: "temporary"}
			}
			o.Workflows["order"] = handler
			worker := retryRuntimeWorker(t, s, o)
			if mode == "timeout" {
				e, _ := runHistory(t, s, r.Key)
				time.Sleep(max(time.Until(e.RunDeadlineAt)+time.Millisecond, 0))
				retryRuntimeTask(t, worker, drt.TaskExecutionTimeout)
			} else {
				retryRuntimeTask(t, worker, durable.TaskWorkflow)
			}
			e, events := runHistory(t, s, r.Key)
			if e.NextRunID == "" || (mode == "timeout" && e.LastSequence != 100002) || (mode != "timeout" && e.LastSequence != 101000) {
				t.Fatalf("boundary handoff missing: %+v", e)
			}
			if _, err := drt.Evaluate(e, events, handler); err != nil {
				t.Fatalf("accepted predecessor cannot replay: %v", err)
			}
			q, err := worker.QueryExecution(t.Context(), drt.QueryRequest{Key: r.Key, BuildID: r.BuildID, Name: "value"})
			if err != nil || string(q.Output) != "retained" {
				t.Fatalf("accepted predecessor cannot query: %+v %v", q, err)
			}
		})
	}
}

// RunWorkflowRetryCapacity checks capacity finality for failures and expired children.
func RunWorkflowRetryCapacity(t *testing.T, s durable.Store) {
	for _, outcome := range []string{"failure", "timeout"} {
		t.Run(outcome, func(t *testing.T) {
			for _, limit := range []string{"signal_count", "signal_bytes", "source_history"} {
				t.Run(limit, func(t *testing.T) {
					r := durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "child", RunID: "root"}, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "children", RetryPolicy: &durable.WorkflowRetryPolicy{MaximumAttempts: 3}}
					var parent durable.StartRequest
					if outcome == "timeout" {
						r.RunTimeout = 15 * time.Second
						if limit == "source_history" {
							r.RunTimeout = 45 * time.Second
						}
						parent = durable.StartRequest{Key: durable.Key{Namespace: r.Namespace, WorkflowID: "parent", RunID: "root"}, RequestID: "parent", WorkflowType: "parent", BuildID: "v1", Queue: "parents"}
						if _, err := s.StartExecution(t.Context(), parent); err != nil {
							t.Fatal(err)
						}
						req := completion(parent, claim(t, s, parent, time.Minute))
						req.Children = []durable.ChildStartSpec{{CommandID: "child", Start: r, ParentQueue: parent.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}
						if _, err := s.CommitTransition(t.Context(), req); err != nil {
							t.Fatal(err)
						}
					} else if _, err := s.StartExecution(t.Context(), r); err != nil {
						t.Fatal(err)
					}
					count, input := 999, []byte("accepted")
					if limit == "signal_bytes" {
						count, input = 4, make([]byte, 1<<20)
					}
					if limit == "source_history" {
						count = 1
					}
					signal := durable.SignalRequest{Key: r.Key, BuildID: r.BuildID, Name: "item", Input: input}
					var accepted durable.SignalReceipt
					for i := 0; i < count; i++ {
						signal.RequestID = fmt.Sprint(i)
						var err error
						accepted, err = s.SignalExecution(t.Context(), signal)
						if err != nil {
							t.Fatal(err)
						}
					}
					if limit == "source_history" {
						fillRunHistory(t, s, r, 100001)
					}
					before, _ := runHistory(t, s, r.Key)
					var receipt durable.Receipt
					var closureErr error
					if outcome == "timeout" {
						time.Sleep(max(time.Until(before.RunDeadlineAt)+time.Millisecond, 0))
						req := executionTimeoutRequest(claimExecutionTimeout(t, s, r, time.Minute))
						receipt, closureErr = s.ApplyExecutionTimeout(t.Context(), req)
						if closureErr != nil {
							t.Fatalf("retry capacity prevented timeout closure: %v", closureErr)
						}
						if again, retryErr := s.ApplyExecutionTimeout(t.Context(), req); retryErr != nil || again != receipt {
							t.Fatalf("timeout receipt: %+v %v", again, retryErr)
						}
					} else {
						req := completion(r, claim(t, s, r, time.Minute))
						req.ExpectedRevision = before.Revision
						req.State = durable.StateFailed
						req.Events = []durable.EventInput{{Type: "workflow.failed", Payload: []byte(`{"type":"temporary","message":""}`)}}
						receipt, closureErr = s.CommitTransition(t.Context(), req)
						if closureErr != nil {
							t.Fatalf("retry capacity prevented failure closure: %v", closureErr)
						}
						if again, retryErr := s.CommitTransition(t.Context(), req); retryErr != nil || again != receipt {
							t.Fatalf("failure receipt: %+v %v", again, retryErr)
						}
					}
					e, events := runHistory(t, s, r.Key)
					if e.State == durable.StateRunning || e.NextRunID != "" || e.LastSequence != before.LastSequence+2 {
						t.Fatalf("capacity did not close source: %+v", e)
					}
					var suppression struct {
						Version     int    `json:"version"`
						Reason      string `json:"reason"`
						FailureType string `json:"failure_type"`
					}
					record := events[len(events)-2]
					if record.Type != "workflow.retry_suppressed" || json.Unmarshal(record.Payload, &suppression) != nil || suppression.Version != 1 || suppression.Reason != limit {
						t.Fatalf("missing suppression reason: %+v %s", record, record.Payload)
					}
					o := retryRuntimeOptions(t)
					o.Namespace, o.BuildID, o.Queue = r.Namespace, r.BuildID, r.Queue
					handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
						w.SetQueryHandler("retained", func([]byte) ([]byte, error) { return []byte("readable"), nil })
						if outcome == "timeout" {
							return w.ReceiveSignal("wait", "never").Get()
						}
						return nil, &drt.ApplicationError{Type: "temporary"}
					}
					o.Workflows["order"] = handler
					if _, replayErr := drt.Evaluate(e, events, handler); replayErr != nil {
						t.Fatalf("suppressed source replay: %v", replayErr)
					}
					q, queryErr := retryRuntimeWorker(t, s, o).QueryExecution(t.Context(), drt.QueryRequest{Key: r.Key, BuildID: r.BuildID, Name: "retained"})
					if queryErr != nil || string(q.Output) != "readable" {
						t.Fatalf("suppressed source query: %+v %v", q, queryErr)
					}
					corrupt := append([]durable.Event(nil), events...)
					corrupt[len(corrupt)-2].Payload = []byte(`{"version":1,"reason":"invented","failure_type":"temporary","source_last_sequence":1}`)
					if _, err := drt.Evaluate(e, corrupt, handler); !errors.Is(err, drt.ErrHistory) {
						t.Fatalf("invented suppression accepted: %v", err)
					}
					retained := 0
					for _, event := range events {
						if event.Type == durable.EventSignalReceived {
							retained++
						}
					}
					if retained != count {
						t.Fatalf("accepted signals lost: %d", retained)
					}
					if again, err := s.SignalExecution(t.Context(), signal); err != nil || again != accepted {
						t.Fatalf("acceptance receipt lost: %+v %v", again, err)
					}
					if _, err := s.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: durable.Key{Namespace: r.Namespace, WorkflowID: r.WorkflowID}, Selection: durable.RunCurrent}); !errors.Is(err, durable.ErrNotFound) {
						t.Fatalf("closed identity retained: %v", err)
					}
					task, taskErr := s.GetTask(t.Context(), r.Key, "workflow:1")
					if taskErr != nil || !task.Done {
						t.Fatalf("source task not fenced: %+v %v", task, taskErr)
					}
					if outcome == "timeout" {
						delivery, err := s.ClaimChildDelivery(t.Context(), durable.ChildDeliveryClaimRequest{Namespace: r.Namespace, BuildID: parent.BuildID, Owner: "parent", LeaseDuration: time.Minute})
						if err != nil || delivery == nil || delivery.Message.State != durable.StateTimedOut || delivery.Message.Child != r.Key {
							t.Fatalf("child result stranded: %+v %v", delivery, err)
						}
						if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(delivery)); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		})
	}
}
