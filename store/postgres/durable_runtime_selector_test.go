//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type lostSelectionResponseStore struct {
	durable.Store
	lost    bool
	retries int
	digest  string
}

func (s *lostSelectionResponseStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	selected := false
	for _, event := range r.Events {
		if event.Type == drt.EventSelected {
			selected = true
		}
	}
	if selected && s.lost {
		digest, err := durable.Fingerprint("selection", r)
		if err != nil {
			return durable.Receipt{}, err
		}
		if digest != s.digest {
			return durable.Receipt{}, durable.ErrRequestConflict
		}
		s.retries++
	}
	receipt, err := s.Store.CommitTransition(ctx, r)
	if err == nil && selected && !s.lost {
		s.lost = true
		s.digest, err = durable.Fingerprint("selection", r)
		if err != nil {
			return durable.Receipt{}, err
		}
		return durable.Receipt{}, errors.New("PostgreSQL selection acknowledgement lost")
	}
	return receipt, err
}

func TestDurableRuntimeSelectRecovery(t *testing.T) {
	for _, signalFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(signalFirst), func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "select-v1", Owner: "first", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
				state := "pending"
				w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
				approval := w.ReceiveSignal("approval", "approve")
				timer := w.Timer("deadline", time.Microsecond)
				winner := w.Select("race", approval, timer)
				state = "timeout"
				if winner == approval {
					value, err := winner.Get()
					if err != nil {
						return nil, err
					}
					state = string(value)
				}
				if _, err := w.ReceiveSignal("finish", "finish").Get(); err != nil {
					return nil, err
				}
				// The approval remains available even when the timer won earlier.
				value, err := approval.Get()
				if err != nil {
					return nil, err
				}
				state += "|" + string(value)
				return []byte(state), nil
			}}}
			worker, err := drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
			if _, err = worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
				t.Fatal(err)
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			if _, err = s.GetTask(t.Context(), key, "command:3"); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("selector created a task: %v", err)
			}
			approve := func() {
				t.Helper()
				if _, signalErr := worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "approve", BuildID: options.BuildID, Name: "approve", Input: []byte("approved")}); signalErr != nil {
					t.Fatal(signalErr)
				}
			}
			if signalFirst {
				approve()
				runQueryTask(t, worker, durable.TaskTimer)
			} else {
				runQueryTask(t, worker, durable.TaskTimer)
				approve()
			}
			// A fresh worker sees both ready outcomes and must use their recorded order.
			s = reopenAsyncStore(t, s, dsn)
			options.Owner = "replacement"
			lost := &lostSelectionResponseStore{Store: s}
			worker, err = drt.NewWorker(lost, options)
			if err != nil {
				t.Fatal(err)
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			if !lost.lost || lost.retries != 1 {
				t.Fatalf("lost response not recovered: lost=%t retries=%d", lost.lost, lost.retries)
			}
			want := "timeout"
			consumed := 0
			if signalFirst {
				want = "approved"
				consumed = 1
			}
			query := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
			checkPostgresQuery(t, s, worker, query, want, durable.StateRunning)
			checkSelectionEvents(t, s, key, consumed)
			s = reopenAsyncStore(t, s, dsn)
			options.Owner = "after-selection"
			worker, err = drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "finish", BuildID: options.BuildID, Name: "finish"}); err != nil {
				t.Fatal(err)
			}
			runQueryTask(t, worker, durable.TaskWorkflow)
			checkPostgresQuery(t, s, worker, query, want+"|approved", durable.StateCompleted)
			checkSelectionEvents(t, s, key, 2)
		})
	}
}

func checkSelectionEvents(t *testing.T, s durable.Store, key durable.Key, wantConsumed int) {
	t.Helper()
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	selected, consumed := 0, 0
	for _, event := range events {
		if event.Type == drt.EventSelected {
			selected++
		}
		if event.Type == drt.EventSignalConsumed {
			consumed++
		}
	}
	if selected != 1 || consumed != wantConsumed {
		t.Fatalf("selection history: selected=%d consumed=%d want=%d", selected, consumed, wantConsumed)
	}
}
