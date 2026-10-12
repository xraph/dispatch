// Run this example with the memory store to inspect a failed run and its retry.
// Use a qualified persistent store when histories must survive process restarts.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func order(w *drt.Workflow, _ []byte) ([]byte, error) {
	info := w.RunInfo()
	w.SetQueryHandler("attempt", func([]byte) ([]byte, error) { return []byte(fmt.Sprint(info.RetryAttempt)), nil })
	// Simulate a retryable application failure. Real external work belongs in
	// activities and needs an idempotency key that covers repeated workflow runs.
	if info.RetryAttempt == 1 {
		return nil, &drt.ApplicationError{Type: "inventory_unavailable", Message: "retry inventory lookup"}
	}
	return w.ReceiveSignal("approval", "approve").Get()
}

func run(ctx context.Context) error {
	s := memory.New()
	o := drt.Options{Namespace: "example", Queue: "orders", BuildID: "orders-v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": order}}
	w, err := drt.NewWorker(s, o)
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: o.Namespace, WorkflowID: "order-42", RunID: "first"}
	if _, err = w.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: o.BuildID, Queue: o.Queue, RunTimeout: time.Minute, ExecutionTimeout: time.Hour,
		RetryPolicy: &durable.WorkflowRetryPolicy{InitialInterval: 10 * time.Millisecond, MaximumAttempts: 3, NonRetryableTypes: []string{"invalid_order"}}}); err != nil {
		return err
	}
	if _, err = w.SignalExecution(ctx, durable.SignalRequest{Key: key, RequestID: "approval", Name: "approve", BuildID: o.BuildID, Input: []byte("approved")}); err != nil {
		return err
	}
	current := key
	for range 2 {
		e, readErr := s.GetExecution(ctx, current)
		if readErr != nil {
			return readErr
		}
		// Backoff is saved by the store. This example waits outside workflow code.
		time.Sleep(max(time.Until(e.AvailableAt()), 0))
		w, err = drt.NewWorker(s, o)
		if err != nil {
			return err
		}
		if worked, runErr := w.RunOnce(ctx, durable.TaskWorkflow); runErr != nil {
			return runErr
		} else if !worked {
			return fmt.Errorf("workflow task missing for %s", current.RunID)
		}
		e, err = s.GetExecution(ctx, current)
		if err != nil {
			return err
		}
		fmt.Printf("run %d, attempt %d: %s, output: %s\n", e.RunNumber, e.WorkflowAttempt(), e.State, e.Output)
		if e.NextRunID != "" {
			current.RunID = e.NextRunID
		}
	}
	q, err := w.QueryExecution(ctx, drt.QueryRequest{Key: key, BuildID: o.BuildID, Name: "attempt"})
	if err != nil {
		return err
	}
	fmt.Printf("original run query: attempt %s (%s)\n", q.Output, q.State)
	return nil
}
