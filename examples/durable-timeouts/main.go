// This example uses memory for development. You need a qualified persistent
// store to retain deadlines and history after a process restart.
package main

import (
	"context"
	"errors"
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

func run(ctx context.Context) error {
	s := memory.New()
	options := drt.Options{Namespace: "example", Queue: "orders", BuildID: "timeouts-v1", Owner: "worker", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{
		"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			state := "waiting for child"
			w.SetQueryHandler("status", func([]byte) ([]byte, error) { return []byte(state), nil })
			// No worker serves this build. The namespace timeout processor can still close it.
			_, err := w.ChildWorkflow("shipment", "ship", nil, drt.ChildOptions{BuildID: "retired", Queue: "retired", RunTimeout: 50 * time.Millisecond, ExecutionTimeout: time.Second}).Get()
			var child *drt.ChildWorkflowError
			if !errors.As(err, &child) || !errors.Is(err, drt.ErrChildTimedOut) || child.Timeout == nil {
				return nil, err
			}
			state = "child timed out; awaiting approval"
			return w.ReceiveSignal("approval", "approve").Get()
		},
	}}
	worker, err := drt.NewWorker(s, options)
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order-42", RunID: "run-1"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start-42", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue, RunTimeout: 250 * time.Millisecond, ExecutionTimeout: time.Minute}); err != nil {
		return err
	}
	workCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(workCtx) }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		execution, readErr := s.GetExecution(workCtx, key)
		if readErr != nil {
			cancel()
			<-done
			return readErr
		}
		if execution.State != durable.StateRunning {
			cancel()
			if runErr := <-done; runErr != nil {
				return runErr
			}
			if execution.State != durable.StateTimedOut {
				return fmt.Errorf("unexpected workflow state: %s", execution.State)
			}
			query, queryErr := worker.QueryExecution(ctx, drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"})
			if queryErr != nil {
				return queryErr
			}
			fmt.Printf("workflow: %s, saved state: %s\n", query.State, query.Output)
			return nil
		}
		select {
		case runErr := <-done:
			if runErr != nil {
				return fmt.Errorf("worker stopped before timeout: %w", runErr)
			}
			return errors.New("worker stopped before timeout")
		case <-workCtx.Done():
			cancel()
			<-done
			return context.Cause(workCtx)
		case <-ticker.C:
		}
	}
}
