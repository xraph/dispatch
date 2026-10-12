// This example uses memory for development. Use a qualified persistent store
// when parent and child histories must survive a process restart.
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
	options := drt.Options{Namespace: "example", Queue: "orders", BuildID: "children-v1", Owner: "worker", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{
		"order": func(w *drt.Workflow, input []byte) ([]byte, error) {
			shipment := w.ChildWorkflow("shipment", "ship", input, drt.ChildOptions{})
			if _, err := shipment.Started(); err != nil {
				return nil, err
			}
			return shipment.Get()
		},
		"ship": func(*drt.Workflow, []byte) ([]byte, error) { return []byte("shipment prepared"), nil },
	}}
	worker, err := drt.NewWorker(s, options)
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order-42", RunID: "run-1"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start-42", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue, Input: []byte("order-42")}); err != nil {
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
		if execution.State == durable.StateCompleted {
			cancel()
			if runErr := <-done; runErr != nil {
				return runErr
			}
			child, childErr := s.GetChildExecution(ctx, key, "shipment")
			if childErr != nil {
				return childErr
			}
			fmt.Printf("parent: %s, child: %s, result: %s\n", execution.State, child.State, execution.Output)
			return nil
		}
		select {
		case runErr := <-done:
			if runErr != nil {
				return fmt.Errorf("worker stopped before parent completion: %w", runErr)
			}
			return errors.New("worker stopped before parent completion")
		case <-workCtx.Done():
			cancel()
			<-done
			return context.Cause(workCtx)
		case <-ticker.C:
		}
	}
}
