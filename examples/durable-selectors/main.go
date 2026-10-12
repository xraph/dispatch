// This example uses memory for development. Use a qualified persistent execution
// store to retain workflow history and deadlines through a process restart.
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
	worker, err := drt.NewWorker(s, drt.Options{Namespace: "example", Queue: "orders", BuildID: "select-v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		approval := w.ReceiveSignal("approval", "approve")
		deadline := w.Timer("deadline", time.Hour)
		winner := w.Select("approval-or-timeout", approval, deadline)
		if winner == deadline {
			return []byte("timed out"), nil
		}
		return winner.Get()
	}}})
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: "example", WorkflowID: "order-42", RunID: "run-1"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start-42", WorkflowType: "order", BuildID: "select-v1", Queue: "orders"}); err != nil {
		return err
	}
	if worked, runErr := worker.RunOnce(ctx, durable.TaskWorkflow); runErr != nil {
		return fmt.Errorf("initial decision: %w", runErr)
	} else if !worked {
		return errors.New("initial decision found no task")
	}
	if _, err = worker.SignalExecution(ctx, durable.SignalRequest{Key: key, RequestID: "approval-42", BuildID: "select-v1", Name: "approve", Input: []byte("approved")}); err != nil {
		return err
	}
	if worked, runErr := worker.RunOnce(ctx, durable.TaskWorkflow); runErr != nil {
		return fmt.Errorf("selection decision: %w", runErr)
	} else if !worked {
		return errors.New("selection decision found no task")
	}
	execution, err := s.GetExecution(ctx, key)
	if err != nil {
		return err
	}
	if execution.State != durable.StateCompleted {
		return fmt.Errorf("workflow is %s", execution.State)
	}
	fmt.Println(string(execution.Output))
	return nil
}
