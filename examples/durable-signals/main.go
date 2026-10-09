// This example uses memory for development. Use a qualified persistent store
// when you need workflow state and messages to survive a process restart.
package main

import (
	"context"
	"fmt"
	"log"

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
	worker, err := drt.NewWorker(s, drt.Options{Namespace: "example", Queue: "orders", BuildID: "v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.ReceiveSignal("approval", "approve").Get() }}})
	if err != nil {
		return err
	}
	receipt, err := worker.SignalWithStart(ctx, durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: "example", WorkflowID: "order-42", RunID: "run-1"}, RequestID: "approval-42", WorkflowType: "order", BuildID: "v1", Queue: "orders"}, Name: "approve", Input: []byte("approved")})
	if err != nil {
		return err
	}
	if worked, workErr := worker.RunOnce(ctx, durable.TaskWorkflow); workErr != nil {
		return workErr
	} else if !worked {
		return fmt.Errorf("workflow task was not available")
	}
	execution, err := s.GetExecution(ctx, receipt.Key)
	if err != nil {
		return err
	}
	if execution.State != durable.StateCompleted {
		return fmt.Errorf("unexpected execution state: %s", execution.State)
	}
	fmt.Printf("completed: %s\n", execution.Output)
	return nil
}
