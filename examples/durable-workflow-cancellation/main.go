// This example uses memory for development. Use a qualified persistent execution
// store to retain workflow history through a process restart.
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
	worker, err := drt.NewWorker(s, drt.Options{Namespace: "example", Queue: "orders", BuildID: "workflow-cancel-v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetCancellationHandler(func(cleanup *drt.Workflow, request durable.ExecutionCancellation) ([]byte, error) {
			if _, cleanupErr := cleanup.Activity("release", "release", "", []byte(request.Reason)).Get(); cleanupErr != nil {
				return nil, cleanupErr
			}
			return nil, drt.ErrWorkflowCancelled
		})
		return w.Timer("wait", time.Hour).Get()
	}}, Activities: map[string]drt.ActivityFunc{"release": func(_ context.Context, _ drt.ActivityInfo, reason []byte) ([]byte, error) {
		fmt.Printf("cleanup: %s\n", reason)
		return nil, nil
	}}})
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: "example", WorkflowID: "order-42", RunID: "run-1"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start-42", WorkflowType: "order", BuildID: "workflow-cancel-v1", Queue: "orders"}); err != nil {
		return err
	}
	if _, err = worker.RequestCancelExecution(ctx, durable.CancelExecutionRequest{Key: key, RequestID: "cancel-42", BuildID: "workflow-cancel-v1", Reason: "order withdrawn"}); err != nil {
		return err
	}
	// Acceptance leaves the run open. Save the fence, schedule cleanup, execute
	// its activity, then record the terminal cancellation decision.
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskWorkflow, durable.TaskActivity, durable.TaskWorkflow} {
		worked, runErr := worker.RunOnce(ctx, kind)
		if runErr != nil {
			return runErr
		}
		if !worked {
			return errors.New("missing cancellation task")
		}
	}
	execution, err := s.GetExecution(ctx, key)
	if err != nil {
		return err
	}
	if execution.State != durable.StateCancelled {
		return fmt.Errorf("workflow is %s", execution.State)
	}
	fmt.Println(execution.State)
	return nil
}
