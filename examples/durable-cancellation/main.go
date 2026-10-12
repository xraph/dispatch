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
	worker, err := drt.NewWorker(s, drt.Options{Namespace: "example", Queue: "orders", BuildID: "cancel-v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		timer := w.Timer("deadline", time.Hour)
		if _, ackErr := w.Cancel("stop-deadline", timer).Get(); ackErr != nil {
			return nil, ackErr
		}
		if _, timerErr := timer.Get(); !errors.Is(timerErr, drt.ErrCancelled) {
			return nil, errors.New("timer was not canceled")
		}
		return []byte("cancelled"), nil
	}}})
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: "example", WorkflowID: "order-42", RunID: "run-1"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start-42", WorkflowType: "order", BuildID: "cancel-v1", Queue: "orders"}); err != nil {
		return err
	}
	for range 2 {
		if worked, runErr := worker.RunOnce(ctx, durable.TaskWorkflow); runErr != nil {
			return runErr
		} else if !worked {
			return errors.New("missing workflow decision")
		}
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
