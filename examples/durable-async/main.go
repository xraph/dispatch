// This example uses memory for development. Use a persistent execution store
// when you need a deferred activity to survive a process restart.
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

func run(ctx context.Context) error {
	backend := memory.New()
	var handle drt.AsyncActivityHandle
	options := drt.Options{Namespace: "example", Queue: "orders", BuildID: "v1", Owner: "worker",
		Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ActivityWithOptions("charge", "charge", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Minute}).Get()
		}},
		Activities: map[string]drt.ActivityFunc{"charge": func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
			var err error
			handle, err = info.DeferCompletion(ctx)
			// Deliver the handle securely after this succeeds. Use info.IdempotencyKey()
			// for external side effects. Do not log or expose the handle's JSON.
			return nil, err
		}}}
	worker, err := drt.NewWorker(backend, options)
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
		return err
	}
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity} {
		worked, workErr := worker.RunOnce(ctx, kind)
		if workErr != nil {
			return fmt.Errorf("run %s: %w", kind, workErr)
		}
		if !worked {
			return fmt.Errorf("no %s task available", kind)
		}
	}
	// These requests stand in for an authorized external callback. Retry their
	// exact contents and IDs if the response is lost, including after closure.
	if _, err = worker.HeartbeatAsyncActivity(ctx, drt.AsyncHeartbeatRequest{Handle: handle, RequestID: "approval-progress",
		Sequence: handle.InitialHeartbeatSequence + 1, Details: []byte("approved")}); err != nil {
		return err
	}
	if _, err = worker.CompleteAsyncActivity(ctx, drt.AsyncCompletionRequest{Handle: handle, RequestID: "payment-result", Output: []byte("paid")}); err != nil {
		return err
	}
	worked, err := worker.RunOnce(ctx, durable.TaskWorkflow)
	if err != nil {
		return fmt.Errorf("finish workflow: %w", err)
	}
	if !worked {
		return fmt.Errorf("no workflow task available after completion")
	}
	execution, err := backend.GetExecution(ctx, key)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", execution.State, execution.Output)
	return nil
}
