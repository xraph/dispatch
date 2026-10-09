// This example uses memory so you can run it without a database. Use a qualified
// persistent store when histories must survive a process restart.
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

func batch(w *drt.Workflow, input []byte) ([]byte, error) {
	w.SetQueryHandler("input", func([]byte) ([]byte, error) { return input, nil })
	item, err := w.ReceiveSignal("item", "items").Get()
	if err != nil {
		return nil, err
	}
	if string(input) == "first batch" {
		return nil, w.ContinueAsNew([]byte("final batch"), drt.ContinueOptions{})
	}
	return item, nil
}

func run(ctx context.Context) error {
	s := memory.New()
	options := drt.Options{Namespace: "example", Queue: "batches", BuildID: "batches-v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"batch": batch}}
	worker, err := drt.NewWorker(s, options)
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "batch-42", RunID: "first"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "batch", BuildID: options.BuildID, Queue: options.Queue, Input: []byte("first batch"), RunTimeout: time.Minute, ExecutionTimeout: time.Hour}); err != nil {
		return err
	}
	for _, item := range []string{"first item", "last item"} {
		if _, err = worker.SignalExecution(ctx, durable.SignalRequest{Key: key, RequestID: item, Name: "items", BuildID: options.BuildID, Input: []byte(item)}); err != nil {
			return err
		}
	}
	current := key
	for range 2 {
		// Replace the worker between runs. The second accepted signal is carried
		// with its original identity, while the successor has a fresh history.
		worker, err = drt.NewWorker(s, options)
		if err != nil {
			return err
		}
		if worked, runErr := worker.RunOnce(ctx, durable.TaskWorkflow); runErr != nil {
			return runErr
		} else if !worked {
			return fmt.Errorf("workflow task missing for %s", current.RunID)
		}
		execution, readErr := s.GetExecution(ctx, current)
		if readErr != nil {
			return readErr
		}
		fmt.Printf("run %d: %s, output: %s\n", execution.RunNumber, execution.State, execution.Output)
		if execution.NextRunID != "" {
			current.RunID = execution.NextRunID
		}
	}
	historical, err := worker.QueryExecution(ctx, drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "input"})
	if err != nil {
		return err
	}
	fmt.Printf("first run query: %s (%s)\n", historical.Output, historical.State)
	return nil
}
