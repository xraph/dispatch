// This example uses memory for development. Use a qualified persistent execution
// store when queries must remain available after a process restart.
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
	worker, err := drt.NewWorker(s, drt.Options{Namespace: "example", Queue: "orders", BuildID: "v1", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		status := "pending"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(status), nil })
		value, receiveErr := w.ReceiveSignal("approval", "approve").Get()
		status = string(value)
		return value, receiveErr
	}}})
	if err != nil {
		return err
	}
	key := durable.Key{Namespace: "example", WorkflowID: "order-42", RunID: "run-1"}
	if _, err = worker.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start-42", WorkflowType: "order", BuildID: "v1", Queue: "orders"}); err != nil {
		return err
	}
	request := drt.QueryRequest{Key: key, BuildID: "v1", Name: "status"}
	before, err := worker.QueryExecution(ctx, request)
	if err != nil {
		return err
	}
	fmt.Println(string(before.Output))
	if _, err = worker.SignalExecution(ctx, durable.SignalRequest{Key: key, RequestID: "approval-42", BuildID: "v1", Name: "approve", Input: []byte("approved")}); err != nil {
		return err
	}
	after, err := worker.QueryExecution(ctx, request)
	if err != nil {
		return err
	}
	fmt.Println(string(after.Output))
	// Querying reconstructs the accepted signal but does not commit its consumption.
	execution, err := s.GetExecution(ctx, key)
	if err != nil {
		return err
	}
	if execution.Revision != 2 || execution.State != durable.StateRunning {
		return fmt.Errorf("query changed persisted workflow state")
	}
	return nil
}
