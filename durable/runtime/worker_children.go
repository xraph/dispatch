package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/xraph/dispatch/durable"
)

func addChildDecision(task durable.Task, decision Decision, request *durable.CommitRequest) {
	if decision.State != durable.StateRunning {
		return
	}
	for _, command := range decision.Commands {
		switch command.Kind {
		case CommandChild:
			queue := command.Queue
			if queue == "" {
				queue = task.Queue
			}
			request.Children = append(request.Children, durable.ChildStartSpec{CommandID: command.ID, Start: durable.StartRequest{Key: command.Child.Key, RequestID: "child-start", WorkflowType: command.Name, BuildID: command.Child.BuildID, Queue: queue, Input: command.Input, RunTimeout: command.Child.RunTimeout, ExecutionTimeout: command.Child.ExecutionTimeout}, ParentQueue: task.Queue, ParentClosePolicy: command.Child.ParentClosePolicy})
		case CommandCancelChild:
			request.CancelChildren = append(request.CancelChildren, durable.ChildCancellationSpec{CommandID: command.ID, TargetID: command.TargetID})
		}
	}
	if len(request.Children) != 0 {
		addChildWakeup(task.Queue, request)
	}
}

func addChildWakeup(queue string, request *durable.CommitRequest) {
	id := fmt.Sprintf("workflow:child-start:%d", request.ExpectedRevision+1)
	if !slices.ContainsFunc(request.Tasks, func(task durable.TaskSpec) bool { return task.ID == id }) {
		request.Tasks = append(request.Tasks, durable.TaskSpec{ID: id, Kind: durable.TaskWorkflow, Queue: queue})
	}
}

// Each definitive creation conflict replaces one child start with a failure.
// All remaining children and the parent decision still commit atomically.
func (w *Worker) persistChildDecision(ctx context.Context, task durable.Task, events []durable.Event, request durable.CommitRequest) error {
	if len(request.Children) == 0 && len(request.CancelChildren) == 0 {
		return w.persist(ctx, request)
	}
	failed := make(map[string]ChildStartFailure)
	for _, event := range events {
		if event.Type != EventChildStartFailed {
			continue
		}
		var failure ChildStartFailure
		if err := decode(event.Payload, &failure); err != nil {
			return err
		}
		failed[failure.CommandID] = failure
	}
	for attempt := 0; attempt <= len(request.Children); attempt++ {
		candidate, err := childDecisionWithFailures(task.Queue, request, failed)
		if err != nil {
			return err
		}
		err = w.persist(ctx, candidate)
		var conflict *durable.ChildStartError
		if !errors.As(err, &conflict) || !errors.Is(conflict, durable.ErrExists) {
			return err
		}
		index := slices.IndexFunc(request.Children, func(child durable.ChildStartSpec) bool { return child.CommandID == conflict.CommandID })
		if index < 0 {
			return err
		}
		if _, already := failed[conflict.CommandID]; already {
			return err
		}
		failed[conflict.CommandID] = ChildStartFailure{Version: 1, CommandID: conflict.CommandID, Child: request.Children[index].Start.Key, Failure: &ApplicationError{Type: FailureChildStartConflict, Message: "workflow identity is already in use", NonRetryable: true}}
	}
	return fmt.Errorf("%w: child conflict limit exceeded", durable.ErrInvalid)
}

func childDecisionWithFailures(queue string, original durable.CommitRequest, failed map[string]ChildStartFailure) (durable.CommitRequest, error) {
	candidate := original
	candidate.Children = nil
	candidate.CancelChildren = nil
	candidate.Tasks = slices.Clone(original.Tasks)
	candidate.Events = slices.Clone(original.Events[:len(original.Events)-1])
	for _, child := range original.Children {
		failure, exists := failed[child.CommandID]
		if !exists {
			candidate.Children = append(candidate.Children, child)
			continue
		}
		payload, err := json.Marshal(failure)
		if err != nil {
			return durable.CommitRequest{}, err
		}
		candidate.Events = append(candidate.Events, durable.EventInput{Type: EventChildStartFailed, Payload: payload})
	}
	for _, cancel := range original.CancelChildren {
		failure, exists := failed[cancel.TargetID]
		if !exists {
			candidate.CancelChildren = append(candidate.CancelChildren, cancel)
			continue
		}
		payload, err := json.Marshal(ChildCancellationFailure{Version: 1, CommandID: cancel.CommandID, TargetID: cancel.TargetID, Child: failure.Child})
		if err != nil {
			return durable.CommitRequest{}, err
		}
		candidate.Events = append(candidate.Events, durable.EventInput{Type: EventChildCancellationFailed, Payload: payload})
		addChildWakeup(queue, &candidate)
	}
	candidate.Events = append(candidate.Events, original.Events[len(original.Events)-1])
	return candidate, nil
}

func (w *Worker) runChildDelivery(ctx context.Context) (bool, error) {
	delivery, err := storeCall(ctx, w, func(callCtx context.Context) (*durable.ChildDelivery, error) {
		return w.store.ClaimChildDelivery(callCtx, durable.ChildDeliveryClaimRequest{Namespace: w.options.Namespace, BuildID: w.options.BuildID, Owner: w.options.Owner, LeaseDuration: w.options.LeaseDuration})
	})
	if err != nil || delivery == nil {
		return false, err
	}
	// Close/cancel claims follow the current child build. Their saved routing
	// still identifies the original invocation, even after it continues.
	toParent := delivery.Kind == durable.ChildDeliveryResult || delivery.Kind == durable.ChildDeliveryCancelAck
	if delivery.Source.Namespace != w.options.Namespace || toParent && delivery.TargetBuildID != w.options.BuildID || delivery.Owner != w.options.Owner || delivery.Validate() != nil {
		return true, fmt.Errorf("%w: delivery does not match worker routing", durable.ErrInvalid)
	}
	request := durable.ChildDeliveryRequest{Source: delivery.Source, DeliveryID: delivery.ID, RequestID: fmt.Sprintf("delivery:%d", delivery.Epoch), Owner: delivery.Owner, Epoch: delivery.Epoch}
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return true, context.Cause(ctx)
		}
		_, last = storeCall(ctx, w, func(callCtx context.Context) (durable.ChildDeliveryReceipt, error) {
			return w.store.ApplyChildDelivery(callCtx, request)
		})
		if last == nil || definitiveCommitError(last) {
			return true, last
		}
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); waitErr != nil {
				return true, waitErr
			}
		}
	}
	return true, fmt.Errorf("apply child delivery after retries: %w", last)
}
