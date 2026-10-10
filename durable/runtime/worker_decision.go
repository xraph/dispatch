package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

type taskPayload struct {
	Version       int     `json:"version"`
	Command       Command `json:"command"`
	WorkflowQueue string  `json:"workflow_queue"`
}

func (w *Worker) processWorkflow(ctx context.Context, task durable.Task, lease *taskLease) error {
	var conflict error
	for range 16 {
		execution, events, err := w.snapshot(ctx, task.Key)
		if err != nil {
			return err
		}
		handler := w.options.Workflows[execution.WorkflowType]
		if handler == nil {
			return fmt.Errorf("%w: workflow %q", ErrHandlerNotFound, execution.WorkflowType)
		}
		decision, err := Evaluate(execution, events, handler)
		if err != nil {
			return err
		}
		request, err := decisionRequest(task, execution, decision)
		if err != nil {
			return err
		}
		err = w.prepareCancellations(ctx, task, execution, events, decision, &request)
		if err == nil {
			err = w.persistChildDecision(ctx, task, events, request)
		}
		var refusal *durable.BuildAdmissionError
		if errors.As(err, &refusal) {
			err = w.deferWorkflow(ctx, task, execution.Revision, refusal, lease)
		}
		if errors.Is(err, durable.ErrAdmissionChanged) {
			conflict = err
			continue
		}
		if !errors.Is(err, durable.ErrRevisionConflict) && !errors.Is(err, durable.ErrTaskConflict) {
			return err
		}
		conflict = err
	}
	return conflict
}

func decisionRequest(task durable.Task, execution durable.Execution, decision Decision) (durable.CommitRequest, error) {
	request := taskRequest(task, execution.Revision)
	if decision.CancellationStart != nil {
		data, err := json.Marshal(decision.CancellationStart)
		if err != nil {
			return request, err
		}
		request.Events = []durable.EventInput{{Type: EventCancellationStarted, Payload: data}}
		request.CancelPendingTasks, request.State = true, durable.StateRunning
		request.Tasks = []durable.TaskSpec{{ID: fmt.Sprintf("workflow:cancel-cleanup:%d", execution.Revision+1), Kind: durable.TaskWorkflow, Queue: task.Queue}}
		return request, nil
	}
	for _, command := range decision.Commands {
		data, err := json.Marshal(command)
		if err != nil {
			return request, err
		}
		request.Events = append(request.Events, durable.EventInput{Type: EventCommandScheduled, Payload: data})
		if decision.State == durable.StateRunning && command.Kind != CommandSignal && command.Kind != CommandSelect && command.Kind != CommandCancel && command.Kind != CommandChild && command.Kind != CommandCancelChild {
			payload, payloadErr := json.Marshal(taskPayload{Version: 1, Command: command, WorkflowQueue: task.Queue})
			if payloadErr != nil {
				return request, payloadErr
			}
			queue := command.Queue
			if queue == "" {
				queue = task.Queue
			}
			request.Tasks = append(request.Tasks, durable.TaskSpec{ID: fmt.Sprintf("command:%d", command.Index),
				Kind: command.Kind, Queue: queue, Payload: payload, AvailableAt: command.Deadline, DeadlineAfter: firstActivityTimeout(command.ActivityOptions)})
		}
	}
	for _, consumed := range decision.Signals {
		data, err := json.Marshal(consumed)
		if err != nil {
			return request, err
		}
		request.Events = append(request.Events, durable.EventInput{Type: EventSignalConsumed, Payload: data})
	}
	for _, selected := range decision.Selections {
		data, err := json.Marshal(selected)
		if err != nil {
			return request, err
		}
		request.Events = append(request.Events, durable.EventInput{Type: EventSelected, Payload: data})
	}
	addChildDecision(task, decision, &request)
	request.State, request.Output = decision.State, decision.Output
	event := durable.EventInput{Type: EventWorkflowWaiting}
	switch decision.State {
	case durable.StateContinuedAsNew:
		if decision.Continuation == nil {
			return request, fmt.Errorf("%w: missing continuation decision", durable.ErrInvalid)
		}
		value := *decision.Continuation
		if value.Next.Queue == "" {
			value.Next.Queue = task.Queue
		}
		data, err := json.Marshal(value)
		if err != nil {
			return request, err
		}
		request.Continuation = &value.Next
		event.Type, event.Payload = EventContinuationRequested, data
	case durable.StateCompleted:
		event.Type, event.Payload = EventWorkflowCompleted, decision.Output
	case durable.StateCancelled:
		data, err := json.Marshal(decision.Cancelled)
		if err != nil {
			return request, err
		}
		event.Type, event.Payload = EventWorkflowCancelled, data
	case durable.StateFailed:
		data, err := json.Marshal(decision.Failure)
		if err != nil {
			return request, err
		}
		event.Type, event.Payload = EventWorkflowFailed, data
	}
	request.Events = append(request.Events, event)
	return request, nil
}
