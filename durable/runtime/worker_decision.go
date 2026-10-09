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

func (w *Worker) processWorkflow(ctx context.Context, task durable.Task) error {
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
		if err = w.persist(ctx, request); !errors.Is(err, durable.ErrRevisionConflict) {
			return err
		}
	}
	return durable.ErrRevisionConflict
}

func decisionRequest(task durable.Task, execution durable.Execution, decision Decision) (durable.CommitRequest, error) {
	request := taskRequest(task, execution.Revision)
	for _, command := range decision.Commands {
		data, err := json.Marshal(command)
		if err != nil {
			return request, err
		}
		request.Events = append(request.Events, durable.EventInput{Type: EventCommandScheduled, Payload: data})
		if decision.State == durable.StateRunning && command.Kind != CommandSignal && command.Kind != CommandSelect {
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
	request.State, request.Output = decision.State, decision.Output
	event := durable.EventInput{Type: EventWorkflowWaiting}
	switch decision.State {
	case durable.StateCompleted:
		event.Type, event.Payload = EventWorkflowCompleted, decision.Output
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
