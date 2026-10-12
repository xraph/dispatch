package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/xraph/dispatch/durable"
)

func (w *Worker) prepareCancellations(ctx context.Context, task durable.Task, execution durable.Execution, events []durable.Event, decision Decision, request *durable.CommitRequest) error {
	if !slices.ContainsFunc(decision.Commands, func(c Command) bool { return c.Kind == CommandCancel }) {
		return nil
	}
	history, err := parseHistory(execution, events)
	if err != nil {
		return err
	}
	commands := make(map[string]Command)
	for _, command := range history.commands {
		commands[command.ID] = command
	}
	for _, command := range decision.Commands {
		commands[command.ID] = command
	}
	// A receive consumed before awaiting cancellation wins in this transaction.
	for _, consumed := range decision.Signals {
		message := history.signals[consumed.SignalID]
		history.outcomes[consumed.CommandID] = recordedOutcome{value: Outcome{Version: 1, CommandID: consumed.CommandID, Output: bytes.Clone(message.value.Input)}, at: message.at, sequence: message.sequence}
	}
	stateEvent := request.Events[len(request.Events)-1]
	request.Events = request.Events[:len(request.Events)-1]
	suppressed := make(map[string]bool)
	for _, command := range decision.Commands {
		if command.Kind != CommandCancel {
			continue
		}
		target := commands[command.TargetID]
		_, resolved := history.outcomes[target.ID]
		cancellation := Cancellation{Version: 1, CommandID: command.ID, TargetID: target.ID, Cancelled: !resolved}
		if !resolved {
			if target.Kind != CommandSignal {
				if target.Index > int64(len(history.commands)) {
					suppressed[fmt.Sprintf("command:%d", target.Index)] = true
				} else if err := w.prepareTaskCancellation(ctx, task.Key, target, history.attempts[target.ID], &cancellation, request); err != nil {
					return err
				}
			}
			// Only coordinator bookkeeping: workflow code sees the saved event later.
			history.outcomes[target.ID] = recordedOutcome{cancellation: &cancellation}
		}
		data, marshalErr := json.Marshal(cancellation)
		if marshalErr != nil {
			return marshalErr
		}
		request.Events = append(request.Events, durable.EventInput{Type: EventFutureCancelled, Payload: data})
	}
	request.Events = append(request.Events, stateEvent)
	request.Tasks = slices.DeleteFunc(request.Tasks, func(spec durable.TaskSpec) bool { return suppressed[spec.ID] })
	if decision.State == durable.StateRunning {
		request.Tasks = append(request.Tasks, durable.TaskSpec{ID: fmt.Sprintf("workflow:cancel:%d", execution.Revision+1), Kind: durable.TaskWorkflow, Queue: task.Queue})
	}
	return nil
}

func (w *Worker) prepareTaskCancellation(ctx context.Context, key durable.Key, target Command, prior recordedAttempt, cancellation *Cancellation, request *durable.CommitRequest) error {
	id := fmt.Sprintf("command:%d", target.Index)
	current, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Task, error) {
		return w.store.GetTask(callCtx, key, id)
	})
	if errors.Is(err, durable.ErrNotFound) {
		return fmt.Errorf("%w: cancellation target task is missing", ErrHistory)
	}
	if err != nil {
		return err
	}
	if current.Key != key || current.ID != id || current.Kind != target.Kind || current.Version < 1 {
		return fmt.Errorf("%w: cancellation target task identity mismatch", ErrHistory)
	}
	if current.Done {
		return durable.ErrRevisionConflict
	}
	var payload taskPayload
	if err := decode(current.Payload, &payload); err != nil {
		return err
	}
	if payload.Version != 1 || !sameCommand(target, payload.Command) {
		return fmt.Errorf("%w: cancellation target task command mismatch", ErrHistory)
	}
	request.Conditions = append(request.Conditions, durable.TaskCondition{TaskID: id, Version: current.Version})
	request.CancelTasks = append(request.CancelTasks, id)
	if target.Kind != durable.TaskActivity || target.Version == 1 {
		return nil
	}
	cancellation.Attempt = prior.value.Attempt
	if prior.failed {
		cancellation.Heartbeat = cloneHeartbeat(prior.value.Heartbeat)
	} else if prior.value.HeartbeatEnabled {
		cancellation.Heartbeat = &HeartbeatCheckpoint{At: current.HeartbeatAt, Epoch: current.HeartbeatEpoch, Sequence: current.HeartbeatSequence, Details: bytes.Clone(current.Progress)}
		// The commit timestamp is assigned by storage. Check the observation now;
		// task version guards prevent subsequent progress from being overwritten.
		if err := validateHeartbeat(prior, cancellation.Heartbeat, current.HeartbeatAt); err != nil {
			latest, readErr := storeCall(ctx, w, func(callCtx context.Context) (durable.Execution, error) {
				return w.store.GetExecution(callCtx, key)
			})
			if readErr != nil {
				return readErr
			}
			if latest.Key == key && latest.Revision != request.ExpectedRevision {
				return durable.ErrRevisionConflict
			}
			return err
		}
	}
	return nil
}
