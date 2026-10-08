package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

func (w *Worker) processEffect(ctx context.Context, task durable.Task) error {
	var payload taskPayload
	if err := decode(task.Payload, &payload); err != nil {
		return err
	}
	command := payload.Command
	if payload.Version != 1 || command.validate() != nil || command.Kind != task.Kind ||
		task.ID != fmt.Sprintf("command:%d", command.Index) || !validID(payload.WorkflowQueue) {
		return fmt.Errorf("%w: invalid effect task payload", ErrHistory)
	}
	if task.LeaseKind == durable.LeaseTimeout {
		return w.processActivityTimeout(ctx, task, payload)
	}
	if command.Kind == durable.TaskActivity && command.Version == 2 {
		return w.processActivity(ctx, task, payload)
	}
	execution, _, err := w.effectSnapshot(ctx, task, command)
	if err != nil {
		return err
	}
	outcome := Outcome{Version: 1, CommandID: command.ID}
	if task.Kind == durable.TaskActivity {
		handler := w.options.Activities[command.Name]
		if handler == nil {
			return fmt.Errorf("%w: activity %q", ErrHandlerNotFound, command.Name)
		}
		outcome, err = callActivity(ctx, handler, ActivityInfo{Key: task.Key, CommandID: command.ID,
			BuildID: execution.BuildID, Attempt: task.Attempt}, command.Input)
		if err != nil {
			return err
		}
	}
	data, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	for attempt := range 16 {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if attempt > 0 {
			execution, _, err = w.effectSnapshot(ctx, task, command)
			if err != nil {
				return err
			}
		}
		request := taskRequest(task, execution.Revision)
		eventType := EventActivityCompleted
		if task.Kind == durable.TaskTimer {
			eventType = EventTimerFired
		}
		request.Events = []durable.EventInput{{Type: eventType, Payload: data}}
		request.Tasks = []durable.TaskSpec{{ID: fmt.Sprintf("workflow:%d", execution.Revision+1),
			Kind: durable.TaskWorkflow, Queue: payload.WorkflowQueue}}
		if err = w.persist(ctx, request); !errors.Is(err, durable.ErrRevisionConflict) {
			return err
		}
	}
	return durable.ErrRevisionConflict
}

func (w *Worker) effectSnapshot(ctx context.Context, task durable.Task, command Command) (durable.Execution, replayHistory, error) {
	execution, events, err := w.snapshot(ctx, task.Key)
	if err != nil {
		return execution, replayHistory{}, err
	}
	history, err := parseHistory(execution, events)
	if err != nil {
		return execution, history, err
	}
	if command.Index > int64(len(history.commands)) || !sameCommand(command, history.commands[command.Index-1]) {
		return execution, history, fmt.Errorf("%w: effect task does not match command history", ErrHistory)
	}
	if _, exists := history.outcomes[command.ID]; exists {
		// A timeout processor or replacement worker can publish a valid result
		// before this worker observes lease loss. Its result is now superseded.
		return execution, history, fmt.Errorf("%w: effect task already completed", durable.ErrLeaseLost)
	}
	return execution, history, nil
}

// A valid history can advance beyond a delayed claim or handler result. Check
// current ownership before treating a disagreement with that claim as corrupt.
func (w *Worker) effectConflict(ctx context.Context, task durable.Task, reason string) error {
	current, err := storeCall(ctx, w, func(callCtx context.Context) (durable.Task, error) {
		return w.store.GetTask(callCtx, task.Key, task.ID)
	})
	if err != nil {
		return err
	}
	if current.Done || current.Token() != task.Token() {
		return fmt.Errorf("%w: %s", durable.ErrLeaseLost, reason)
	}
	return fmt.Errorf("%w: %s", ErrHistory, reason)
}

func callActivity(ctx context.Context, handler ActivityFunc, info ActivityInfo, input []byte) (outcome Outcome, callErr error) {
	outcome = Outcome{Version: 1, CommandID: info.CommandID}
	if ctx.Err() != nil {
		return outcome, context.Cause(ctx)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome.Output = nil
			outcome.Failure = &ApplicationError{Type: "activity_panic", Message: fmt.Sprint(recovered)}
		}
		if callErr == nil && outcome.Failure != nil && !validFailure(outcome.Failure) {
			callErr = fmt.Errorf("%w: invalid activity failure", durable.ErrInvalid)
		}
		if ctx.Err() != nil {
			callErr = context.Cause(ctx)
		}
	}()
	output, err := handler(ctx, info, bytes.Clone(input))
	if err != nil {
		var failure *ApplicationError
		if errors.As(err, &failure) {
			if failure == nil || !validFailure(failure) {
				return outcome, fmt.Errorf("%w: invalid activity failure", durable.ErrInvalid)
			}
			copyFailure := *failure
			outcome.Failure = &copyFailure
		} else {
			outcome.Failure = &ApplicationError{Type: "application", Message: err.Error()}
		}
	} else {
		outcome.Output = bytes.Clone(output)
	}
	return outcome, nil
}
