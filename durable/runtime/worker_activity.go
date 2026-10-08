package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// FailureWorkerLost denotes an attempt whose grant expired without a result.
// The external operation may have succeeded; retries still require idempotency.
const FailureWorkerLost = "activity_worker_lost"

func (w *Worker) processActivity(ctx context.Context, task durable.Task, payload taskPayload) error {
	command := payload.Command
	handler := w.options.Activities[command.Name]
	if handler == nil {
		return fmt.Errorf("%w: activity %q", ErrHandlerNotFound, command.Name)
	}
	for range 16 {
		execution, history, err := w.effectSnapshot(ctx, task, command)
		if err != nil {
			return err
		}
		prior := history.attempts[command.ID]
		if prior.value.Epoch >= task.Epoch {
			return w.effectConflict(ctx, task, "activity grant already started")
		}
		if prior.value.Attempt > 0 && !prior.failed {
			// This claim belongs to a replacement worker. Resolve the previous attempt
			// through its policy before scheduling another call to external code.
			outcome := Outcome{Version: 2, CommandID: command.ID, Attempt: prior.value.Attempt,
				Failure: &ApplicationError{Type: FailureWorkerLost, Message: "activity ownership expired before its result was committed"}}
			return w.finishActivity(ctx, task, payload, prior.value.Epoch, outcome)
		}
		if prior.failed && !task.AvailableAt.Equal(prior.retryAt) {
			return w.effectConflict(ctx, task, "activity retry availability differs from history")
		}
		attempt := ActivityAttempt{Version: 1, CommandID: command.ID, Attempt: prior.value.Attempt + 1, Epoch: task.Epoch}
		data, err := json.Marshal(attempt)
		if err != nil {
			return err
		}
		request := taskRequest(task, execution.Revision)
		request.Events = []durable.EventInput{{Type: EventActivityAttemptStarted, Payload: data}}
		request.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep}
		if hasActivityTimeout(command.ActivityOptions) {
			limit, limitErr := activityOverallLimit(command, history.scheduled[command.ID])
			if limitErr != nil {
				return limitErr
			}
			deadline := command.ActivityOptions.StartToCloseTimeout
			request.TaskUpdate.DeadlineAfter, request.TaskUpdate.DeadlineLimit, request.TaskUpdate.LeaseDuration = &deadline, limit, w.options.LeaseDuration
		}
		// Start the cooperative timer before persistence so acknowledgement
		// latency cannot extend it. Store deadlines remain authoritative.
		attemptCtx, cancelAttempt := activityAttemptContext(ctx, command.ActivityOptions.StartToCloseTimeout)
		if err = w.persist(ctx, request); errors.Is(err, durable.ErrRevisionConflict) {
			cancelAttempt()
			continue
		}
		if err != nil {
			cancelAttempt()
			return err
		}
		outcome, callErr := callActivity(attemptCtx, handler, ActivityInfo{Key: task.Key, CommandID: command.ID, BuildID: execution.BuildID, Attempt: attempt.Attempt}, command.Input)
		cancelAttempt()
		if callErr != nil {
			return callErr
		}
		outcome.Version, outcome.Attempt = 2, attempt.Attempt
		return w.finishActivity(ctx, task, payload, attempt.Epoch, outcome)
	}
	return durable.ErrRevisionConflict
}

func (w *Worker) finishActivity(ctx context.Context, task durable.Task, payload taskPayload, epoch int64, outcome Outcome) error {
	command := payload.Command
	delay := retryDelay(*command.ActivityOptions.RetryPolicy, outcome.Attempt, outcome.Failure)
	for range 16 {
		execution, history, err := w.effectSnapshot(ctx, task, command)
		if err != nil {
			return err
		}
		prior := history.attempts[command.ID]
		if prior.failed || prior.value.Attempt != outcome.Attempt || prior.value.Epoch != epoch {
			return w.effectConflict(ctx, task, "activity result does not match active attempt")
		}
		request := taskRequest(task, execution.Revision)
		if outcome.Failure != nil {
			failed := ActivityAttempt{Version: 1, CommandID: command.ID, Attempt: outcome.Attempt, Epoch: epoch, Failure: outcome.Failure, RetryAfter: delay, Timeout: outcome.Timeout}
			data, marshalErr := json.Marshal(failed)
			if marshalErr != nil {
				return marshalErr
			}
			request.Events = append(request.Events, durable.EventInput{Type: EventActivityAttemptFailed, Payload: data})
		}
		if delay > 0 {
			request.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskRetry, RetryAfter: delay}
			if hasActivityTimeout(command.ActivityOptions) {
				limit, limitErr := activityOverallLimit(command, history.scheduled[command.ID])
				if limitErr != nil {
					return limitErr
				}
				queueDeadline := command.ActivityOptions.ScheduleToStartTimeout
				request.TaskUpdate.DeadlineAfter, request.TaskUpdate.DeadlineLimit = &queueDeadline, limit
			}
		} else {
			data, marshalErr := json.Marshal(outcome)
			if marshalErr != nil {
				return marshalErr
			}
			request.Events = append(request.Events, durable.EventInput{Type: EventActivityCompleted, Payload: data})
			request.Tasks = []durable.TaskSpec{{ID: fmt.Sprintf("workflow:%d", execution.Revision+1), Kind: durable.TaskWorkflow, Queue: payload.WorkflowQueue}}
		}
		if err = w.persist(ctx, request); !errors.Is(err, durable.ErrRevisionConflict) {
			return err
		}
	}
	return durable.ErrRevisionConflict
}
