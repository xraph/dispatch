package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

// TaskTimeout polls expired activity deadlines across queues in the worker's
// namespace and pinned build. It is a poller kind, not a scheduled task kind.
const TaskTimeout durable.TaskKind = "timeout"

func (w *Worker) processActivityTimeout(ctx context.Context, task durable.Task, payload taskPayload) error {
	command := payload.Command
	if task.LeaseKind != durable.LeaseTimeout || command.Kind != durable.TaskActivity || command.Version != 2 || !hasActivityTimeout(command.ActivityOptions) {
		return fmt.Errorf("%w: invalid activity timeout task", ErrHistory)
	}
	for range 16 {
		execution, history, err := w.effectSnapshot(ctx, task, command)
		if err != nil {
			return err
		}
		prior := history.attempts[command.ID]
		deadline, kind, deadlineErr := activityDeadline(command, history.scheduled[command.ID], prior)
		if deadlineErr != nil || deadline.IsZero() || !deadline.Equal(task.DeadlineAt) {
			return w.effectConflict(ctx, task, "task deadline differs from activity history")
		}
		outcome := Outcome{Version: 2, CommandID: command.ID, Attempt: prior.value.Attempt, Timeout: kind, Failure: timeoutFailure(kind)}
		if prior.value.Attempt > 0 && !prior.failed {
			return w.finishActivity(ctx, task, payload, prior.value.Epoch, outcome)
		}
		if kind == TimeoutStartToClose {
			return w.effectConflict(ctx, task, "attempt timeout without an active attempt")
		}
		data, marshalErr := json.Marshal(outcome)
		if marshalErr != nil {
			return marshalErr
		}
		request := taskRequest(task, execution.Revision)
		request.Events = []durable.EventInput{{Type: EventActivityCompleted, Payload: data}}
		request.Tasks = []durable.TaskSpec{{ID: fmt.Sprintf("workflow:%d", execution.Revision+1), Kind: durable.TaskWorkflow, Queue: payload.WorkflowQueue}}
		if err = w.persist(ctx, request); !errors.Is(err, durable.ErrRevisionConflict) {
			return err
		}
	}
	return durable.ErrRevisionConflict
}

func activityAttemptContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeoutCause(ctx, timeout, durable.ErrTaskDeadline)
	}
	return context.WithCancel(ctx)
}

func (w *Worker) renewInterval(task durable.Task) time.Duration {
	interval := w.options.LeaseDuration / 3
	if task.Kind != durable.TaskActivity || task.LeaseKind == durable.LeaseTimeout {
		return interval
	}
	var payload taskPayload
	if decode(task.Payload, &payload) != nil || payload.Command.ActivityOptions == nil {
		return interval
	}
	options := payload.Command.ActivityOptions
	for _, deadline := range []time.Duration{options.StartToCloseTimeout, options.ScheduleToCloseTimeout} {
		if deadline > 0 {
			interval = min(interval, max(time.Microsecond, deadline/3))
		}
	}
	return interval
}
