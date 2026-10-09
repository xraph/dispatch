package runtime

import (
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// CommandCancel requests a recorded cancellation without creating a polled task.
const CommandCancel durable.TaskKind = "cancel"

// EventFutureCancelled acknowledges fencing or preserves an existing outcome.
const EventFutureCancelled = "workflow.future_cancelled"

// ErrCancelled identifies a future whose result was fenced by cancellation.
var ErrCancelled = errors.New("durable runtime: future cancelled")

// Cancellation is the persisted disposition of one cancellation command.
// Attempt and Heartbeat describe a newly canceled version 2 activity only.
type Cancellation struct {
	Version   int                  `json:"version"`
	CommandID string               `json:"command_id"`
	TargetID  string               `json:"target_id"`
	Cancelled bool                 `json:"cancelled"`
	Attempt   int64                `json:"attempt,omitempty"`
	Heartbeat *HeartbeatCheckpoint `json:"heartbeat,omitempty"`
}

// CancelledError carries copied progress from a canceled activity, or zero
// metadata for a timer or receive. Storage fencing cannot undo external effects.
type CancelledError struct {
	CommandID string
	Attempt   int64
	Heartbeat *HeartbeatCheckpoint
}

func (e *CancelledError) Error() string { return fmt.Sprintf("%s: %s", ErrCancelled, e.CommandID) }
func (e *CancelledError) Unwrap() error { return ErrCancelled }

// Cancel requests cancellation of an activity, timer or signal-receive future
// from this evaluation. Use a unique stable ID for each request. Wait on the
// returned future for persistence acknowledgment; it returns nil output/error
// whether cancellation fenced the target or an existing result was preserved.
// An acknowledgment does not prove an external operation physically stopped.
func (w *Workflow) Cancel(id string, target *Future) *Future {
	w.checkOperation()
	if target == nil || target.workflow != w || !w.ids[target.id] || !cancellableKind(target.kind) {
		w.stop(fmt.Errorf("%w: cancellation needs an activity, timer or receive from this evaluation", durable.ErrInvalid))
	}
	return w.schedule(Command{ID: id, Kind: CommandCancel, TargetID: target.id})
}

func cancellableKind(kind durable.TaskKind) bool {
	return kind == durable.TaskActivity || kind == durable.TaskTimer || kind == CommandSignal
}

func validateCancellationCommand(command Command) error {
	if command.Version != 1 || !validID(command.TargetID) || command.Name != "" || command.Queue != "" || len(command.Input) != 0 || command.Delay != 0 || !command.Deadline.IsZero() || command.ActivityOptions != nil || len(command.Candidates) != 0 {
		return fmt.Errorf("%w: invalid cancellation command", durable.ErrInvalid)
	}
	return nil
}
