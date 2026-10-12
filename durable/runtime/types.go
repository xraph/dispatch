package runtime

import (
	"errors"
	"time"

	"github.com/xraph/dispatch/durable"
)

// History event types understood by this version of the Go runtime.
const (
	EventStarted                = "execution.started"
	EventCommandScheduled       = "workflow.command_scheduled"
	EventWorkflowWaiting        = "workflow.waiting"
	EventActivityCompleted      = "activity.completed"
	EventActivityDeferred       = "activity.deferred"
	EventActivityAttemptStarted = "activity.attempt_started"
	EventActivityAttemptFailed  = "activity.attempt_failed"
	EventTimerFired             = "timer.fired"
	EventWorkflowCompleted      = "workflow.completed"
	EventWorkflowFailed         = "workflow.failed"
)

// Evaluation errors leave the workflow task uncommitted for operator recovery.
var (
	ErrNondeterministic = errors.New("durable runtime: workflow does not match history")
	ErrHistory          = errors.New("durable runtime: invalid history")
	ErrWorkflowPanic    = errors.New("durable runtime: workflow panicked")
)

// WorkflowFunc contains deterministic decisions, never external effects.
// The runtime replays it from its beginning whenever an outcome becomes available.
type WorkflowFunc func(*Workflow, []byte) ([]byte, error)

// Command records one decision before its associated task can execute.
// Index is one-based and monotonically increasing within a run.
type Command struct {
	Version         int              `json:"version"`
	Index           int64            `json:"index"`
	ID              string           `json:"id"`
	Kind            durable.TaskKind `json:"kind"`
	Name            string           `json:"name,omitempty"`
	Queue           string           `json:"queue,omitempty"`
	Input           []byte           `json:"input,omitempty"`
	Delay           time.Duration    `json:"delay,omitempty"`
	Deadline        time.Time        `json:"deadline,omitempty"`
	ActivityOptions *ActivityOptions `json:"activity_options,omitempty"`
	Candidates      []string         `json:"candidates,omitempty"`
	TargetID        string           `json:"target_id,omitempty"`
	Child           *ChildCommand    `json:"child,omitempty"`
}

// ApplicationError is a recorded application failure. Type allows deterministic handling.
type ApplicationError struct {
	Type         string `json:"type"`
	Message      string `json:"message"`
	NonRetryable bool   `json:"non_retryable,omitempty"`
}

func (f *ApplicationError) Error() string { return f.Message }

// Outcome binds an activity result or timer firing to its scheduled command.
// Output and Failure are mutually exclusive.
type Outcome struct {
	Version   int                  `json:"version"`
	CommandID string               `json:"command_id"`
	Attempt   int64                `json:"attempt,omitempty"`
	Timeout   ActivityTimeoutKind  `json:"timeout,omitempty"`
	Output    []byte               `json:"output,omitempty"`
	Failure   *ApplicationError    `json:"failure,omitempty"`
	Heartbeat *HeartbeatCheckpoint `json:"heartbeat,omitempty"`
}

// Decision contains new commands, signal consumptions and selections. Running means a future is unresolved.
// An evaluation error never returns a usable decision.
type Decision struct {
	Continuation      *Continuation
	Commands          []Command
	Signals           []SignalConsumption
	Selections        []Selection
	State             durable.State
	Output            []byte
	Failure           *ApplicationError
	CancellationStart *CancellationStart
	Cancelled         *durable.ExecutionCancellation
}
