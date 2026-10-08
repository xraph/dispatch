// Package durable defines the transactional persistence contract for execution
// history and tasks. The coordinator validates workflow commands before using it.
package durable

import "time"

// Key identifies one run. Namespace is mandatory on every operation.
type Key struct {
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

// State is the persisted execution lifecycle.
type State string

// Execution states. Only Running accepts new transitions.
const (
	StateRunning        State = "running"
	StateCompleted      State = "completed"
	StateFailed         State = "failed"
	StateCancelled      State = "cancelled"
	StateTerminated     State = "terminated"
	StateTimedOut       State = "timed_out"
	StateContinuedAsNew State = "continued_as_new"
)

// Execution is the current state projected alongside its ordered history.
type Execution struct {
	Key
	WorkflowType string    `json:"workflow_type"`
	BuildID      string    `json:"build_id"`
	State        State     `json:"state"`
	Revision     int64     `json:"revision"`
	LastSequence int64     `json:"last_sequence"`
	Input        []byte    `json:"input,omitempty"`
	Output       []byte    `json:"output,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// EventInput supplies an event's content. The store assigns sequence and time.
type EventInput struct {
	Type    string `json:"type"`
	Payload []byte `json:"payload,omitempty"`
}

// Event is an immutable history entry, ordered within one run.
type Event struct {
	EventInput
	Sequence int64     `json:"sequence"`
	Time     time.Time `json:"time"`
}

// TaskKind separates workflow decisions, activities and timer processing.
type TaskKind string

// Task kinds use separate pollers even when they share a queue name.
const (
	TaskWorkflow TaskKind = "workflow"
	TaskActivity TaskKind = "activity"
	TaskTimer    TaskKind = "timer"
)

// TaskSpec schedules work in the same transaction as its history events.
// IDs are unique for the lifetime of a run, including completed tasks.
// A zero AvailableAt means immediately; timers require an explicit deadline.
type TaskSpec struct {
	ID          string    `json:"id"`
	Kind        TaskKind  `json:"kind"`
	Queue       string    `json:"queue"`
	Payload     []byte    `json:"payload,omitempty"`
	AvailableAt time.Time `json:"available_at"`
}

// Task is a claimed unit of work. Epoch fences previous ownership grants.
type Task struct {
	Key
	TaskSpec
	Owner      string    `json:"owner"`
	Epoch      int64     `json:"epoch"`
	Attempt    int64     `json:"attempt"`
	LeaseUntil time.Time `json:"lease_until"`
}

// TaskToken identifies a particular ownership grant, not just a worker.
type TaskToken struct {
	TaskID string `json:"task_id"`
	Owner  string `json:"owner"`
	Epoch  int64  `json:"epoch"`
}

// Token returns the ownership grant required for renewal and completion.
func (t Task) Token() TaskToken {
	return TaskToken{TaskID: t.ID, Owner: t.Owner, Epoch: t.Epoch}
}

// StartRequest creates a run, its first event and its initial workflow task.
// Reuse the entire request, including RunID, when retrying an unknown outcome.
type StartRequest struct {
	Key
	RequestID    string `json:"request_id"`
	WorkflowType string `json:"workflow_type"`
	BuildID      string `json:"build_id"`
	Queue        string `json:"queue"`
	Input        []byte `json:"input,omitempty"`
}

// ClaimRequest polls a single namespace, task kind and queue.
type ClaimRequest struct {
	Namespace     string
	Queue         string
	Kind          TaskKind
	Owner         string
	LeaseDuration time.Duration
}

// CommitRequest atomically finishes a claimed task and advances an execution.
// Empty State retains the current state. Closing a run cancels all pending work
// and cannot schedule more tasks. RequestID is unique across mutations of a run.
type CommitRequest struct {
	Key
	RequestID        string       `json:"request_id"`
	ExpectedRevision int64        `json:"expected_revision"`
	Token            TaskToken    `json:"token"`
	Events           []EventInput `json:"events"`
	Tasks            []TaskSpec   `json:"tasks,omitempty"`
	State            State        `json:"state,omitempty"`
	Output           []byte       `json:"output,omitempty"`
}

// Receipt records the original result of an accepted request.
// It is returned unchanged on retries, even after subsequent transitions.
type Receipt struct {
	Revision      int64 `json:"revision"`
	FirstSequence int64 `json:"first_sequence"`
	LastSequence  int64 `json:"last_sequence"`
}
