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
	// Relative availability and deadline are resolved by the store on insertion.
	// AvailableAt and AvailableAfter are mutually exclusive. DeadlineAfter is
	// measured from the resolved availability, including a queued retry delay.
	AvailableAfter time.Duration `json:"available_after,omitempty"`
	DeadlineAfter  time.Duration `json:"deadline_after,omitempty"`
	// DeadlineLimit caps the resolved deadline, including when availability is later.
	DeadlineLimit *time.Time `json:"deadline_limit,omitempty"`
}

// Task is a persisted unit of work. Claims attach an ownership grant; its epoch
// fences earlier grants. Version protects task observations independently.
type Task struct {
	Key
	TaskSpec
	Owner      string        `json:"owner"`
	Epoch      int64         `json:"epoch"`
	Attempt    int64         `json:"attempt"`
	LeaseUntil time.Time     `json:"lease_until"`
	Version    int64         `json:"version"`
	DeadlineAt time.Time     `json:"deadline_at,omitempty"`
	Progress   []byte        `json:"progress,omitempty"`
	Done       bool          `json:"done"`
	LeaseKind  TaskLeaseKind `json:"lease_kind,omitempty"`
	// HeartbeatAt starts at attempt activation and advances on accepted progress.
	HeartbeatAt       time.Time     `json:"heartbeat_at,omitempty"`
	HeartbeatTimeout  time.Duration `json:"heartbeat_timeout,omitempty"`
	HeartbeatLimit    time.Time     `json:"heartbeat_limit,omitempty"`
	HeartbeatSequence int64         `json:"heartbeat_sequence,omitempty"`
	HeartbeatEpoch    int64         `json:"heartbeat_epoch,omitempty"`
	// AsyncKeyHash verifies callback secrets without exposing them in task JSON.
	AsyncKeyHash string `json:"-"`
}

// TaskLeaseKind distinguishes worker, asynchronous and timeout grants.
// The zero value is execution, preserving older task-token serialization.
type TaskLeaseKind string

const LeaseTimeout TaskLeaseKind = "timeout"

// LeaseAsync waits for an external result under the activity deadline.
const LeaseAsync TaskLeaseKind = "async"

// TaskToken identifies a particular ownership grant, not just a worker.
type TaskToken struct {
	TaskID    string        `json:"task_id"`
	Owner     string        `json:"owner"`
	Epoch     int64         `json:"epoch"`
	LeaseKind TaskLeaseKind `json:"lease_kind,omitempty"`
}

// Token returns a grant's identity. An asynchronous mutation also needs its secret.
func (t Task) Token() TaskToken {
	return TaskToken{TaskID: t.ID, Owner: t.Owner, Epoch: t.Epoch, LeaseKind: t.LeaseKind}
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
	// BuildID restricts claims to a pinned build. Empty permits any build.
	BuildID       string
	Namespace     string
	Queue         string
	Kind          TaskKind
	Owner         string
	LeaseDuration time.Duration
}

// TimeoutClaimRequest polls expired activities across queues in one namespace.
// BuildID is optional at the trusted store boundary; runtime workers require it.
type TimeoutClaimRequest struct {
	Namespace     string
	BuildID       string
	Owner         string
	LeaseDuration time.Duration
}

// CommitRequest atomically advances an execution and updates its claimed task.
// A nil TaskUpdate finishes the task; Keep and Retry retain its identity.
// Empty State retains the current state. Closing a run cancels all pending work
// and cannot schedule more tasks. RequestID is unique across mutations of a run.
type CommitRequest struct {
	Key
	RequestID        string          `json:"request_id"`
	ExpectedRevision int64           `json:"expected_revision"`
	Token            TaskToken       `json:"token"`
	Events           []EventInput    `json:"events"`
	Tasks            []TaskSpec      `json:"tasks,omitempty"`
	State            State           `json:"state,omitempty"`
	Output           []byte          `json:"output,omitempty"`
	TaskUpdate       *TaskUpdate     `json:"task_update,omitempty"`
	Conditions       []TaskCondition `json:"conditions,omitempty"`
	CancelTasks      []string        `json:"cancel_tasks,omitempty"`
	// CancelPendingTasks fences existing pending tasks before creating Tasks.
	// The source must complete; new cleanup work remains runnable.
	CancelPendingTasks bool `json:"cancel_pending_tasks,omitempty"`
	// AsyncSecret is required only for an asynchronous grant. Receipts store its
	// request fingerprint, never this secret. Do not log callback requests.
	AsyncSecret string `json:"async_secret,omitempty"`
	// IntentDigest binds the complete client request, independently of derived
	// revision/task observations. Only the trusted coordinator may construct it.
	// Keep it private; LookupReceipt uses it as a recovery proof.
	IntentDigest string `json:"intent_digest,omitempty"`
}

// Receipt records the original result of an accepted request.
// It is returned unchanged on retries, even after subsequent transitions.
type Receipt struct {
	Revision      int64 `json:"revision"`
	FirstSequence int64 `json:"first_sequence"`
	LastSequence  int64 `json:"last_sequence"`
}

// TaskAction determines whether a transition consumes or retains its source task.
type TaskAction string

const (
	TaskComplete TaskAction = "complete"
	TaskKeep     TaskAction = "keep"
	TaskRetry    TaskAction = "retry"
	TaskAwait    TaskAction = "await"
)

// TaskUpdate modifies the source task atomically with its execution history.
// A nil update finishes the task. Retry clears its ownership grant immediately.
// A nil deadline/progress leaves that field unchanged. A zero pointed deadline
// explicitly clears it; a nonzero deadline is relative to store time (Keep) or
// the resolved retry availability (Retry).
type TaskUpdate struct {
	Action        TaskAction     `json:"action"`
	RetryAt       time.Time      `json:"retry_at,omitempty"`
	RetryAfter    time.Duration  `json:"retry_after,omitempty"`
	DeadlineAfter *time.Duration `json:"deadline_after,omitempty"`
	Progress      *[]byte        `json:"progress,omitempty"`
	// DeadlineLimit caps a newly calculated or retained deadline.
	DeadlineLimit *time.Time `json:"deadline_limit,omitempty"`
	// LeaseDuration renews a retained execution grant in the same transaction.
	LeaseDuration time.Duration `json:"lease_duration,omitempty"`
	// Heartbeat enables progress recording for this retained activity grant.
	Heartbeat *HeartbeatConfig `json:"heartbeat,omitempty"`
	// AsyncKeyHash transfers a live activity grant to asynchronous ownership.
	// Only TaskAwait accepts it; all other update fields must be absent.
	AsyncKeyHash string `json:"async_key_hash,omitempty"`
}

// TaskCondition protects an observation of an unfinished task in the same run.
// DeadlineElapsed additionally requires a nonzero deadline at or before store time.
// Cancellation requires a condition for every target and cannot target the source.
type TaskCondition struct {
	TaskID          string `json:"task_id"`
	Version         int64  `json:"version"`
	DeadlineElapsed bool   `json:"deadline_elapsed,omitempty"`
}
