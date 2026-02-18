package job

import (
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
)

// State represents the lifecycle state of a job.
type State string

const (
	// StatePending means the job is waiting to be picked up by a worker.
	StatePending State = "pending"
	// StateRunning means a worker is currently executing the job.
	StateRunning State = "running"
	// StateCompleted means the job finished successfully.
	StateCompleted State = "completed"
	// StateFailed means the job failed and will not be retried.
	StateFailed State = "failed"
	// StateRetrying means the job failed but is scheduled for retry.
	StateRetrying State = "retrying"
	// StateCancelled means the job was explicitly cancelled.
	StateCancelled State = "cancelled"
)

// Job represents a unit of work to be processed by a worker.
type Job struct {
	dispatch.Entity

	ID          id.JobID      `json:"id"`
	Name        string        `json:"name"`
	Queue       string        `json:"queue"`
	Payload     []byte        `json:"payload"`
	State       State         `json:"state"`
	Priority    int           `json:"priority"`
	MaxRetries  int           `json:"max_retries"`
	RetryCount  int           `json:"retry_count"`
	LastError   string        `json:"last_error,omitempty"`
	ScopeAppID  string        `json:"scope_app_id,omitempty"`
	ScopeOrgID  string        `json:"scope_org_id,omitempty"`
	WorkerID    id.WorkerID   `json:"worker_id,omitempty"`
	RunAt       time.Time     `json:"run_at"`
	StartedAt   *time.Time    `json:"started_at,omitempty"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
	HeartbeatAt *time.Time    `json:"heartbeat_at,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"`
}
