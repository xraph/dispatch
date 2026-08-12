package job

import (
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
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

	// ArtifactBindings carries the encoded map of declared input names to
	// artifact refs. It travels with the job because Payload is opaque to
	// the engine: bindings placed inside it would be invisible to the
	// scheduler and to the staging middleware.
	ArtifactBindings []byte `json:"artifact_bindings,omitempty"`

	// Resources is the resolved requirement, computed once at enqueue.
	// Scheduling reads this rather than calling user code.
	Resources resource.Set `json:"resources,omitempty"`

	// ResourceLimits is the resolved enforcement ceiling.
	ResourceLimits resource.Set `json:"resource_limits,omitempty"`

	// ResourceClass is forwarded to the isolation backend uninterpreted.
	ResourceClass string `json:"resource_class,omitempty"`

	// InputBytes is the total size of the declared artifact inputs. It
	// is the estimator's primary feature and the measurement bucket key.
	InputBytes int64 `json:"input_bytes,omitempty"`

	// PrimaryInputHash is the content hash of the largest declared
	// input, used as the locality-scheduling signal. Often empty: the
	// artifact plane fills content_hash at first staging, not at
	// registration, so locality helps from an artifact's second use on.
	PrimaryInputHash string `json:"primary_input_hash,omitempty"`

	// LeaseEpoch is the fencing token for the current lease. It increments
	// on every grant and every reclamation. A worker holding a stale epoch
	// has its writes rejected with ErrLeaseLost.
	LeaseEpoch int `json:"lease_epoch"`

	// LeaseExpiresAt is when the current lease lapses if not renewed.
	// Nil means no lease is held.
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	// LeaseTTL is how long each renewal extends the lease for this job,
	// copied from the definition at enqueue. Zero means the pool's default.
	//
	// This is what makes per-definition thresholds work: a 30-second job
	// and a six-hour job carry different values on their own rows, so one
	// reclaim query serves both.
	LeaseTTL time.Duration `json:"lease_ttl,omitempty"`

	// EvictCount is how many times this job has lost a worker to
	// infrastructure — a reclaimed lease, or later a graceful drain. It is
	// deliberately separate from RetryCount: a preempted job has not
	// failed, and charging preemptions to the retry budget would send a
	// healthy job to the DLQ having never once errored.
	EvictCount int `json:"evict_count"`
}
