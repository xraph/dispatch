package dlq

import (
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// Entry represents a job that has exhausted its retry budget and been
// moved to the dead letter queue for inspection or replay.
//
// The fields below the identity block exist so Replay can rebuild a job
// that behaves like the one that failed. Replay calls EnqueueJob directly
// rather than going back through the engine, so nothing re-derives these
// for it: whatever the entry does not carry, the replayed job silently
// takes a default for. They are stored as the effective values from the
// failed job rather than looked up from the definition by name, because a
// definition's declaration is only half the story. The enqueue site can
// override every one of them, and a definition can be changed or removed
// between the failure and the replay.
type Entry struct {
	ID         id.DLQID   `json:"id"`
	JobID      id.JobID   `json:"job_id"`
	JobName    string     `json:"job_name"`
	Queue      string     `json:"queue"`
	Payload    []byte     `json:"payload"`
	Error      string     `json:"error"`
	RetryCount int        `json:"retry_count"`
	MaxRetries int        `json:"max_retries"`
	ScopeAppID string     `json:"scope_app_id,omitempty"`
	ScopeOrgID string     `json:"scope_org_id,omitempty"`
	FailedAt   time.Time  `json:"failed_at"`
	ReplayedAt *time.Time `json:"replayed_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`

	// Priority is the claim ordering the job was enqueued with.
	Priority int `json:"priority,omitempty"`

	// Timeout is how long the handler was allowed to run.
	Timeout time.Duration `json:"timeout,omitempty"`

	// LeaseTTL is how long each renewal extended the job's lease. Losing
	// it is the reason this block exists: a six-hour job replayed without
	// it falls back to the pool default, which is measured in seconds, so
	// its lease lapses mid-run and it is reclaimed and restarted over and
	// over without ever finishing. That is the exact failure long-running
	// jobs carry a per-job TTL to avoid.
	LeaseTTL time.Duration `json:"lease_ttl,omitempty"`

	// ArtifactBindings is the encoded map of declared input names to
	// artifacts, carried verbatim. A handler that declares inputs cannot
	// run without them.
	ArtifactBindings []byte `json:"artifact_bindings,omitempty"`

	// Resources and ResourceLimits are what the job asked for and what it
	// was capped at. Without them a replayed job looks free to schedule
	// and can be claimed by a worker that cannot actually host it.
	Resources      resource.Set `json:"resources,omitempty"`
	ResourceLimits resource.Set `json:"resource_limits,omitempty"`

	// ResourceClass is the named class the job was placed in.
	ResourceClass string `json:"resource_class,omitempty"`

	// InputBytes and PrimaryInputHash are derived from the bindings by the
	// engine at enqueue. Replay does not run that derivation, so they are
	// carried rather than recomputed, and stay consistent with the
	// bindings above.
	InputBytes       int64  `json:"input_bytes,omitempty"`
	PrimaryInputHash string `json:"primary_input_hash,omitempty"`
}
