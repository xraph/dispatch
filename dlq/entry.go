package dlq

import (
	"time"

	"github.com/xraph/dispatch/id"
)

// Entry represents a job that has exhausted its retry budget and been
// moved to the dead letter queue for inspection or replay.
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
}
