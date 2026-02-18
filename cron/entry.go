package cron

import (
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
)

// Entry represents a scheduled cron job.
type Entry struct {
	dispatch.Entity

	ID          id.CronID  `json:"id"`
	Name        string     `json:"name"`
	Schedule    string     `json:"schedule"`
	JobName     string     `json:"job_name"`
	Queue       string     `json:"queue,omitempty"`
	Payload     []byte     `json:"payload,omitempty"`
	ScopeAppID  string     `json:"scope_app_id,omitempty"`
	ScopeOrgID  string     `json:"scope_org_id,omitempty"`
	LastRunAt   *time.Time `json:"last_run_at,omitempty"`
	NextRunAt   *time.Time `json:"next_run_at,omitempty"`
	LockedBy    string     `json:"locked_by,omitempty"`
	LockedUntil *time.Time `json:"locked_until,omitempty"`
	Enabled     bool       `json:"enabled"`
}
