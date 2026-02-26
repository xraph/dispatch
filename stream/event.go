// Package stream provides a real-time event broker for Dispatch lifecycle events.
// It bridges the ext.Extension system to connected clients via topic-based pub/sub.
package stream

import (
	"encoding/json"
	"time"
)

// EventType identifies the kind of lifecycle event.
type EventType string

const (
	// Job events.
	EventJobEnqueued  EventType = "job.enqueued"
	EventJobStarted   EventType = "job.started"
	EventJobCompleted EventType = "job.completed"
	EventJobFailed    EventType = "job.failed"
	EventJobRetrying  EventType = "job.retrying"
	EventJobDLQ       EventType = "job.dlq"

	// Workflow events.
	EventWorkflowStarted       EventType = "workflow.started"
	EventWorkflowStepCompleted EventType = "workflow.step_completed"
	EventWorkflowStepFailed    EventType = "workflow.step_failed"
	EventWorkflowCompleted     EventType = "workflow.completed"
	EventWorkflowFailed        EventType = "workflow.failed"

	// Cron events.
	EventCronFired EventType = "cron.fired"
)

// Event is the envelope sent to subscribers on a topic channel.
type Event struct {
	// Type identifies the lifecycle event.
	Type EventType `json:"type"`

	// Timestamp is when the event was emitted.
	Timestamp time.Time `json:"ts"`

	// Topic is the channel this event was published on.
	Topic string `json:"topic"`

	// Data is the event-specific payload.
	Data json.RawMessage `json:"data"`
}

// JobEventData is the payload for job lifecycle events.
type JobEventData struct {
	JobID      string `json:"job_id"`
	JobName    string `json:"job_name"`
	Queue      string `json:"queue"`
	ScopeAppID string `json:"scope_app_id,omitempty"`
	ScopeOrgID string `json:"scope_org_id,omitempty"`
	ElapsedMs  int64  `json:"elapsed_ms,omitempty"`
	Error      string `json:"error,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
	NextRunAt  string `json:"next_run_at,omitempty"`
}

// WorkflowEventData is the payload for workflow lifecycle events.
type WorkflowEventData struct {
	RunID      string `json:"run_id"`
	Name       string `json:"name"`
	StepName   string `json:"step_name,omitempty"`
	ScopeAppID string `json:"scope_app_id,omitempty"`
	ScopeOrgID string `json:"scope_org_id,omitempty"`
	ElapsedMs  int64  `json:"elapsed_ms,omitempty"`
	Error      string `json:"error,omitempty"`
}

// CronEventData is the payload for cron lifecycle events.
type CronEventData struct {
	EntryName string `json:"entry_name"`
	JobID     string `json:"job_id"`
}
