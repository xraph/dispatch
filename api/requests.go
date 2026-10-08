// Package api provides request and response types for the Dispatch API.
package api

import (
	"time"

	"github.com/xraph/dispatch/job"
)

// ──────────────────────────────────────────────────
// Job request/response DTOs
// ──────────────────────────────────────────────────

// ListJobsRequest is the request for listing jobs by state.
type ListJobsRequest struct {
	State  string `query:"state" description:"Filter by job state (pending, running, completed, failed, retrying, cancelled)"`
	Queue  string `query:"queue" description:"Filter by queue name"`
	Limit  int    `query:"limit" description:"Maximum number of results (default: 50)"`
	Offset int    `query:"offset" description:"Number of results to skip"`
}

// GetJobRequest is the request for fetching a single job.
type GetJobRequest struct {
	JobID string `path:"jobId" description:"Job ID"`
}

// CancelJobRequest is the request for cancelling a job.
type CancelJobRequest struct {
	JobID string `path:"jobId" description:"Job ID"`
}

// RetryJobRequest is the request for retrying a failed job.
type RetryJobRequest struct {
	JobID string `path:"jobId" description:"Job ID"`
}

// JobCountsResponse contains job counts by state.
type JobCountsResponse struct {
	Pending   int64 `json:"pending"`
	Running   int64 `json:"running"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Retrying  int64 `json:"retrying"`
	Cancelled int64 `json:"cancelled"`
}

// ──────────────────────────────────────────────────
// Workflow request/response DTOs
// ──────────────────────────────────────────────────

// ListWorkflowRunsRequest is the request for listing workflow runs.
type ListWorkflowRunsRequest struct {
	State  string `query:"state" description:"Filter by run state (running, completed, failed)"`
	Limit  int    `query:"limit" description:"Maximum number of results (default: 50)"`
	Offset int    `query:"offset" description:"Number of results to skip"`
}

// GetWorkflowRunRequest is the request for fetching a single workflow run.
type GetWorkflowRunRequest struct {
	RunID string `path:"runId" description:"Workflow run ID"`
}

// PlanWorkflowReplayRequest asks what replaying a run from a step would do.
type PlanWorkflowReplayRequest struct {
	RunID string `path:"runId" description:"Workflow run ID"`
	Step  string `query:"step" required:"true" description:"Checkpointed step to replay from"`
}

// ReplayWorkflowRequest replays a finished run from a step.
type ReplayWorkflowRequest struct {
	RunID string `path:"runId" description:"Workflow run ID"`
	Step  string `json:"step" description:"Checkpointed step to replay from; it and every step before it are kept"`
}

// ListWorkflowNamesResponse contains the registered workflow names.
type ListWorkflowNamesResponse struct {
	Names []string `json:"names"`
}

// ──────────────────────────────────────────────────
// DLQ request/response DTOs
// ──────────────────────────────────────────────────

// ListDLQRequest is the request for listing DLQ entries.
type ListDLQRequest struct {
	Queue  string `query:"queue" description:"Filter by queue name"`
	Limit  int    `query:"limit" description:"Maximum number of results (default: 50)"`
	Offset int    `query:"offset" description:"Number of results to skip"`
}

// GetDLQRequest is the request for fetching a single DLQ entry.
type GetDLQRequest struct {
	EntryID string `path:"entryId" description:"DLQ entry ID"`
}

// ReplayDLQRequest is the request for replaying a DLQ entry.
type ReplayDLQRequest struct {
	EntryID string `path:"entryId" description:"DLQ entry ID"`
}

// DeleteDLQRequest is the request for deleting a DLQ entry.
type DeleteDLQRequest struct {
	EntryID string `path:"entryId" description:"DLQ entry ID"`
}

// ReplayAllDLQRequest selects the entries replay-all tries. Both fields
// are query parameters, so a POST with no body still works.
type ReplayAllDLQRequest struct {
	Queue string `query:"queue" optional:"true" description:"Replay only entries from this queue (default: every queue)"`
	Limit int    `query:"limit" optional:"true" description:"Maximum number of entries to try, 1 to 1000 (default: 1000)"`
}

// ReplayAllDLQResponse counts what replay-all did with each entry it
// tried. Errors is the number that failed, as it always was.
type ReplayAllDLQResponse struct {
	Replayed      int64    `json:"replayed"`
	Conflicts     int64    `json:"conflicts"`
	Errors        int64    `json:"errors"`
	ErrorMessages []string `json:"error_messages"`
}

// PurgeDLQRequest sets the purge cutoff. Give before or older_than, not
// both; with neither the cutoff is 30 days ago. Query parameters, so a
// POST with no body still works.
type PurgeDLQRequest struct {
	Before    string `query:"before" optional:"true" description:"Purge entries that failed before this RFC 3339 time"`
	OlderThan string `query:"older_than" optional:"true" description:"Purge entries that failed longer ago than this Go duration, e.g. 72h"`
	DryRun    bool   `query:"dry_run" optional:"true" description:"Count the entries the cutoff matches without deleting any"`
}

// PurgeDLQResponse reports a purge. Matched is how many entries failed
// before the cutoff; Purged is how many were deleted, zero on a dry run.
type PurgeDLQResponse struct {
	Purged  int64     `json:"purged"`
	Matched int64     `json:"matched"`
	DryRun  bool      `json:"dry_run"`
	Before  time.Time `json:"before"`
}

// DLQCountResponse contains the DLQ entry count.
type DLQCountResponse struct {
	Count int64 `json:"count"`
}

// ──────────────────────────────────────────────────
// Cron request/response DTOs
// ──────────────────────────────────────────────────

// ListCronsRequest is the request for listing cron entries.
type ListCronsRequest struct {
	Limit  int `query:"limit" description:"Maximum number of results (default: 50)"`
	Offset int `query:"offset" description:"Number of results to skip"`
}

// GetCronRequest is the request for fetching a single cron entry.
type GetCronRequest struct {
	CronID string `path:"cronId" description:"Cron entry ID"`
}

// EnableCronRequest is the request for enabling a cron entry.
type EnableCronRequest struct {
	CronID string `path:"cronId" description:"Cron entry ID"`
}

// DisableCronRequest is the request for disabling a cron entry.
type DisableCronRequest struct {
	CronID string `path:"cronId" description:"Cron entry ID"`
}

// DeleteCronRequest is the request for deleting a cron entry.
type DeleteCronRequest struct {
	CronID string `path:"cronId" description:"Cron entry ID"`
}

// TriggerCronRequest is the request for running a cron entry's job now.
type TriggerCronRequest struct {
	CronID string `path:"cronId" description:"Cron entry ID"`
}

// ──────────────────────────────────────────────────
// Error response
// ──────────────────────────────────────────────────

// ErrorResponse is the body the router writes for an error. The operator
// routes declare it for 409 and 503, which WithErrorResponses leaves out.
type ErrorResponse struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

// ──────────────────────────────────────────────────
// Stats response
// ──────────────────────────────────────────────────

// StatsResponse contains aggregate dispatch statistics.
type StatsResponse struct {
	Jobs      JobCountsResponse `json:"jobs"`
	DLQCount  int64             `json:"dlq_count"`
	Workflows WorkflowCounts    `json:"workflows"`
}

// WorkflowCounts contains workflow run counts by state.
type WorkflowCounts struct {
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// ──────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────

func jobStateFromString(s string) job.State {
	switch s {
	case "pending":
		return job.StatePending
	case "running":
		return job.StateRunning
	case "completed":
		return job.StateCompleted
	case "failed":
		return job.StateFailed
	case "retrying":
		return job.StateRetrying
	case "cancelled":
		return job.StateCancelled
	default:
		return ""
	}
}

func defaultLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}
