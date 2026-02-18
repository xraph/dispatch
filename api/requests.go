package api

import "github.com/xraph/dispatch/job"

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

// PurgeDLQResponse contains the number of entries purged.
type PurgeDLQResponse struct {
	Purged int64 `json:"purged"`
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
