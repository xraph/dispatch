package contract

import (
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/resource"
)

type DLQRow struct {
	ID            string  `json:"id"`
	JobID         string  `json:"jobId"`
	JobName       string  `json:"jobName"`
	Queue         string  `json:"queue"`
	Error         *string `json:"error"`
	RetryCount    int     `json:"retryCount"`
	MaxRetries    int     `json:"maxRetries"`
	ScopeAppID    *string `json:"scopeAppId"`
	ScopeOrgID    *string `json:"scopeOrgId"`
	FailedAt      *string `json:"failedAt"`
	CreatedAt     *string `json:"createdAt"`
	ReplayedAt    *string `json:"replayedAt"`
	ReplayedJobID *string `json:"replayedJobId"`
}

func projectDLQ(e *dlq.Entry) DLQRow {
	var replayedID *string
	if e.ReplayedJobID != nil && !e.ReplayedJobID.IsNil() {
		raw := e.ReplayedJobID.String()
		replayedID = &raw
	}
	return DLQRow{ID: e.ID.String(), JobID: e.JobID.String(), JobName: e.JobName, Queue: e.Queue, Error: nullable(e.Error),
		RetryCount: e.RetryCount, MaxRetries: e.MaxRetries, ScopeAppID: nullable(e.ScopeAppID), ScopeOrgID: nullable(e.ScopeOrgID),
		FailedAt: timestamp(e.FailedAt), CreatedAt: timestamp(e.CreatedAt), ReplayedAt: timestampPtr(e.ReplayedAt), ReplayedJobID: replayedID}
}

type DLQDetail struct {
	DLQRow
	Payload          Payload      `json:"payload"`
	Priority         int          `json:"priority"`
	Timeout          Duration     `json:"timeout"`
	LeaseTTL         Duration     `json:"leaseTtl"`
	ArtifactBindings Payload      `json:"artifactBindings"`
	Resources        resource.Set `json:"resources"`
	ResourceLimits   resource.Set `json:"resourceLimits"`
	ResourceClass    *string      `json:"resourceClass"`
	InputBytes       int64        `json:"inputBytes"`
	PrimaryInputHash *string      `json:"primaryInputHash"`
	AsOf             string       `json:"asOf"`
}
