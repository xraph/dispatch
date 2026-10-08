package contract

import (
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

type JobRow struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Queue       string    `json:"queue"`
	State       job.State `json:"state"`
	Priority    int       `json:"priority"`
	MaxRetries  int       `json:"maxRetries"`
	RetryCount  int       `json:"retryCount"`
	WorkerID    *string   `json:"workerId"`
	ScopeAppID  *string   `json:"scopeAppId"`
	ScopeOrgID  *string   `json:"scopeOrgId"`
	CreatedAt   *string   `json:"createdAt"`
	RunAt       *string   `json:"runAt"`
	StartedAt   *string   `json:"startedAt"`
	CompletedAt *string   `json:"completedAt"`
}

func projectJob(j *job.Job) JobRow {
	var workerID *string
	if !j.WorkerID.IsNil() {
		value := j.WorkerID.String()
		workerID = &value
	}
	return JobRow{
		ID:          j.ID.String(),
		Name:        j.Name,
		Queue:       j.Queue,
		State:       j.State,
		Priority:    j.Priority,
		MaxRetries:  j.MaxRetries,
		RetryCount:  j.RetryCount,
		WorkerID:    workerID,
		ScopeAppID:  nullable(j.ScopeAppID),
		ScopeOrgID:  nullable(j.ScopeOrgID),
		CreatedAt:   timestamp(j.CreatedAt),
		RunAt:       timestamp(j.RunAt),
		StartedAt:   timestampPtr(j.StartedAt),
		CompletedAt: timestampPtr(j.CompletedAt),
	}
}

type ArtifactLinkRow struct {
	ArtifactID string  `json:"artifactId"`
	Role       string  `json:"role"`
	Name       string  `json:"name"`
	Attempt    int     `json:"attempt"`
	CreatedAt  *string `json:"createdAt"`
}

type JobArtifactLinks struct {
	Enabled bool              `json:"enabled"`
	Links   []ArtifactLinkRow `json:"links"`
}

type JobDetail struct {
	JobRow
	Payload           Payload          `json:"payload"`
	LastError         *string          `json:"lastError"`
	HeartbeatAt       *string          `json:"heartbeatAt"`
	Timeout           Duration         `json:"timeout"`
	LeaseEpoch        int              `json:"leaseEpoch"`
	LeaseExpiresAt    *string          `json:"leaseExpiresAt"`
	LeaseTTL          Duration         `json:"leaseTtl"`
	EffectiveLeaseTTL Duration         `json:"effectiveLeaseTtl"`
	EvictCount        int              `json:"evictCount"`
	Resources         resource.Set     `json:"resources"`
	ResourceLimits    resource.Set     `json:"resourceLimits"`
	ResourceClass     *string          `json:"resourceClass"`
	InputBytes        int64            `json:"inputBytes"`
	PrimaryInputHash  *string          `json:"primaryInputHash"`
	ArtifactBindings  Payload          `json:"artifactBindings"`
	Artifacts         JobArtifactLinks `json:"artifacts"`
	DLQEntryID        *string          `json:"dlqEntryId"`
	AsOf              string           `json:"asOf"`
}

func resourceValues(values resource.Set) resource.Set {
	if values == nil {
		return resource.Set{}
	}
	return values.Clone()
}
