package dlq

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Service provides high-level DLQ operations over a Store.
type Service struct {
	store    Store
	jobStore job.Store
}

// NewService creates a DLQ service.
func NewService(store Store, jobStore job.Store) *Service {
	return &Service{store: store, jobStore: jobStore}
}

// Push builds a DLQ Entry from a failed job and persists it.
// The error string is captured from the original handler error.
func (s *Service) Push(ctx context.Context, j *job.Job, jobErr error) error {
	now := time.Now().UTC()
	entry := &Entry{
		ID:         id.NewDLQID(),
		JobID:      j.ID,
		JobName:    j.Name,
		Queue:      j.Queue,
		Payload:    j.Payload,
		Error:      jobErr.Error(),
		RetryCount: j.RetryCount,
		MaxRetries: j.MaxRetries,
		ScopeAppID: j.ScopeAppID,
		ScopeOrgID: j.ScopeOrgID,
		FailedAt:   now,
		CreatedAt:  now,

		// Everything below is carried so Replay can rebuild a job that
		// behaves like this one. See the Entry doc for why it is copied
		// from the job rather than looked up from the definition.
		Priority:         j.Priority,
		Timeout:          j.Timeout,
		LeaseTTL:         j.LeaseTTL,
		ArtifactBindings: j.ArtifactBindings,
		Resources:        j.Resources,
		ResourceLimits:   j.ResourceLimits,
		ResourceClass:    j.ResourceClass,
		InputBytes:       j.InputBytes,
		PrimaryInputHash: j.PrimaryInputHash,
	}
	return s.store.PushDLQ(ctx, entry)
}

// DLQStore returns the underlying DLQ store for direct access
// to List, Get, Purge, and Count operations.
func (s *Service) DLQStore() Store {
	return s.store
}
