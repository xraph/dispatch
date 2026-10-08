package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

var (
	_ dlq.ReplayClaimer    = (*Store)(nil)
	_ cron.TargetedUpdater = (*Store)(nil)
	_ workflow.Reopener    = (*Store)(nil)
)

// The writes below replace the stored struct with an updated copy rather
// than writing through the stored pointer. This store keeps the caller's
// pointer on create and GetDLQ, GetCron and GetRun hand that same pointer
// out, so an in-place write would change values a caller is holding, and
// race with any caller reading them.

// cloneDLQEntry deep-copies the fields of an entry that are reference
// types, for the reads that hand an entry out.
func cloneDLQEntry(e *dlq.Entry) *dlq.Entry {
	out := *e
	out.Resources = e.Resources.Clone()
	out.ResourceLimits = e.ResourceLimits.Clone()

	if e.Payload != nil {
		out.Payload = make([]byte, len(e.Payload))
		copy(out.Payload, e.Payload)
	}

	if e.ArtifactBindings != nil {
		out.ArtifactBindings = make([]byte, len(e.ArtifactBindings))
		copy(out.ArtifactBindings, e.ArtifactBindings)
	}

	return &out
}

// ClaimReplay marks an unreplayed entry replayed by jobID. The check and
// the write happen under one lock, so of two concurrent claims exactly
// one wins.
func (m *Store) ClaimReplay(_ context.Context, entryID id.DLQID, jobID id.JobID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := entryID.String()
	e, ok := m.dlqs[key]
	if !ok {
		return dispatch.ErrDLQNotFound
	}
	if e.ReplayedAt != nil {
		return dispatch.ErrDLQAlreadyReplayed
	}

	now := time.Now().UTC()
	claimed := *e
	claimed.ReplayedAt = &now
	claimed.ReplayedJobID = &jobID
	m.dlqs[key] = &claimed

	return nil
}

// ReleaseReplay undoes a claim, but only the one jobID made.
func (m *Store) ReleaseReplay(_ context.Context, entryID id.DLQID, jobID id.JobID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := entryID.String()
	e, ok := m.dlqs[key]
	if !ok {
		return dispatch.ErrDLQNotFound
	}
	if e.ReplayedJobID == nil || *e.ReplayedJobID != jobID {
		return nil
	}

	released := *e
	released.ReplayedAt = nil
	released.ReplayedJobID = nil
	m.dlqs[key] = &released

	return nil
}

// GetDLQByJobID returns the newest entry, by ID, for a failed job.
func (m *Store) GetDLQByJobID(_ context.Context, jobID id.JobID) (*dlq.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var newest *dlq.Entry
	for _, e := range m.dlqs {
		if e.JobID != jobID {
			continue
		}
		if newest == nil || e.ID.String() > newest.ID.String() {
			newest = e
		}
	}
	if newest == nil {
		return nil, dispatch.ErrDLQNotFound
	}

	return cloneDLQEntry(newest), nil
}

// DeleteDLQ removes one entry.
func (m *Store) DeleteDLQ(_ context.Context, entryID id.DLQID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := entryID.String()
	if _, ok := m.dlqs[key]; !ok {
		return dispatch.ErrDLQNotFound
	}
	delete(m.dlqs, key)

	return nil
}

// SetCronEnabled sets enabled, and next_run_at when one is given.
func (m *Store) SetCronEnabled(_ context.Context, entryID id.CronID, enabled bool, nextRunAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := entryID.String()
	e, ok := m.crons[key]
	if !ok {
		return dispatch.ErrCronNotFound
	}

	updated := *e
	updated.Enabled = enabled
	if nextRunAt != nil {
		next := *nextRunAt
		updated.NextRunAt = &next
	}
	updated.UpdatedAt = time.Now().UTC()
	m.crons[key] = &updated

	return nil
}

// UpdateCronNextRun sets next_run_at and nothing else the scheduler does
// not own.
func (m *Store) UpdateCronNextRun(_ context.Context, entryID id.CronID, nextRunAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := entryID.String()
	e, ok := m.crons[key]
	if !ok {
		return dispatch.ErrCronNotFound
	}

	updated := *e
	updated.NextRunAt = &nextRunAt
	updated.UpdatedAt = time.Now().UTC()
	m.crons[key] = &updated

	return nil
}

// ReopenRun moves a finished run back to running. The check and the write
// happen under one lock, so of two concurrent reopens exactly one wins.
func (m *Store) ReopenRun(_ context.Context, runID id.RunID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := runID.String()
	r, ok := m.runs[key]
	if !ok {
		return dispatch.ErrRunNotFound
	}
	if r.State == workflow.RunStateRunning {
		return fmt.Errorf("%w: run %s is %s", dispatch.ErrInvalidState, runID, r.State)
	}

	reopened := *r
	reopened.State = workflow.RunStateRunning
	reopened.Error = ""
	reopened.CompletedAt = nil
	reopened.UpdatedAt = time.Now().UTC()
	m.runs[key] = &reopened

	return nil
}
