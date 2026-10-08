package dlq

import (
	"context"

	"github.com/xraph/dispatch/id"
)

// ReplayClaimer is the conditional write that makes replay idempotent.
//
// A replay used to read the entry, enqueue a new job, then mark the entry
// replayed, so two operators clicking replay at once both got a job. The
// claim moves the check into the store: whoever sets replayed_at first
// owns the replay, and everyone after them is told so.
type ReplayClaimer interface {
	// ClaimReplay sets replayed_at = now and replayed_job_id = jobID only
	// if replayed_at is null. It returns dispatch.ErrDLQAlreadyReplayed
	// when replayed_at was already set, and dispatch.ErrDLQNotFound when
	// the entry does not exist. The check and the write are one atomic
	// step on every backend: of two concurrent claims, exactly one wins.
	ClaimReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error

	// ReleaseReplay clears replayed_at and replayed_job_id, but only if
	// replayed_job_id still equals jobID, so a release never undoes
	// somebody else's claim. When it does not match, including on an
	// entry nobody claimed, it is a no-op and returns nil. It returns
	// dispatch.ErrDLQNotFound when the entry does not exist. Like the
	// claim, the comparison and the write are one atomic step.
	ReleaseReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error

	// GetDLQByJobID returns the newest entry for a failed job, newest by
	// entry ID (the order ListDLQPage uses), or dispatch.ErrDLQNotFound
	// when the job has none.
	GetDLQByJobID(ctx context.Context, jobID id.JobID) (*Entry, error)

	// DeleteDLQ hard-deletes one entry. It returns dispatch.ErrDLQNotFound
	// when the entry does not exist.
	DeleteDLQ(ctx context.Context, entryID id.DLQID) error
}
