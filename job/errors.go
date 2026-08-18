package job

import "errors"

// Lease sentinels.
//
// These live in job rather than the root dispatch package — where every
// other sentinel lives — because the root package already exports
// ErrLeadershipLost for cluster leadership. A sibling ErrLeaseLost for job
// leases one line away would be a standing invitation to grab the wrong
// one. Qualified as job.ErrLeaseLost, the call site is unambiguous.
var (
	// ErrLeaseLost means the worker no longer holds the lease it tried to
	// act on: the job was reclaimed, reassigned, or deleted while the
	// worker believed it was still running it. A worker receiving this must
	// stop working on the job immediately — someone else owns it now.
	ErrLeaseLost = errors.New("dispatch/job: lease lost")

	// ErrLeaseWithoutWorker means a dequeue asked for a lease
	// (DequeueOpts.LeaseUntil) without naming the worker that would hold
	// it. It is a programming error, not a degenerate case: RenewLease
	// matches on worker ID, so a lease held by the zero worker can never
	// be renewed and the job would be claimed and reclaimed on every
	// cycle forever. Backends refuse the claim rather than granting it.
	ErrLeaseWithoutWorker = errors.New("dispatch/job: DequeueOpts.LeaseUntil set without WorkerID")
)
