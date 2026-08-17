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

	// ErrLeaseNotSupported means the configured store does not implement
	// LeaseStore, so per-definition lease TTLs and epoch fencing are
	// unavailable.
	//
	// Nothing in this module returns it. A pool given a store without the
	// capability degrades to the heartbeat reaper and logs a warning
	// rather than failing, because refusing to start over a missing
	// optional capability would be worse than running without it. The
	// sentinel is kept for a caller that type-asserts LeaseStore itself
	// and wants a shared error to report, and because removing an
	// exported symbol is a breaking change.
	ErrLeaseNotSupported = errors.New("dispatch/job: store does not implement job.LeaseStore")

	// ErrLeaseWithoutWorker means a dequeue asked for a lease
	// (DequeueOpts.LeaseUntil) without naming the worker that would hold
	// it. It is a programming error, not a degenerate case: RenewLease
	// matches on worker ID, so a lease held by the zero worker can never
	// be renewed and the job would be claimed and reclaimed on every
	// cycle forever. Backends refuse the claim rather than granting it.
	ErrLeaseWithoutWorker = errors.New("dispatch/job: DequeueOpts.LeaseUntil set without WorkerID")
)
