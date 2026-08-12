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
	ErrLeaseNotSupported = errors.New("dispatch/job: store does not implement job.LeaseStore")
)
