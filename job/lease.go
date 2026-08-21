package job

import (
	"time"
)

// DefaultLeaseTTL is how long a lease survives without renewal when
// neither the definition nor the pool specifies otherwise. It matches the
// historical Config.StaleJobThreshold so that adopting leases does not
// change reclamation timing for an existing deployment.
const DefaultLeaseTTL = 30 * time.Second

// UnleasedReclaimGrace is how long a running job carrying no lease at all
// must have been silent before reclamation will adopt it.
//
// It exists because a null expiry has two very different causes and the
// reclaim predicate cannot tell them apart from the expiry alone. One is a
// job left running by a build that predates leases, which nothing will
// ever look at again: Lease.IsExpired reports false for a zero expiry, the
// pool stopped calling ReapStaleJobs for a store implementing LeaseStore,
// and dequeue claims only pending and retrying rows. The other is a live,
// perfectly healthy job, because DequeueOpts.Grants() is false whenever
// LeaseUntil is zero, so any caller claiming through Store without lease
// options holds a running job with no lease by design.
//
// Silence is what separates them, which is why every backend gates the
// exception on heartbeat_at, falling back to started_at for a worker that
// died before its first beat, rather than on the null expiry alone. A row
// with neither timestamp is never adopted: there is nothing to measure age
// against, and guessing would mean guessing against a running job.
//
// The value is arbitrary and an operator cannot tune it, which is worth
// saying plainly rather than burying. ReclaimExpiredLeases(ctx, limit)
// carries no threshold, and widening that signature would change all five
// backends. Fifteen minutes is chosen to be conservative rather than
// precise: before leases these same rows were reaped at
// Config.StaleJobThreshold, 30 seconds by default, so any value well above
// that is strictly less aggressive than what already shipped. Overshooting
// costs only how long an abandoned job waits to come back.
const UnleasedReclaimGrace = 15 * time.Minute

// Lease is the grant a worker holds over a running job.
//
// It carries only the expiry, because that is the single question the
// backends ask of it: IsExpired is the one authority on whether a lease
// has lapsed, and the zero-means-never-leased rule below is the reason
// that question cannot simply be asked of a time.Time inline. The holder
// and the fencing token live on the job row itself, as Job.WorkerID and
// Job.LeaseEpoch, which is where every writer already reads them.
type Lease struct {
	// ExpiresAt is when the lease lapses if not renewed. A zero value
	// means no lease has been granted.
	ExpiresAt time.Time
}

// IsExpired reports whether the lease has lapsed as of now.
//
// A zero ExpiresAt reports false: no lease was ever granted, which is
// "not held" rather than "expired". Reporting true would let the reclaim
// loop steal jobs that were never leased.
//
// Reclamation does eventually take such a job, but never through this
// function. It applies a separate and much coarser rule, gated on the row
// having gone silent for UnleasedReclaimGrace, precisely so that the
// question this function answers stays "is a lease lapsed" rather than
// blurring into "is a job abandoned". The two are not the same, and the
// backends depend on them staying separate.
func (l Lease) IsExpired(now time.Time) bool {
	if l.ExpiresAt.IsZero() {
		return false
	}

	return !now.Before(l.ExpiresAt)
}
