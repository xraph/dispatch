package job

import (
	"time"

	"github.com/xraph/dispatch/id"
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

// EvictReason classifies why a job stopped being run by the worker that
// held it. Every reason here is infrastructure taking the worker away
// rather than the handler failing, which is why they increment EvictCount
// and never RetryCount.
type EvictReason string

const (
	// EvictLeaseExpired means the lease was reclaimed because it was not
	// renewed in time — the worker died, froze, or was partitioned from
	// the store.
	EvictLeaseExpired EvictReason = "lease_expired"

	// EvictLeaseLost means a worker discovered on renewal that it no
	// longer owned the job, and stopped. This is the fencing path: the
	// job has already been reclaimed and possibly already restarted
	// elsewhere.
	EvictLeaseLost EvictReason = "lease_lost"
)

// Lease is the grant a worker holds over a running job.
//
// Epoch is the fencing token. It increments on every grant and every
// reclamation, so a worker that was reclaimed while paused holds a stale
// epoch. RenewLease, the grant inside DequeueJobs, ReclaimExpiredLeases,
// and UpdateLeasedJob check the epoch, so that worker's next renewal
// fails and the pool cancels the job within one heartbeat interval, and
// any terminal write it still attempts is refused with ErrLeaseLost
// rather than applied. UpdateJob does not check it: it is a whole-row
// write with no epoch predicate, so a caller that wants the fence must
// use UpdateLeasedJob instead. Without the renewal check, a worker
// resuming from a long GC pause would keep renewing a lease on a job
// another worker now owns.
type Lease struct {
	// JobID is the leased job.
	JobID id.JobID

	// WorkerID is the holder.
	WorkerID id.WorkerID

	// Epoch is the fencing token this holder was granted.
	Epoch int

	// ExpiresAt is when the lease lapses if not renewed. A zero value
	// means no lease has been granted.
	ExpiresAt time.Time
}

// IsExpired reports whether the lease has lapsed as of now.
//
// A zero ExpiresAt reports false: no lease was ever granted, which is
// "not held" rather than "expired". Reporting true would let the reclaim
// loop steal jobs that were never leased.
func (l Lease) IsExpired(now time.Time) bool {
	if l.ExpiresAt.IsZero() {
		return false
	}

	return !now.Before(l.ExpiresAt)
}
