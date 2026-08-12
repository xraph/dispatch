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
// epoch and every write it attempts is rejected. Without it, a worker
// resuming from a long GC pause would keep writing to a job another
// worker now owns.
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
