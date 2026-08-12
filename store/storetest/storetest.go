// Package storetest provides conformance suites that every Dispatch store
// backend must pass. The suites are shared so five implementations cannot
// quietly disagree about semantics that only one of them has tests for.
//
// RunLeaseSuite covers the opt-in lease capability. RunDequeueSuite
// covers the resource-aware dequeue contract, where disagreement is not
// cosmetic: the same job would become eligible on different workers
// depending only on which store the operator chose, and the dimension
// that silently drifts is the one deciding whether a 32 GB job lands on a
// 4 GB machine. Its two load-bearing cases are
// ZeroBudgetSelectsEverything, the guarantee that an unconstrained caller
// still sees exactly what it saw before the option existed, and
// ClaimIsAtomicUnderConcurrency, which proves the fit predicate did not
// cost the claim its atomicity.
//
// Known limitations of RunDequeueSuite, so a backend author knows what is
// unpinned rather than guaranteed:
//
//   - ZeroBudgetSelectsEverything asserts set membership, not order. The
//     ordering contract is pinned by PriorityOrderingPreservedWithinBudget,
//     LimitTruncatesAfterOrdering, and the two PreferHashes cases; a
//     backend that ordered correctly only when a budget was present would
//     not be caught.
//   - Every case names its queues explicitly, so empty DequeueOpts.Queues
//     — "all queues" — is never exercised. It cannot be, while the suite
//     supports backends that share one store across subtests: an
//     all-queues claim would take other cases' jobs.
//   - Requirements are built with resource.Set literals or nil, so a
//     non-nil empty Set is never round-tripped through a backend here.
//
// The package depends only on job, resource, id, and the root package. It
// must never import a store backend: the backends import this, not the
// reverse.
package storetest

import (
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// LeaseStore is what the lease suite requires of a backend: the base job
// store plus the opt-in lease capability.
type LeaseStore interface {
	job.Store
	job.LeaseStore
}

// PendingJob builds a job ready to be dequeued now, on the given queue and
// with the given lease TTL. A zero ttl leaves LeaseTTL unset, meaning the
// pool default.
//
// The queue is a parameter because the suite may run against one shared
// store — spinning a fresh Postgres or Redis container per subtest would
// cost more than the coverage is worth — so each case dequeues from its own
// queue to stay isolated from its neighbours.
func PendingJob(name, queue string, ttl time.Duration) *job.Job {
	now := time.Now().UTC()

	return &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      queue,
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      now.Add(-time.Second),
		LeaseTTL:   ttl,
	}
}

// RunningJob builds a job already in the running state with an expired
// lease held by an unknown worker, for testing reclamation directly.
func RunningJob(name, queue string, ttl time.Duration) *job.Job {
	now := time.Now().UTC()
	started := now.Add(-time.Minute)
	expired := now.Add(-time.Second)

	j := PendingJob(name, queue, ttl)
	j.State = job.StateRunning
	j.StartedAt = &started
	j.LeaseExpiresAt = &expired
	j.LeaseEpoch = 1
	j.WorkerID = id.NewWorkerID()

	return j
}

// Contains reports whether jobs includes the given ID.
//
// Reclamation is not queue-scoped, so cases that exercise it must assert on
// the job they created rather than on the length of the returned slice —
// otherwise a shared store makes every such case depend on what its
// neighbours left behind.
func Contains(jobs []*job.Job, jobID id.JobID) bool {
	for _, j := range jobs {
		if j.ID == jobID {
			return true
		}
	}

	return false
}
