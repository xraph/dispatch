package job

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// ListOpts controls pagination and filtering for job list queries.
type ListOpts struct {
	// Limit is the maximum number of jobs to return. Zero means no limit.
	Limit int
	// Offset is the number of jobs to skip.
	Offset int
	// Queue filters by queue name. Empty means all queues.
	Queue string
}

// CountOpts controls filtering for job count queries.
type CountOpts struct {
	// Queue filters by queue name. Empty means all queues.
	Queue string
	// State filters by job state. Empty means all states.
	State State
}

// budgetedKeys are the canonical dimensions a store compares numerically
// at dequeue. They are exactly the keys every backend persists as its own
// indexed scalar column, which is what lets the fit test be an indexable
// range predicate rather than a document comparison.
//
// Custom keys are deliberately absent: they are matched by containment
// (see DequeueOpts.CustomKeys), not by quantity.
var budgetedKeys = [...]string{resource.CPU, resource.Memory, resource.Disk, resource.GPU}

// DequeueOpts narrows a dequeue to the jobs the caller can actually run.
//
// The fit predicate lives in the query rather than in the worker because
// DequeueJobs claims a job and marks it running atomically: by the time a
// worker can read a job's requirements it already owns it. Filtering after
// the claim would mean requeueing, and a 32 GB job would then bounce
// between small workers, burning a write on every bounce and delaying
// exactly the job that is hardest to place. Every constraint here must
// therefore be evaluated as part of the claim, never applied to the rows
// the claim returned.
type DequeueOpts struct {
	// Queues restricts the claim to these queue names.
	//
	// An EMPTY list is not a portable request and callers should not
	// send one. The backends genuinely disagree, and the disagreement
	// predates this option:
	//
	//	memory    claims from every queue
	//	postgres  claims nothing (queue = ANY(NULL) matches no row)
	//	sqlite    claims nothing, by an explicit early return before the
	//	          query is built
	//	redis     claims nothing — the index is one sorted set per queue
	//	          name and there has never been a cross-queue index
	//	mongo     claims nothing, by an explicit early return
	//
	// Mongo's guard is the one that had to be added: {queue: {$in: nil}}
	// marshals to BSON null and the server rejects the whole query with
	// "$in needs an array", so an empty list was a hard error rather
	// than any behaviour at all. It returns empty to match the three
	// backends a real deployment would be running, rather than inventing
	// an all-queues scan that no other persistent backend performs.
	//
	// The conformance suite does not exercise an empty list and no
	// caller in this repository sends one. Unifying the five would be a
	// behaviour change to store/memory and is deliberately not made
	// here; documenting the split is what stops a caller assuming it.
	Queues []string

	// Limit is the maximum number of jobs to claim. It counts eligible
	// jobs only: a job excluded by Budget or CustomKeys must not consume
	// a slot, or one oversized job at the head of the queue would starve
	// a worker that had capacity for everything behind it.
	Limit int

	// Budget is the free capacity the caller is offering, in canonical
	// units (cpu millicores, memory and disk bytes, gpu milli-devices).
	// A job is eligible on a dimension when its requirement is <= the
	// budget for that dimension.
	//
	// An ABSENT key is unconstrained, not zero. This inverts
	// resource.Set.Fits, which treats absent capacity as zero, and the
	// inversion is deliberate: a worker that declares only memory must
	// still claim GPU-requiring jobs, because otherwise adding a
	// dimension to one worker's config would silently strand work on
	// every worker that had not been updated yet.
	//
	// A key present with the value zero is a real constraint — a worker
	// with no free memory — and excludes any job requiring more than
	// zero of it. That is why IsUnbounded tests key presence rather than
	// resource.Set.IsZero.
	//
	// Custom keys in Budget are ignored by the predicate. Quantity
	// matching on a custom dimension would need a document comparison or
	// a join table in five backends to serve a rare case; offer custom
	// keys through CustomKeys instead.
	Budget resource.Set

	// CustomKeys are the custom resource keys the caller offers,
	// typically free.CustomKeys(). A job is eligible only if every custom
	// key it requires appears here.
	//
	// An empty list is read against IsUnbounded rather than on its own,
	// because "no custom keys" means two different things to two
	// different callers. If the whole of o is unbounded, the caller does
	// not use the resource model at all and claims everything, custom
	// keys included — that is the backward-compatibility guarantee. If o
	// is bounded in any other way, an empty list means this worker
	// genuinely has no custom resources, and a job requiring an fpga must
	// not be handed to it.
	//
	// Reading an empty list as unconstrained in the bounded case would
	// let a resource-aware worker claim work it cannot possibly run;
	// reading it as "offers nothing" in the unbounded case would strand
	// every custom-key job in the fleet the day this option shipped.
	//
	// Only key containment is tested at dequeue; the quantity is enforced
	// locally after the claim, by the admission path that already owns
	// the accounting. Matching a quantity here would need a document
	// comparison or a join table in five backends to serve a rare case.
	//
	// Backends match against the delimited string
	// resource.EncodeCustomKeys produced at enqueue, whose leading and
	// trailing separators are what stop ",fpga," matching a worker that
	// only offers ",fpga-large,". The test is subset, not substring: a
	// job needing {fpga, tpu} is eligible for a caller offering
	// {fpga, nvme, tpu}, even though the offered list interleaves a key
	// the job does not want.
	CustomKeys []string

	// PreferHashes are PrimaryInputHash values the caller already has
	// staged locally. A job whose PrimaryInputHash appears here sorts
	// ahead of jobs at the same priority, saving a re-download.
	//
	// Backends must bind PreferredHashes rather than this field: an
	// empty string here is not a locality signal and must not become one.
	//
	// This is advisory and must NEVER filter, and must never outrank
	// priority: locality that could reorder across priority bands would
	// let a steady stream of locally cached work starve the high-priority
	// job the pool exists to run first. The full ordering is priority
	// descending, then preferred before unpreferred, then RunAt
	// ascending.
	//
	// It is deliberately NOT a term of IsUnbounded. If it were, opts
	// carrying only PreferHashes would count as bounded, and the empty
	// CustomKeys rule would then reject every custom-resource job — this
	// field would filter transitively, contradicting the paragraph above.
	PreferHashes []string

	// ReservedFor restricts the claim to a single job. When set, no other
	// job may be returned, and that job is still subject to every other
	// constraint here — a targeted claim that could bypass Budget would
	// reintroduce exactly the overcommit this predicate prevents.
	ReservedFor *id.JobID

	// WorkerID is the worker taking the lease. It is required when
	// LeaseUntil is set and ignored otherwise.
	WorkerID id.WorkerID

	// LeaseUntil, when non-zero, makes the claim grant a lease: the
	// claimed rows get WorkerID, this expiry, and an incremented
	// lease_epoch, in the same statement that claims them.
	//
	// A zero value grants no lease and leaves every lease column
	// untouched, which is exactly how DequeueJobs behaved before leases
	// existed. That is the backward-compatibility guarantee: a caller
	// that does not opt in cannot be affected by this.
	//
	// The grant must be part of the claim, not a second write, and the
	// reason is the opposite of the obvious one. Reclamation cannot
	// rescue a half-granted job: every backend requires a non-null
	// expiry to consider a row at all, and Lease.IsExpired reports false
	// for a zero ExpiresAt precisely so the reclaim loop never steals a
	// job that was never leased. So a crash between a claim and a
	// separate grant would leave a row running with no expiry that
	// nothing in the lease machinery can see — not a job at risk of
	// being reclaimed, a job that can never be reclaimed. It would sit
	// there until the coarse global stale-job threshold noticed, which
	// is the mechanism leases exist to replace.
	//
	// One write means a claimed job always carries a lease something can
	// act on.
	LeaseUntil time.Time
}

// Grants reports whether o asks the claim to grant a lease. Backends test
// intent through this rather than open-coding the zero check five times.
func (o DequeueOpts) Grants() bool {
	return !o.LeaseUntil.IsZero()
}

// Validate reports whether o is a coherent request, and is the one place
// every backend checks before it writes anything.
//
// The only incoherent combination is a grant with no holder. A lease
// granted to the zero worker can never be renewed, because RenewLease
// matches on worker ID — so the job would be claimed, left to expire, and
// reclaimed on every cycle forever, which presents as a queue that never
// drains rather than as an error. Refusing the claim turns a silent
// livelock into a caller bug reported at the call that caused it.
func (o DequeueOpts) Validate() error {
	if o.Grants() && o.WorkerID.IsNil() {
		return ErrLeaseWithoutWorker
	}

	return nil
}

// IsUnbounded reports whether o restricts WHICH jobs may be claimed.
//
// A dequeue asks two independent questions, and this answers only the
// first:
//
//	should I filter?  — Budget, CustomKeys, ReservedFor. IsUnbounded.
//	should I order?   — PreferHashes. len(o.PreferHashes) > 0.
//
// Do not reuse this for the second. A backend skips the fit predicate
// when IsUnbounded is true, and separately adds the locality term to its
// ORDER BY whenever PreferHashes is non-empty. A caller that sets only
// PreferHashes therefore gets locality ordering over an unfiltered
// candidate set, which is precisely what "advisory, never a filter"
// means. Folding PreferHashes in here would make it bounded, and the
// empty-CustomKeys rule would then reject every custom-resource job on
// its behalf.
//
// It tests Budget for key presence rather than calling
// resource.Set.IsZero: a Budget of {"memory": 0} is an exhausted worker,
// which must claim nothing that needs memory. Treating it as unbounded
// would hand that worker a job it cannot run.
func (o DequeueOpts) IsUnbounded() bool {
	return len(o.Budget) == 0 &&
		len(o.CustomKeys) == 0 &&
		o.ReservedFor == nil
}

// Allows reports whether j satisfies every constraint in o except Queues,
// Limit, and ordering. It is the executable definition of the fit
// predicate: backends that select candidates in Go should call it instead
// of reimplementing the rules, and backends that express the predicate in
// their query language must return the same answer for every job.
//
// It must be applied BEFORE the claim. Claiming a job and then rejecting
// it here is not an implementation of this contract — it is the
// claim-then-requeue behaviour the whole option exists to avoid.
func (o DequeueOpts) Allows(j *Job) bool {
	if j == nil {
		return false
	}

	// A caller that constrains nothing claims everything, including jobs
	// declaring custom resources. Backends should reach the same result
	// by skipping the predicate entirely on IsUnbounded.
	if o.IsUnbounded() {
		return true
	}

	if o.ReservedFor != nil && j.ID != *o.ReservedFor {
		return false
	}

	for _, k := range budgetedKeys {
		budget, declared := o.Budget[k]
		if !declared {
			continue
		}

		if j.Resources[k] > budget {
			return false
		}
	}

	// o is bounded by this point, so an empty offer means the caller has
	// no custom resources — not that it declined to say. See CustomKeys.
	required := j.Resources.CustomKeys()
	if len(required) == 0 {
		return true
	}

	offered := make(map[string]struct{}, len(o.CustomKeys))
	for _, k := range o.CustomKeys {
		offered[k] = struct{}{}
	}

	for _, k := range required {
		if _, ok := offered[k]; !ok {
			return false
		}
	}

	return true
}

// Prefers reports whether j's PrimaryInputHash is one the caller already
// has staged. It is the sort key backends apply after priority.
func (o DequeueOpts) Prefers(j *Job) bool {
	if j == nil || j.PrimaryInputHash == "" {
		return false
	}

	for _, h := range o.PreferHashes {
		if h == j.PrimaryInputHash {
			return true
		}
	}

	return false
}

// Less orders two eligible jobs the way every backend must return them:
// priority descending, then preferred-by-locality before not, then RunAt
// ascending. Ties beyond that are unspecified.
func (o DequeueOpts) Less(a, b *Job) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}

	if pa, pb := o.Prefers(a), o.Prefers(b); pa != pb {
		return pa
	}

	return a.RunAt.Before(b.RunAt)
}

// PreferredHashes returns the locality hashes worth matching on:
// deduplicated, in caller order, with the empty string dropped. It is
// what every backend must bind, never PreferHashes itself.
//
// The empty string is the case that matters. Prefers reports false for a
// job with no PrimaryInputHash, so an empty entry offers no information
// — but primary_input_hash is a plain string column, and a job that was
// never hashed stores ” rather than NULL on the SQL backends. Bound
// verbatim, ” = ANY('{""}') is TRUE, so a single empty entry would make
// EVERY unhashed job "locally staged" and hand it the head of its
// priority band. Under a tight Limit that is not a reordering, it is a
// filter: the jobs the caller actually has staged stop being claimed at
// all. Mongo already stripped empties; postgres and sqlite bound them.
func (o DequeueOpts) PreferredHashes() []string {
	if len(o.PreferHashes) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(o.PreferHashes))
	out := make([]string, 0, len(o.PreferHashes))

	for _, h := range o.PreferHashes {
		if _, dup := seen[h]; dup || h == "" {
			continue
		}

		seen[h] = struct{}{}

		out = append(out, h)
	}

	return out
}

// OfferedCustomKeys returns CustomKeys sorted, for backends that build a
// delimited parameter and need a stable, deduplicated order.
func (o DequeueOpts) OfferedCustomKeys() []string {
	if len(o.CustomKeys) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(o.CustomKeys))
	out := make([]string, 0, len(o.CustomKeys))

	for _, k := range o.CustomKeys {
		if _, dup := seen[k]; dup || k == "" {
			continue
		}

		seen[k] = struct{}{}

		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// Store defines the persistence contract for jobs.
type Store interface {
	// EnqueueJob persists a new job in pending state.
	EnqueueJob(ctx context.Context, j *Job) error

	// DequeueJobs atomically claims up to opts.Limit ready jobs from
	// opts.Queues that fit opts, sets them to running, and returns them
	// ordered by priority descending, then locality-preferred first, then
	// RunAt ascending.
	//
	// The fit test is part of the claim, not a filter over claimed rows.
	// A job that does not fit stays pending and untouched, available to
	// the next worker that does have room for it.
	//
	// When opts.Grants() the same statement also grants a lease: the
	// claimed rows get opts.WorkerID, opts.LeaseUntil, and an incremented
	// lease_epoch, and the returned jobs carry the epoch they were
	// granted. The grant travels in the claiming write itself, never as a
	// follow-up: a row left running with no expiry is invisible to
	// LeaseStore.ReclaimExpiredLeases, so a crash between two writes
	// would strand it rather than expose it. See DequeueOpts.LeaseUntil.
	//
	// Opts that do not grant leave every lease column untouched. A grant
	// with no WorkerID is refused with ErrLeaseWithoutWorker and claims
	// nothing.
	//
	// Every backend must pass storetest.RunDequeueSuite, which is the
	// contract this signature only sketches.
	DequeueJobs(ctx context.Context, opts DequeueOpts) ([]*Job, error)

	// GetJob retrieves a job by ID.
	GetJob(ctx context.Context, jobID id.JobID) (*Job, error)

	// UpdateJob persists changes to an existing job.
	UpdateJob(ctx context.Context, j *Job) error

	// DeleteJob removes a job by ID.
	DeleteJob(ctx context.Context, jobID id.JobID) error

	// ListJobsByState returns jobs matching the given state.
	ListJobsByState(ctx context.Context, state State, opts ListOpts) ([]*Job, error)

	// HeartbeatJob updates the heartbeat timestamp for a running job,
	// indicating the worker is still alive.
	HeartbeatJob(ctx context.Context, jobID id.JobID, workerID id.WorkerID) error

	// ReapStaleJobs returns running jobs whose last heartbeat is older than
	// the given threshold, indicating the worker may have crashed.
	ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*Job, error)

	// CountJobs returns the number of jobs matching the given options.
	CountJobs(ctx context.Context, opts CountOpts) (int64, error)
}

// LeaseStore is the opt-in lease capability.
//
// It is deliberately not part of Store. A backend that implements Store
// alone keeps compiling and keeps behaving exactly as it does today,
// reaped on the pool's single global threshold. A backend that also
// implements LeaseStore gets per-definition lease TTLs, epoch fencing,
// and atomic reclamation. This mirrors the capability idiom the artifact
// backend already uses for RangeReader and Presigner.
//
// Where an expiry is passed at all — RenewLease here, and
// DequeueOpts.LeaseUntil for the grant — it is an absolute timestamp
// rather than a TTL. If the store computed now+ttl it would need
// per-dialect interval arithmetic over a nanosecond integer, and SQLite,
// Mongo, and Redis have no interval type at all. Passing a timestamp
// means every backend only writes a value, and lease policy lives in one
// place. ReclaimExpiredLeases takes no expiry: it acts on the ones
// already written.
//
// The GRANT is deliberately not here. It travels on DequeueOpts instead
// (WorkerID and LeaseUntil), so a leased claim is an ordinary claim that
// also writes the lease columns and therefore carries the budget, the
// custom-key containment, the locality preference and the reservation
// like any other. A second dequeue entry point taking (queues, limit)
// existed here until the two paths had to be kept in sync by hand; it
// carried none of those, so turning leases and resources on together —
// the natural upgrade — claimed jobs that could not fit and requeued
// them on every poll. This interface is what a backend adds ON TOP of
// Store to make that grant renewable and reclaimable.
type LeaseStore interface {
	// RenewLease extends the lease to leaseUntil, but only if the job is
	// still running, still assigned to workerID, and still at epoch.
	//
	// It returns ErrLeaseLost when that condition does not hold. That
	// return is the entire fencing mechanism: a worker that was reclaimed
	// while paused learns it no longer owns the job within one heartbeat
	// interval, instead of continuing to write for hours.
	RenewLease(
		ctx context.Context,
		jobID id.JobID,
		workerID id.WorkerID,
		epoch int,
		leaseUntil time.Time,
	) error

	// ReclaimExpiredLeases returns to pending every running job whose
	// lease has expired, clearing the worker assignment, incrementing
	// lease_epoch to fence the previous holder, and incrementing
	// evict_count. RetryCount is never touched — a lost lease is
	// infrastructure, not a handler failure.
	//
	// The claim and the read are one atomic statement, so two pools
	// reclaiming concurrently cannot both take the same job.
	ReclaimExpiredLeases(ctx context.Context, limit int) ([]*Job, error)

	// UpdateLeasedJob persists j only while the caller still holds the
	// lease.
	//
	// The write applies iff the row is still running, still assigned to
	// workerID, and still at epoch. Otherwise it returns ErrLeaseLost and
	// leaves the row untouched — the lease has moved on, and a worker
	// that no longer owns a job has no coherent claim to make about it.
	//
	// It writes the same columns as UpdateJob EXCEPT the lease-owned
	// ones: lease_epoch, lease_expires_at, worker_id, and heartbeat_at
	// are never written here. Those have exactly three writers — the
	// grant in DequeueJobs, RenewLease, and ReclaimExpiredLeases — and
	// this method deliberately is not a fourth.
	//
	// j is the caller's claim-time (or otherwise stale) snapshot, so
	// j.LeaseExpiresAt is whatever the expiry was when this worker last
	// read the row — every renewal since has pushed the real value
	// forward. A whole-row write that passed the epoch predicate would
	// still roll the expiry backwards, shortening the current holder's
	// lease by however long this caller ran. Excluding the lease-owned
	// columns is what makes the epoch predicate a genuine fence rather
	// than a check that only looks like one.
	//
	// A missing row returns dispatch.ErrJobNotFound rather than
	// ErrLeaseLost: zero rows affected by the fence predicate means "the
	// lease moved on", but a row that no longer exists was never a
	// question of who holds it.
	UpdateLeasedJob(ctx context.Context, j *Job, workerID id.WorkerID, epoch int) error
}
