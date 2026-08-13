package sqlite

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// EnqueueJob persists a new job in pending state.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	m, err := toJobModel(j)
	if err != nil {
		return err
	}

	_, err = s.sdb.NewInsert(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/sqlite: enqueue job: %w", err)
	}
	return nil
}

// DequeueJobs atomically claims up to opts.Limit ready jobs from
// opts.Queues that fit opts, sets them to running, and returns them
// ordered by priority descending, then locality-preferred first, then
// RunAt ascending.
//
// SQLite doesn't support FOR UPDATE SKIP LOCKED, so the claim is a single
// UPDATE ... WHERE id IN (SELECT ... ORDER BY ... LIMIT ?) RETURNING *:
// one statement, which SQLite runs inside an implicit immediate
// transaction with the database write lock held, so two concurrent
// claimers cannot select the same candidate. That mechanism is unchanged
// here — the fit predicate is simply another conjunct of the inner
// SELECT's WHERE, so a job that does not fit is never written to. It stays
// pending and untouched for the next worker that does have room.
//
// When opts.Grants() the lease columns are additional assignments in that
// same UPDATE's SET clause, never a follow-up statement: a job running
// with no lease is a job ReclaimExpiredLeases is entitled to take back.
func (s *Store) DequeueJobs(ctx context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	// A worker computing zero free slots must claim zero jobs, never the
	// whole queue. This early return is load-bearing on SQLite rather than
	// a saved round trip: `LIMIT -1` means UNLIMITED here, the exact
	// opposite of Postgres, where it is an error. Without this, an
	// exhausted worker asking for -1 would claim the entire queue.
	if opts.Limit <= 0 {
		return nil, nil
	}

	// `queue IN ()` is a SQLite syntax error, where Postgres's
	// `queue = ANY('{}')` is merely false. Claiming nothing is what
	// store/postgres does for the same input, and there is no existing
	// "all queues" behaviour to preserve: this backend has never had a
	// query that could run without a queue list.
	if len(opts.Queues) == 0 {
		return nil, nil
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: dequeue jobs: %w", err)
	}

	query, args := buildDequeueQuery(opts, time.Now().UTC())

	// SQLite serializes writers with a single database-wide write lock, and
	// grove's sqlitedriver sets no busy_timeout, so a claimer that loses the
	// race for that lock fails immediately with SQLITE_BUSY rather than
	// blocking. Retrying is what turns "another worker is claiming right
	// now" back into "claim shortly", which is what the atomicity guarantee
	// looks like from the caller's side; the claim itself is atomic either
	// way, since only one statement can hold the write lock. Same helper the
	// lease writes use (store/sqlite/lease.go:38).
	var models []jobModel

	err := withBusyRetry(ctx, func() error {
		models = nil

		return s.sdb.NewRaw(query, args...).Scan(ctx, &models)
	})
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: dequeue jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: dequeue convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	// SQLite defines no order for the rows an UPDATE ... RETURNING emits,
	// and unlike Postgres it has no data-modifying CTE to wrap the claim in
	// and order the output of. So the statement's ORDER BY decides WHICH
	// jobs the LIMIT keeps — the part that must happen inside the claim —
	// and the returned slice is ordered here, by the contract's own
	// comparator rather than a fifth restatement of it.
	sort.SliceStable(jobs, func(a, b int) bool { return opts.Less(jobs[a], jobs[b]) })

	return jobs, nil
}

// budgetColumns maps each canonical dimension the dequeue predicate
// compares to the scalar column that holds it. These are exactly the
// dimensions job.DequeueOpts.Allows loops over, and exactly the columns
// idx_dispatch_jobs_dequeue_res carries in its key list — SQLite has no
// INCLUDE clause, so the migration folds them into the key instead, and
// each comparison is still a scalar range test rather than a probe into
// the resource_requests JSON.
//
// Every column is NOT NULL DEFAULT 0, which is what lets the comparisons
// below be bare rather than COALESCEd: a row written before migration
// 20260812130000 reads back the default 0, never NULL, so
// `req_memory_bytes <= ?` cannot silently evaluate to NULL and drop a
// legacy job. TestDequeueClaimsRowsWrittenBeforeTheResourceColumns and
// TestResourceColumnsRejectNull pin both halves of that.
//
// The column names are compile-time constants and are the only
// identifiers ever concatenated into the statement below; every value
// travels as a bind parameter.
var budgetColumns = []struct {
	key    string
	column string
}{
	{resource.CPU, "req_cpu_milli"},
	{resource.Memory, "req_memory_bytes"},
	{resource.Disk, "req_disk_bytes"},
	{resource.GPU, "req_gpu_milli"},
}

// dequeueSQL is the claim statement with everything the caller decides
// filled in: the started_at and updated_at placeholders, the lease grant,
// the queue list, the run_at placeholder, the fit predicate, the ordering,
// and the limit placeholder.
//
// The grant is a suffix of the SET clause rather than a statement of its
// own, which is what makes "claimed" and "leased" the same event.
//
// Unlike the Postgres statement this mirrors, the ordering appears once,
// not twice: there is no outer SELECT to order because SQLite has no
// data-modifying CTE. The one occurrence is the load-bearing one — it
// decides which rows the LIMIT keeps.
//
// Do not delete it on the grounds that the returned slice is sorted in
// Go anyway. Re-measured against the current 21-case
// storetest.RunDequeueSuite, with this ORDER BY removed entirely:
// exactly ONE case fails, LocalityDecidesWhichRowsSurviveATightLimit.
// Every other case still passes, LimitTruncatesAfterOrdering included,
// because SQLite answers the candidate scan from
// idx_dispatch_jobs_dequeue_res and that index's leading key order is
// priority DESC, run_at ASC — so twenty of twenty-one right answers
// arrive for the wrong reason and would stop arriving the moment the
// planner picked another index.
//
// The single case that does catch it is the one that cannot be baked
// into any index: locality depends on the caller's staged set, which was
// not known when the rows were written. That case was added for exactly
// this hole. TestBuildDequeueQueryOrdersLocalityBelowPriority and
// TestDequeueSelectsPreferredOverNullHashUnderLimit pin it here too.
const dequeueSQL = `
		UPDATE dispatch_jobs
		SET state = 'running', started_at = %s, updated_at = %s%s
		WHERE id IN (
			SELECT id FROM dispatch_jobs
			WHERE state IN ('pending', 'retrying')
			  AND queue IN (%s)
			  AND run_at <= %s%s
			ORDER BY %s
			LIMIT %s
		)
		RETURNING *`

// buildDequeueQuery compiles opts into the claim statement and its bind
// parameters. It is the SQL expression of job.DequeueOpts.Allows and
// Less, and must answer identically for every job.
//
// SQLite binds `?` parameters positionally, so args must be appended in
// the order the placeholders appear in the finished statement. Every
// helper below therefore binds as it writes, and they are called in
// textual order: SET, queues, run_at, fit predicate, ORDER BY, LIMIT.
func buildDequeueQuery(opts job.DequeueOpts, now time.Time) (query string, args []any) {
	args = make([]any, 0, len(opts.Queues)+len(opts.CustomKeys)*2+len(opts.PreferHashes)+8)

	// bind appends v and returns the placeholder that reads it. Values
	// never reach the statement text.
	bind := func(v any) string {
		args = append(args, v)

		return "?"
	}

	startedAt, updatedAt := bind(now), bind(now)
	grant := buildLeaseGrant(opts, bind)

	queues := make([]string, len(opts.Queues))
	for i, q := range opts.Queues {
		queues[i] = bind(q)
	}

	runAt := bind(now)
	fit := buildFitPredicate(opts, bind)
	order := buildDequeueOrder(opts, bind)
	limit := bind(opts.Limit)

	return fmt.Sprintf(dequeueSQL,
		startedAt, updatedAt, grant, strings.Join(queues, ","), runAt, fit, order, limit,
	), args
}

// buildLeaseGrant renders the lease assignments appended to the claim's
// SET clause, or "" when opts grants no lease.
//
// Empty is the whole backward-compatibility guarantee: a caller that does
// not opt in emits the statement it emitted before leases existed and
// leaves worker_id, lease_epoch and lease_expires_at exactly as they were.
//
// lease_epoch = lease_epoch + 1 rather than a bound value, because the
// epoch is the fence: it must advance from whatever the row currently
// holds, which only the row knows. Reading it and writing back a computed
// successor would be the read-modify-write this statement exists to avoid.
//
// It binds between updated_at and the queue list because `?` is
// positional here — see buildDequeueQuery.
func buildLeaseGrant(opts job.DequeueOpts, bind func(any) string) string {
	if !opts.Grants() {
		return ""
	}

	return ",\n\t\t    worker_id = " + bind(opts.WorkerID.String()) +
		",\n\t\t    lease_epoch = lease_epoch + 1" +
		",\n\t\t    lease_expires_at = " + bind(opts.LeaseUntil.UTC())
}

// buildFitPredicate renders the conjuncts that decide WHICH jobs may be
// claimed, or "" when opts constrains nothing.
func buildFitPredicate(opts job.DequeueOpts, bind func(any) string) string {
	// Unbounded opts emit the original query verbatim: a caller that does
	// not use the resource model claims everything, including jobs
	// declaring custom resources it could not possibly satisfy. Anything
	// else strands work the day this option ships.
	if opts.IsUnbounded() {
		return ""
	}

	var b strings.Builder

	if opts.ReservedFor != nil {
		b.WriteString("\n\t\t\t  AND id = " + bind(opts.ReservedFor.String()))
	}

	// An absent budget key is unconstrained, not zero, so only declared
	// dimensions produce a comparison. A key present with the value zero
	// is a real constraint and still emits one — that is an exhausted
	// worker, which must claim nothing that needs the dimension.
	//
	// The test is requirement <= budget: a job needing exactly the free
	// capacity is claimable, or the last slot on every worker is
	// permanently unusable.
	for _, dim := range budgetColumns {
		budget, declared := opts.Budget[dim.key]
		if !declared {
			continue
		}

		b.WriteString("\n\t\t\t  AND " + dim.column + " <= " + bind(budget))
	}

	b.WriteString("\n\t\t\t  AND " + buildCustomKeyPredicate(opts, bind))

	return b.String()
}

// buildCustomKeyPredicate renders custom-resource containment as a
// genuine SUBSET test, character for character the recipe store/postgres
// uses — it was written in nested REPLACE rather than an array operator
// precisely so this backend could copy it.
//
// req_custom_keys holds resource.EncodeCustomKeys' output — the sorted
// required keys wrapped in leading and trailing separators, e.g.
// ",fpga,tpu,". The obvious formulation, a LIKE/GLOB containment test
// against the offered list, is a SUBSTRING test: it passes every
// single-key case including the prefix collision, then silently strands a
// job needing {fpga,tpu} from a caller offering {fpga,nvme,tpu}, because
// the interleaved key breaks the contiguous run. The job it strands is
// the specialised one that is hardest to place anywhere else.
//
// Instead each offered key is stripped from the stored list by a nested
// REPLACE of ",key," with ",", which restores the separator the removal
// consumed and so composes in any order. What remains is "" or a lone
// separator exactly when every required key was offered.
//
// The nesting order is what keeps the bindings positional: the innermost
// REPLACE is written leftmost, so binding the key and then the separator
// once per wrapper, in loop order, matches the order SQLite reads the
// placeholders in. The separator cannot be bound once and reused the way
// Postgres reuses $7 — `?` has no number to refer back to.
func buildCustomKeyPredicate(opts job.DequeueOpts, bind func(any) string) string {
	// Bounded opts with an empty offer are a resource-aware worker that
	// genuinely has no custom resources, so only jobs requiring none are
	// eligible. This is the case IsUnbounded above has already excluded.
	expr := "req_custom_keys"
	for _, k := range opts.OfferedCustomKeys() {
		expr = "REPLACE(" + expr + ", " +
			bind(resource.CustomKeySep+k+resource.CustomKeySep) + ", " +
			bind(resource.CustomKeySep) + ")"
	}

	return expr + " IN ('', " + bind(resource.CustomKeySep) + ")"
}

// buildDequeueOrder renders the ordering every backend must return:
// priority descending, then locality-preferred before not, then RunAt
// ascending.
//
// Locality ranks strictly BELOW priority. Above it, a steady stream of
// low-priority jobs whose inputs are already staged would beat a
// high-priority job with cold inputs — an optimization overriding
// user-expressed intent, and the exact starvation the predicate exists to
// prevent. A preferred job jumps its own priority band and no further.
//
// The term is applied whenever PreferHashes is non-empty, including on
// otherwise-unbounded opts: IsUnbounded governs filtering only.
func buildDequeueOrder(opts job.DequeueOpts, bind func(any) string) string {
	// PreferredHashes, not PreferHashes: an empty entry would bind as ''
	// and match every unhashed job, since primary_input_hash is a plain
	// string and an unhashed job stores '' rather than NULL. See
	// job.DequeueOpts.PreferredHashes.
	preferred := opts.PreferredHashes()
	if len(preferred) == 0 {
		return "priority DESC, run_at ASC"
	}

	hashes := make([]string, len(preferred))
	for i, h := range preferred {
		hashes[i] = bind(h)
	}

	// primary_input_hash is nullable — rows written before the resource
	// migration have no value — and `NULL IN (...)` is NULL, not 0.
	//
	// The COALESCE is needed for a different reason than the identical
	// call in store/postgres. Postgres sorts NULLs FIRST under DESC, so
	// there an uncoalesced term would rank the rows with no locality
	// signal ABOVE the staged ones. SQLite sorts NULLs LAST under DESC,
	// which looks harmless — but it would sort a NULL-hash job below every
	// merely-unpreferred one, and those two are equally unpreferred under
	// job.DequeueOpts.Prefers, so RunAt is what must separate them.
	// COALESCE makes "unknown" mean exactly "not preferred", nothing
	// stronger. Note the result is the integer 0/1 rather than a boolean,
	// which DESC orders correctly but nothing may compare to TRUE.
	return "priority DESC, COALESCE(primary_input_hash IN (" +
		strings.Join(hashes, ",") + "), 0) DESC, run_at ASC"
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	m := new(jobModel)
	err := s.sdb.NewSelect(m).
		Where("id = ?", jobID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get job: %w", err)
	}
	return fromJobModel(m)
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	m, err := toJobModel(j)
	if err != nil {
		return err
	}

	m.UpdatedAt = time.Now().UTC()
	res, err := s.sdb.NewUpdate(m).WherePK().Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	res, err := s.sdb.NewDelete((*jobModel)(nil)).
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: delete job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (s *Store) ListJobsByState(ctx context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	var models []jobModel
	q := s.sdb.NewSelect(&models).
		Where("state = ?", string(state))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}

	q = q.OrderExpr("created_at ASC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list jobs by state: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: list jobs convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	now := time.Now().UTC()
	res, err := s.sdb.NewUpdate((*jobModel)(nil)).
		Set("heartbeat_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: heartbeat job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ReapStaleJobs returns running jobs whose last heartbeat — or, for jobs
// whose worker died before the first heartbeat, whose start time — is older
// than the given threshold.
func (s *Store) ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*job.Job, error) {
	cutoff := time.Now().UTC().Add(-threshold)
	var models []jobModel
	err := s.sdb.NewSelect(&models).
		Where("state = 'running'").
		Where("(heartbeat_at IS NOT NULL AND heartbeat_at < ?) OR (heartbeat_at IS NULL AND started_at IS NOT NULL AND started_at < ?)", cutoff, cutoff).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: reap stale jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: reap stale convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	q := s.sdb.NewSelect((*jobModel)(nil))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: count jobs: %w", err)
	}
	return count, nil
}
