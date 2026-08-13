package postgres

import (
	"context"
	"fmt"
	"strconv"
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

	_, err = s.pgdb.NewInsert(m).Exec(ctx)
	if err != nil {
		if isDuplicateKey(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf(errPrefix+"enqueue job: %w", err)
	}
	s.notifyWake(ctx)
	return nil
}

// DequeueJobs atomically claims up to opts.Limit ready jobs from
// opts.Queues that fit opts, sets them to running, and returns them
// ordered by priority descending, then locality-preferred first, then
// RunAt ascending.
//
// The fit predicate is compiled into the same statement that performs
// the claim, so a job that does not fit is never written to: it stays
// pending and untouched for the next worker that does have room. The
// UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) shape that
// makes the claim atomic is unchanged; the predicate is simply another
// conjunct of the inner SELECT's WHERE.
//
// When opts.Grants() the lease columns are additional assignments in that
// same UPDATE's SET clause, never a follow-up statement. ReclaimExpiredLeases
// requires lease_expires_at IS NOT NULL, so a row left running with a null
// expiry is not a row at risk of reclamation — it is one reclamation can
// never see. See job.DequeueOpts.LeaseUntil.
func (s *Store) DequeueJobs(ctx context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	// A worker computing zero free slots must claim zero jobs, never the
	// whole queue. Postgres would already return nothing for LIMIT 0, but
	// a negative LIMIT is an error rather than an empty result, and
	// neither is worth a round trip.
	if opts.Limit <= 0 {
		return nil, nil
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf(errPrefix+"dequeue jobs: %w", err)
	}

	query, args := buildDequeueQuery(opts)

	var models []jobModel

	err := s.pgdb.NewRaw(query, args...).Scan(ctx, &models)
	if err != nil {
		return nil, fmt.Errorf(errPrefix+"dequeue jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf(errPrefix+"dequeue convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// budgetColumns maps each canonical dimension the dequeue predicate
// compares to the scalar column that holds it. These are exactly the
// dimensions job.DequeueOpts.Allows loops over, and exactly the columns
// idx_dispatch_jobs_dequeue_res INCLUDEs, so each comparison is a scalar
// range test the index can answer from its own tuples rather than a JSON
// probe into resource_requests.
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

// dequeueSQL is the claim statement with four things filled in: the lease
// grant, the fit predicate, the ordering, and the limit placeholder. The
// ordering is substituted twice because the inner SELECT decides WHICH
// rows the LIMIT keeps and the outer SELECT decides the order they come
// back in — ordering only the outer one would hand a small-limit worker
// an arbitrary slice of the eligible set in tidy order.
//
// The grant is a suffix of the SET clause rather than a statement of its
// own, which is what makes "claimed" and "leased" the same event.
const dequeueSQL = `
		WITH dequeued AS (
			UPDATE dispatch_jobs
			SET state = 'running', started_at = NOW(), updated_at = NOW()%s
			WHERE id IN (
				SELECT id FROM dispatch_jobs
				WHERE state IN ('pending', 'retrying')
				  AND queue = ANY($1)
				  AND run_at <= NOW()%s
				ORDER BY %s
				FOR UPDATE SKIP LOCKED
				LIMIT %s
			)
			RETURNING *
		)
		SELECT * FROM dequeued ORDER BY %s`

// buildDequeueQuery compiles opts into the claim statement and its bind
// parameters. It is the SQL expression of job.DequeueOpts.Allows and
// Less, and must answer identically for every job.
func buildDequeueQuery(opts job.DequeueOpts) (query string, args []any) {
	args = []any{opts.Queues}

	// bind appends v and returns the placeholder that reads it. Values
	// never reach the statement text.
	bind := func(v any) string {
		args = append(args, v)

		return "$" + strconv.Itoa(len(args))
	}

	grant := buildLeaseGrant(opts, bind)
	fit := buildFitPredicate(opts, bind)
	order := buildDequeueOrder(opts, bind)
	limit := bind(opts.Limit)

	return fmt.Sprintf(dequeueSQL, grant, fit, order, limit, order), args
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
func buildLeaseGrant(opts job.DequeueOpts, bind func(any) string) string {
	if !opts.Grants() {
		return ""
	}

	return ",\n\t\t\t    worker_id = " + bind(opts.WorkerID.String()) +
		",\n\t\t\t    lease_epoch = lease_epoch + 1" +
		",\n\t\t\t    lease_expires_at = " + bind(opts.LeaseUntil.UTC())
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
		b.WriteString("\n\t\t\t\t  AND id = " + bind(opts.ReservedFor.String()))
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

		b.WriteString("\n\t\t\t\t  AND " + dim.column + " <= " + bind(budget))
	}

	b.WriteString("\n\t\t\t\t  AND " + buildCustomKeyPredicate(opts, bind))

	return b.String()
}

// buildCustomKeyPredicate renders custom-resource containment as a
// genuine SUBSET test.
//
// req_custom_keys holds resource.EncodeCustomKeys' output — the sorted
// required keys wrapped in leading and trailing separators, e.g.
// ",fpga,tpu,". The obvious formulation, LIKE '%' || req_custom_keys ||
// '%' against the offered list, is a SUBSTRING test: it passes every
// single-key case including the prefix collision, then silently strands a
// job needing {fpga,tpu} from a caller offering {fpga,nvme,tpu}, because
// the interleaved key breaks the contiguous run. The job it strands is
// the specialised one that is hardest to place anywhere else.
//
// Instead each offered key is stripped from the stored list by a nested
// REPLACE of ",key," with ",", which restores the separator the removal
// consumed and so composes in any order. What remains is "" or a lone
// separator exactly when every required key was offered. That is the
// portable formulation SQLite can copy verbatim; string_to_array(...) <@
// ARRAY[...] would be the Postgres-native alternative, but it has to
// filter the empty elements the wrapping separators produce, and keeping
// the two SQL backends identical is worth more than the array operator.
func buildCustomKeyPredicate(opts job.DequeueOpts, bind func(any) string) string {
	sep := bind(resource.CustomKeySep)
	offered := opts.OfferedCustomKeys()

	// Bounded opts with an empty offer are a resource-aware worker that
	// genuinely has no custom resources, so only jobs requiring none are
	// eligible. This is the case IsUnbounded above has already excluded.
	expr := "req_custom_keys"
	for _, k := range offered {
		expr = "REPLACE(" + expr + ", " + bind(resource.CustomKeySep+k+resource.CustomKeySep) + ", " + sep + ")"
	}

	return expr + " IN ('', " + sep + ")"
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
	// PreferredHashes, not PreferHashes: an empty entry would bind as
	// '' and match every unhashed job, since primary_input_hash is a
	// plain string and an unhashed job stores '' rather than NULL. See
	// job.DequeueOpts.PreferredHashes.
	hashes := opts.PreferredHashes()
	if len(hashes) == 0 {
		return "priority DESC, run_at ASC"
	}

	// primary_input_hash is nullable — rows written before the resource
	// migration have no value — and NULL = ANY(...) is NULL, not false.
	// Postgres sorts NULLs FIRST under DESC, so an uncoalesced term would
	// rank exactly the rows with no locality signal ABOVE the ones the
	// caller has staged. COALESCE makes "unknown" mean "not preferred".
	return "priority DESC, COALESCE(primary_input_hash = ANY(" +
		bind(hashes) + "), FALSE) DESC, run_at ASC"
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	m := new(jobModel)
	err := s.pgdb.NewSelect(m).
		Where("id = ?", jobID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf(errPrefix+"get job: %w", err)
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
	res, err := s.pgdb.NewUpdate(m).WherePK().Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"update job: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrJobNotFound
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	res, err := s.pgdb.NewDelete((*jobModel)(nil)).
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"delete job: %w", err)
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
	q := s.pgdb.NewSelect(&models).
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
		return nil, fmt.Errorf(errPrefix+"list jobs by state: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf(errPrefix+"list jobs convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	res, err := s.pgdb.NewUpdate((*jobModel)(nil)).
		Set("heartbeat_at = NOW()").
		Set("updated_at = NOW()").
		Where("id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"heartbeat job: %w", err)
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
	var models []jobModel
	err := s.pgdb.NewSelect(&models).
		Where("state = 'running'").
		Where("COALESCE(heartbeat_at, started_at) IS NOT NULL").
		Where("COALESCE(heartbeat_at, started_at) < NOW() - ?::interval", threshold.String()).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf(errPrefix+"reap stale jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf(errPrefix+"reap stale convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	q := s.pgdb.NewSelect((*jobModel)(nil))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf(errPrefix+"count jobs: %w", err)
	}
	return count, nil
}
