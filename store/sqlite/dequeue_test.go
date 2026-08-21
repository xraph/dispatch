package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/sqlitedriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	sqlitestore "github.com/xraph/dispatch/store/sqlite"
	"github.com/xraph/dispatch/store/storetest"
)

// TestDequeueConformance runs the shared resource-aware dequeue contract
// against SQLite. openSqliteStore (store/sqlite/reap_test.go:19) opens a
// migrated store on a per-test temp directory, so every subtest gets its
// own database file for free.
func TestDequeueConformance(t *testing.T) {
	storetest.RunDequeueSuite(t, func(t *testing.T) job.Store {
		t.Helper()

		return openSqliteStore(t)
	})
}

// ──────────────────────────────────────────────────
// The two SQLite dialect traps, stated as tests
// ──────────────────────────────────────────────────

// TestNegativeLimitWouldBeUnlimitedInSQLite is the reason DequeueJobs
// returns early on Limit <= 0 rather than letting the statement handle it.
//
// The suite's NonPositiveLimitClaimsNothing pins the behaviour; this pins
// the hazard behind it, which is dialect-specific and easy to "simplify"
// away. On Postgres a negative LIMIT is an error. On SQLite it means
// UNLIMITED, so deleting the early return would hand a worker that just
// computed zero free slots the entire queue.
func TestNegativeLimitWouldBeUnlimitedInSQLite(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	const queue = "negative-limit"

	for i := range 3 {
		j := &job.Job{
			Entity:  dispatch.NewEntity(),
			ID:      id.NewJobID(),
			Name:    "job",
			Queue:   queue,
			Payload: []byte(`{}`),
			State:   job.StatePending,
			RunAt:   time.Now().UTC().Add(-time.Hour).Add(time.Duration(i) * time.Minute),
		}
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob: %v", err)
		}
	}

	var reachable int

	err := sqlitedriver.Unwrap(s.DB()).NewRaw(`
		SELECT COUNT(*) FROM (
			SELECT id FROM dispatch_jobs WHERE queue = ? LIMIT -1
		)`, queue,
	).Scan(ctx, &reachable)
	if err != nil {
		t.Fatalf("probe LIMIT -1: %v", err)
	}

	if reachable != 3 {
		t.Fatalf("LIMIT -1 returned %d of 3 rows; if SQLite no longer reads a "+
			"negative limit as unlimited, the early return in DequeueJobs can be "+
			"re-justified — until then it is load-bearing", reachable)
	}

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{queue}, Limit: -1})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("a negative limit claimed %d jobs, want 0", len(got))
	}
}

// TestDequeueWithNoQueuesClaimsNothing covers the other dialect trap:
// expanding an empty queue list would emit `queue IN ()`, which SQLite
// rejects as a SYNTAX ERROR where Postgres's `queue = ANY('{}')` is
// merely false. The caller must get the same empty result Postgres gives,
// not an error.
func TestDequeueWithNoQueuesClaimsNothing(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	j := &job.Job{
		Entity:  dispatch.NewEntity(),
		ID:      id.NewJobID(),
		Name:    "unreachable",
		Queue:   "no-queues",
		Payload: []byte(`{}`),
		State:   job.StatePending,
		RunAt:   time.Now().UTC().Add(-time.Hour),
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{Limit: 10})
	if err != nil {
		t.Fatalf("DequeueJobs with no queues: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("claimed %v with an empty queue list, want nothing", names(got))
	}

	// And the job was left alone for a caller that does name its queue.
	got, err = s.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{"no-queues"}, Limit: 10})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("claimed %v, want the one job", names(got))
	}
}

// ──────────────────────────────────────────────────
// Gap 1: rows written before the resource columns existed
// ──────────────────────────────────────────────────

// TestDequeueClaimsRowsWrittenBeforeTheResourceColumns covers the shape
// the shared suite cannot construct: a row inserted before migration
// 20260812130000 added req_cpu_milli and friends.
//
// It matters because a bare `req_memory_bytes <= ?` against a NULL
// evaluates to NULL, which is not true, so such a row would be silently
// dropped from every bounded dequeue — a job that was fine yesterday
// becoming unclaimable after an upgrade, with nothing reporting it.
//
// SQLite's ALTER TABLE ADD COLUMN ... NOT NULL DEFAULT 0 is what stops
// that: existing rows read back the default rather than NULL. This test
// inserts a row naming only the pre-migration columns — the only way to
// reproduce a legacy row, since no Go write path in this package can omit
// a column — asserts the defaults landed, and then claims it under
// BOUNDED opts.
//
// Bounded is load-bearing. Unbounded opts emit no fit predicate at all,
// so they would claim the row however badly the columns read, and the
// test would prove nothing.
func TestDequeueClaimsRowsWrittenBeforeTheResourceColumns(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	const queue = "legacy-resource-columns"

	legacyID := id.NewJobID()
	runAt := time.Now().UTC().Add(-time.Hour)
	raw := sqlitedriver.Unwrap(s.DB())

	_, err := raw.NewRaw(`
		INSERT INTO dispatch_jobs
			(id, name, queue, payload, state, priority, max_retries,
			 retry_count, run_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'pending', 0, 3, 0, ?, ?, ?)`,
		legacyID.String(), "written-before-the-columns", queue,
		[]byte(`{}`), runAt, runAt, runAt,
	).Exec(ctx)
	if err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	// Precondition: the row really does carry the added columns' defaults,
	// and really does carry SQL NULL in the nullable JSON/hash columns —
	// otherwise the claim below would be proving something easier.
	var (
		cpu, mem, disk, gpu int64
		custom              string
		requestsNull        int
		hashNull            int
	)

	// -1 is a sentinel a NULL column would produce: scanning a real NULL
	// into an int64 is an error, and erroring here would hide the failure
	// that actually matters, which is the claim below coming back empty.
	err = raw.NewRaw(`
		SELECT COALESCE(req_cpu_milli, -1), COALESCE(req_memory_bytes, -1),
		       COALESCE(req_disk_bytes, -1), COALESCE(req_gpu_milli, -1),
		       COALESCE(req_custom_keys, '<null>'),
		       resource_requests IS NULL, primary_input_hash IS NULL
		FROM dispatch_jobs WHERE id = ?`, legacyID.String(),
	).Scan(ctx, &cpu, &mem, &disk, &gpu, &custom, &requestsNull, &hashNull)
	if err != nil {
		t.Fatalf("read back legacy row: %v", err)
	}

	if cpu != 0 || mem != 0 || disk != 0 || gpu != 0 || custom != "" {
		t.Errorf("legacy row read back req_* = %d/%d/%d/%d/%q, want 0/0/0/0/\"\" "+
			"(-1 or <null> means the column lost its NOT NULL DEFAULT and the "+
			"bare comparison in buildFitPredicate now evaluates to NULL)",
			cpu, mem, disk, gpu, custom)
	}

	if requestsNull != 1 || hashNull != 1 {
		t.Fatalf("legacy row is not the pre-migration shape: "+
			"resource_requests IS NULL = %d, primary_input_hash IS NULL = %d, want 1/1",
			requestsNull, hashNull)
	}

	opts := job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{resource.Memory: 4 * storetest.GiB},
	}

	if opts.IsUnbounded() {
		t.Fatal("opts carrying a memory budget report IsUnbounded() = true; " +
			"an unbounded dequeue skips the predicate and would prove nothing here")
	}

	got, err := s.DequeueJobs(ctx, opts)
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 1 || got[0].ID != legacyID {
		t.Fatalf("claimed %v, want the one legacy row: a job written before the "+
			"req_* columns existed declares no requirement and must stay claimable",
			names(got))
	}
}

// TestResourceColumnsRejectNull pins the schema invariant the dequeue
// predicate leans on. The budget comparisons are deliberately NOT wrapped
// in COALESCE — that would cost the dequeue index, since SQLite cannot
// answer an expression from a plain column index — and they are only safe
// bare because NULL cannot reach those columns.
//
// If a future migration relaxes NOT NULL on any of them, this test fails
// and points at buildFitPredicate, rather than the change landing quietly
// and stranding rows.
func TestResourceColumnsRejectNull(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	j := &job.Job{
		Entity:  dispatch.NewEntity(),
		ID:      id.NewJobID(),
		Name:    "not-null-probe",
		Queue:   "not-null-probe",
		Payload: []byte(`{}`),
		State:   job.StatePending,
		RunAt:   time.Now().UTC().Add(-time.Hour),
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	raw := sqlitedriver.Unwrap(s.DB())

	for _, column := range []string{
		"req_cpu_milli", "req_memory_bytes", "req_disk_bytes",
		"req_gpu_milli", "req_custom_keys",
	} {
		// column comes from the compile-time list above, never from input.
		_, err := raw.NewRaw(
			`UPDATE dispatch_jobs SET `+column+` = NULL WHERE id = ?`, j.ID.String(),
		).Exec(ctx)
		if err == nil {
			t.Errorf("%s accepted NULL; the dequeue predicate compares it bare, "+
				"so a NULL there silently drops the row from every bounded claim", column)
		}
	}
}

// ──────────────────────────────────────────────────
// Gap 2: NULL primary_input_hash in the locality ordering
// ──────────────────────────────────────────────────

const stagedHash = "blake3:staged-here"

// nullHashFixtures enqueues four same-priority jobs on queue and then
// rewrites one job's primary_input_hash to a genuine SQL NULL, which the
// Go write path cannot produce (jobModel.PrimaryInputHash is a plain
// string, so an unset hash stores ”).
//
// The fixture set is chosen so a broken locality term cannot pass by
// accident, which is the trap two earlier backends fell into:
//
//   - "remote-high" carries a hash that collates ABOVE the staged one. With
//     only one hash value present, a NULL and an empty string both collate
//     BELOW every real hash, so a sort on the hash VALUE would put the
//     staged job first for entirely the wrong reason. "zzz:never-staged"
//     is what makes that mutation visible.
//   - The staged job is the LAST by RunAt, so it must jump the whole band;
//     every unpreferred fixture beats it on any tie the locality term
//     fails to break.
//   - The NULL-hash job is the EARLIEST by RunAt, so it must come first
//     among the unpreferred. Dropping the COALESCE would sort it below
//     them all — SQLite puts NULL last under DESC — even though it is no
//     less preferred than an empty or unmatched hash.
func nullHashFixtures(t *testing.T, s *sqlitestore.Store, queue string) {
	t.Helper()

	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	fixtures := []struct {
		name   string
		hash   string
		offset time.Duration
	}{
		{"null-hash", "", 0},
		{"remote-high", "zzz:never-staged", time.Minute},
		{"empty-hash", "", 2 * time.Minute},
		{"cached", stagedHash, 3 * time.Minute},
	}

	var nullHashID id.JobID

	for _, f := range fixtures {
		j := &job.Job{
			Entity:           dispatch.NewEntity(),
			ID:               id.NewJobID(),
			Name:             f.name,
			Queue:            queue,
			Payload:          []byte(`{}`),
			State:            job.StatePending,
			MaxRetries:       3,
			RunAt:            base.Add(f.offset),
			PrimaryInputHash: f.hash,
		}

		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob(%s): %v", f.name, err)
		}

		if f.name == "null-hash" {
			nullHashID = j.ID
		}
	}

	_, err := sqlitedriver.Unwrap(s.DB()).NewRaw(
		`UPDATE dispatch_jobs SET primary_input_hash = NULL WHERE id = ?`,
		nullHashID.String(),
	).Exec(ctx)
	if err != nil {
		t.Fatalf("null out primary_input_hash: %v", err)
	}
}

// TestDequeueOrdersNullPrimaryInputHashAsUnpreferred covers the returned
// ORDER: a row with no locality signal at all is neither preferred nor
// penalised, it is simply unpreferred, and RunAt then separates it from
// the other unpreferred rows.
func TestDequeueOrdersNullPrimaryInputHashAsUnpreferred(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	const queue = "null-hash-order"

	nullHashFixtures(t, s, queue)

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:       []string{queue},
		Limit:        10,
		PreferHashes: []string{stagedHash},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	wantSequence(t, got, "cached", "null-hash", "remote-high", "empty-hash")
}

// TestDequeueSelectsPreferredOverNullHashUnderLimit covers the other half,
// and it is the half only SQL can answer: WHICH jobs a tight limit keeps.
//
// The returned slice is ordered in Go by job.DequeueOpts.Less, so the test
// above would still pass with no locality term in the statement at all.
// Here only two of four eligible jobs may be claimed, so the choice is
// made entirely by the statement's ORDER BY, and the COALESCE that decides
// where the NULL-hash row sits is what picks the second one.
func TestDequeueSelectsPreferredOverNullHashUnderLimit(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	const queue = "null-hash-limit"

	nullHashFixtures(t, s, queue)

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:       []string{queue},
		Limit:        2,
		PreferHashes: []string{stagedHash},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	wantSequence(t, got, "cached", "null-hash")
}

// ──────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────

func names(jobs []*job.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Name)
	}

	return out
}

func wantSequence(t *testing.T, got []*job.Job, want ...string) {
	t.Helper()

	gotNames := names(got)
	if len(gotNames) != len(want) {
		t.Fatalf("claimed %v, want %v", gotNames, want)
	}

	for i := range want {
		if gotNames[i] != want[i] {
			t.Fatalf("claimed %v, want %v (differs at index %d)", gotNames, want, i)
		}
	}
}
