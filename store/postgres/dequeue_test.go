//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/storetest"
)

// TestDequeueConformance runs the resource-aware dequeue suite against the
// Postgres store — the reference SQL implementation the SQLite backend
// follows.
//
// The container is stood up once and shared by every subtest. The suite
// documents that this is safe: each case enqueues onto its own queue and
// asserts only on the jobs it created, and standing up a fresh container
// per subtest would dominate the runtime of the whole file.
func TestDequeueConformance(t *testing.T) {
	shared := setupTestStore(t)

	storetest.RunDequeueSuite(t, func(t *testing.T) job.Store {
		t.Helper()

		return shared
	})
}

// TestDequeueOrdersNullPrimaryInputHashAsUnpreferred covers the one row
// shape the shared suite cannot produce.
//
// primary_input_hash is nullable, and rows written before the resource
// migration carry a genuine SQL NULL rather than the empty string the
// current insert path writes. `NULL = ANY(...)` evaluates to NULL, not
// false, and Postgres sorts NULLs FIRST under DESC — so an uncoalesced
// locality term would rank exactly the rows with no locality signal ABOVE
// the ones the caller has already staged, inverting the optimization.
//
// The NULL is written with raw SQL because no Go path can produce one:
// jobModel.PrimaryInputHash is a plain string.
func TestDequeueOrdersNullPrimaryInputHashAsUnpreferred(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const (
		queue = "null-hash-order"
		local = "blake3:staged-here"
	)

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	// nullHash is the earlier of the two by RunAt, so it wins any tie the
	// ordering fails to break: if the locality term is not NULL-safe it
	// comes back first.
	nullHash := newHashFixture("null-hash", queue, base)
	cached := newHashFixture("cached", queue, base.Add(time.Minute))
	cached.PrimaryInputHash = local

	for _, j := range []*job.Job{nullHash, cached} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	if _, err := pgdriver.Unwrap(s.DB()).NewRaw(
		`UPDATE dispatch_jobs SET primary_input_hash = NULL WHERE id = $1`,
		nullHash.ID.String(),
	).Exec(ctx); err != nil {
		t.Fatalf("null out primary_input_hash: %v", err)
	}

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:       []string{queue},
		Limit:        10,
		PreferHashes: []string{local},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("claimed %d jobs, want 2", len(got))
	}

	if got[0].Name != "cached" || got[1].Name != "null-hash" {
		t.Fatalf("claimed [%s %s], want [cached null-hash]: a NULL hash must sort as "+
			"not preferred, not ahead of the job the caller has staged",
			got[0].Name, got[1].Name)
	}
}

// TestDequeueBoundedQueryPlanUsesDequeueIndex proves the fit predicate did
// not cost the candidate scan its index.
//
// idx_dispatch_jobs_dequeue_res is what keeps that scan from reading every
// pending row on a busy queue, and a predicate written so the planner
// *could not* use it — a function wrapped around a compared column, a
// value cast the wrong way — would surface only as a latency regression
// under load, long after this change shipped. The plan is taken for the
// inner candidate SELECT, which is the half the index serves.
//
// The check asks whether the predicate CAN use the index, not which plan
// the planner happens to cost lowest: at any table size a test can
// populate in a second, a sequential scan over dispatch_jobs is genuinely
// cheaper, so a bare EXPLAIN would only be measuring the fixture. So
// sequential scans are disabled and the two indexes that would otherwise
// win on cost are dropped inside a transaction that is always rolled
// back — DDL is transactional in Postgres, so the schema is untouched.
// All of it runs on one dedicated connection, or the pool could hand the
// EXPLAIN a session that never saw the setup.
func TestDequeueBoundedQueryPlanUsesDequeueIndex(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const (
		queue = "plan-check"
		rows  = 500
	)

	base := time.Now().UTC().Add(-time.Hour)

	for i := range rows {
		j := newHashFixture("plan", queue, base)
		j.Resources = resource.Set{resource.Memory: storetest.GiB}
		j.Priority = i % 7

		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	conn, err := pgdriver.Unwrap(s.DB()).AcquireConn(ctx)
	if err != nil {
		t.Fatalf("acquire dedicated conn: %v", err)
	}

	defer conn.Release()

	for _, stmt := range []string{
		`ANALYZE dispatch_jobs`,
		`SET enable_seqscan = off`,
		`BEGIN`,
		// idx_dispatch_jobs_state is cheaper on a table this small, and
		// idx_dispatch_jobs_dequeue is the same key without the INCLUDE,
		// so either would satisfy a name check without proving anything.
		`DROP INDEX idx_dispatch_jobs_state`,
		`DROP INDEX idx_dispatch_jobs_dequeue`,
	} {
		if _, execErr := conn.Exec(ctx, stmt); execErr != nil {
			t.Fatalf("%s: %v", stmt, execErr)
		}
	}

	defer func() {
		if _, rbErr := conn.Exec(ctx, `ROLLBACK`); rbErr != nil {
			t.Errorf("rollback index drops: %v", rbErr)
		}
	}()

	var plan []byte

	// The fully bounded predicate: all four dimensions plus the nested
	// REPLACE containment test.
	err = conn.QueryRow(ctx, `
		EXPLAIN (FORMAT JSON)
		SELECT id FROM dispatch_jobs
		WHERE state IN ('pending', 'retrying')
		  AND queue = ANY($1)
		  AND run_at <= NOW()
		  AND req_cpu_milli <= $2
		  AND req_memory_bytes <= $3
		  AND req_disk_bytes <= $4
		  AND req_gpu_milli <= $5
		  AND REPLACE(req_custom_keys, $6, $7) IN ('', $7)
		ORDER BY priority DESC, run_at ASC
		LIMIT 4`,
		[]string{queue},
		8*resource.MilliScale, 4*storetest.GiB, 100*storetest.GiB, 4*resource.MilliScale,
		",fpga,", ",",
	).Scan(&plan)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}

	if !strings.Contains(string(plan), "idx_dispatch_jobs_dequeue_res") {
		t.Errorf("bounded dequeue plan does not use idx_dispatch_jobs_dequeue_res:\n%s", plan)
	}

	t.Logf("bounded dequeue plan:\n%s", plan)
}

func newHashFixture(name, queue string, runAt time.Time) *job.Job {
	return &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      queue,
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      runAt,
	}
}
