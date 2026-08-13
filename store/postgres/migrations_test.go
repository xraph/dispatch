//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/postgres"
)

// leaseMigrationVersion is the version string of the lease migration,
// restated here so a test can delete its bookkeeping row and force the
// re-run every restarting pod performs after a failure.
const leaseMigrationVersion = "20260812120000"

// remigrate re-runs the migration group against the same database, which
// is what a pod does on every start.
func remigrate(t *testing.T, s *postgres.Store) {
	t.Helper()

	if err := postgres.New(s.DB(), postgres.WithLogger(log.NewNoopLogger())).
		Migrate(context.Background()); err != nil {
		t.Fatalf("re-running the migration group must succeed, got: %v", err)
	}
}

// forgetLeaseMigration deletes the lease migration's grove_migrations row,
// reproducing a crash between Up and RecordApplied — a real window,
// because migrate.Orchestrator calls them as two separate steps.
func forgetLeaseMigration(t *testing.T, conn driver.DedicatedConn) {
	t.Helper()

	if _, err := conn.Exec(context.Background(),
		`DELETE FROM grove_migrations WHERE version = $1`, leaseMigrationVersion); err != nil {
		t.Fatalf("delete migration row: %v", err)
	}
}

// indexIsValid reports whether the named index exists and is usable.
//
// The two are different states and the difference is the whole hazard of
// CREATE INDEX CONCURRENTLY: a build that fails leaves the index present
// in the catalog but INVALID, which the planner ignores and IF NOT EXISTS
// would then skip forever.
func indexIsValid(t *testing.T, conn driver.DedicatedConn, name string) (exists, valid bool) {
	t.Helper()

	rows, err := conn.Query(context.Background(), `
		SELECT i.indisvalid
		FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
		WHERE c.relname = $1`, name)
	if err != nil {
		t.Fatalf("read indisvalid for %s: %v", name, err)
	}

	defer rows.Close()

	if !rows.Next() {
		return false, false
	}

	if err = rows.Scan(&valid); err != nil {
		t.Fatalf("scan indisvalid: %v", err)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("iterate indisvalid: %v", err)
	}

	return true, valid
}

// TestLeaseMigrationBuildsAValidIndex pins that the lease index survives
// a normal run as a USABLE index.
//
// It is not a tautology. The migration builds it CONCURRENTLY, because a
// plain CREATE INDEX holds a SHARE lock for the whole build and blocks
// every INSERT, UPDATE and DELETE on dispatch_jobs while the rest of the
// fleet is still enqueueing and completing on it. The price is that a
// failed CONCURRENTLY build leaves an INVALID index that nothing reports:
// migrations keep succeeding, the planner keeps ignoring it, and the
// reclaim sweep degrades to a sequential scan of the whole table.
func TestLeaseMigrationBuildsAValidIndex(t *testing.T) {
	s := setupTestStore(t)

	conn, err := pgdriver.Unwrap(s.DB()).AcquireConn(context.Background())
	if err != nil {
		t.Fatalf("acquire dedicated conn: %v", err)
	}

	defer conn.Release()

	exists, valid := indexIsValid(t, conn, "idx_dispatch_jobs_lease")
	if !exists {
		t.Fatal("idx_dispatch_jobs_lease is missing after a clean migration")
	}

	if !valid {
		t.Error("idx_dispatch_jobs_lease is INVALID: a failed CONCURRENTLY build was left " +
			"in place, so the reclaim sweep has no usable index and nothing reports it")
	}
}

// TestLeaseMigrationConvergesFromAnInvalidIndex is the reason the
// migration drops an invalid leftover before building.
//
// CREATE INDEX CONCURRENTLY IF NOT EXISTS sees an INVALID index, decides
// there is nothing to do, and returns success. Without the pre-drop the
// table would then be permanently without a usable lease index while
// every subsequent migration run reported that everything was fine.
func TestLeaseMigrationConvergesFromAnInvalidIndex(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	conn, err := pgdriver.Unwrap(s.DB()).AcquireConn(ctx)
	if err != nil {
		t.Fatalf("acquire dedicated conn: %v", err)
	}

	defer conn.Release()

	// The catalog state a failed CONCURRENTLY build leaves behind. There
	// is no way to reach it through DDL, so it is written directly.
	if _, err = conn.Exec(ctx, `
		UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = 'idx_dispatch_jobs_lease'::regclass`); err != nil {
		t.Fatalf("mark the index invalid: %v", err)
	}

	if _, valid := indexIsValid(t, conn, "idx_dispatch_jobs_lease"); valid {
		t.Fatal("fixture is wrong: the index should be INVALID")
	}

	forgetLeaseMigration(t, conn)
	remigrate(t, s)

	exists, valid := indexIsValid(t, conn, "idx_dispatch_jobs_lease")
	if !exists {
		t.Fatal("idx_dispatch_jobs_lease is missing after the retry")
	}

	if !valid {
		t.Error("the retry left the index INVALID: CREATE INDEX CONCURRENTLY IF NOT EXISTS " +
			"skipped the unusable leftover instead of replacing it, and reported success")
	}
}

// TestLeaseMigrationSurvivesAPartialApplication is the postgres half of
// the re-runnability proof.
//
// Grove executes Up outside any transaction, so a process killed partway
// through leaves some of the change applied and no row in
// grove_migrations. Every statement in Up is written to be re-runnable
// from any point for exactly that reason; this asserts it rather than
// trusting the IF NOT EXISTS clauses by inspection.
func TestLeaseMigrationSurvivesAPartialApplication(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	conn, err := pgdriver.Unwrap(s.DB()).AcquireConn(ctx)
	if err != nil {
		t.Fatalf("acquire dedicated conn: %v", err)
	}

	defer conn.Release()

	// What a crash after the ALTER's first two columns would have left.
	for _, stmt := range []string{
		`ALTER TABLE dispatch_jobs DROP COLUMN lease_ttl`,
		`ALTER TABLE dispatch_jobs DROP COLUMN evict_count`,
		`DROP INDEX IF EXISTS idx_dispatch_jobs_lease`,
	} {
		if _, err = conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	forgetLeaseMigration(t, conn)
	remigrate(t, s)

	for _, col := range []string{
		"lease_epoch", "lease_expires_at", "lease_ttl", "evict_count",
	} {
		var present bool

		if err = conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'dispatch_jobs' AND column_name = $1)`,
			col).Scan(&present); err != nil {
			t.Fatalf("look up %s: %v", col, err)
		}

		if !present {
			t.Errorf("column %s missing after the retry", col)
		}
	}

	// And the schema is usable, not merely present.
	j := storetestPendingJob("after-lease-retry", "lease-retry", 6*time.Hour)
	if err = s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob after the retry: %v", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob after the retry: %v", err)
	}

	if got.LeaseTTL != 6*time.Hour {
		t.Errorf("LeaseTTL = %v, want the declaration intact", got.LeaseTTL)
	}
}

// TestLeaseMigrationBackfillsRunningJobs is the regression test for jobs
// that were mid-flight when the fleet upgraded.
//
// Without the backfill those jobs are stranded permanently and nothing
// reports it. lease_expires_at arrives NULL; ReclaimExpiredLeases
// requires a non-NULL expiry to consider a row at all (job.Lease.IsExpired
// deliberately reads a zero expiry as "never leased", not "expired"); the
// pool's reaper no longer calls ReapStaleJobs once the backend implements
// job.LeaseStore; and dequeue claims only pending and retrying rows. A job
// running at the instant of the upgrade is therefore never looked at by
// anything again — it holds its slot forever.
//
// Each case sets up one branch of the COALESCE and asserts the outcome
// that matters: not that a column is non-NULL, but that the normal
// reclaim path actually collects the row.
func TestLeaseMigrationBackfillsRunningJobs(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	older := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)

	tests := []struct {
		name string
		// heartbeat and started are written onto the running row before
		// the migration re-runs; the zero time writes NULL.
		heartbeat time.Time
		started   time.Time
		// want is the expiry the backfill must produce, or the zero time
		// when the migration has to render NOW() itself.
		want time.Time
	}{
		{
			name:      "heartbeat_at is the freshest evidence and wins",
			heartbeat: past,
			started:   older,
			want:      past,
		},
		{
			name:    "started_at when the job never heartbeated",
			started: older,
			want:    older,
		},
		{
			name: "NOW() when the row predates both",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()

			conn, err := pgdriver.Unwrap(s.DB()).AcquireConn(ctx)
			if err != nil {
				t.Fatalf("acquire dedicated conn: %v", err)
			}

			defer conn.Release()

			j := storetestPendingJob("mid-flight", "backfill", 0)
			if err = s.EnqueueJob(ctx, j); err != nil {
				t.Fatalf("EnqueueJob: %v", err)
			}

			// Rewind the row to what an upgrading fleet finds: a job the
			// old code left running, with no lease because leases did not
			// exist when it was claimed.
			if _, err = conn.Exec(ctx, `
				UPDATE dispatch_jobs
				SET state = 'running', heartbeat_at = $1, started_at = $2,
				    lease_expires_at = NULL
				WHERE id = $3`,
				nullableTime(tt.heartbeat), nullableTime(tt.started), j.ID.String()); err != nil {
				t.Fatalf("rewind the row: %v", err)
			}

			forgetLeaseMigration(t, conn)
			remigrate(t, s)

			var expiry *time.Time

			if err = conn.QueryRow(ctx,
				`SELECT lease_expires_at FROM dispatch_jobs WHERE id = $1`,
				j.ID.String()).Scan(&expiry); err != nil {
				t.Fatalf("read lease_expires_at: %v", err)
			}

			if expiry == nil {
				t.Fatal("lease_expires_at is still NULL after the migration: this job is " +
					"unreclaimable forever — reclaim skips NULL expiries and dequeue " +
					"never looks at running rows")
			}

			if !tt.want.IsZero() && !expiry.UTC().Equal(tt.want) {
				t.Errorf("lease_expires_at = %v, want %v", expiry.UTC(), tt.want)
			}

			// The outcome the backfill exists for.
			reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
			if err != nil {
				t.Fatalf("ReclaimExpiredLeases: %v", err)
			}

			if len(reclaimed) != 1 || reclaimed[0].ID != j.ID {
				t.Fatalf("reclaimed %d jobs, want the backfilled one (%s); "+
					"lease_expires_at = %v was written but the reclaim predicate "+
					"did not match it", len(reclaimed), j.ID, expiry)
			}

			if reclaimed[0].State != job.StatePending {
				t.Errorf("reclaimed job state = %v, want pending", reclaimed[0].State)
			}
		})
	}
}

// nullableTime renders the zero time as a NULL bind rather than as year
// one, so a test case can express "this column was never written".
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}

	return t
}

// storetestPendingJob builds a pending job directly rather than through
// storetest.PendingJob, so these tests do not depend on the conformance
// suite's fixture shape.
func storetestPendingJob(name, queue string, ttl time.Duration) *job.Job {
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
