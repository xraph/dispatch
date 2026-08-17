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

// TestReclaimAdoptsRunningJobsWithoutLease covers a running job carrying
// no lease at all, which is the row shape that used to be stranded
// permanently. lease_expires_at is NULL, ReclaimExpiredLeases required a
// non-NULL expiry to consider a row at all (job.Lease.IsExpired
// deliberately reads a zero expiry as "never leased", not "expired"), the
// pool's reaper no longer calls ReapStaleJobs once the backend implements
// job.LeaseStore, and dequeue claims only pending and retrying rows. So
// nothing looked at these again and they held their slots forever.
//
// Migration 008 used to seed an expiry for them. It no longer does, for
// the reasons given at that migration: a one-shot backfill misses every
// row created after it runs, including anything an old pod claims later in
// a rolling upgrade and anything claimed through job.Store without lease
// options, which never grants a lease at all. Reclamation adopts them
// instead, on every sweep and gated on silence.
//
// The negative cases are the ones that matter. A NULL expiry does not by
// itself mean the job was abandoned, so evicting on that alone would take
// live work away from a healthy caller.
func TestReclaimAdoptsRunningJobsWithoutLease(t *testing.T) {
	silent := time.Now().UTC().Add(-job.UnleasedReclaimGrace - time.Minute)
	older := silent.Add(-time.Hour)
	fresh := time.Now().UTC()

	tests := []struct {
		name      string
		heartbeat time.Time
		started   time.Time
		want      bool
		why       string
	}{
		{
			name:      "silent heartbeat is adopted",
			heartbeat: silent,
			started:   older,
			want:      true,
			why:       "abandoned by a worker that stopped reporting",
		},
		{
			name:      "fresh heartbeat is left alone",
			heartbeat: fresh,
			started:   older,
			want:      false,
			why:       "still reporting, so it belongs to a healthy worker",
		},
		{
			name:    "silent started_at is adopted when it never heartbeated",
			started: silent,
			want:    true,
			why:     "died before its first beat",
		},
		{
			name:    "fresh started_at is left alone",
			started: fresh,
			want:    false,
			why:     "just claimed; its first heartbeat is not due yet",
		},
		{
			name: "neither timestamp is left alone",
			want: false,
			why:  "no timestamp to establish age from",
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

			j := storetestPendingJob("mid-flight", "adopt", 0)
			if err = s.EnqueueJob(ctx, j); err != nil {
				t.Fatalf("EnqueueJob: %v", err)
			}

			// The row an upgrading fleet finds, or that a caller without
			// lease options writes: running, with no lease at all.
			if _, err = conn.Exec(ctx, `
				UPDATE dispatch_jobs
				SET state = 'running', heartbeat_at = $1, started_at = $2,
				    lease_expires_at = NULL
				WHERE id = $3`,
				nullableTime(tt.heartbeat), nullableTime(tt.started), j.ID.String()); err != nil {
				t.Fatalf("rewind the row: %v", err)
			}

			reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
			if err != nil {
				t.Fatalf("ReclaimExpiredLeases: %v", err)
			}

			got := len(reclaimed) == 1 && reclaimed[0].ID == j.ID
			if got != tt.want {
				if tt.want {
					t.Fatalf("job was not reclaimed but should have been: %s", tt.why)
				}

				t.Fatalf("job was reclaimed but must not be: %s", tt.why)
			}
			if tt.want && reclaimed[0].State != job.StatePending {
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
