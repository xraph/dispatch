package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/sqlitedriver"
	_ "github.com/xraph/grove/drivers/sqlitedriver/sqlitemigrate" // registers the sqlite migrate executor

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	sqlitestore "github.com/xraph/dispatch/store/sqlite"
)

// resourceMigrationVersion is the version string of the migration under
// test, restated here so the test can delete its bookkeeping row.
const resourceMigrationVersion = "20260812130000"

// leaseMigrationVersion is the same for the lease migration, which runs
// immediately before it.
const leaseMigrationVersion = "20260812120000"

// leaseColumns is every column the lease migration adds, in the order it
// adds them.
var leaseColumns = []string{
	"lease_epoch", "lease_expires_at", "lease_ttl", "evict_count",
}

// openMigratedWithDriver is openSqliteStore with the raw driver handed
// back too, so a test can reach past the store and mutate the schema
// into shapes no code path produces.
func openMigratedWithDriver(t *testing.T) (*sqlitestore.Store, driver.Driver, *grove.DB) {
	t.Helper()

	ctx := context.Background()

	drv := sqlitedriver.New()
	if err := drv.Open(ctx, filepath.Join(t.TempDir(), "dispatch.db")); err != nil {
		t.Fatalf("open sqlitedriver: %v", err)
	}

	db, err := grove.Open(drv)
	if err != nil {
		t.Fatalf("grove open: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	s := sqlitestore.New(db)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return s, drv, db
}

func mustExec(t *testing.T, drv driver.Driver, stmt string, args ...any) {
	t.Helper()

	if _, err := drv.Exec(context.Background(), stmt, args...); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// scanText reads one nullable text column from a single-row query,
// returning "" for NULL. It exists so a test can look at what a migration
// actually wrote, rather than at what the model layer renders it back as.
func scanText(t *testing.T, drv driver.Driver, query string, args ...any) string {
	t.Helper()

	rows, err := drv.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			t.Errorf("close rows: %v", closeErr)
		}
	}()

	if !rows.Next() {
		t.Fatalf("query %q returned no rows", query)
	}

	var v *string
	if err = rows.Scan(&v); err != nil {
		t.Fatalf("scan %q: %v", query, err)
	}

	if v == nil {
		return ""
	}

	return *v
}

// hasColumn reports whether dispatch_jobs currently has the named column.
//
// The table is fixed rather than a parameter: every migration in this file
// changes the one table, and a parameter that only ever receives one value
// reads as generality the tests do not have.
func hasColumn(t *testing.T, drv driver.Driver, column string) bool {
	t.Helper()

	rows, err := drv.Query(context.Background(),
		`SELECT 1 FROM pragma_table_info('dispatch_jobs') WHERE name = ?`, column)
	if err != nil {
		t.Fatalf("pragma_table_info: %v", err)
	}

	found := rows.Next()

	if err = rows.Close(); err != nil {
		t.Fatalf("close pragma rows: %v", err)
	}

	return found
}

// TestResourceMigrationSurvivesAPartialApplication is the proof that the
// resource migration can no longer wedge a SQLite deployment
// permanently.
//
// The failure it reproduces is not hypothetical and has no workaround.
// SQLite has no ADD COLUMN IF NOT EXISTS, and grove executes Up outside
// any transaction (migrate.Orchestrator.Migrate calls m.Up and only then
// RecordApplied). So a process killed — or a disk filled, or a context
// cancelled — partway through the ten ADD COLUMNs leaves some columns
// added and NO row in grove_migrations. The next start re-runs Up from
// the top, hits ALTER TABLE ADD COLUMN on a column that already exists,
// and fails with "duplicate column name". It then fails identically on
// every subsequent start, forever, on every pod, with no recovery short
// of an operator hand-writing DDL against a live database.
//
// The state below is exactly what a crash after the fourth statement
// leaves behind: the first four columns present, the remaining six
// absent, the index absent, and the migration unrecorded. A retry must
// complete it and the store must work afterwards.
//
// Mutation-verified: reverting Up to unguarded ALTER TABLE ADD COLUMN
// fails here with "duplicate column name: req_cpu_milli".
func TestResourceMigrationSurvivesAPartialApplication(t *testing.T) {
	s, drv, db := openMigratedWithDriver(t)
	ctx := context.Background()

	// Rewind to the halfway state a crash would have left.
	for _, col := range []string{
		"req_custom_keys", "resource_requests", "resource_limits",
		"resource_class", "input_bytes", "primary_input_hash",
	} {
		mustExec(t, drv, `ALTER TABLE dispatch_jobs DROP COLUMN `+col)
	}

	mustExec(t, drv, `DROP INDEX IF EXISTS idx_dispatch_jobs_dequeue_res`)
	mustExec(t, drv, `DELETE FROM grove_migrations WHERE version = '`+resourceMigrationVersion+`'`)

	for _, col := range []string{"req_cpu_milli", "req_gpu_milli"} {
		if !hasColumn(t, drv, col) {
			t.Fatalf("fixture is wrong: %s should still be present", col)
		}
	}

	if hasColumn(t, drv, "primary_input_hash") {
		t.Fatal("fixture is wrong: primary_input_hash should have been dropped")
	}

	// The retry every restarting pod performs.
	retried := sqlitestore.New(db)
	if err := retried.Migrate(ctx); err != nil {
		t.Fatalf("re-running a half-applied migration must succeed, got: %v\n"+
			"a SQLite deployment that failed partway through this migration would be "+
			"unrecoverable without hand-written DDL", err)
	}

	for _, col := range []string{
		"req_cpu_milli", "req_memory_bytes", "req_disk_bytes", "req_gpu_milli",
		"req_custom_keys", "resource_requests", "resource_limits",
		"resource_class", "input_bytes", "primary_input_hash",
	} {
		if !hasColumn(t, drv, col) {
			t.Errorf("column %s missing after the retry", col)
		}
	}

	// And the schema is usable, not merely present.
	j := &job.Job{
		Entity:           dispatch.NewEntity(),
		ID:               id.NewJobID(),
		Name:             "after-retry",
		Queue:            "default",
		State:            job.StatePending,
		Payload:          []byte(`{}`),
		Resources:        resource.Set{resource.Memory: 8 << 30, "fpga": 1},
		PrimaryInputHash: "blake3:abc",
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob after the retry: %v", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob after the retry: %v", err)
	}

	if got.Resources[resource.Memory] != 8<<30 || got.Resources["fpga"] != 1 {
		t.Errorf("resources = %v, want the declaration intact", got.Resources)
	}
}

// TestResourceMigrationIsFullyIdempotent covers the other partial shape:
// a crash AFTER the last statement but BEFORE RecordApplied, which is a
// real window because the two are separate calls. Re-running Up with
// every column and the index already in place must be a no-op.
func TestResourceMigrationIsFullyIdempotent(t *testing.T) {
	_, drv, db := openMigratedWithDriver(t)
	ctx := context.Background()

	mustExec(t, drv, `DELETE FROM grove_migrations WHERE version = '`+resourceMigrationVersion+`'`)

	if err := sqlitestore.New(db).Migrate(ctx); err != nil {
		t.Fatalf("re-running a fully applied migration must be a no-op, got: %v", err)
	}
}

// TestResourceMigrationDropsTheRedundantDequeueIndex pins the other half
// of the change: idx_dispatch_jobs_dequeue is a strict key prefix of
// idx_dispatch_jobs_dequeue_res over the identical partial predicate, so
// shipping both means every enqueue pays two B-tree inserts and every
// claim two deletes, forever, for a plan SQLite would never choose.
func TestResourceMigrationDropsTheRedundantDequeueIndex(t *testing.T) {
	_, drv, _ := openMigratedWithDriver(t)

	rows, err := drv.Query(context.Background(),
		`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'dispatch_jobs'`)
	if err != nil {
		t.Fatalf("read sqlite_master: %v", err)
	}

	var names []string

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			t.Fatalf("scan index name: %v", err)
		}

		names = append(names, name)
	}

	if err = rows.Close(); err != nil {
		t.Fatalf("close sqlite_master rows: %v", err)
	}

	var sawSuperset bool

	for _, n := range names {
		if n == "idx_dispatch_jobs_dequeue" {
			t.Errorf("idx_dispatch_jobs_dequeue survived the resource migration; it is a key "+
				"prefix of idx_dispatch_jobs_dequeue_res, so both are maintained on every "+
				"write for no plan. Indexes present: %s", strings.Join(names, ", "))
		}

		if n == "idx_dispatch_jobs_dequeue_res" {
			sawSuperset = true
		}
	}

	if !sawSuperset {
		t.Fatalf("idx_dispatch_jobs_dequeue_res is missing; indexes present: %s",
			strings.Join(names, ", "))
	}
}

// TestLeaseMigrationSurvivesAPartialApplication is the lease migration's
// version of the proof above, and it needs its own because 008 runs
// BEFORE 009: a deployment wedged here never reaches the resource
// migration at all.
//
// Same mechanism, same consequence. SQLite has no ADD COLUMN IF NOT
// EXISTS and grove executes Up outside any transaction (Orchestrator
// calls m.Up and only then RecordApplied), so a process killed partway
// through the four ADD COLUMNs leaves some columns added and no row in
// grove_migrations. Unguarded, the next start re-runs Up from the top and
// dies on "duplicate column name" — identically, forever, on every pod.
//
// The state below is what a crash after the second statement leaves: the
// first two lease columns present, the last two absent, the index absent,
// and the migration unrecorded.
//
// Mutation-verified: reverting Up to bare ALTER TABLE ADD COLUMN fails
// here with "duplicate column name: lease_epoch".
func TestLeaseMigrationSurvivesAPartialApplication(t *testing.T) {
	s, drv, db := openMigratedWithDriver(t)
	ctx := context.Background()

	for _, col := range []string{"lease_ttl", "evict_count"} {
		mustExec(t, drv, `ALTER TABLE dispatch_jobs DROP COLUMN `+col)
	}

	mustExec(t, drv, `DROP INDEX IF EXISTS idx_dispatch_jobs_lease`)
	mustExec(t, drv, `DELETE FROM grove_migrations WHERE version = '`+leaseMigrationVersion+`'`)

	if !hasColumn(t, drv, "lease_epoch") {
		t.Fatal("fixture is wrong: lease_epoch should still be present")
	}

	if hasColumn(t, drv, "evict_count") {
		t.Fatal("fixture is wrong: evict_count should have been dropped")
	}

	// The retry every restarting pod performs.
	if err := sqlitestore.New(db).Migrate(ctx); err != nil {
		t.Fatalf("re-running a half-applied lease migration must succeed, got: %v\n"+
			"a SQLite deployment that failed partway through this migration would be "+
			"unrecoverable without hand-written DDL", err)
	}

	for _, col := range leaseColumns {
		if !hasColumn(t, drv, col) {
			t.Errorf("column %s missing after the retry", col)
		}
	}

	// And the schema is usable, not merely present.
	j := &job.Job{
		Entity:   dispatch.NewEntity(),
		ID:       id.NewJobID(),
		Name:     "after-lease-retry",
		Queue:    "default",
		State:    job.StatePending,
		Payload:  []byte(`{}`),
		LeaseTTL: 6 * time.Hour,
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
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

// TestLeaseMigrationIsFullyIdempotent covers the other partial shape: a
// crash AFTER the last statement but BEFORE RecordApplied, which is a
// real window because the two are separate calls.
func TestLeaseMigrationIsFullyIdempotent(t *testing.T) {
	_, drv, db := openMigratedWithDriver(t)

	mustExec(t, drv, `DELETE FROM grove_migrations WHERE version = '`+leaseMigrationVersion+`'`)

	if err := sqlitestore.New(db).Migrate(context.Background()); err != nil {
		t.Fatalf("re-running a fully applied lease migration must be a no-op, got: %v", err)
	}
}

// TestLeaseMigrationBackfillsRunningJobs is the regression test for jobs
// that were mid-flight when the fleet upgraded.
//
// Without the backfill those jobs are stranded permanently, and nothing
// reports it. lease_expires_at arrives NULL; ReclaimExpiredLeases
// requires a non-NULL expiry to consider a row at all (job.Lease.IsExpired
// deliberately reads a zero expiry as "never leased", not "expired"); the
// pool's reaper no longer calls ReapStaleJobs once the backend implements
// job.LeaseStore; and dequeue claims only pending and retrying rows. So a
// job that was running at the instant of the upgrade is never looked at
// by anything again — it holds its slot forever.
//
// Each case sets up one branch of the COALESCE and then asserts the
// outcome that matters: not that a column is non-NULL, but that the
// normal reclaim path actually collects the row. That is also what proves
// the backfilled text is comparable with the reclaim predicate, which no
// assertion on the column's contents could.
func TestLeaseMigrationBackfillsRunningJobs(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	older := time.Now().UTC().Add(-2 * time.Hour)

	tests := []struct {
		name string
		// heartbeat and started are written onto the running row before
		// the migration re-runs; the zero time writes NULL.
		heartbeat time.Time
		started   time.Time
		// wantSource is the column the expiry must be copied from, or ""
		// when the migration has to render "now" itself.
		wantSource string
	}{
		{
			name:       "heartbeat_at is the freshest evidence and wins",
			heartbeat:  past,
			started:    older,
			wantSource: "heartbeat_at",
		},
		{
			name:       "started_at when the job never heartbeated",
			started:    older,
			wantSource: "started_at",
		},
		{
			name: "now when the row predates both",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, drv, db := openMigratedWithDriver(t)
			ctx := context.Background()

			j := &job.Job{
				Entity:  dispatch.NewEntity(),
				ID:      id.NewJobID(),
				Name:    "mid-flight",
				Queue:   "default",
				State:   job.StatePending,
				Payload: []byte(`{}`),
			}
			if err := s.EnqueueJob(ctx, j); err != nil {
				t.Fatalf("EnqueueJob: %v", err)
			}

			// Rewind the row to what an upgrading fleet finds: a job the
			// old code left running, with no lease because leases did not
			// exist when it was claimed. The timestamps are bound as
			// time.Time so they are rendered by the same driver path the
			// store itself writes through.
			mustExec(t, drv, `
				UPDATE dispatch_jobs
				SET state = 'running', worker_id = 'w-1',
				    heartbeat_at = ?, started_at = ?, lease_expires_at = NULL
				WHERE id = ?`,
				nullableTime(tt.heartbeat), nullableTime(tt.started), j.ID.String())

			mustExec(t, drv,
				`DELETE FROM grove_migrations WHERE version = '`+leaseMigrationVersion+`'`)

			if err := sqlitestore.New(db).Migrate(ctx); err != nil {
				t.Fatalf("re-run migration: %v", err)
			}

			expiry := scanText(t, drv,
				`SELECT lease_expires_at FROM dispatch_jobs WHERE id = ?`, j.ID.String())
			if expiry == "" {
				t.Fatal("lease_expires_at is still NULL after the migration: this job is " +
					"unreclaimable forever — reclaim skips NULL expiries and dequeue " +
					"never looks at running rows")
			}

			if tt.wantSource != "" {
				want := scanText(t, drv,
					`SELECT `+tt.wantSource+` FROM dispatch_jobs WHERE id = ?`, j.ID.String())
				if expiry != want {
					t.Errorf("lease_expires_at = %q, want it copied from %s (%q)",
						expiry, tt.wantSource, want)
				}
			}

			// The outcome the backfill exists for.
			reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
			if err != nil {
				t.Fatalf("ReclaimExpiredLeases: %v", err)
			}

			if len(reclaimed) != 1 || reclaimed[0].ID != j.ID {
				t.Fatalf("reclaimed %d jobs, want the backfilled one (%s); "+
					"lease_expires_at = %q was written but the reclaim predicate "+
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
