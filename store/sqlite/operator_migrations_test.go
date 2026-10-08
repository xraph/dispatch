package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"

	sqlitestore "github.com/xraph/dispatch/store/sqlite"
)

// operatorSchema is what the two operator-action migrations add: columns
// and an index on one table per migration, in the order the migrations
// run.
var operatorSchema = []struct {
	migration, version string
	table              string
	columns            []string
	index              string
	indexColumns       []string
}{
	{
		migration: "dlq_replayed_job_id", version: "20261009120000",
		table: "dispatch_dlq", columns: []string{"replayed_job_id"},
		index: "idx_dispatch_dlq_job_id", indexColumns: []string{"job_id", "id"},
	},
	{
		migration: "workflow_run_version_parent", version: "20261009130000",
		table: "dispatch_workflow_runs", columns: []string{"version", "parent_run_id"},
		index: "idx_dispatch_workflow_runs_parent", indexColumns: []string{"parent_run_id", "id"},
	},
}

// assertOperatorSchema checks every column and index in operatorSchema is
// present (want true) or absent (want false).
func assertOperatorSchema(t *testing.T, drv driver.Driver, want bool) {
	t.Helper()

	for _, s := range operatorSchema {
		for _, column := range s.columns {
			found := queryStrings(t, drv,
				`SELECT name FROM pragma_table_info(?) WHERE name = ?`, s.table, column)
			if got := len(found) == 1; got != want {
				t.Errorf("%s.%s present = %v, want %v", s.table, column, got, want)
			}
		}

		owner := queryStrings(t, drv,
			`SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name = ?`, s.index)
		if !want {
			if len(owner) != 0 {
				t.Errorf("%s still exists on %v", s.index, owner)
			}

			continue
		}
		if len(owner) != 1 || owner[0][0] != s.table {
			t.Errorf("%s: sqlite_master has %v, want one index on %s", s.index, owner, s.table)

			continue
		}

		var keys []string
		for _, r := range queryStrings(t, drv,
			`SELECT name FROM pragma_index_info(?) ORDER BY seqno`, s.index) {
			keys = append(keys, r[0])
		}
		if strings.Join(keys, ",") != strings.Join(s.indexColumns, ",") {
			t.Errorf("%s: columns %v, want %v", s.index, keys, s.indexColumns)
		}
	}
}

func TestOperatorMigrationsAddColumnsAndIndexes(t *testing.T) {
	_, drv, _ := openMigratedWithDriver(t)

	assertOperatorSchema(t, drv, true)
}

// TestOperatorMigrationsSurviveASecondMigrate runs Migrate again on a
// migrated database, as every process start does.
func TestOperatorMigrationsSurviveASecondMigrate(t *testing.T) {
	s, drv, _ := openMigratedWithDriver(t)

	for range 2 {
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatalf("migrate again: %v", err)
		}
	}

	assertOperatorSchema(t, drv, true)
}

// TestOperatorMigrationsRollBackAndReapply rolls both migrations back
// through grove, runs each Down a second time to prove it is guarded (a
// Down that failed halfway must be re-runnable, and SQLite refuses to drop
// a column an index still covers), then migrates forward again.
func TestOperatorMigrationsRollBackAndReapply(t *testing.T) {
	s, drv, _ := openMigratedWithDriver(t)
	ctx := context.Background()

	exec, err := migrate.NewExecutorFor(drv)
	if err != nil {
		t.Fatalf("NewExecutorFor: %v", err)
	}
	orch := migrate.NewOrchestrator(exec, sqlitestore.Migrations)

	// Rollback undoes the most recent migration first.
	for i := len(operatorSchema) - 1; i >= 0; i-- {
		res, rbErr := orch.Rollback(ctx)
		if rbErr != nil {
			t.Fatalf("rollback: %v", rbErr)
		}
		undone := make([]string, 0, len(res.Rollback))
		for _, m := range res.Rollback {
			undone = append(undone, m.Name)
		}
		if len(undone) != 1 || undone[0] != operatorSchema[i].migration {
			t.Fatalf("rollback undid %v, want [%s]", undone, operatorSchema[i].migration)
		}
	}

	assertOperatorSchema(t, drv, false)

	byVersion := map[string]*migrate.Migration{}
	for _, m := range sqlitestore.Migrations.Migrations() {
		byVersion[m.Version] = m
	}
	for _, sc := range operatorSchema {
		if downErr := byVersion[sc.version].Down(ctx, exec); downErr != nil {
			t.Fatalf("second Down of %s: %v", sc.migration, downErr)
		}
	}

	if err = s.Migrate(ctx); err != nil {
		t.Fatalf("migrate after rollback: %v", err)
	}

	assertOperatorSchema(t, drv, true)
}
