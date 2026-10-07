//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/hook"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/postgres"
	"github.com/xraph/dispatch/store/storetest"
	"github.com/xraph/dispatch/workflow"
)

// listIndex is one index migration list_order_indexes builds, with the
// tail of the definition Postgres reports for it in pg_indexes.
type listIndex struct {
	name, table, def string
}

var listIndexes = []listIndex{
	{"idx_dispatch_jobs_list_state", "dispatch_jobs", "USING btree (state, id)"},
	{"idx_dispatch_jobs_list_queue", "dispatch_jobs", "USING btree (queue, id)"},
	{"idx_dispatch_dlq_list_queue", "dispatch_dlq", "USING btree (queue, id)"},
	{"idx_dispatch_workflow_runs_list_state", "dispatch_workflow_runs", "USING btree (state, id)"},
	{"idx_dispatch_artifacts_list_scope", "dispatch_artifacts", "USING btree (scope_app_id, id)"},
}

// assertListIndexes checks every list index exists on the right table
// with the right columns and is valid. Valid matters because the indexes
// are built CONCURRENTLY, and a failed concurrent build leaves an index
// the planner ignores.
func assertListIndexes(t *testing.T, s *postgres.Store) {
	t.Helper()

	ctx := context.Background()

	conn, err := pgdriver.Unwrap(s.DB()).AcquireConn(ctx)
	if err != nil {
		t.Fatalf("acquire dedicated conn: %v", err)
	}

	defer conn.Release()

	for _, ix := range listIndexes {
		var table, def string

		err = conn.QueryRow(ctx,
			`SELECT tablename, indexdef FROM pg_indexes WHERE indexname = $1`, ix.name,
		).Scan(&table, &def)
		if err != nil {
			t.Errorf("%s: not in pg_indexes: %v", ix.name, err)

			continue
		}

		if table != ix.table || !strings.HasSuffix(def, ix.def) {
			t.Errorf("%s: on %s as %q, want on %s ending %q", ix.name, table, def, ix.table, ix.def)
		}

		if _, valid := indexIsValid(t, conn, ix.name); !valid {
			t.Errorf("%s: INVALID, so the planner ignores it", ix.name)
		}
	}
}

func TestListIndexesExistAfterMigrate(t *testing.T) {
	s := setupTestStore(t)

	assertListIndexes(t, s)
}

// TestListIndexesSurviveASecondMigrate runs the whole group again the
// way every pod start does. Each index is dropped only when INVALID and
// created with IF NOT EXISTS, so a second run must change nothing.
func TestListIndexesSurviveASecondMigrate(t *testing.T) {
	s := setupTestStore(t)

	remigrate(t, s)
	remigrate(t, s)

	assertListIndexes(t, s)
}

// selectCapture records the last SELECT grove built for each table, as
// sent: the SQL with its placeholders and the bound arguments. grove sets
// RawQuery and RawArgs before it runs post-query hooks.
type selectCapture struct {
	mu   sync.Mutex
	last map[string]capturedSelect
}

type capturedSelect struct {
	query string
	args  []any
}

func (c *selectCapture) AfterQuery(_ context.Context, qc *hook.QueryContext, _ any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.last[qc.Table] = capturedSelect{query: qc.RawQuery, args: qc.RawArgs}

	return nil
}

func (c *selectCapture) take(t *testing.T, table string) capturedSelect {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	got, ok := c.last[table]
	if !ok {
		t.Fatalf("no SELECT on %s was captured", table)
	}

	delete(c.last, table)

	return got
}

// explain returns Postgres's plan for one captured statement.
//
// The three planner switches leave it one question: which index reads
// the filtered rows already in id order. With sequential scans, bitmap
// scans and explicit sorts priced out, the candidates are the primary
// key walked backwards with the filter applied row by row, or a list
// index walked backwards with the filter as its index condition. That
// keeps the test independent of table size and statistics, which on a
// fresh container are empty. SET LOCAL ends with the transaction, so
// nothing leaks back into the pool.
func explain(t *testing.T, s *postgres.Store, q capturedSelect) string {
	t.Helper()

	ctx := context.Background()

	tx, err := pgdriver.Unwrap(s.DB()).BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	defer func() { _ = tx.Rollback() }()

	for _, set := range []string{
		`SET LOCAL enable_seqscan = off`,
		`SET LOCAL enable_bitmapscan = off`,
		`SET LOCAL enable_sort = off`,
	} {
		if _, err = tx.Exec(ctx, set); err != nil {
			t.Fatalf("%s: %v", set, err)
		}
	}

	rows, err := tx.Query(ctx, `EXPLAIN `+q.query, q.args...)
	if err != nil {
		t.Fatalf("EXPLAIN %s: %v", q.query, err)
	}

	defer rows.Close()

	var plan []string

	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			t.Fatalf("scan plan line: %v", err)
		}

		plan = append(plan, line)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("read plan: %v", err)
	}

	return strings.Join(plan, "\n")
}

// TestFilteredListsReadInIndexOrder runs each paged list with one filter,
// captures the statement the store actually sent, and checks Postgres
// would answer it by walking that filter's list index backwards: the
// filter becomes the index condition and the rows come out newest first
// with no sort.
//
// Capturing rather than restating the SQL is the point. ListJobs used to
// send one state as state = ANY($1), and Postgres cannot read a
// (state, id) index in id order for an array, even a one-element one. It
// walked the primary key instead, the plan this index exists to replace.
func TestFilteredListsReadInIndexOrder(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	capture := &selectCapture{last: map[string]capturedSelect{}}
	s.DB().Hooks().AddHook(capture, hook.Scope{Operations: []hook.Operation{hook.OpSelect}})

	cases := []struct {
		index string
		table string
		list  func() error
	}{
		{"idx_dispatch_jobs_list_state", "dispatch_jobs", func() error {
			_, err := s.ListJobs(ctx, job.ListJobsOpts{States: []job.State{job.StateFailed}})
			return err
		}},
		{"idx_dispatch_jobs_list_queue", "dispatch_jobs", func() error {
			_, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: "reports"})
			return err
		}},
		{"idx_dispatch_dlq_list_queue", "dispatch_dlq", func() error {
			_, err := s.ListDLQPage(ctx, dlq.PageOpts{Queue: "reports"})
			return err
		}},
		{"idx_dispatch_workflow_runs_list_state", "dispatch_workflow_runs", func() error {
			_, err := s.ListRunsPage(ctx, workflow.ListRunsPageOpts{State: workflow.RunStateFailed})
			return err
		}},
		{"idx_dispatch_artifacts_list_scope", "dispatch_artifacts", func() error {
			_, err := s.ListArtifactsPage(ctx, artifact.PageOpts{ScopeAppID: "app-1"})
			return err
		}},
	}

	for _, c := range cases {
		t.Run(c.index, func(t *testing.T) {
			if err := c.list(); err != nil {
				t.Fatalf("list: %v", err)
			}

			plan := explain(t, s, capture.take(t, c.table))

			if !strings.Contains(plan, "Index Scan Backward using "+c.index+" ") {
				t.Errorf("plan does not walk %s backwards:\n%s", c.index, plan)
			}
		})
	}
}

// TestListJobsOneStateMatchesOnlyThatState covers the equality branch
// ListJobs takes for exactly one state. The shared list suite filters on
// two states, which still goes through ANY.
func TestListJobsOneStateMatchesOnlyThatState(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const queue = "one-state"

	var failed []string

	for i, st := range []job.State{
		job.StateFailed, job.StatePending, job.StateFailed, job.StateRetrying, job.StateFailed,
	} {
		j := storetest.PendingJob(fmt.Sprintf("one-state-%d", i), queue, 0)
		j.State = st

		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}

		if st == job.StateFailed {
			failed = append(failed, j.ID.String())
		}
	}

	page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: queue, States: []job.State{job.StateFailed}})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}

	got := make([]string, len(page.Jobs))
	for i, j := range page.Jobs {
		got[i] = j.ID.String()
	}

	slices.Reverse(failed)

	if !slices.Equal(got, failed) {
		t.Fatalf("one state = %v, want the failed jobs newest first %v", got, failed)
	}
}
