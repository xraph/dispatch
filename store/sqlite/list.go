package sqlite

import (
	"context"
	"fmt"
	"unicode/utf8"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/paging"
	"github.com/xraph/dispatch/workflow"
)

var (
	_ job.Lister          = (*Store)(nil)
	_ workflow.PageLister = (*Store)(nil)
	_ dlq.PageLister      = (*Store)(nil)
	_ artifact.PageLister = (*Store)(nil)
)

// namePrefixWhere returns a predicate that holds when column starts with
// prefix, compared literally and case-sensitively, plus its arguments.
//
// LIKE is the obvious choice and the wrong one on SQLite: it folds ASCII
// case by default and reads % and _ as wildcards, so "a_b" would match
// "axb" and "A_b". substr with the = operator compares with the BINARY
// collation instead. substr counts characters on TEXT, not bytes, so the
// length is the prefix's rune count: len(prefix) counts bytes, and for a
// multi-byte prefix it would take too many characters to ever match.
func namePrefixWhere(column, prefix string) (where string, args []any) {
	return "substr(" + column + ", 1, ?) = ?", []any{utf8.RuneCountInString(prefix), prefix}
}

// pageCut trims a result fetched with limit+1 rows down to limit and
// returns the cursor for the following page: the ID of the last row kept,
// or empty when the extra row was not there.
func pageCut[M any](models []M, limit int, idOf func(*M) string) (kept []M, next string) {
	if len(models) <= limit {
		return models, ""
	}

	kept = models[:limit]

	return kept, idOf(&kept[limit-1])
}

// ListJobs returns jobs newest first by ID, filtered and paged.
func (s *Store) ListJobs(ctx context.Context, opts job.ListJobsOpts) (job.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixJob)
	if err != nil {
		return job.Page{}, err
	}

	limit := paging.Limit(opts.Limit)

	var models []jobModel
	q := s.sdb.NewSelect(&models)

	if len(opts.States) > 0 {
		states := make([]any, len(opts.States))
		for i, st := range opts.States {
			states[i] = string(st)
		}
		q = q.Where("state IN ("+placeholders(len(states))+")", states...)
	}
	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	if opts.NamePrefix != "" {
		where, args := namePrefixWhere("name", opts.NamePrefix)
		q = q.Where(where, args...)
	}
	if opts.ScopeAppID != "" {
		q = q.Where("scope_app_id = ?", opts.ScopeAppID)
	}
	if opts.ScopeOrgID != "" {
		q = q.Where("scope_org_id = ?", opts.ScopeOrgID)
	}
	if !cursor.IsNil() {
		q = q.Where("id < ?", cursor.String())
	}

	if scanErr := q.OrderExpr("id DESC").Limit(limit + 1).Scan(ctx); scanErr != nil {
		return job.Page{}, fmt.Errorf("dispatch/sqlite: list jobs: %w", scanErr)
	}

	models, next := pageCut(models, limit, func(m *jobModel) string { return m.ID })

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return job.Page{}, fmt.Errorf("dispatch/sqlite: list jobs convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	return job.Page{Jobs: jobs, NextCursor: next, Complete: true}, nil
}

// ListRunsPage returns workflow runs newest first by ID, filtered and paged.
func (s *Store) ListRunsPage(ctx context.Context, opts workflow.ListRunsPageOpts) (workflow.RunPage, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixRun)
	if err != nil {
		return workflow.RunPage{}, err
	}

	limit := paging.Limit(opts.Limit)

	var models []workflowRunModel
	q := s.sdb.NewSelect(&models)

	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}
	if opts.NamePrefix != "" {
		where, args := namePrefixWhere("name", opts.NamePrefix)
		q = q.Where(where, args...)
	}
	if opts.ScopeAppID != "" {
		q = q.Where("scope_app_id = ?", opts.ScopeAppID)
	}
	if opts.ScopeOrgID != "" {
		q = q.Where("scope_org_id = ?", opts.ScopeOrgID)
	}
	if !cursor.IsNil() {
		q = q.Where("id < ?", cursor.String())
	}

	if scanErr := q.OrderExpr("id DESC").Limit(limit + 1).Scan(ctx); scanErr != nil {
		return workflow.RunPage{}, fmt.Errorf("dispatch/sqlite: list runs page: %w", scanErr)
	}

	models, next := pageCut(models, limit, func(m *workflowRunModel) string { return m.ID })

	runs := make([]*workflow.Run, 0, len(models))
	for i := range models {
		r, convErr := fromRunModel(&models[i])
		if convErr != nil {
			return workflow.RunPage{}, fmt.Errorf("dispatch/sqlite: list runs page convert: %w", convErr)
		}
		runs = append(runs, r)
	}

	return workflow.RunPage{Runs: runs, NextCursor: next, Complete: true}, nil
}

// CountRuns counts workflow runs by state and exact name.
func (s *Store) CountRuns(ctx context.Context, opts workflow.CountRunsOpts) (int64, error) {
	q := s.sdb.NewSelect((*workflowRunModel)(nil))

	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}
	if opts.Name != "" {
		q = q.Where("name = ?", opts.Name)
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: count runs: %w", err)
	}

	return count, nil
}

// ListDLQPage returns dead letter entries newest first by ID, filtered and
// paged.
func (s *Store) ListDLQPage(ctx context.Context, opts dlq.PageOpts) (dlq.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixDLQ)
	if err != nil {
		return dlq.Page{}, err
	}

	limit := paging.Limit(opts.Limit)

	var models []dlqEntryModel
	q := s.sdb.NewSelect(&models)

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	if opts.NamePrefix != "" {
		where, args := namePrefixWhere("job_name", opts.NamePrefix)
		q = q.Where(where, args...)
	}
	if opts.ScopeAppID != "" {
		q = q.Where("scope_app_id = ?", opts.ScopeAppID)
	}
	if opts.ScopeOrgID != "" {
		q = q.Where("scope_org_id = ?", opts.ScopeOrgID)
	}
	if opts.Replayed != nil {
		q = q.Where(replayedWhere(*opts.Replayed))
	}
	if !cursor.IsNil() {
		q = q.Where("id < ?", cursor.String())
	}

	if scanErr := q.OrderExpr("id DESC").Limit(limit + 1).Scan(ctx); scanErr != nil {
		return dlq.Page{}, fmt.Errorf("dispatch/sqlite: list dlq page: %w", scanErr)
	}

	models, next := pageCut(models, limit, func(m *dlqEntryModel) string { return m.ID })

	entries := make([]*dlq.Entry, 0, len(models))
	for i := range models {
		e, convErr := fromDLQModel(&models[i])
		if convErr != nil {
			return dlq.Page{}, fmt.Errorf("dispatch/sqlite: list dlq page convert: %w", convErr)
		}
		entries = append(entries, e)
	}

	return dlq.Page{Entries: entries, NextCursor: next, Complete: true}, nil
}

// CountDLQEntries counts dead letter entries under the given filters.
// FailedBefore is bound as a time.Time, exactly as PurgeDLQ binds it, so
// the driver writes both sides of failed_at < ? in the same UTC text form
// and the count matches what a purge at that time would delete.
func (s *Store) CountDLQEntries(ctx context.Context, opts dlq.CountOpts) (int64, error) {
	q := s.sdb.NewSelect((*dlqEntryModel)(nil))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	if opts.Replayed != nil {
		q = q.Where(replayedWhere(*opts.Replayed))
	}
	if !opts.FailedBefore.IsZero() {
		q = q.Where("failed_at < ?", opts.FailedBefore)
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: count dlq entries: %w", err)
	}

	return count, nil
}

// replayedWhere is the predicate for a dlq Replayed filter.
func replayedWhere(replayed bool) string {
	if replayed {
		return "replayed_at IS NOT NULL"
	}

	return "replayed_at IS NULL"
}

// ListArtifactsPage returns artifacts newest first by ID, filtered and
// paged.
func (s *Store) ListArtifactsPage(ctx context.Context, opts artifact.PageOpts) (artifact.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixArtifact)
	if err != nil {
		return artifact.Page{}, err
	}

	limit := paging.Limit(opts.Limit)

	var models []artifactModel
	q := s.sdb.NewSelect(&models)

	if !opts.IncludeDeleted {
		q = q.Where("deleted_at IS NULL")
	}
	if opts.Lifecycle != "" {
		q = q.Where("lifecycle = ?", string(opts.Lifecycle))
	}
	if opts.ScopeAppID != "" {
		q = q.Where("scope_app_id = ?", opts.ScopeAppID)
	}
	if opts.ScopeOrgID != "" {
		q = q.Where("scope_org_id = ?", opts.ScopeOrgID)
	}
	if !cursor.IsNil() {
		q = q.Where("id < ?", cursor.String())
	}

	if scanErr := q.OrderExpr("id DESC").Limit(limit + 1).Scan(ctx); scanErr != nil {
		return artifact.Page{}, fmt.Errorf("dispatch/sqlite: list artifacts page: %w", scanErr)
	}

	models, next := pageCut(models, limit, func(m *artifactModel) string { return m.ID })

	arts, err := fromArtifactModels(models)
	if err != nil {
		return artifact.Page{}, fmt.Errorf("dispatch/sqlite: list artifacts page convert: %w", err)
	}

	return artifact.Page{Artifacts: arts, NextCursor: next, Complete: true}, nil
}
