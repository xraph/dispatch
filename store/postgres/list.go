package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/drivers/pgdriver"

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

// pageQuery finishes a keyset page: rows strictly below the cursor, newest
// first by ID, and one row past the limit so the caller can tell whether
// another page exists without a second query. The primary key index
// serves both the predicate and the ordering.
func pageQuery(q *pgdriver.SelectQuery, cursor id.ID, limit int) *pgdriver.SelectQuery {
	if !cursor.IsNil() {
		q = q.Where("id < ?", cursor.String())
	}

	return q.OrderExpr("id DESC").Limit(limit + 1)
}

// cutPage drops the extra row pageQuery fetched. When there was one, the
// next cursor is the ID of the last row kept.
func cutPage[M any](models []M, limit int, idOf func(*M) string) (page []M, next string) {
	if len(models) <= limit {
		return models, ""
	}

	return models[:limit], idOf(&models[limit-1])
}

// whereNamePrefix filters col to values that start with prefix, taken
// literally and case-sensitively. starts_with does no pattern matching,
// so '%' and '_' in a name are ordinary characters and need no escaping.
func whereNamePrefix(q *pgdriver.SelectQuery, col, prefix string) *pgdriver.SelectQuery {
	if prefix == "" {
		return q
	}

	return q.Where("starts_with("+col+", ?)", prefix)
}

// whereScope filters on the tenant columns. An empty value is no filter,
// so an empty scope lists every tenant.
func whereScope(q *pgdriver.SelectQuery, appID, orgID string) *pgdriver.SelectQuery {
	if appID != "" {
		q = q.Where("scope_app_id = ?", appID)
	}
	if orgID != "" {
		q = q.Where("scope_org_id = ?", orgID)
	}

	return q
}

// whereReplayed filters dead letters on whether anyone has replayed them.
// Nil is no filter.
func whereReplayed(q *pgdriver.SelectQuery, replayed *bool) *pgdriver.SelectQuery {
	switch {
	case replayed == nil:
		return q
	case *replayed:
		return q.Where("replayed_at IS NOT NULL")
	default:
		return q.Where("replayed_at IS NULL")
	}
}

// ListJobs returns jobs newest first by ID, filtered and paged.
func (s *Store) ListJobs(ctx context.Context, opts job.ListJobsOpts) (job.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixJob)
	if err != nil {
		return job.Page{}, err
	}
	limit := paging.Limit(opts.Limit)

	var models []jobModel
	q := s.pgdb.NewSelect(&models)

	if len(opts.States) > 0 {
		states := make([]string, len(opts.States))
		for i, st := range opts.States {
			states[i] = string(st)
		}
		q = q.Where("state = ANY(?)", states)
	}
	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	q = whereNamePrefix(q, "name", opts.NamePrefix)
	q = whereScope(q, opts.ScopeAppID, opts.ScopeOrgID)

	if scanErr := pageQuery(q, cursor, limit).Scan(ctx); scanErr != nil {
		return job.Page{}, fmt.Errorf(errPrefix+"list jobs page: %w", scanErr)
	}

	models, next := cutPage(models, limit, func(m *jobModel) string { return m.ID })

	jobs := make([]*job.Job, 0, len(models))
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return job.Page{}, fmt.Errorf(errPrefix+"list jobs page convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	return job.Page{Jobs: jobs, NextCursor: next, Complete: true}, nil
}

// ListRunsPage returns workflow runs newest first by ID, filtered and
// paged.
func (s *Store) ListRunsPage(ctx context.Context, opts workflow.ListRunsPageOpts) (workflow.RunPage, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixRun)
	if err != nil {
		return workflow.RunPage{}, err
	}
	limit := paging.Limit(opts.Limit)

	var models []workflowRunModel
	q := s.pgdb.NewSelect(&models)

	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}
	q = whereNamePrefix(q, "name", opts.NamePrefix)
	q = whereScope(q, opts.ScopeAppID, opts.ScopeOrgID)

	if scanErr := pageQuery(q, cursor, limit).Scan(ctx); scanErr != nil {
		return workflow.RunPage{}, fmt.Errorf(errPrefix+"list runs page: %w", scanErr)
	}

	models, next := cutPage(models, limit, func(m *workflowRunModel) string { return m.ID })

	runs := make([]*workflow.Run, 0, len(models))
	for i := range models {
		r, convErr := fromRunModel(&models[i])
		if convErr != nil {
			return workflow.RunPage{}, fmt.Errorf(errPrefix+"list runs page convert: %w", convErr)
		}
		runs = append(runs, r)
	}

	return workflow.RunPage{Runs: runs, NextCursor: next, Complete: true}, nil
}

// CountRuns counts workflow runs by state and exact name.
func (s *Store) CountRuns(ctx context.Context, opts workflow.CountRunsOpts) (int64, error) {
	q := s.pgdb.NewSelect((*workflowRunModel)(nil))

	if opts.State != "" {
		q = q.Where("state = ?", string(opts.State))
	}
	if opts.Name != "" {
		q = q.Where("name = ?", opts.Name)
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf(errPrefix+"count runs: %w", err)
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
	q := s.pgdb.NewSelect(&models)

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	q = whereNamePrefix(q, "job_name", opts.NamePrefix)
	q = whereScope(q, opts.ScopeAppID, opts.ScopeOrgID)
	q = whereReplayed(q, opts.Replayed)

	if scanErr := pageQuery(q, cursor, limit).Scan(ctx); scanErr != nil {
		return dlq.Page{}, fmt.Errorf(errPrefix+"list dlq page: %w", scanErr)
	}

	models, next := cutPage(models, limit, func(m *dlqEntryModel) string { return m.ID })

	entries := make([]*dlq.Entry, 0, len(models))
	for i := range models {
		e, convErr := fromDLQModel(&models[i])
		if convErr != nil {
			return dlq.Page{}, fmt.Errorf(errPrefix+"list dlq page convert: %w", convErr)
		}
		entries = append(entries, e)
	}

	return dlq.Page{Entries: entries, NextCursor: next, Complete: true}, nil
}

// CountDLQEntries counts dead letter entries under the given filters. A
// set FailedBefore counts entries that failed strictly before it, the same
// boundary PurgeDLQ deletes on.
func (s *Store) CountDLQEntries(ctx context.Context, opts dlq.CountOpts) (int64, error) {
	q := s.pgdb.NewSelect((*dlqEntryModel)(nil))

	if opts.Queue != "" {
		q = q.Where("queue = ?", opts.Queue)
	}
	q = whereReplayed(q, opts.Replayed)
	if !opts.FailedBefore.IsZero() {
		q = q.Where("failed_at < ?", opts.FailedBefore)
	}

	count, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf(errPrefix+"count dlq entries: %w", err)
	}

	return count, nil
}

// ListArtifactsPage returns artifacts newest first by ID, filtered and
// paged. Soft-deleted artifacts are left out unless IncludeDeleted is set.
func (s *Store) ListArtifactsPage(ctx context.Context, opts artifact.PageOpts) (artifact.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixArtifact)
	if err != nil {
		return artifact.Page{}, err
	}
	limit := paging.Limit(opts.Limit)

	var models []artifactModel
	q := s.pgdb.NewSelect(&models)

	if !opts.IncludeDeleted {
		q = q.Where("deleted_at IS NULL")
	}
	if opts.Lifecycle != "" {
		q = q.Where("lifecycle = ?", string(opts.Lifecycle))
	}
	q = whereScope(q, opts.ScopeAppID, opts.ScopeOrgID)

	if scanErr := pageQuery(q, cursor, limit).Scan(ctx); scanErr != nil {
		return artifact.Page{}, fmt.Errorf(errPrefix+"list artifacts page: %w", scanErr)
	}

	models, next := cutPage(models, limit, func(m *artifactModel) string { return m.ID })

	arts, err := fromArtifactModels(models)
	if err != nil {
		return artifact.Page{}, fmt.Errorf(errPrefix+"list artifacts page convert: %w", err)
	}

	return artifact.Page{Artifacts: arts, NextCursor: next, Complete: true}, nil
}
