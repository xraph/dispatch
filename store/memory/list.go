package memory

import (
	"context"
	"sort"

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

// pageByID orders rows newest first by ID, drops every row at or above the
// cursor, and cuts the page. It is the reference the other backends are
// held to by the list suite.
func pageByID[T any](rows []T, idOf func(T) string, cursor id.ID, limit int) (page []T, next string) {
	sort.Slice(rows, func(i, k int) bool { return idOf(rows[i]) > idOf(rows[k]) })

	if !cursor.IsNil() {
		c := cursor.String()
		start := sort.Search(len(rows), func(i int) bool { return idOf(rows[i]) < c })
		rows = rows[start:]
	}

	limit = paging.Limit(limit)
	if len(rows) <= limit {
		return rows, ""
	}

	return rows[:limit], idOf(rows[limit-1])
}

// ListJobs returns jobs newest first by ID, filtered and paged.
func (m *Store) ListJobs(_ context.Context, opts job.ListJobsOpts) (job.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixJob)
	if err != nil {
		return job.Page{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	rows := make([]*job.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if opts.Match(j) {
			rows = append(rows, cloneJob(j))
		}
	}

	rows, next := pageByID(rows, func(j *job.Job) string { return j.ID.String() }, cursor, opts.Limit)

	return job.Page{Jobs: rows, NextCursor: next, Complete: true}, nil
}

// ListRunsPage returns workflow runs newest first by ID, filtered and paged.
func (m *Store) ListRunsPage(_ context.Context, opts workflow.ListRunsPageOpts) (workflow.RunPage, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixRun)
	if err != nil {
		return workflow.RunPage{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	rows := make([]*workflow.Run, 0, len(m.runs))
	for _, r := range m.runs {
		if opts.Match(r) {
			cp := *r
			rows = append(rows, &cp)
		}
	}

	rows, next := pageByID(rows, func(r *workflow.Run) string { return r.ID.String() }, cursor, opts.Limit)

	return workflow.RunPage{Runs: rows, NextCursor: next, Complete: true}, nil
}

// CountRuns counts workflow runs by state and exact name.
func (m *Store) CountRuns(_ context.Context, opts workflow.CountRunsOpts) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var n int64
	for _, r := range m.runs {
		if opts.Match(r) {
			n++
		}
	}

	return n, nil
}

// ListDLQPage returns dead letter entries newest first by ID, filtered and
// paged.
func (m *Store) ListDLQPage(_ context.Context, opts dlq.PageOpts) (dlq.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixDLQ)
	if err != nil {
		return dlq.Page{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	rows := make([]*dlq.Entry, 0, len(m.dlqs))
	for _, e := range m.dlqs {
		if opts.Match(e) {
			cp := *e
			rows = append(rows, &cp)
		}
	}

	rows, next := pageByID(rows, func(e *dlq.Entry) string { return e.ID.String() }, cursor, opts.Limit)

	return dlq.Page{Entries: rows, NextCursor: next, Complete: true}, nil
}

// CountDLQEntries counts dead letter entries under the given filters.
func (m *Store) CountDLQEntries(_ context.Context, opts dlq.CountOpts) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var n int64
	for _, e := range m.dlqs {
		if opts.Match(e) {
			n++
		}
	}

	return n, nil
}

// ListArtifactsPage returns artifacts newest first by ID, filtered and
// paged.
func (m *Store) ListArtifactsPage(_ context.Context, opts artifact.PageOpts) (artifact.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixArtifact)
	if err != nil {
		return artifact.Page{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	rows := make([]*artifact.Artifact, 0, len(m.artifacts))
	for _, a := range m.artifacts {
		if opts.Match(a) {
			rows = append(rows, a.Clone())
		}
	}

	rows, next := pageByID(rows, func(a *artifact.Artifact) string { return a.ID.String() }, cursor, opts.Limit)

	return artifact.Page{Artifacts: rows, NextCursor: next, Complete: true}, nil
}
