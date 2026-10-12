package workflow

import (
	"context"
	"strings"
)

// ListRunsPageOpts filters and pages ListRunsPage. The rules are the same
// as job.ListJobsOpts: exact matches except NamePrefix, empty means no
// filter, newest first by ID.
type ListRunsPageOpts struct {
	State      RunState
	NamePrefix string
	ScopeAppID string
	ScopeOrgID string
	Cursor     string
	Limit      int
}

// RunPage is one page of ListRunsPage. NextCursor and Complete mean what
// they mean on job.Page.
type RunPage struct {
	Runs       []*Run
	NextCursor string
	Complete   bool
}

// CountRunsOpts filters CountRuns. Name is an exact workflow name; empty
// State or Name counts every value.
type CountRunsOpts struct {
	State RunState
	Name  string
}

// PageLister is the paged run read and the run count the dashboard uses.
// ListRuns is kept, unchanged, for the engine's own callers.
type PageLister interface {
	ListRunsPage(ctx context.Context, opts ListRunsPageOpts) (RunPage, error)
	CountRuns(ctx context.Context, opts CountRunsOpts) (int64, error)
}

// Match reports whether r passes every filter in o.
func (o ListRunsPageOpts) Match(r *Run) bool {
	if o.State != "" && r.State != o.State {
		return false
	}
	if o.NamePrefix != "" && !strings.HasPrefix(r.Name, o.NamePrefix) {
		return false
	}
	if o.ScopeAppID != "" && r.ScopeAppID != o.ScopeAppID {
		return false
	}
	if o.ScopeOrgID != "" && r.ScopeOrgID != o.ScopeOrgID {
		return false
	}

	return true
}

// Match reports whether r is counted under o.
func (o CountRunsOpts) Match(r *Run) bool {
	if o.State != "" && r.State != o.State {
		return false
	}
	if o.Name != "" && r.Name != o.Name {
		return false
	}

	return true
}
