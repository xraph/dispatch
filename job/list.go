package job

import (
	"context"
	"slices"
	"strings"
)

// ListJobsOpts filters and pages ListJobs.
//
// Every string filter is an exact match except NamePrefix, which is a
// case-sensitive literal prefix. An empty value means "no filter": an
// empty States lists every state, and an empty ScopeAppID lists every
// app. The second is pinned by the conformance suite because it is the
// dangerous one: the dashboard is operator-wide on purpose, and a caller
// that meant to scope must pass the scope.
type ListJobsOpts struct {
	States     []State
	Queue      string
	NamePrefix string
	ScopeAppID string
	ScopeOrgID string

	// Cursor is the ID of the last job on the previous page. Empty is the
	// first page. See package paging for the ordering contract.
	Cursor string

	// Limit is the page size; zero or less means paging.DefaultLimit.
	Limit int
}

// Page is one page of ListJobs, newest first.
type Page struct {
	Jobs []*Job

	// NextCursor is the cursor for the following page, or empty when there
	// is none. It is always set when Complete is false.
	NextCursor string

	// Complete is false when a backend stopped scanning at its budget
	// before it could fill the page or prove there was nothing left. The
	// page is then not evidence that no more rows match: continue from
	// NextCursor. Backends that filter at their index always report true.
	Complete bool
}

// Lister is the paged, filtered job read the dashboard uses. It is a
// separate interface from Store so backends could gain it one at a time;
// store.Store embeds it once all five have.
type Lister interface {
	ListJobs(ctx context.Context, opts ListJobsOpts) (Page, error)
}

// Match reports whether j passes every filter in o. The cursor and limit
// are not filters and are ignored. Backends that filter in Go share this
// so the predicate cannot drift between them.
func (o ListJobsOpts) Match(j *Job) bool {
	if len(o.States) > 0 && !slices.Contains(o.States, j.State) {
		return false
	}
	if o.Queue != "" && j.Queue != o.Queue {
		return false
	}
	if o.NamePrefix != "" && !strings.HasPrefix(j.Name, o.NamePrefix) {
		return false
	}
	if o.ScopeAppID != "" && j.ScopeAppID != o.ScopeAppID {
		return false
	}
	if o.ScopeOrgID != "" && j.ScopeOrgID != o.ScopeOrgID {
		return false
	}

	return true
}
