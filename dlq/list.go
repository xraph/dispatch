package dlq

import (
	"context"
	"strings"
	"time"
)

// PageOpts filters and pages ListDLQPage, newest first by ID. NamePrefix
// matches the entry's JobName. Replayed nil lists both; a pointer to false
// lists only entries nobody has replayed yet, which is the dashboard's
// default view.
type PageOpts struct {
	Queue      string
	NamePrefix string
	ScopeAppID string
	ScopeOrgID string
	Replayed   *bool
	Cursor     string
	Limit      int
}

// Page is one page of ListDLQPage. NextCursor and Complete mean what they
// mean on job.Page.
type Page struct {
	Entries    []*Entry
	NextCursor string
	Complete   bool
}

// CountOpts filters CountDLQEntries. A zero FailedBefore counts every
// entry; a set one counts entries whose FailedAt is strictly before it,
// the same boundary PurgeDLQ deletes on, so a purge confirmation can show
// exactly how many rows will go.
type CountOpts struct {
	Queue        string
	Replayed     *bool
	FailedBefore time.Time
}

// PageLister is the paged DLQ read and the filtered count the dashboard
// uses. ListDLQ and CountDLQ are kept, unchanged, for existing callers.
type PageLister interface {
	ListDLQPage(ctx context.Context, opts PageOpts) (Page, error)
	CountDLQEntries(ctx context.Context, opts CountOpts) (int64, error)
}

// Match reports whether e passes every filter in o.
func (o PageOpts) Match(e *Entry) bool {
	if o.Queue != "" && e.Queue != o.Queue {
		return false
	}
	if o.NamePrefix != "" && !strings.HasPrefix(e.JobName, o.NamePrefix) {
		return false
	}
	if o.ScopeAppID != "" && e.ScopeAppID != o.ScopeAppID {
		return false
	}
	if o.ScopeOrgID != "" && e.ScopeOrgID != o.ScopeOrgID {
		return false
	}
	if o.Replayed != nil && *o.Replayed != (e.ReplayedAt != nil) {
		return false
	}

	return true
}

// Match reports whether e is counted under o.
func (o CountOpts) Match(e *Entry) bool {
	if o.Queue != "" && e.Queue != o.Queue {
		return false
	}
	if o.Replayed != nil && *o.Replayed != (e.ReplayedAt != nil) {
		return false
	}
	if !o.FailedBefore.IsZero() && !e.FailedAt.Before(o.FailedBefore) {
		return false
	}

	return true
}
