package artifact

import "context"

// PageOpts filters and pages ListArtifactsPage, newest first by ID. The
// filters mean what they mean on ListOpts; only the paging differs.
type PageOpts struct {
	Lifecycle      Lifecycle
	ScopeAppID     string
	ScopeOrgID     string
	IncludeDeleted bool
	Cursor         string
	Limit          int
}

// Page is one page of ListArtifactsPage. NextCursor and Complete mean what
// they mean on job.Page.
type Page struct {
	Artifacts  []*Artifact
	NextCursor string
	Complete   bool
}

// PageLister is the cursor-paged artifact read. ListArtifacts is kept for
// existing callers.
type PageLister interface {
	ListArtifactsPage(ctx context.Context, opts PageOpts) (Page, error)
}

// Match reports whether a passes every filter in o.
func (o PageOpts) Match(a *Artifact) bool {
	if a.DeletedAt != nil && !o.IncludeDeleted {
		return false
	}
	if o.Lifecycle != "" && a.Lifecycle != o.Lifecycle {
		return false
	}
	if o.ScopeAppID != "" && a.ScopeAppID != o.ScopeAppID {
		return false
	}
	if o.ScopeOrgID != "" && a.ScopeOrgID != o.ScopeOrgID {
		return false
	}

	return true
}
