package mongo

import (
	"context"
	"fmt"
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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

// namePrefix matches strings that start with prefix, read literally and
// case-sensitively. QuoteMeta escapes every regex metacharacter, so a
// name like "a.b" or "a(b" is a prefix and not a pattern. An anchored,
// case-sensitive regex is also the only kind Mongo can turn into an index
// range, should the field ever be indexed.
func namePrefix(prefix string) bson.M {
	return bson.M{"$regex": "^" + regexp.QuoteMeta(prefix)}
}

// findPage reads one page of col newest first by _id. Every dispatch _id
// is a TypeID string whose suffix sorts in creation order, and Mongo
// compares strings bytewise, so _id descending is newest first. A row is
// after the cursor exactly when its _id sorts strictly below it, which
// holds whether or not the cursor row still exists.
//
// It asks for one row more than the page so it can tell whether another
// page follows without a second query; more reports that, and the extra
// row is dropped.
func findPage[M any](
	ctx context.Context, s *Store, col string, filter bson.M, cursor id.ID, limit int,
) (rows []M, more bool, err error) {
	if !cursor.IsNil() {
		filter["_id"] = bson.M{"$lt": cursor.String()}
	}

	limit = paging.Limit(limit)
	find := options.Find().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(int64(limit) + 1)

	cur, err := s.mdb.Collection(col).Find(ctx, filter, find)
	if err != nil {
		return nil, false, err
	}

	if err := cur.All(ctx, &rows); err != nil {
		return nil, false, err
	}

	if len(rows) > limit {
		return rows[:limit], true, nil
	}

	return rows, false, nil
}

// ListJobs returns jobs newest first by ID, filtered and paged.
func (s *Store) ListJobs(ctx context.Context, opts job.ListJobsOpts) (job.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixJob)
	if err != nil {
		return job.Page{}, err
	}

	filter := bson.M{}
	if len(opts.States) > 0 {
		states := make([]string, len(opts.States))
		for i, st := range opts.States {
			states[i] = string(st)
		}
		filter["state"] = bson.M{"$in": states}
	}
	if opts.Queue != "" {
		filter["queue"] = opts.Queue
	}
	if opts.NamePrefix != "" {
		filter["name"] = namePrefix(opts.NamePrefix)
	}
	if opts.ScopeAppID != "" {
		filter["scope_app_id"] = opts.ScopeAppID
	}
	if opts.ScopeOrgID != "" {
		filter["scope_org_id"] = opts.ScopeOrgID
	}

	models, more, err := findPage[jobModel](ctx, s, colJobs, filter, cursor, opts.Limit)
	if err != nil {
		return job.Page{}, fmt.Errorf("dispatch/mongo: list jobs page: %w", err)
	}

	page := job.Page{Jobs: make([]*job.Job, 0, len(models)), Complete: true}
	for i := range models {
		j, convErr := fromJobModel(&models[i])
		if convErr != nil {
			return job.Page{}, fmt.Errorf("dispatch/mongo: list jobs page convert: %w", convErr)
		}
		page.Jobs = append(page.Jobs, j)
	}
	if more {
		page.NextCursor = page.Jobs[len(page.Jobs)-1].ID.String()
	}

	return page, nil
}

// ListRunsPage returns workflow runs newest first by ID, filtered and paged.
func (s *Store) ListRunsPage(ctx context.Context, opts workflow.ListRunsPageOpts) (workflow.RunPage, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixRun)
	if err != nil {
		return workflow.RunPage{}, err
	}

	filter := bson.M{}
	if opts.State != "" {
		filter["state"] = string(opts.State)
	}
	if opts.NamePrefix != "" {
		filter["name"] = namePrefix(opts.NamePrefix)
	}
	if opts.ScopeAppID != "" {
		filter["scope_app_id"] = opts.ScopeAppID
	}
	if opts.ScopeOrgID != "" {
		filter["scope_org_id"] = opts.ScopeOrgID
	}

	models, more, err := findPage[workflowRunModel](ctx, s, colWorkflowRuns, filter, cursor, opts.Limit)
	if err != nil {
		return workflow.RunPage{}, fmt.Errorf("dispatch/mongo: list runs page: %w", err)
	}

	page := workflow.RunPage{Runs: make([]*workflow.Run, 0, len(models)), Complete: true}
	for i := range models {
		r, convErr := fromRunModel(&models[i])
		if convErr != nil {
			return workflow.RunPage{}, fmt.Errorf("dispatch/mongo: list runs page convert: %w", convErr)
		}
		page.Runs = append(page.Runs, r)
	}
	if more {
		page.NextCursor = page.Runs[len(page.Runs)-1].ID.String()
	}

	return page, nil
}

// CountRuns counts workflow runs by state and exact name.
func (s *Store) CountRuns(ctx context.Context, opts workflow.CountRunsOpts) (int64, error) {
	filter := bson.M{}
	if opts.State != "" {
		filter["state"] = string(opts.State)
	}
	if opts.Name != "" {
		filter["name"] = opts.Name
	}

	n, err := s.mdb.Collection(colWorkflowRuns).CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: count runs: %w", err)
	}

	return n, nil
}

// replayedFilter is the replayed_at condition for a Replayed filter.
// PushDLQ goes through grove's insert, which writes an unreplayed entry's
// replayed_at as an explicit null, while a document written by the raw
// driver's encoder drops the key under "omitempty". Equality with nil
// matches both shapes, and $ne nil matches neither, so both answers hold
// whichever path wrote the entry. "Not replayed" is a plain nil, the same
// equality ListArtifacts uses on deleted_at.
func replayedFilter(replayed bool) any {
	if replayed {
		return bson.M{"$ne": nil}
	}

	return nil
}

// ListDLQPage returns dead letter entries newest first by ID, filtered and
// paged.
func (s *Store) ListDLQPage(ctx context.Context, opts dlq.PageOpts) (dlq.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixDLQ)
	if err != nil {
		return dlq.Page{}, err
	}

	filter := bson.M{}
	if opts.Queue != "" {
		filter["queue"] = opts.Queue
	}
	if opts.NamePrefix != "" {
		filter["job_name"] = namePrefix(opts.NamePrefix)
	}
	if opts.ScopeAppID != "" {
		filter["scope_app_id"] = opts.ScopeAppID
	}
	if opts.ScopeOrgID != "" {
		filter["scope_org_id"] = opts.ScopeOrgID
	}
	if opts.Replayed != nil {
		filter["replayed_at"] = replayedFilter(*opts.Replayed)
	}

	models, more, err := findPage[dlqEntryModel](ctx, s, colDLQ, filter, cursor, opts.Limit)
	if err != nil {
		return dlq.Page{}, fmt.Errorf("dispatch/mongo: list dlq page: %w", err)
	}

	page := dlq.Page{Entries: make([]*dlq.Entry, 0, len(models)), Complete: true}
	for i := range models {
		e, convErr := fromDLQModel(&models[i])
		if convErr != nil {
			return dlq.Page{}, fmt.Errorf("dispatch/mongo: list dlq page convert: %w", convErr)
		}
		page.Entries = append(page.Entries, e)
	}
	if more {
		page.NextCursor = page.Entries[len(page.Entries)-1].ID.String()
	}

	return page, nil
}

// CountDLQEntries counts dead letter entries under the given filters.
// FailedBefore is strictly before, the same $lt PurgeDLQ deletes on, so
// the count a purge confirmation shows is the count the purge removes.
func (s *Store) CountDLQEntries(ctx context.Context, opts dlq.CountOpts) (int64, error) {
	filter := bson.M{}
	if opts.Queue != "" {
		filter["queue"] = opts.Queue
	}
	if opts.Replayed != nil {
		filter["replayed_at"] = replayedFilter(*opts.Replayed)
	}
	if !opts.FailedBefore.IsZero() {
		filter["failed_at"] = bson.M{"$lt": opts.FailedBefore}
	}

	n, err := s.mdb.Collection(colDLQ).CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: count dlq entries: %w", err)
	}

	return n, nil
}

// ListArtifactsPage returns artifacts newest first by ID, filtered and
// paged. A live artifact's deleted_at may be absent or null, as in
// ListArtifacts, and equality with nil matches both.
func (s *Store) ListArtifactsPage(ctx context.Context, opts artifact.PageOpts) (artifact.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixArtifact)
	if err != nil {
		return artifact.Page{}, err
	}

	filter := bson.M{}
	if !opts.IncludeDeleted {
		filter["deleted_at"] = nil
	}
	if opts.Lifecycle != "" {
		filter["lifecycle"] = string(opts.Lifecycle)
	}
	if opts.ScopeAppID != "" {
		filter["scope_app_id"] = opts.ScopeAppID
	}
	if opts.ScopeOrgID != "" {
		filter["scope_org_id"] = opts.ScopeOrgID
	}

	models, more, err := findPage[artifactModel](ctx, s, colArtifacts, filter, cursor, opts.Limit)
	if err != nil {
		return artifact.Page{}, fmt.Errorf("dispatch/mongo: list artifacts page: %w", err)
	}

	arts, err := fromArtifactModels(models)
	if err != nil {
		return artifact.Page{}, fmt.Errorf("dispatch/mongo: list artifacts page convert: %w", err)
	}

	page := artifact.Page{Artifacts: arts, Complete: true}
	if more {
		page.NextCursor = arts[len(arts)-1].ID.String()
	}

	return page, nil
}
