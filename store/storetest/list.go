package storetest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/paging"
	"github.com/xraph/dispatch/workflow"
)

// ListStore is what the list suite requires: the four stores whose lists
// grow without bound, each with its paged capability.
type ListStore interface {
	job.Store
	job.Lister
	workflow.Store
	workflow.PageLister
	dlq.Store
	dlq.PageLister
	artifact.Store
	artifact.PageLister
}

// RunListSuite pins the paged list contract: newest first by ID, a cursor
// that is the last ID returned, filters that are exact except a literal
// case-sensitive name prefix, and an empty scope that means every tenant.
//
// newStore may return a shared store, so each case isolates itself with a
// queue, name prefix or scope nobody else uses, and asserts on the
// identity of what comes back, never on a total.
func RunListSuite(t *testing.T, newStore func(t *testing.T) ListStore) {
	t.Helper()

	cases := []struct {
		name string
		fn   func(t *testing.T, s ListStore)
	}{
		{"JobsNewestFirstAcrossStates", testJobsNewestFirstAcrossStates},
		{"JobsFilterByStates", testJobsFilterByStates},
		{"JobsCursorPagesJoin", testJobsCursorPagesJoin},
		{"JobsCursorSurvivesDeletedRow", testJobsCursorSurvivesDeletedRow},
		{"JobsInvalidCursor", testJobsInvalidCursor},
		{"JobsNamePrefixIsLiteralAndCaseSensitive", testJobsNamePrefixIsLiteralAndCaseSensitive},
		{"JobsScopeFilterAndEmptyMeansAll", testJobsScopeFilterAndEmptyMeansAll},
		{"RunsNewestFirstFiltersAndCounts", testRunsNewestFirstFiltersAndCounts},
		{"RunsCursorPagesJoin", testRunsCursorPagesJoin},
		{"RunsInvalidCursor", testRunsInvalidCursor},
		{"DLQNewestFirstAndReplayedFilter", testDLQNewestFirstAndReplayedFilter},
		{"DLQCountFilters", testDLQCountFilters},
		{"DLQCursorNameAndScope", testDLQCursorNameAndScope},
		{"ArtifactsNewestFirstAndFilters", testArtifactsNewestFirstAndFilters},
		{"ArtifactsCursorPagesJoin", testArtifactsCursorPagesJoin},
		{"JobsDefaultLimitIsFifty", testJobsDefaultLimitIsFifty},
		{"JobsExactMultipleHasNoExtraCursor", testJobsExactMultipleHasNoExtraCursor},
		{"RunsExactMultipleHasNoExtraCursor", testRunsExactMultipleHasNoExtraCursor},
		{"DLQExactMultipleHasNoExtraCursor", testDLQExactMultipleHasNoExtraCursor},
		{"ArtifactsExactMultipleHasNoExtraCursor", testArtifactsExactMultipleHasNoExtraCursor},
		{"JobsOrderByIDNotCreatedAtOrRunAt", testJobsOrderByIDNotCreatedAtOrRunAt},
		{"DLQOrderByIDNotFailedAt", testDLQOrderByIDNotFailedAt},
		{"JobsNamePrefixTreatsRegexMetacharactersLiterally", testJobsNamePrefixRegexMetacharacters},
		{"RunsNamePrefixIsLiteralAndCaseSensitive", testRunsNamePrefixIsLiteralAndCaseSensitive},
		{"DLQNamePrefixIsLiteralAndCaseSensitive", testDLQNamePrefixIsLiteralAndCaseSensitive},
		{"JobsExactMatchFiltersDoNotPrefixMatch", testJobsExactMatchFilters},
		{"RunsExactMatchFiltersDoNotPrefixMatch", testRunsExactMatchFilters},
		{"DLQExactMatchFiltersDoNotPrefixMatch", testDLQExactMatchFilters},
		{"ArtifactsExactMatchScopeDoesNotPrefixMatch", testArtifactsExactMatchScope},
		{"ArtifactsInvalidCursor", testArtifactsInvalidCursor},
		{"DLQEmptyScopeMeansAllTenants", testDLQEmptyScopeMeansAllTenants},
		{"ArtifactsEmptyScopeMeansAllTenants", testArtifactsEmptyScopeMeansAllTenants},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newStore(t)) })
	}
}

// uniq returns a label no other case or run will use.
func uniq(kind string) string { return kind + "-" + id.NewJobID().String() }

func listJob(name, queue string, state job.State) *job.Job {
	j := PendingJob(name, queue, 0)
	j.State = state

	return j
}

func enqueueAll(t *testing.T, s ListStore, jobs ...*job.Job) {
	t.Helper()

	for _, j := range jobs {
		if err := s.EnqueueJob(context.Background(), j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}
}

func jobIDs(jobs []*job.Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID.String()
	}

	return out
}

// newestFirst returns the IDs of jobs in reverse creation order, which is
// the order every list must return them in.
func newestFirst(jobs ...*job.Job) []string {
	ids := jobIDs(jobs)
	slices.Reverse(ids)

	return ids
}

func assertIDs(t *testing.T, what string, got, want []string) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Fatalf("%s:\n got  %v\n want %v", what, got, want)
	}
}

func testJobsNewestFirstAcrossStates(t *testing.T, s ListStore) {
	q := uniq("q")
	jobs := make([]*job.Job, 0, 6)
	for _, st := range []job.State{
		job.StatePending, job.StateRunning, job.StateCompleted,
		job.StateFailed, job.StateRetrying, job.StateCancelled,
	} {
		jobs = append(jobs, listJob("all-"+string(st), q, st))
	}
	enqueueAll(t, s, jobs...)

	page, err := s.ListJobs(context.Background(), job.ListJobsOpts{Queue: q})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	assertIDs(t, "every state, newest first", jobIDs(page.Jobs), newestFirst(jobs...))
	if !page.Complete || page.NextCursor != "" {
		t.Fatalf("page = complete %v, next %q; want complete with no next cursor", page.Complete, page.NextCursor)
	}
}

func testJobsFilterByStates(t *testing.T, s ListStore) {
	q := uniq("q")
	pending := listJob("p", q, job.StatePending)
	failed := listJob("f", q, job.StateFailed)
	retrying := listJob("r", q, job.StateRetrying)
	enqueueAll(t, s, pending, failed, retrying)

	page, err := s.ListJobs(context.Background(), job.ListJobsOpts{
		Queue:  q,
		States: []job.State{job.StateFailed, job.StateRetrying},
	})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	assertIDs(t, "failed and retrying only", jobIDs(page.Jobs), newestFirst(failed, retrying))
}

func testJobsCursorPagesJoin(t *testing.T, s ListStore) {
	q := uniq("q")
	jobs := make([]*job.Job, 0, 7)
	for i := range 7 {
		jobs = append(jobs, listJob(fmt.Sprintf("page-%d", i), q, job.StatePending))
	}
	enqueueAll(t, s, jobs...)

	var got []string
	cursor := ""
	for pageNo := 1; ; pageNo++ {
		page, err := s.ListJobs(context.Background(), job.ListJobsOpts{Queue: q, Cursor: cursor, Limit: 3})
		if err != nil {
			t.Fatalf("page %d: %v", pageNo, err)
		}
		if len(page.Jobs) > 3 {
			t.Fatalf("page %d returned %d jobs with limit 3", pageNo, len(page.Jobs))
		}
		got = append(got, jobIDs(page.Jobs)...)
		if !page.Complete {
			t.Fatalf("page %d is not complete", pageNo)
		}
		if page.NextCursor == "" {
			break
		}
		if want := page.Jobs[len(page.Jobs)-1].ID.String(); page.NextCursor != want {
			t.Fatalf("page %d NextCursor = %q, want the last returned id %q", pageNo, page.NextCursor, want)
		}
		if pageNo > 5 {
			t.Fatal("more than 5 pages for 7 jobs at limit 3: the cursor is not advancing")
		}
		cursor = page.NextCursor
	}
	assertIDs(t, "pages joined", got, newestFirst(jobs...))
}

func testJobsCursorSurvivesDeletedRow(t *testing.T, s ListStore) {
	q := uniq("q")
	a := listJob("a", q, job.StatePending)
	b := listJob("b", q, job.StatePending)
	c := listJob("c", q, job.StatePending)
	d := listJob("d", q, job.StatePending)
	enqueueAll(t, s, a, b, c, d)

	ctx := context.Background()
	first, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	assertIDs(t, "first page", jobIDs(first.Jobs), newestFirst(c, d))
	if first.NextCursor != c.ID.String() {
		t.Fatalf("first page NextCursor = %q, want the last returned id %q", first.NextCursor, c.ID)
	}

	// The cursor row is c. Delete it, then ask for what comes after it.
	if delErr := s.DeleteJob(ctx, c.ID); delErr != nil {
		t.Fatalf("delete cursor row: %v", delErr)
	}

	next, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, Cursor: first.NextCursor, Limit: 2})
	if err != nil {
		t.Fatalf("next page after deleted cursor row: %v", err)
	}
	assertIDs(t, "next page", jobIDs(next.Jobs), newestFirst(a, b))
}

func testJobsInvalidCursor(t *testing.T, s ListStore) {
	for _, cursor := range []string{"not-an-id", id.NewRunID().String()} {
		_, err := s.ListJobs(context.Background(), job.ListJobsOpts{Cursor: cursor})
		if !errors.Is(err, paging.ErrInvalidCursor) {
			t.Errorf("ListJobs(cursor %q) error = %v, want paging.ErrInvalidCursor", cursor, err)
		}
	}
}

func testJobsNamePrefixIsLiteralAndCaseSensitive(t *testing.T, s ListStore) {
	q := uniq("q")
	underscore := listJob("a_b-1", q, job.StatePending)
	wildcardHit := listJob("axb-1", q, job.StatePending)
	upper := listJob("A_b-2", q, job.StatePending)
	percent := listJob("a%c", q, job.StatePending)
	enqueueAll(t, s, underscore, wildcardHit, upper, percent)

	ctx := context.Background()
	page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, NamePrefix: "a_b"})
	if err != nil {
		t.Fatalf("ListJobs a_b: %v", err)
	}
	assertIDs(t, `prefix "a_b"`, jobIDs(page.Jobs), []string{underscore.ID.String()})

	page, err = s.ListJobs(ctx, job.ListJobsOpts{Queue: q, NamePrefix: "a%"})
	if err != nil {
		t.Fatalf("ListJobs a%%: %v", err)
	}
	assertIDs(t, `prefix "a%"`, jobIDs(page.Jobs), []string{percent.ID.String()})
}

func testJobsScopeFilterAndEmptyMeansAll(t *testing.T, s ListStore) {
	q := uniq("q")
	appOne, appTwo := uniq("app"), uniq("app")
	orgOne, orgTwo := uniq("org"), uniq("org")

	first := listJob("scoped-1", q, job.StatePending)
	first.ScopeAppID, first.ScopeOrgID = appOne, orgOne
	second := listJob("scoped-2", q, job.StatePending)
	second.ScopeAppID, second.ScopeOrgID = appTwo, orgTwo
	unscoped := listJob("unscoped", q, job.StatePending)
	enqueueAll(t, s, first, second, unscoped)

	ctx := context.Background()
	for _, tc := range []struct {
		what string
		opts job.ListJobsOpts
		want []string
	}{
		{"app one", job.ListJobsOpts{Queue: q, ScopeAppID: appOne}, []string{first.ID.String()}},
		{"org two", job.ListJobsOpts{Queue: q, ScopeOrgID: orgTwo}, []string{second.ID.String()}},
		{"app one in org two", job.ListJobsOpts{Queue: q, ScopeAppID: appOne, ScopeOrgID: orgTwo}, []string{}},
		// Pinned on purpose: an empty scope is "every tenant", not "none".
		{"empty scope", job.ListJobsOpts{Queue: q}, newestFirst(first, second, unscoped)},
	} {
		page, err := s.ListJobs(ctx, tc.opts)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		assertIDs(t, tc.what, jobIDs(page.Jobs), tc.want)
	}
}

func listRun(name string, state workflow.RunState) *workflow.Run {
	return &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      name,
		State:     state,
		StartedAt: time.Now().UTC(),
	}
}

func createRuns(t *testing.T, s ListStore, runs ...*workflow.Run) {
	t.Helper()

	for _, r := range runs {
		if err := s.CreateRun(context.Background(), r); err != nil {
			t.Fatalf("create run %s: %v", r.Name, err)
		}
	}
}

func runIDs(runs []*workflow.Run) []string {
	out := make([]string, len(runs))
	for i, r := range runs {
		out[i] = r.ID.String()
	}

	return out
}

func runsNewestFirst(runs ...*workflow.Run) []string {
	ids := runIDs(runs)
	slices.Reverse(ids)

	return ids
}

func testRunsNewestFirstFiltersAndCounts(t *testing.T, s ListStore) {
	prefix := uniq("wf")
	a := listRun(prefix+"-a", workflow.RunStateRunning)
	b := listRun(prefix+"-b", workflow.RunStateCompleted)
	c := listRun(prefix+"-b", workflow.RunStateFailed)
	d := listRun(prefix+"-c", workflow.RunStateCompleted)
	d.ScopeAppID = uniq("app")
	createRuns(t, s, a, b, c, d)

	ctx := context.Background()
	page, err := s.ListRunsPage(ctx, workflow.ListRunsPageOpts{NamePrefix: prefix})
	if err != nil {
		t.Fatalf("ListRunsPage: %v", err)
	}
	assertIDs(t, "runs newest first", runIDs(page.Runs), runsNewestFirst(a, b, c, d))

	page, err = s.ListRunsPage(ctx, workflow.ListRunsPageOpts{NamePrefix: prefix, State: workflow.RunStateCompleted})
	if err != nil {
		t.Fatalf("ListRunsPage completed: %v", err)
	}
	assertIDs(t, "completed runs", runIDs(page.Runs), runsNewestFirst(b, d))

	page, err = s.ListRunsPage(ctx, workflow.ListRunsPageOpts{NamePrefix: prefix, ScopeAppID: d.ScopeAppID})
	if err != nil {
		t.Fatalf("ListRunsPage scoped: %v", err)
	}
	assertIDs(t, "scoped runs", runIDs(page.Runs), []string{d.ID.String()})

	for _, tc := range []struct {
		opts workflow.CountRunsOpts
		want int64
	}{
		{workflow.CountRunsOpts{Name: prefix + "-b"}, 2},
		{workflow.CountRunsOpts{Name: prefix + "-b", State: workflow.RunStateFailed}, 1},
		{workflow.CountRunsOpts{Name: prefix + "-z"}, 0},
	} {
		n, err := s.CountRuns(ctx, tc.opts)
		if err != nil {
			t.Fatalf("CountRuns %+v: %v", tc.opts, err)
		}
		if n != tc.want {
			t.Errorf("CountRuns %+v = %d, want %d", tc.opts, n, tc.want)
		}
	}
}

func testRunsCursorPagesJoin(t *testing.T, s ListStore) {
	prefix := uniq("wf")
	runs := make([]*workflow.Run, 0, 5)
	for i := range 5 {
		runs = append(runs, listRun(fmt.Sprintf("%s-%d", prefix, i), workflow.RunStateCompleted))
	}
	createRuns(t, s, runs...)

	var got []string
	cursor := ""
	for pageNo := 1; ; pageNo++ {
		page, err := s.ListRunsPage(context.Background(), workflow.ListRunsPageOpts{NamePrefix: prefix, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("page %d: %v", pageNo, err)
		}
		got = append(got, runIDs(page.Runs)...)
		if page.NextCursor == "" {
			break
		}
		if pageNo > 4 {
			t.Fatal("cursor is not advancing")
		}
		cursor = page.NextCursor
	}
	assertIDs(t, "run pages joined", got, runsNewestFirst(runs...))
}

func testRunsInvalidCursor(t *testing.T, s ListStore) {
	_, err := s.ListRunsPage(context.Background(), workflow.ListRunsPageOpts{Cursor: id.NewJobID().String()})
	if !errors.Is(err, paging.ErrInvalidCursor) {
		t.Fatalf("ListRunsPage(job cursor) error = %v, want paging.ErrInvalidCursor", err)
	}
}

func listEntry(jobName, queue string, failedAt time.Time) *dlq.Entry {
	return &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      id.NewJobID(),
		JobName:    jobName,
		Queue:      queue,
		Payload:    []byte(`{}`),
		Error:      "boom",
		MaxRetries: 3,
		FailedAt:   failedAt,
		CreatedAt:  failedAt,
	}
}

func pushAll(t *testing.T, s ListStore, entries ...*dlq.Entry) {
	t.Helper()

	for _, e := range entries {
		if err := s.PushDLQ(context.Background(), e); err != nil {
			t.Fatalf("push %s: %v", e.JobName, err)
		}
	}
}

func entryIDs(entries []*dlq.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.ID.String()
	}

	return out
}

func entriesNewestFirst(entries ...*dlq.Entry) []string {
	ids := entryIDs(entries)
	slices.Reverse(ids)

	return ids
}

func testDLQNewestFirstAndReplayedFilter(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)
	a := listEntry("a", q, now.Add(-2*time.Hour))
	b := listEntry("b", q, now.Add(-time.Hour))
	c := listEntry("c", q, now)
	pushAll(t, s, a, b, c)

	ctx := context.Background()
	if err := s.ReplayDLQ(ctx, b.ID); err != nil {
		t.Fatalf("replay b: %v", err)
	}

	yes, no := true, false
	for _, tc := range []struct {
		what     string
		replayed *bool
		want     []string
	}{
		{"both", nil, entriesNewestFirst(a, b, c)},
		{"unreplayed", &no, entriesNewestFirst(a, c)},
		{"replayed", &yes, []string{b.ID.String()}},
	} {
		page, err := s.ListDLQPage(ctx, dlq.PageOpts{Queue: q, Replayed: tc.replayed})
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		assertIDs(t, tc.what, entryIDs(page.Entries), tc.want)
	}
}

func testDLQCountFilters(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)
	old := listEntry("old", q, now.Add(-48*time.Hour))
	mid := listEntry("mid", q, now.Add(-24*time.Hour))
	fresh := listEntry("fresh", q, now)
	pushAll(t, s, old, mid, fresh)

	ctx := context.Background()
	if err := s.ReplayDLQ(ctx, mid.ID); err != nil {
		t.Fatalf("replay mid: %v", err)
	}

	no := false
	for _, tc := range []struct {
		what string
		opts dlq.CountOpts
		want int64
	}{
		{"queue", dlq.CountOpts{Queue: q}, 3},
		{"unreplayed", dlq.CountOpts{Queue: q, Replayed: &no}, 2},
		// Strictly before, the boundary PurgeDLQ uses: mid itself is not counted.
		{"failed before mid", dlq.CountOpts{Queue: q, FailedBefore: mid.FailedAt}, 1},
		{"failed before now", dlq.CountOpts{Queue: q, FailedBefore: now.Add(time.Second)}, 3},
	} {
		n, err := s.CountDLQEntries(ctx, tc.opts)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		if n != tc.want {
			t.Errorf("CountDLQEntries %s = %d, want %d", tc.what, n, tc.want)
		}
	}
}

func testDLQCursorNameAndScope(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)
	entries := make([]*dlq.Entry, 0, 6)
	for i := range 5 {
		entries = append(entries, listEntry(fmt.Sprintf("render-%d", i), q, now))
	}
	other := listEntry("email-0", q, now)
	other.ScopeAppID = uniq("app")
	pushAll(t, s, append(entries, other)...)

	ctx := context.Background()
	var got []string
	cursor := ""
	for pageNo := 1; ; pageNo++ {
		page, err := s.ListDLQPage(ctx, dlq.PageOpts{Queue: q, NamePrefix: "render-", Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("page %d: %v", pageNo, err)
		}
		got = append(got, entryIDs(page.Entries)...)
		if page.NextCursor == "" {
			break
		}
		if pageNo > 4 {
			t.Fatal("cursor is not advancing")
		}
		cursor = page.NextCursor
	}
	assertIDs(t, "render entries paged", got, entriesNewestFirst(entries...))

	page, err := s.ListDLQPage(ctx, dlq.PageOpts{Queue: q, ScopeAppID: other.ScopeAppID})
	if err != nil {
		t.Fatalf("scoped: %v", err)
	}
	assertIDs(t, "scoped entry", entryIDs(page.Entries), []string{other.ID.String()})

	if _, err := s.ListDLQPage(ctx, dlq.PageOpts{Cursor: id.NewJobID().String()}); !errors.Is(err, paging.ErrInvalidCursor) {
		t.Fatalf("ListDLQPage(job cursor) error = %v, want paging.ErrInvalidCursor", err)
	}
}

func listArtifact(app string, lifecycle artifact.Lifecycle) *artifact.Artifact {
	artID := id.NewArtifactID()

	return &artifact.Artifact{
		ID:         artID,
		Backend:    "mem",
		Bucket:     "list-suite",
		Key:        artID.String(),
		Size:       42,
		Lifecycle:  lifecycle,
		ScopeAppID: app,
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
}

func createArtifacts(t *testing.T, s ListStore, arts ...*artifact.Artifact) {
	t.Helper()

	for _, a := range arts {
		if err := s.CreateArtifact(context.Background(), a, nil); err != nil {
			t.Fatalf("create artifact %s: %v", a.ID, err)
		}
	}
}

func artifactIDs(arts []*artifact.Artifact) []string {
	out := make([]string, len(arts))
	for i, a := range arts {
		out[i] = a.ID.String()
	}

	return out
}

func artifactsNewestFirst(arts ...*artifact.Artifact) []string {
	ids := artifactIDs(arts)
	slices.Reverse(ids)

	return ids
}

func testArtifactsNewestFirstAndFilters(t *testing.T, s ListStore) {
	app := uniq("app")
	durable := listArtifact(app, artifact.Durable)
	ephemeral := listArtifact(app, artifact.Ephemeral)

	// Artifacts are only ever soft-deleted by the sweeper, and UpdateArtifact
	// deliberately preserves DeletedAt, so the suite deletes the way
	// production does: an unlinked ephemeral artifact old enough for
	// SweepOrphans. The two-hour age keeps every other case's artifacts,
	// all created just now, out of the sweep.
	swept := listArtifact(app, artifact.Ephemeral)
	swept.CreatedAt = swept.CreatedAt.Add(-2 * time.Hour)
	createArtifacts(t, s, durable, ephemeral, swept)

	ctx := context.Background()
	marked, err := s.SweepOrphans(ctx, time.Now().UTC().Add(-time.Hour), 0)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if !slices.Contains(artifactIDs(marked), swept.ID.String()) {
		t.Fatalf("SweepOrphans did not mark %s; marked %v", swept.ID, artifactIDs(marked))
	}

	for _, tc := range []struct {
		what string
		opts artifact.PageOpts
		want []string
	}{
		{"live", artifact.PageOpts{ScopeAppID: app}, artifactsNewestFirst(durable, ephemeral)},
		{"with deleted", artifact.PageOpts{ScopeAppID: app, IncludeDeleted: true}, artifactsNewestFirst(durable, ephemeral, swept)},
		{"ephemeral", artifact.PageOpts{ScopeAppID: app, Lifecycle: artifact.Ephemeral}, []string{ephemeral.ID.String()}},
	} {
		page, err := s.ListArtifactsPage(ctx, tc.opts)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		assertIDs(t, tc.what, artifactIDs(page.Artifacts), tc.want)
	}
}

func testArtifactsCursorPagesJoin(t *testing.T, s ListStore) {
	app := uniq("app")
	arts := make([]*artifact.Artifact, 0, 5)
	for range 5 {
		arts = append(arts, listArtifact(app, artifact.Durable))
	}
	createArtifacts(t, s, arts...)

	var got []string
	cursor := ""
	for pageNo := 1; ; pageNo++ {
		page, err := s.ListArtifactsPage(context.Background(), artifact.PageOpts{ScopeAppID: app, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("page %d: %v", pageNo, err)
		}
		got = append(got, artifactIDs(page.Artifacts)...)
		if page.NextCursor == "" {
			break
		}
		if pageNo > 4 {
			t.Fatal("cursor is not advancing")
		}
		cursor = page.NextCursor
	}
	assertIDs(t, "artifact pages joined", got, artifactsNewestFirst(arts...))
}

// walkEvenPages follows NextCursor from the first page until it is empty and
// returns every id seen. It is for a list whose size is exactly twice limit.
// It pins what every pager shares: a page never exceeds limit, Complete is
// true, NextCursor is the last id the page returned whenever it is set, and
// the walk takes exactly two pages (so a backend that sets a cursor on a
// final, evenly filled page needs a third page and fails).
func walkEvenPages(t *testing.T, what string, limit int, fetch func(cursor string) (ids []string, next string, complete bool)) []string {
	t.Helper()

	const wantPages = 2

	var all []string
	cursor := ""
	for pageNo := 1; ; pageNo++ {
		if pageNo > wantPages {
			t.Fatalf("%s: still paging after %d pages, want %d", what, wantPages, wantPages)
		}

		ids, next, complete := fetch(cursor)
		if len(ids) > limit {
			t.Fatalf("%s: page %d returned %d rows with limit %d", what, pageNo, len(ids), limit)
		}
		if !complete {
			t.Fatalf("%s: page %d is not complete", what, pageNo)
		}
		all = append(all, ids...)

		if next == "" {
			if pageNo != wantPages {
				t.Fatalf("%s: paging ended after %d pages, want %d", what, pageNo, wantPages)
			}

			return all
		}
		if len(ids) == 0 || next != ids[len(ids)-1] {
			t.Fatalf("%s: page %d NextCursor = %q, want the last returned id of %v", what, pageNo, next, ids)
		}
		cursor = next
	}
}

func testJobsDefaultLimitIsFifty(t *testing.T, s ListStore) {
	q := uniq("q")
	jobs := make([]*job.Job, 0, paging.DefaultLimit+1)
	for i := range paging.DefaultLimit + 1 {
		jobs = append(jobs, listJob(fmt.Sprintf("bulk-%d", i), q, job.StatePending))
	}
	enqueueAll(t, s, jobs...)

	ctx := context.Background()
	for _, limit := range []int{0, -1} {
		page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, Limit: limit})
		if err != nil {
			t.Fatalf("limit %d: %v", limit, err)
		}
		want := newestFirst(jobs...)[:paging.DefaultLimit]
		assertIDs(t, fmt.Sprintf("limit %d returns the default page size", limit), jobIDs(page.Jobs), want)
		if page.NextCursor != want[len(want)-1] {
			t.Fatalf("limit %d NextCursor = %q, want the 50th id %q", limit, page.NextCursor, want[len(want)-1])
		}
		if !page.Complete {
			t.Fatalf("limit %d page is not complete", limit)
		}

		rest, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, Limit: limit, Cursor: page.NextCursor})
		if err != nil {
			t.Fatalf("limit %d rest: %v", limit, err)
		}
		assertIDs(t, fmt.Sprintf("limit %d last row", limit), jobIDs(rest.Jobs), []string{jobs[0].ID.String()})
		if rest.NextCursor != "" {
			t.Fatalf("limit %d last page NextCursor = %q, want empty", limit, rest.NextCursor)
		}
	}
}

func testJobsExactMultipleHasNoExtraCursor(t *testing.T, s ListStore) {
	q := uniq("q")
	jobs := make([]*job.Job, 0, 6)
	for i := range 6 {
		jobs = append(jobs, listJob(fmt.Sprintf("even-%d", i), q, job.StatePending))
	}
	enqueueAll(t, s, jobs...)

	got := walkEvenPages(t, "jobs", 3, func(cursor string) ([]string, string, bool) {
		page, err := s.ListJobs(context.Background(), job.ListJobsOpts{Queue: q, Cursor: cursor, Limit: 3})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}

		return jobIDs(page.Jobs), page.NextCursor, page.Complete
	})
	assertIDs(t, "pages joined", got, newestFirst(jobs...))
}

func testRunsExactMultipleHasNoExtraCursor(t *testing.T, s ListStore) {
	prefix := uniq("wf")
	runs := make([]*workflow.Run, 0, 4)
	for i := range 4 {
		runs = append(runs, listRun(fmt.Sprintf("%s-%d", prefix, i), workflow.RunStateCompleted))
	}
	createRuns(t, s, runs...)

	got := walkEvenPages(t, "runs", 2, func(cursor string) ([]string, string, bool) {
		page, err := s.ListRunsPage(context.Background(), workflow.ListRunsPageOpts{NamePrefix: prefix, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("ListRunsPage: %v", err)
		}

		return runIDs(page.Runs), page.NextCursor, page.Complete
	})
	assertIDs(t, "run pages joined", got, runsNewestFirst(runs...))
}

func testDLQExactMultipleHasNoExtraCursor(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)
	entries := make([]*dlq.Entry, 0, 4)
	for i := range 4 {
		entries = append(entries, listEntry(fmt.Sprintf("even-%d", i), q, now))
	}
	pushAll(t, s, entries...)

	got := walkEvenPages(t, "dlq", 2, func(cursor string) ([]string, string, bool) {
		page, err := s.ListDLQPage(context.Background(), dlq.PageOpts{Queue: q, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("ListDLQPage: %v", err)
		}

		return entryIDs(page.Entries), page.NextCursor, page.Complete
	})
	assertIDs(t, "dlq pages joined", got, entriesNewestFirst(entries...))
}

func testArtifactsExactMultipleHasNoExtraCursor(t *testing.T, s ListStore) {
	app := uniq("app")
	arts := make([]*artifact.Artifact, 0, 4)
	for range 4 {
		arts = append(arts, listArtifact(app, artifact.Durable))
	}
	createArtifacts(t, s, arts...)

	got := walkEvenPages(t, "artifacts", 2, func(cursor string) ([]string, string, bool) {
		page, err := s.ListArtifactsPage(context.Background(), artifact.PageOpts{ScopeAppID: app, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("ListArtifactsPage: %v", err)
		}

		return artifactIDs(page.Artifacts), page.NextCursor, page.Complete
	})
	assertIDs(t, "artifact pages joined", got, artifactsNewestFirst(arts...))
}

// testJobsOrderByIDNotCreatedAtOrRunAt mints ids in one order and gives the
// rows the opposite timestamps, so only ordering by id passes.
func testJobsOrderByIDNotCreatedAtOrRunAt(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)

	first := listJob("minted-first", q, job.StatePending)
	first.CreatedAt, first.UpdatedAt, first.RunAt = now, now, now.Add(-time.Minute)
	first.Priority = 0
	second := listJob("minted-second", q, job.StatePending)
	second.CreatedAt, second.UpdatedAt, second.RunAt = now.Add(-time.Hour), now.Add(-time.Hour), now.Add(-time.Hour)
	second.Priority = -1
	enqueueAll(t, s, first, second)

	page, err := s.ListJobs(context.Background(), job.ListJobsOpts{Queue: q})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	assertIDs(t, "newest id first despite older timestamps", jobIDs(page.Jobs), newestFirst(first, second))
}

func testDLQOrderByIDNotFailedAt(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)

	first := listEntry("minted-first", q, now)
	second := listEntry("minted-second", q, now.Add(-time.Hour))
	pushAll(t, s, first, second)

	page, err := s.ListDLQPage(context.Background(), dlq.PageOpts{Queue: q})
	if err != nil {
		t.Fatalf("ListDLQPage: %v", err)
	}
	assertIDs(t, "newest id first despite older failed_at", entryIDs(page.Entries), entriesNewestFirst(first, second))
}

func testJobsNamePrefixRegexMetacharacters(t *testing.T, s ListStore) {
	q := uniq("q")
	dot := listJob("a.b-1", q, job.StatePending)
	dotMiss := listJob("axb-1", q, job.StatePending)
	star := listJob("a*c", q, job.StatePending)
	starMiss := listJob("aac", q, job.StatePending)
	paren := listJob("a(b", q, job.StatePending)
	backslash := listJob(`a\b`, q, job.StatePending)
	caret := listJob("^a-x", q, job.StatePending)
	enqueueAll(t, s, dot, dotMiss, star, starMiss, paren, backslash, caret)

	ctx := context.Background()
	for _, tc := range []struct {
		prefix string
		want   []string
	}{
		{"a.", []string{dot.ID.String()}},
		{"a*", []string{star.ID.String()}},
		{"a(", []string{paren.ID.String()}},
		{`a\`, []string{backslash.ID.String()}},
		{"^a", []string{caret.ID.String()}},
	} {
		page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, NamePrefix: tc.prefix})
		if err != nil {
			t.Fatalf("ListJobs %q: %v", tc.prefix, err)
		}
		assertIDs(t, fmt.Sprintf("prefix %q", tc.prefix), jobIDs(page.Jobs), tc.want)
	}
}

func testRunsNamePrefixIsLiteralAndCaseSensitive(t *testing.T, s ListStore) {
	p := uniq("wf") + "-"
	underscore := listRun(p+"a_b", workflow.RunStateRunning)
	underscoreMiss := listRun(p+"axb", workflow.RunStateRunning)
	upper := listRun(p+"A_b", workflow.RunStateRunning)
	percent := listRun(p+"a%c", workflow.RunStateRunning)
	dot := listRun(p+"a.d", workflow.RunStateRunning)
	createRuns(t, s, underscore, underscoreMiss, upper, percent, dot)

	for _, tc := range []struct {
		prefix string
		want   []string
	}{
		{p + "a_b", []string{underscore.ID.String()}},
		{p + "a%", []string{percent.ID.String()}},
		{p + "A", []string{upper.ID.String()}},
		{p + "a.", []string{dot.ID.String()}},
	} {
		page, err := s.ListRunsPage(context.Background(), workflow.ListRunsPageOpts{NamePrefix: tc.prefix})
		if err != nil {
			t.Fatalf("ListRunsPage %q: %v", tc.prefix, err)
		}
		assertIDs(t, fmt.Sprintf("prefix %q", tc.prefix), runIDs(page.Runs), tc.want)
	}
}

func testDLQNamePrefixIsLiteralAndCaseSensitive(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)
	underscore := listEntry("a_b", q, now)
	underscoreMiss := listEntry("axb", q, now)
	upper := listEntry("A_b", q, now)
	percent := listEntry("a%c", q, now)
	dot := listEntry("a.d", q, now)
	pushAll(t, s, underscore, underscoreMiss, upper, percent, dot)

	for _, tc := range []struct {
		prefix string
		want   []string
	}{
		{"a_b", []string{underscore.ID.String()}},
		{"a%", []string{percent.ID.String()}},
		{"A", []string{upper.ID.String()}},
		{"a.", []string{dot.ID.String()}},
	} {
		page, err := s.ListDLQPage(context.Background(), dlq.PageOpts{Queue: q, NamePrefix: tc.prefix})
		if err != nil {
			t.Fatalf("ListDLQPage %q: %v", tc.prefix, err)
		}
		assertIDs(t, fmt.Sprintf("prefix %q", tc.prefix), entryIDs(page.Entries), tc.want)
	}
}

// testJobsExactMatchFilters pins that queue and scope are exact matches: a
// value that is a prefix of another must not pick up the longer one.
func testJobsExactMatchFilters(t *testing.T, s ListStore) {
	q := uniq("q")
	longQ := q + "-2"
	app := uniq("app")
	org := uniq("org")

	short := listJob("exact-short", q, job.StatePending)
	short.ScopeAppID, short.ScopeOrgID = app, org
	long := listJob("exact-long", longQ, job.StatePending)
	long.ScopeAppID, long.ScopeOrgID = app+"0", org+"0"
	enqueueAll(t, s, short, long)

	ctx := context.Background()
	for _, tc := range []struct {
		what string
		opts job.ListJobsOpts
		want []string
	}{
		{"queue", job.ListJobsOpts{Queue: q}, []string{short.ID.String()}},
		{"app", job.ListJobsOpts{Queue: q, ScopeAppID: app}, []string{short.ID.String()}},
		{"org", job.ListJobsOpts{Queue: q, ScopeOrgID: org}, []string{short.ID.String()}},
		{"long app", job.ListJobsOpts{Queue: longQ, ScopeAppID: app + "0"}, []string{long.ID.String()}},
		{"app misses long queue", job.ListJobsOpts{Queue: longQ, ScopeAppID: app}, []string{}},
		{"org misses long queue", job.ListJobsOpts{Queue: longQ, ScopeOrgID: org}, []string{}},
	} {
		page, err := s.ListJobs(ctx, tc.opts)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		assertIDs(t, tc.what, jobIDs(page.Jobs), tc.want)
	}
}

func testRunsExactMatchFilters(t *testing.T, s ListStore) {
	prefix := uniq("wf")
	app := uniq("app")
	org := uniq("org")

	short := listRun(prefix+"-b", workflow.RunStateRunning)
	short.ScopeAppID, short.ScopeOrgID = app, org
	long := listRun(prefix+"-b2", workflow.RunStateRunning)
	long.ScopeAppID, long.ScopeOrgID = app+"0", org+"0"
	createRuns(t, s, short, long)

	ctx := context.Background()
	for _, tc := range []struct {
		what string
		opts workflow.ListRunsPageOpts
		want []string
	}{
		{"app", workflow.ListRunsPageOpts{NamePrefix: prefix, ScopeAppID: app}, []string{short.ID.String()}},
		{"org", workflow.ListRunsPageOpts{NamePrefix: prefix, ScopeOrgID: org}, []string{short.ID.String()}},
		{"long app", workflow.ListRunsPageOpts{NamePrefix: prefix, ScopeAppID: app + "0"}, []string{long.ID.String()}},
	} {
		page, err := s.ListRunsPage(ctx, tc.opts)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		assertIDs(t, tc.what, runIDs(page.Runs), tc.want)
	}

	n, err := s.CountRuns(ctx, workflow.CountRunsOpts{Name: prefix + "-b"})
	if err != nil {
		t.Fatalf("CountRuns: %v", err)
	}
	if n != 1 {
		t.Fatalf("CountRuns name %q = %d, want 1: the name is an exact match, not a prefix", prefix+"-b", n)
	}
}

func testDLQExactMatchFilters(t *testing.T, s ListStore) {
	q := uniq("q")
	longQ := q + "-2"
	app := uniq("app")
	org := uniq("org")
	now := time.Now().UTC().Truncate(time.Millisecond)

	short := listEntry("exact-short", q, now)
	short.ScopeAppID, short.ScopeOrgID = app, org
	long := listEntry("exact-long", longQ, now)
	long.ScopeAppID, long.ScopeOrgID = app+"0", org+"0"
	pushAll(t, s, short, long)

	ctx := context.Background()
	for _, tc := range []struct {
		what string
		opts dlq.PageOpts
		want []string
	}{
		{"queue", dlq.PageOpts{Queue: q}, []string{short.ID.String()}},
		{"app", dlq.PageOpts{Queue: q, ScopeAppID: app}, []string{short.ID.String()}},
		{"org", dlq.PageOpts{Queue: q, ScopeOrgID: org}, []string{short.ID.String()}},
		{"long app", dlq.PageOpts{Queue: longQ, ScopeAppID: app + "0"}, []string{long.ID.String()}},
		{"app misses long queue", dlq.PageOpts{Queue: longQ, ScopeAppID: app}, []string{}},
	} {
		page, err := s.ListDLQPage(ctx, tc.opts)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		assertIDs(t, tc.what, entryIDs(page.Entries), tc.want)
	}

	n, err := s.CountDLQEntries(ctx, dlq.CountOpts{Queue: q})
	if err != nil {
		t.Fatalf("CountDLQEntries: %v", err)
	}
	if n != 1 {
		t.Fatalf("CountDLQEntries queue %q = %d, want 1: the queue is an exact match, not a prefix", q, n)
	}
}

func testArtifactsExactMatchScope(t *testing.T, s ListStore) {
	app := uniq("app")
	org := uniq("org")

	short := listArtifact(app, artifact.Durable)
	short.ScopeOrgID = org
	long := listArtifact(app+"0", artifact.Durable)
	long.ScopeOrgID = org + "0"
	createArtifacts(t, s, short, long)

	ctx := context.Background()
	for _, tc := range []struct {
		what string
		opts artifact.PageOpts
		want []string
	}{
		{"app", artifact.PageOpts{ScopeAppID: app}, []string{short.ID.String()}},
		{"org", artifact.PageOpts{ScopeOrgID: org}, []string{short.ID.String()}},
		{"long app", artifact.PageOpts{ScopeAppID: app + "0"}, []string{long.ID.String()}},
	} {
		page, err := s.ListArtifactsPage(ctx, tc.opts)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		assertIDs(t, tc.what, artifactIDs(page.Artifacts), tc.want)
	}
}

func testArtifactsInvalidCursor(t *testing.T, s ListStore) {
	for _, cursor := range []string{"not-an-id", id.NewJobID().String()} {
		_, err := s.ListArtifactsPage(context.Background(), artifact.PageOpts{Cursor: cursor})
		if !errors.Is(err, paging.ErrInvalidCursor) {
			t.Errorf("ListArtifactsPage(cursor %q) error = %v, want paging.ErrInvalidCursor", cursor, err)
		}
	}
}

func testDLQEmptyScopeMeansAllTenants(t *testing.T, s ListStore) {
	q := uniq("q")
	now := time.Now().UTC().Truncate(time.Millisecond)
	scoped := listEntry("scoped", q, now)
	scoped.ScopeAppID, scoped.ScopeOrgID = uniq("app"), uniq("org")
	unscoped := listEntry("unscoped", q, now)
	pushAll(t, s, scoped, unscoped)

	// Pinned on purpose: an empty scope is "every tenant", not "none".
	page, err := s.ListDLQPage(context.Background(), dlq.PageOpts{Queue: q})
	if err != nil {
		t.Fatalf("ListDLQPage: %v", err)
	}
	assertIDs(t, "empty scope", entryIDs(page.Entries), entriesNewestFirst(scoped, unscoped))
}

// testArtifactsEmptyScopeMeansAllTenants has no unique filter to isolate
// itself with, so it pages from the top with no scope until it has seen both
// of its artifacts, and gives up after a bounded number of pages.
func testArtifactsEmptyScopeMeansAllTenants(t *testing.T, s ListStore) {
	scoped := listArtifact(uniq("app"), artifact.Durable)
	scoped.ScopeOrgID = uniq("org")
	unscoped := listArtifact("", artifact.Durable)
	createArtifacts(t, s, scoped, unscoped)

	seen := map[string]bool{}
	cursor := ""
	for pageNo := 0; pageNo < 20; pageNo++ {
		page, err := s.ListArtifactsPage(context.Background(), artifact.PageOpts{Cursor: cursor, Limit: 50})
		if err != nil {
			t.Fatalf("page %d: %v", pageNo+1, err)
		}
		for _, a := range page.Artifacts {
			seen[a.ID.String()] = true
		}
		if seen[scoped.ID.String()] && seen[unscoped.ID.String()] {
			return
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	t.Fatalf("an empty scope did not list both artifacts: scoped seen %v, unscoped seen %v",
		seen[scoped.ID.String()], seen[unscoped.ID.String()])
}
