//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/store/storetest"
)

func TestListConformance(t *testing.T) {
	// One container, shared keyspace: the suite isolates its cases by
	// queue, name prefix and scope, so one store per case on the same
	// Redis is what it expects.
	connStr := startRedis(t)

	storetest.RunListSuite(t, func(t *testing.T) storetest.ListStore {
		t.Helper()

		return openRedisStore(t, connStr)
	})
}

func listedJobIDs(jobs []*job.Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID.String()
	}

	return out
}

// A Redis written by the release before this one has jobs and their ID
// set but no created-order index. The first list call must build the
// index from the set, or every job written before the upgrade would be
// missing from the dashboard.
func TestListJobs_backfillsIndexForPreexistingRows(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)

	q := "backfill-" + id.NewJobID().String()
	older := storetest.PendingJob("before-the-index-1", q, 0)
	newer := storetest.PendingJob("before-the-index-2", q, 0)
	for _, j := range []*job.Job{older, newer} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	if err := kvStore.Delete(ctx, "dispatch:job_by_created"); err != nil {
		t.Fatalf("drop the index to simulate pre-release data: %v", err)
	}

	page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	want := []string{newer.ID.String(), older.ID.String()}
	if got := listedJobIDs(page.Jobs); !slices.Equal(got, want) {
		t.Fatalf("jobs after backfill:\n got  %v\n want %v", got, want)
	}
	if !page.Complete {
		t.Fatal("backfilled page reported incomplete")
	}

	n, err := kvStore.ZCard(ctx, "dispatch:job_by_created")
	if err != nil {
		t.Fatalf("ZCARD index: %v", err)
	}
	if n != 2 {
		t.Fatalf("index holds %d members after the backfill, want 2", n)
	}
}

// During a rolling upgrade a process still on the previous release keeps
// adding jobs to the ID set without touching the index, after this
// release has already built it. The next list call must notice the set
// has outgrown the index and pick those jobs up.
func TestListJobs_picksUpRowsWrittenByThePreviousRelease(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)

	q := "rollout-" + id.NewJobID().String()
	current := storetest.PendingJob("written-by-this-release", q, 0)
	if err := s.EnqueueJob(ctx, current); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Use the index once, so it exists before the old process writes.
	page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q})
	if err != nil {
		t.Fatalf("first ListJobs: %v", err)
	}
	if got := listedJobIDs(page.Jobs); !slices.Equal(got, []string{current.ID.String()}) {
		t.Fatalf("first page = %v, want only %s", got, current.ID)
	}

	// What the previous release's EnqueueJob leaves behind: the entity and
	// its ID-set membership, and nothing in the created-order index.
	old := storetest.PendingJob("written-by-the-previous-release", q, 0)
	raw, err := json.Marshal(map[string]any{
		"id":          old.ID.String(),
		"name":        old.Name,
		"queue":       old.Queue,
		"payload":     old.Payload,
		"state":       string(old.State),
		"max_retries": old.MaxRetries,
		"run_at":      old.RunAt,
		"created_at":  old.CreatedAt,
		"updated_at":  old.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal old-release job: %v", err)
	}
	rdb := redisdriver.UnwrapClient(kvStore)
	if setErr := rdb.Set(ctx, "dispatch:job:"+old.ID.String(), raw, 0).Err(); setErr != nil {
		t.Fatalf("SET old-release job: %v", setErr)
	}
	if addErr := rdb.SAdd(ctx, "dispatch:job_ids", old.ID.String()).Err(); addErr != nil {
		t.Fatalf("SADD old-release job: %v", addErr)
	}

	page, err = s.ListJobs(ctx, job.ListJobsOpts{Queue: q})
	if err != nil {
		t.Fatalf("ListJobs after the old-release write: %v", err)
	}
	want := []string{old.ID.String(), current.ID.String()}
	if got := listedJobIDs(page.Jobs); !slices.Equal(got, want) {
		t.Fatalf("jobs after the old-release write:\n got  %v\n want %v", got, want)
	}
}

// When a filter matches rarely, a page can run out of scan budget before
// it fills. It must say so, hand back a cursor that is never empty, and
// a client following the cursors must see every match exactly once.
func TestListJobs_reportsIncompleteScanAndContinues(t *testing.T) {
	ctx := context.Background()
	s := redisstore.New(setupTestKV(t))
	s.SetListScanBudgetForTest(10)

	q := "rare-" + id.NewJobID().String()
	noise := "noise-" + id.NewJobID().String()

	// Oldest first: a match, twelve others, a match, twelve others, a
	// match. Newest first with a budget of ten, no single call can reach
	// two matches.
	matches := make([]*job.Job, 0, 3)
	for i := range 3 {
		if i > 0 {
			for k := range 12 {
				if err := s.EnqueueJob(ctx, storetest.PendingJob(fmt.Sprintf("noise-%d-%d", i, k), noise, 0)); err != nil {
					t.Fatalf("enqueue noise: %v", err)
				}
			}
		}
		m := storetest.PendingJob(fmt.Sprintf("match-%d", i), q, 0)
		if err := s.EnqueueJob(ctx, m); err != nil {
			t.Fatalf("enqueue match: %v", err)
		}
		matches = append(matches, m)
	}

	var (
		got        []string
		incomplete int
		cursor     string
	)
	for pageNo := 1; ; pageNo++ {
		if pageNo > 10 {
			t.Fatal("more than 10 pages for 27 jobs at a budget of 10: the cursor is not advancing")
		}

		page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, Cursor: cursor, Limit: 5})
		if err != nil {
			t.Fatalf("page %d: %v", pageNo, err)
		}
		got = append(got, listedJobIDs(page.Jobs)...)

		if !page.Complete {
			incomplete++
			if page.NextCursor == "" {
				t.Fatalf("page %d is incomplete with no cursor to continue from", pageNo)
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	if incomplete == 0 {
		t.Fatal("no page reported an incomplete scan; the budget is not being applied")
	}
	want := listedJobIDs(matches)
	slices.Reverse(want)
	if !slices.Equal(got, want) {
		t.Fatalf("matches across pages:\n got  %v\n want %v", got, want)
	}
}

// Two tenants on one Redis each list only their own jobs. On an
// unprefixed index both stores would walk the same sorted set.
func TestStore_KeyPrefix_listsAreIsolated(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	a := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_a:"))
	b := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_b:"))

	ja := storetest.PendingJob("tenant-a-only", "default", 0)
	if err := a.EnqueueJob(ctx, ja); err != nil {
		t.Fatalf("enqueue on a: %v", err)
	}
	jb := storetest.PendingJob("tenant-b-only", "default", 0)
	if err := b.EnqueueJob(ctx, jb); err != nil {
		t.Fatalf("enqueue on b: %v", err)
	}

	for _, tc := range []struct {
		tenant string
		store  *redisstore.Store
		want   string
	}{
		{"a", a, ja.ID.String()},
		{"b", b, jb.ID.String()},
	} {
		page, err := tc.store.ListJobs(ctx, job.ListJobsOpts{})
		if err != nil {
			t.Fatalf("ListJobs on %s: %v", tc.tenant, err)
		}
		if got := listedJobIDs(page.Jobs); !slices.Equal(got, []string{tc.want}) {
			t.Fatalf("tenant %s listed %v, want only its own %s", tc.tenant, got, tc.want)
		}
	}
}

// sameMillisecondJobIDs mints n job IDs that all carry the same creation
// millisecond, retrying until a run of n fits inside one. Minting is
// microseconds, so the first or second attempt nearly always does.
func sameMillisecondJobIDs(t *testing.T, n int) []id.ID {
	t.Helper()

	for range 100 {
		ids := make([]id.ID, n)
		for i := range ids {
			ids[i] = id.NewJobID()
		}
		if ids[0].Time().Equal(ids[n-1].Time()) {
			return ids
		}
	}
	t.Fatalf("could not mint %d job IDs inside one millisecond", n)

	return nil
}

// IDs minted in one millisecond share a score, so the index orders them
// by their bytes alone. Paging through more of them than one range read
// returns exercises the step over members a cursor has already passed.
func TestListJobs_pagesThroughIDsFromOneMillisecond(t *testing.T) {
	ctx := context.Background()
	s := redisstore.New(setupTestKV(t))
	s.SetListScanBudgetForTest(10)

	q := "same-ms-" + id.NewJobID().String()
	ids := sameMillisecondJobIDs(t, 25)
	for i, jobID := range ids {
		j := storetest.PendingJob(fmt.Sprintf("same-ms-%d", i), q, 0)
		j.ID = jobID
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var got []string
	cursor := ""
	for pageNo := 1; ; pageNo++ {
		if pageNo > 20 {
			t.Fatal("more than 20 pages for 25 jobs at limit 3: the cursor is not advancing")
		}

		page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: q, Cursor: cursor, Limit: 3})
		if err != nil {
			t.Fatalf("page %d: %v", pageNo, err)
		}
		got = append(got, listedJobIDs(page.Jobs)...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	want := make([]string, len(ids))
	for i, jobID := range ids {
		want[len(ids)-1-i] = jobID.String()
	}
	if !slices.Equal(got, want) {
		t.Fatalf("same-millisecond jobs across pages:\n got  %v\n want %v", got, want)
	}
}
