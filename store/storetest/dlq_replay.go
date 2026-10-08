package storetest

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
)

// DLQReplayStore is what the replay suite requires: the base DLQ store
// plus the claim capability operator replays and retries are built on.
type DLQReplayStore interface {
	dlq.Store
	dlq.ReplayClaimer
}

// concurrentClaimers is how many goroutines race for one claim, and
// concurrentRounds how many fresh entries (or runs) they race over.
// raceAttempts holds the goroutines at a spin barrier so they really call
// together. With it, a claim written as a read then a separate write lost
// against the memory store in 10 of 10 runs under -race, and 7 of 10
// without. A container backend puts a round trip between the read and the
// write, which only widens the window.
const (
	concurrentClaimers = 16
	concurrentRounds   = 10
)

// raceResult counts how a set of simultaneous attempts came out.
type raceResult struct {
	n       int
	wins    int
	winner  int
	refused int
	other   []error
}

// raceAttempts starts n goroutines together, each calling attempt with
// its index, and sorts the outcomes into wins, refusals (errors wrapping
// refusal) and anything else.
func raceAttempts(n int, refusal error, attempt func(i int) error) raceResult {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		ready atomic.Int64
		res   = raceResult{n: n, winner: -1}
	)

	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Spin until every goroutine is running, so they call
			// attempt together rather than in the order they woke.
			ready.Add(1)
			for ready.Load() < int64(n) {
				runtime.Gosched()
			}

			err := attempt(i)

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				res.wins++
				res.winner = i
			case errors.Is(err, refusal):
				res.refused++
			default:
				res.other = append(res.other, err)
			}
		}()
	}
	wg.Wait()

	return res
}

// assertOneWinner fails unless exactly one attempt won and every other one
// was refused.
func (r raceResult) assertOneWinner(t *testing.T, label string) {
	t.Helper()

	if len(r.other) > 0 {
		t.Fatalf("%s: unexpected errors: %v", label, r.other)
	}
	if r.wins != 1 || r.refused != r.n-1 {
		t.Fatalf("%s: %d of %d won and %d were refused, want exactly 1 and %d",
			label, r.wins, r.n, r.refused, r.n-1)
	}
}

// RunDLQReplaySuite pins the replay claim: the first claim wins and every
// later one is refused, a release only undoes its own claim, the job a
// replay created reads back from every path, and a push never overwrites.
//
// Run it with `go test -race`. ClaimReplayConcurrentExactlyOneWinner is
// the case that proves the claim is atomic, and an unguarded
// read-then-write only loses that race reliably under the detector.
//
// newStore may return a shared store, so every case works on entries it
// pushed itself under a queue nobody else uses, and never asserts a total.
func RunDLQReplaySuite(t *testing.T, newStore func(t *testing.T) DLQReplayStore) {
	t.Helper()

	cases := []struct {
		name string
		fn   func(t *testing.T, s DLQReplayStore)
	}{
		{"ClaimReplayFirstWins", testClaimReplayFirstWins},
		{"ClaimReplayConcurrentExactlyOneWinner", testClaimReplayConcurrent},
		{"ClaimReplayUnknownEntry", testClaimReplayUnknownEntry},
		{"ReplayedJobIDReadsBackFromEveryPath", testReplayedJobIDReadsBack},
		{"ReleaseReplayOnlyReleasesItsOwnClaim", testReleaseReplayOnlyOwnClaim},
		{"ReleaseReplayOnUnclaimedEntryIsNoop", testReleaseReplayUnclaimed},
		{"ReleaseReplayUnknownEntry", testReleaseReplayUnknownEntry},
		{"GetDLQByJobIDReturnsNewestByID", testGetDLQByJobIDNewest},
		{"GetDLQByJobIDUnknownJob", testGetDLQByJobIDUnknown},
		{"DeleteDLQRemovesOnlyThatEntry", testDeleteDLQ},
		{"PushDLQRefusesDuplicateID", testPushDLQDuplicate},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

// replayEntry builds an unreplayed entry for jobID on queue.
func replayEntry(jobID id.JobID, queue string, failedAt time.Time) *dlq.Entry {
	return &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      jobID,
		JobName:    "replay-suite",
		Queue:      queue,
		Payload:    []byte(`{"n":1}`),
		Error:      "boom",
		RetryCount: 3,
		MaxRetries: 3,
		FailedAt:   failedAt,
		CreatedAt:  failedAt,
	}
}

// replayQueue is a queue name no other case, and no other run against a
// shared store, uses.
func replayQueue() string {
	return "dlq-replay-" + id.NewDLQID().String()
}

func pushReplayEntry(t *testing.T, s DLQReplayStore) *dlq.Entry {
	t.Helper()

	now := time.Now().UTC().Truncate(time.Millisecond)
	e := replayEntry(id.NewJobID(), replayQueue(), now)
	if err := s.PushDLQ(context.Background(), e); err != nil {
		t.Fatalf("PushDLQ: %v", err)
	}

	return e
}

// mustGetDLQ reads an entry and returns a copy of it, so a store that
// hands out its own pointer cannot change a snapshot behind the test.
func mustGetDLQ(t *testing.T, s DLQReplayStore, entryID id.DLQID) *dlq.Entry {
	t.Helper()

	got, err := s.GetDLQ(context.Background(), entryID)
	if err != nil {
		t.Fatalf("GetDLQ(%s): %v", entryID, err)
	}
	snapshot := *got

	return &snapshot
}

// assertClaimedBy checks the entry is replayed by exactly jobID.
func assertClaimedBy(t *testing.T, label string, e *dlq.Entry, jobID id.JobID) {
	t.Helper()

	if e.ReplayedAt == nil {
		t.Errorf("%s: ReplayedAt = nil, want set", label)
	}
	if e.ReplayedJobID == nil {
		t.Fatalf("%s: ReplayedJobID = nil, want %s", label, jobID)
	}
	if *e.ReplayedJobID != jobID {
		t.Errorf("%s: ReplayedJobID = %s, want %s", label, *e.ReplayedJobID, jobID)
	}
}

// assertUnclaimed checks the entry carries no replay at all.
func assertUnclaimed(t *testing.T, label string, e *dlq.Entry) {
	t.Helper()

	if e.ReplayedAt != nil {
		t.Errorf("%s: ReplayedAt = %v, want nil", label, *e.ReplayedAt)
	}
	if e.ReplayedJobID != nil {
		t.Errorf("%s: ReplayedJobID = %s, want nil", label, *e.ReplayedJobID)
	}
}

func testClaimReplayFirstWins(t *testing.T, s DLQReplayStore) {
	ctx := context.Background()
	e := pushReplayEntry(t, s)
	first, second := id.NewJobID(), id.NewJobID()

	if err := s.ClaimReplay(ctx, e.ID, first); err != nil {
		t.Fatalf("first ClaimReplay: %v", err)
	}

	err := s.ClaimReplay(ctx, e.ID, second)
	if !errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
		t.Fatalf("second ClaimReplay error = %v, want ErrDLQAlreadyReplayed", err)
	}

	// The refused claim must not have moved the claim to its own job.
	assertClaimedBy(t, "after refused claim", mustGetDLQ(t, s, e.ID), first)
}

func testClaimReplayConcurrent(t *testing.T, s DLQReplayStore) {
	ctx := context.Background()

	for round := range concurrentRounds {
		e := pushReplayEntry(t, s)
		claimers := make([]id.JobID, concurrentClaimers)
		for i := range claimers {
			claimers[i] = id.NewJobID()
		}

		res := raceAttempts(concurrentClaimers, dispatch.ErrDLQAlreadyReplayed, func(i int) error {
			return s.ClaimReplay(ctx, e.ID, claimers[i])
		})
		res.assertOneWinner(t, fmt.Sprintf("round %d: concurrent ClaimReplay", round))

		assertClaimedBy(t, fmt.Sprintf("round %d", round), mustGetDLQ(t, s, e.ID), claimers[res.winner])
	}
}

func testClaimReplayUnknownEntry(t *testing.T, s DLQReplayStore) {
	err := s.ClaimReplay(context.Background(), id.NewDLQID(), id.NewJobID())
	if !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Fatalf("ClaimReplay(unknown) error = %v, want ErrDLQNotFound", err)
	}
}

// testReplayedJobIDReadsBack reads the claimed entry through every read
// path. On some backends GetDLQ, ListDLQ, ListDLQPage and GetDLQByJobID
// are separate queries over the same mapper, and a column added to one
// and forgotten in another reads back as nil without any error.
func testReplayedJobIDReadsBack(t *testing.T, s DLQReplayStore) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	queue := replayQueue()

	claimed := replayEntry(id.NewJobID(), queue, now)
	untouched := replayEntry(id.NewJobID(), queue, now)
	for _, e := range []*dlq.Entry{claimed, untouched} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("PushDLQ: %v", err)
		}
	}

	newJob := id.NewJobID()
	if err := s.ClaimReplay(ctx, claimed.ID, newJob); err != nil {
		t.Fatalf("ClaimReplay: %v", err)
	}

	assertClaimedBy(t, "GetDLQ", mustGetDLQ(t, s, claimed.ID), newJob)
	assertUnclaimed(t, "GetDLQ untouched", mustGetDLQ(t, s, untouched.ID))

	byJob, err := s.GetDLQByJobID(ctx, claimed.JobID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}
	assertClaimedBy(t, "GetDLQByJobID", byJob, newJob)

	listed, err := s.ListDLQ(ctx, dlq.ListOpts{Queue: queue})
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("ListDLQ returned %d entries on its own queue, want 2", len(listed))
	}
	for _, e := range listed {
		if e.ID == claimed.ID {
			assertClaimedBy(t, "ListDLQ", e, newJob)
		} else {
			assertUnclaimed(t, "ListDLQ untouched", e)
		}
	}

	// The paged read is the dashboard's path. store.Store includes it, but
	// DLQReplayStore does not, so reach it through the capability.
	pager, ok := s.(dlq.PageLister)
	if !ok {
		return
	}
	replayed := true
	page, err := pager.ListDLQPage(ctx, dlq.PageOpts{Queue: queue, Replayed: &replayed})
	if err != nil {
		t.Fatalf("ListDLQPage: %v", err)
	}
	if len(page.Entries) != 1 || page.Entries[0].ID != claimed.ID {
		t.Fatalf("ListDLQPage(replayed) = %d entries, want only the claimed one", len(page.Entries))
	}
	assertClaimedBy(t, "ListDLQPage", page.Entries[0], newJob)
}

func testReleaseReplayOnlyOwnClaim(t *testing.T, s DLQReplayStore) {
	ctx := context.Background()
	e := pushReplayEntry(t, s)
	owner, stranger := id.NewJobID(), id.NewJobID()

	if err := s.ClaimReplay(ctx, e.ID, owner); err != nil {
		t.Fatalf("ClaimReplay: %v", err)
	}

	// A release for a job that does not hold the claim is a no-op: it is
	// how a replay whose enqueue failed cleans up, and by then someone
	// else may legitimately own the entry.
	if err := s.ReleaseReplay(ctx, e.ID, stranger); err != nil {
		t.Fatalf("ReleaseReplay(stranger): %v", err)
	}
	assertClaimedBy(t, "after stranger release", mustGetDLQ(t, s, e.ID), owner)

	if err := s.ReleaseReplay(ctx, e.ID, owner); err != nil {
		t.Fatalf("ReleaseReplay(owner): %v", err)
	}
	assertUnclaimed(t, "after owner release", mustGetDLQ(t, s, e.ID))

	// Released means claimable again.
	next := id.NewJobID()
	if err := s.ClaimReplay(ctx, e.ID, next); err != nil {
		t.Fatalf("ClaimReplay after release: %v", err)
	}
	assertClaimedBy(t, "after reclaim", mustGetDLQ(t, s, e.ID), next)
}

func testReleaseReplayUnclaimed(t *testing.T, s DLQReplayStore) {
	e := pushReplayEntry(t, s)

	if err := s.ReleaseReplay(context.Background(), e.ID, id.NewJobID()); err != nil {
		t.Fatalf("ReleaseReplay(unclaimed): %v", err)
	}
	assertUnclaimed(t, "after release of unclaimed", mustGetDLQ(t, s, e.ID))
}

func testReleaseReplayUnknownEntry(t *testing.T, s DLQReplayStore) {
	err := s.ReleaseReplay(context.Background(), id.NewDLQID(), id.NewJobID())
	if !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Fatalf("ReleaseReplay(unknown) error = %v, want ErrDLQNotFound", err)
	}
}

// testGetDLQByJobIDNewest pushes two entries for one job, the older ID
// with the later FailedAt, so a backend that picks by FailedAt instead of
// by ID returns the wrong one.
func testGetDLQByJobIDNewest(t *testing.T, s DLQReplayStore) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	queue := replayQueue()
	jobID := id.NewJobID()

	older := replayEntry(jobID, queue, now)
	newer := replayEntry(jobID, queue, now.Add(-time.Hour))
	newer.Error = "second failure"
	neighbour := replayEntry(id.NewJobID(), queue, now)

	for _, e := range []*dlq.Entry{older, newer, neighbour} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("PushDLQ: %v", err)
		}
	}

	got, err := s.GetDLQByJobID(ctx, jobID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}
	if got.ID != newer.ID {
		t.Fatalf("GetDLQByJobID = %s, want the newer entry %s (older %s)", got.ID, newer.ID, older.ID)
	}
	if got.JobID != jobID || got.Error != newer.Error || got.Queue != queue {
		t.Errorf("GetDLQByJobID = {job %s, error %q, queue %q}, want {job %s, error %q, queue %q}",
			got.JobID, got.Error, got.Queue, jobID, newer.Error, queue)
	}
}

func testGetDLQByJobIDUnknown(t *testing.T, s DLQReplayStore) {
	_, err := s.GetDLQByJobID(context.Background(), id.NewJobID())
	if !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Fatalf("GetDLQByJobID(unknown) error = %v, want ErrDLQNotFound", err)
	}
}

func testDeleteDLQ(t *testing.T, s DLQReplayStore) {
	ctx := context.Background()
	gone := pushReplayEntry(t, s)
	kept := pushReplayEntry(t, s)

	if err := s.DeleteDLQ(ctx, gone.ID); err != nil {
		t.Fatalf("DeleteDLQ: %v", err)
	}

	if _, err := s.GetDLQ(ctx, gone.ID); !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Errorf("GetDLQ(deleted) error = %v, want ErrDLQNotFound", err)
	}
	if _, err := s.GetDLQByJobID(ctx, gone.JobID); !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Errorf("GetDLQByJobID(deleted) error = %v, want ErrDLQNotFound", err)
	}
	mustGetDLQ(t, s, kept.ID)

	if err := s.DeleteDLQ(ctx, gone.ID); !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Errorf("second DeleteDLQ error = %v, want ErrDLQNotFound", err)
	}
}

func testPushDLQDuplicate(t *testing.T, s DLQReplayStore) {
	ctx := context.Background()
	e := pushReplayEntry(t, s)

	dup := *e
	dup.Error = "overwritten"
	dup.Queue = replayQueue()

	err := s.PushDLQ(ctx, &dup)
	if !errors.Is(err, dispatch.ErrDLQAlreadyExists) {
		t.Fatalf("PushDLQ(duplicate ID) error = %v, want ErrDLQAlreadyExists", err)
	}

	got := mustGetDLQ(t, s, e.ID)
	if got.Error != e.Error || got.Queue != e.Queue {
		t.Errorf("after refused push: {error %q, queue %q}, want the original {%q, %q}",
			got.Error, got.Queue, e.Error, e.Queue)
	}
}
