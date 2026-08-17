package redis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestReclaimExpiredLeasesNonPositiveLimitReclaimsNothing pins the unified
// non-positive-limit contract for job.LeaseStore.ReclaimExpiredLeases: a
// limit <= 0 claims nothing and returns (nil, nil), and — critically —
// leaves the expired job still reclaimable, so a later call with a
// positive limit still returns it.
func TestReclaimExpiredLeasesNonPositiveLimitReclaimsNothing(t *testing.T) {
	s := openReapRedis(t)
	ctx := context.Background()

	for _, limit := range []int{0, -1} {
		queue := fmt.Sprintf("reclaim-nonpositive-%d", limit)
		j := storetest.RunningJob("expired", queue, 0)
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("limit=%d: enqueue: %v", limit, err)
		}

		got, err := s.ReclaimExpiredLeases(ctx, limit)
		if err != nil {
			t.Fatalf("limit=%d: ReclaimExpiredLeases: %v", limit, err)
		}
		if len(got) != 0 {
			t.Fatalf("limit=%d: reclaimed %d jobs, want 0", limit, len(got))
		}

		after, err := s.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatalf("limit=%d: get: %v", limit, err)
		}
		if after.State != job.StateRunning {
			t.Fatalf("limit=%d: State = %s, want still running (nothing reclaimed)", limit, after.State)
		}

		// The job must still be reclaimable: a non-positive limit must not
		// have silently consumed it.
		reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
		if err != nil {
			t.Fatalf("limit=%d: follow-up ReclaimExpiredLeases: %v", limit, err)
		}
		if !storetest.Contains(reclaimed, j.ID) {
			t.Fatalf("limit=%d: job not reclaimed by a follow-up call with a positive limit", limit)
		}
	}
}

func TestLeaseConformance(t *testing.T) {
	// One container, shared keyspace — do not use openReapRedis here, which
	// calls startRedis on every invocation and would spin twelve containers.
	connStr := startRedis(t)

	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openRedisStore(t, connStr)
	})
}

// TestLeaseLargeDurationRoundTrip is a regression test for a corruption
// bug in the original decode-mutate-cjson.encode-SET implementation of
// RenewLease and ReclaimExpiredLeases: cjson represents every JSON number
// as a Lua double, and doubles stop representing int64 nanosecond
// durations exactly past 2^53 (~104 days). Redis's cjson goes further and
// silently reformats a value that large as scientific notation (e.g.
// `1e+16`) on encode, which encoding/json then refuses to parse back into
// an int64 at all — not a rounding error, a hard unmarshal failure on the
// very next read of the row.
//
// None of the 20 conformance cases exercise a duration anywhere near that
// size, which is why the suite never caught it. This test uses a
// Timeout/LeaseTTL of 200 days (comfortably past the 2^53ns boundary) and
// asserts both fields come back byte-for-byte exact after a renewal and
// after a reclaim — plus that the reclaimed job is actually back on the
// queue, since the bug's failure mode aborted ReclaimExpiredLeases before
// it reached the requeue step.
func TestLeaseLargeDurationRoundTrip(t *testing.T) {
	s := openReapRedis(t)
	ctx := context.Background()

	// 200 days in nanoseconds is ~1.728e16, comfortably past 2^53
	// (~9.007e15, ~104.25 days) where cjson's double-precision numbers
	// stop representing int64 nanosecond counts exactly.
	const bigDuration = 200 * 24 * time.Hour
	const queue = "lease-large-duration"

	j := storetest.PendingJob("large-duration", queue, bigDuration)
	j.Timeout = bigDuration
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	worker := id.NewWorkerID()
	now := time.Now().UTC()

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(got))
	}
	if got[0].Timeout != bigDuration {
		t.Fatalf("Timeout after dequeue = %v, want %v", got[0].Timeout, bigDuration)
	}
	if got[0].LeaseTTL != bigDuration {
		t.Fatalf("LeaseTTL after dequeue = %v, want %v", got[0].LeaseTTL, bigDuration)
	}

	// 1. Renew the lease, then read the job back and check both large
	// durations survived byte-for-byte.
	if renewErr := s.RenewLease(ctx, got[0].ID, worker, got[0].LeaseEpoch, now.Add(time.Hour)); renewErr != nil {
		t.Fatalf("RenewLease: %v", renewErr)
	}

	afterRenew, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get after renew: %v", err)
	}
	if afterRenew.Timeout != bigDuration {
		t.Errorf("Timeout after renew = %v, want %v (exact)", afterRenew.Timeout, bigDuration)
	}
	if afterRenew.LeaseTTL != bigDuration {
		t.Errorf("LeaseTTL after renew = %v, want %v (exact)", afterRenew.LeaseTTL, bigDuration)
	}

	// Force the lease into the past so it is eligible for reclamation.
	// RenewLease only checks state/worker/epoch, not that leaseUntil is
	// in the future, so this is a legitimate way to simulate expiry
	// without reaching into the store's internals.
	expired := now.Add(-time.Second)
	if renewErr := s.RenewLease(ctx, j.ID, worker, afterRenew.LeaseEpoch, expired); renewErr != nil {
		t.Fatalf("RenewLease into the past: %v", renewErr)
	}

	// 2. Reclaim the expired lease, then read the job back and check both
	// large durations again, and confirm it is actually back on the queue.
	reclaimed, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if !storetest.Contains(reclaimed, j.ID) {
		t.Fatalf("reclaimed set does not contain %s", j.ID)
	}

	afterReclaim, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get after reclaim: %v", err)
	}
	if afterReclaim.Timeout != bigDuration {
		t.Errorf("Timeout after reclaim = %v, want %v (exact)", afterReclaim.Timeout, bigDuration)
	}
	if afterReclaim.LeaseTTL != bigDuration {
		t.Errorf("LeaseTTL after reclaim = %v, want %v (exact)", afterReclaim.LeaseTTL, bigDuration)
	}

	requeued, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      1,
		WorkerID:   worker,
		LeaseUntil: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("DequeueJobs after reclaim: %v", err)
	}
	if !storetest.Contains(requeued, j.ID) {
		t.Fatalf("reclaimed job %s was not requeued", j.ID)
	}
}

// TestReclaimAdoptsPreUpgradeRunningJobs covers the fleet upgrade: a job
// that was already running when the lease feature shipped has no lease
// expiry at all, so job.Lease.IsExpired reports false for it (a zero
// expiry means "never leased", not "expired"), the pool no longer calls
// ReapStaleJobs for a lease-capable store, and dequeue claims only pending
// and retrying rows. Nothing would ever look at such a job again.
//
// Redis gets no backfill because it has no migration mechanism to hang one
// on — Migrate is a no-op — so reclamation itself carries a narrow
// compatibility clause instead. The cases below are the boundary of that
// clause, and the negative ones matter more than the positive ones: a
// null expiry does NOT by itself mean the job is abandoned. Any caller
// using job.Store directly without lease options claims a perfectly
// healthy running job with no lease at all (DequeueOpts.Grants() is false
// when LeaseUntil is zero), and stealing that job would be a far worse bug
// than the one being fixed.
func TestReclaimAdoptsPreUpgradeRunningJobs(t *testing.T) {
	s := openReapRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// withHeartbeat returns a pre-upgrade running job — no lease fields —
	// whose last heartbeat is beatAgo old.
	withHeartbeat := func(name string, startedAgo, beatAgo time.Duration) *job.Job {
		j := runningJob(name, startedAgo)
		beat := now.Add(-beatAgo)
		j.HeartbeatAt = &beat

		return j
	}

	// A job with neither timestamp set, so there is nothing to measure age
	// against. Reclaiming on a null expiry alone would take it; the
	// staleness gate is what stops it.
	withoutTimes := func(name string) *job.Job {
		j := runningJob(name, 0)
		j.StartedAt = nil

		return j
	}

	cases := []struct {
		j    *job.Job
		want bool
		why  string
	}{
		{
			j:    withHeartbeat("stale-heartbeat", 30*time.Minute, 20*time.Minute),
			want: true,
			why:  "abandoned by a worker that stopped reporting; this is the bug being fixed",
		},
		{
			// The one that protects a live no-lease caller. Note the start
			// time is old: only the heartbeat says this worker is alive, so
			// this also pins that heartbeat_at takes precedence over
			// started_at rather than both having to be fresh.
			j:    withHeartbeat("fresh-heartbeat", 30*time.Minute, 0),
			want: false,
			why:  "still reporting, so it belongs to a healthy worker",
		},
		{
			j:    runningJob("no-heartbeat-old-start", 20*time.Minute),
			want: true,
			why:  "claimed long ago and never heartbeated: died before its first beat",
		},
		{
			j:    runningJob("no-heartbeat-fresh-start", 0),
			want: false,
			why:  "just claimed; its first heartbeat is not due yet",
		},
		{
			j:    withoutTimes("no-times"),
			want: false,
			why:  "no timestamp to establish age from",
		},
	}

	for _, c := range cases {
		if err := s.EnqueueJob(ctx, c.j); err != nil {
			t.Fatalf("enqueue %s: %v", c.j.Name, err)
		}
	}

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}

	for _, c := range cases {
		got := storetest.Contains(reclaimed, c.j.ID)
		if got == c.want {
			continue
		}
		if c.want {
			t.Errorf("%s was not reclaimed but should have been: %s", c.j.Name, c.why)
		} else {
			t.Errorf("%s was reclaimed but must not be: %s", c.j.Name, c.why)
		}
	}
}

func TestDLQConformance(t *testing.T) {
	connStr := startRedis(t)

	storetest.RunDLQSuite(t, func(t *testing.T) storetest.DLQStore {
		t.Helper()

		return openRedisStore(t, connStr)
	})
}
