package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

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
