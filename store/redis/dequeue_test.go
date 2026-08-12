//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/store/storetest"
)

// rawJobKey mirrors the unexported jobKey helper. These tests live in the
// external test package and have to reach the stored blob directly to
// produce shapes no Go write path can produce, so the key convention is
// restated here rather than exported from the package under test.
func rawJobKey(jobID id.JobID) string { return "dispatch:job:" + jobID.String() }

// TestDequeueConformance runs the resource-aware dequeue suite against
// the Redis store.
//
// One container is stood up and shared by every subtest, which the suite
// documents as safe: each case enqueues onto its own queue and asserts
// only on the jobs it created. A container per subtest would dominate the
// runtime of the whole package.
func TestDequeueConformance(t *testing.T) {
	shared := setupTestStore(t)

	storetest.RunDequeueSuite(t, func(t *testing.T) job.Store {
		t.Helper()

		return shared
	})
}

// newRawFitJob builds a pending job ready to run now, for the two cases
// below that patch the stored blob afterwards.
func newRawFitJob(name, queue string, runAt time.Time) *job.Job {
	return &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      queue,
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      runAt,
	}
}

// patchJobBlob rewrites the stored JSON for one job by applying mutate to
// its decoded object form. Going through a map rather than jobEntity is
// the point: it can delete a key entirely, which no Go struct write path
// in this package can do.
func patchJobBlob(t *testing.T, s *redisstore.Store, jobID id.JobID, mutate func(map[string]any)) {
	t.Helper()

	ctx := context.Background()
	client := redisdriver.UnwrapClient(s.KV())
	key := rawJobKey(jobID)

	raw, err := client.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatalf("read raw blob for %s: %v", jobID, err)
	}

	var blob map[string]any
	if err = json.Unmarshal(raw, &blob); err != nil {
		t.Fatalf("decode raw blob for %s: %v", jobID, err)
	}

	mutate(blob)

	patched, err := json.Marshal(blob)
	if err != nil {
		t.Fatalf("encode patched blob for %s: %v", jobID, err)
	}

	if err = client.Set(ctx, key, patched, 0).Err(); err != nil {
		t.Fatalf("write patched blob for %s: %v", jobID, err)
	}
}

func rawJobNames(jobs []*job.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Name)
	}

	return out
}

// TestDequeueClaimsJobsWrittenBeforeTheResourceFields is the first of the
// two gaps the shared conformance suite structurally cannot cover: it
// only ever creates jobs through EnqueueJob, so every blob it produces
// carries the full current field set. A deployment that has been running
// since before the resource model shipped has blobs that do not.
//
// Such a blob has no req_cpu_milli, no req_memory_bytes, no
// req_disk_bytes, no req_gpu_milli, no req_custom_keys, and no
// resource_requests key at all. It declares NO requirement, so a
// resource-aware worker must still claim it. The two ways to get that
// wrong are to read an absent field as a parse failure and drop the job,
// or to read an absent custom-key list as "unknown, therefore reject"
// under bounded opts — either one silently strands every pre-upgrade job
// in the queue, with nothing reporting it.
//
// Redis reaches the right answer through the decode path rather than
// through a filter clause, which is why this case is worth pinning
// explicitly: encoding/json leaves an absent field at its zero value, and
// resource.DecodeSet maps both a null and an absent resource_requests to
// a nil Set, so the reconstructed job requires nothing and
// job.DequeueOpts.Allows admits it. Mongo needed explicit {field: nil}
// branches for the same property because its range operators are
// type-bracketed; Redis needs none, and this test is what keeps that true.
//
// The opts are deliberately BOUNDED — a budget plus an empty CustomKeys
// offer — because unbounded opts skip the predicate entirely and would
// claim the job no matter how badly the decode handled the missing keys.
func TestDequeueClaimsJobsWrittenBeforeTheResourceFields(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const queue = "redis-legacy-blob"

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	legacy := newRawFitJob("legacy-no-resource-fields", queue, base)
	nulled := newRawFitJob("legacy-null-resource-requests", queue, base.Add(time.Minute))

	for _, j := range []*job.Job{legacy, nulled} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	// The pre-upgrade shape: none of the resource keys exist.
	patchJobBlob(t, s, legacy.ID, func(blob map[string]any) {
		for _, field := range []string{
			"req_cpu_milli", "req_memory_bytes", "req_disk_bytes", "req_gpu_milli",
			"req_custom_keys", "resource_requests", "resource_limits",
		} {
			delete(blob, field)
		}
	})

	// The other shape a hand-written or older blob can carry: the key is
	// present and explicitly null. resource.DecodeSet must read it the
	// same way it reads an absent key.
	patchJobBlob(t, s, nulled.ID, func(blob map[string]any) {
		blob["resource_requests"] = nil
		blob["req_custom_keys"] = nil
	})

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{resource.Memory: 4 * storetest.GiB},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("claimed %v, want both legacy jobs: a job written before the req_* fields "+
			"existed declares no requirement and must stay claimable", rawJobNames(got))
	}

	for _, j := range got {
		if len(j.Resources) != 0 {
			t.Errorf("job %s came back requiring %v, want nothing", j.Name, j.Resources)
		}
	}
}

// TestDequeueOrdersNullPrimaryInputHashAsUnpreferred is the second gap.
// jobEntity.PrimaryInputHash is a plain string, so no Go write path in
// this package can produce a blob whose primary_input_hash is null or
// absent — the shared suite cannot construct the shape at all.
//
// A job with no hash has no locality signal and must therefore sort as
// NOT preferred: behind the job the caller has already staged, and
// otherwise by the ordinary rules. The failure mode this guards is
// implementing "preferred first" as a sort on the hash VALUE, which has
// nothing to do with whether the caller staged it.
//
// The "remote" fixture is what stops the case passing for the wrong
// reason, and it is not optional. Mongo found that transplanting
// Postgres's version of this test produced a FALSE POSITIVE: with only
// one hash value in play, the empty/null hash collates BELOW it, so a
// broken value sort still put the staged job first. "remote" carries
// "zzz:never-staged", which collates ABOVE "blake3:staged-here", so a
// value sort hands back the job the caller has NO local copy of, first.
// Both fixtures with no signal are also enqueued EARLIER than the staged
// one, so they win any tie an unimplemented locality term fails to break.
//
// Mutation-verified: replacing the contract comparator with a descending
// sort on PrimaryInputHash within a priority band yields
// [remote cached null-hash missing-hash] and fails here; treating any
// non-empty hash as preferred yields [cached remote null-hash
// missing-hash] and fails here too.
func TestDequeueOrdersNullPrimaryInputHashAsUnpreferred(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const (
		queue = "redis-null-hash-order"
		local = "blake3:staged-here"
	)

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	nullHash := newRawFitJob("null-hash", queue, base)
	missingHash := newRawFitJob("missing-hash", queue, base.Add(time.Minute))

	cached := newRawFitJob("cached", queue, base.Add(2*time.Minute))
	cached.PrimaryInputHash = local

	remote := newRawFitJob("remote", queue, base.Add(3*time.Minute))
	remote.PrimaryInputHash = "zzz:never-staged"

	for _, j := range []*job.Job{nullHash, missingHash, cached, remote} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	patchJobBlob(t, s, nullHash.ID, func(blob map[string]any) {
		blob["primary_input_hash"] = nil
	})

	patchJobBlob(t, s, missingHash.ID, func(blob map[string]any) {
		delete(blob, "primary_input_hash")
	})

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:       []string{queue},
		Limit:        10,
		PreferHashes: []string{local},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 4 {
		t.Fatalf("claimed %d jobs (%v), want 4 — locality must never filter",
			len(got), rawJobNames(got))
	}

	want := []string{"cached", "null-hash", "missing-hash", "remote"}
	for i, name := range want {
		if got[i].Name != name {
			t.Fatalf("claimed %v, want %v: a null or absent primary_input_hash must sort as "+
				"NOT preferred, never ahead of the job the caller has staged",
				rawJobNames(got), want)
		}
	}
}
