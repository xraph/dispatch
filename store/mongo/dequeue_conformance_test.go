package mongo_test

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/storetest"
)

// TestDequeueConformance runs the resource-aware dequeue suite against the
// Mongo store.
//
// One container is stood up and shared by every subtest, which the suite
// documents as safe: each case enqueues onto its own queue and asserts
// only on the jobs it created. A container per subtest would dominate the
// runtime of the whole package.
func TestDequeueConformance(t *testing.T) {
	uri := startMongo(t)
	shared := openStore(t, uri)

	storetest.RunDequeueSuite(t, func(t *testing.T) job.Store {
		t.Helper()

		return shared
	})
}

// TestDequeueOrdersNullPrimaryInputHashAsUnpreferred covers the one
// document shape the shared suite cannot produce: jobModel.
// PrimaryInputHash is a plain string, so no Go path can write a null or
// leave the key out.
//
// It matters because Mongo would get this backwards by default. A missing
// field reads as BSON null, and null sorts BEFORE strings ascending —
// under the descending locality term an uncoalesced sort on the raw hash
// would rank exactly the documents with NO locality signal above the ones
// the caller has already staged, inverting the optimization. The
// implementation therefore sorts on a computed 0/1 whose $ifNull maps
// both shapes to "not preferred".
//
// Both shapes are written here with the raw driver: an explicit null and
// an absent key. Both carry an earlier RunAt than the staged job, so they
// win any tie the ordering fails to break — if the guard is dropped they
// come back first.
//
// The fourth fixture, "remote", is what stops the case being satisfied by
// accident. Its hash is a real string that sorts ABOVE the staged one
// descending, so a backend that sorted on primary_input_hash itself — the
// obvious way to write "preferred first" with a field-path sort, and the
// one Mongo will happily accept — hands back a job the caller has no
// local copy of, ahead of the one it staged. Without it, a raw-field sort
// produces the correct answer for the wrong reason: BSON null orders
// below every string, so the single staged hash would lead regardless.
func TestDequeueOrdersNullPrimaryInputHashAsUnpreferred(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	rawDB := rawDatabase(t, uri)
	ctx := context.Background()

	const (
		queue = "null-hash-order"
		local = "blake3:staged-here"
	)

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	nullHash := newMongoHashFixture("null-hash", queue, base)
	missingHash := newMongoHashFixture("missing-hash", queue, base.Add(time.Minute))
	cached := newMongoHashFixture("cached", queue, base.Add(2*time.Minute))
	cached.PrimaryInputHash = local
	remote := newMongoHashFixture("remote", queue, base.Add(3*time.Minute))
	remote.PrimaryInputHash = "zzz:never-staged"

	for _, j := range []*job.Job{nullHash, missingHash, cached, remote} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	col := rawDB.Collection("dispatch_jobs")

	if _, err := col.UpdateByID(ctx, nullHash.ID.String(),
		bson.M{"$set": bson.M{"primary_input_hash": nil}}); err != nil {
		t.Fatalf("null out primary_input_hash: %v", err)
	}

	if _, err := col.UpdateByID(ctx, missingHash.ID.String(),
		bson.M{"$unset": bson.M{"primary_input_hash": ""}}); err != nil {
		t.Fatalf("unset primary_input_hash: %v", err)
	}

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:       []string{queue},
		Limit:        10,
		PreferHashes: []string{local},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 4 {
		t.Fatalf("claimed %d jobs, want 4 — locality must never filter", len(got))
	}

	want := []string{"cached", "null-hash", "missing-hash", "remote"}
	for i, name := range want {
		if got[i].Name != name {
			t.Fatalf("claimed %v, want %v: a null or absent primary_input_hash must sort "+
				"as NOT preferred, never ahead of the job the caller has staged",
				mongoJobNames(got), want)
		}
	}
}

// TestUndeclaredJobMatchesNullEqualityOnBothWritePaths is the empirical
// proof behind the filter's null branch, and behind the warning carried
// on jobModel.ResourceRequests.
//
// Mongo stores two different shapes for the same logical "declares
// nothing" state, because the two write paths differ: EnqueueJob goes
// through grove's structToMapInsert, which reflects over grove tags and
// never consults the bson tag, so `omitempty` has no effect and the key
// is written PRESENT-and-null; UpdateJob hands the struct to the raw
// driver's ReplaceOne, which honours `omitempty` and drops the key
// ENTIRELY.
//
// The test drives both paths and then asserts, against the real
// documents, that:
//
//   - a single plain equality against nil matches BOTH shapes, so one
//     clause covers both write paths and no $or is needed;
//   - $exists:false matches only the ReplaceOne shape, which is why the
//     filter does not use it — a predicate built on $exists:false would
//     silently drop every job still on its original inserted document;
//   - a type-bracketed range operator matches NEITHER, which is the
//     reason the scalar comparisons in dequeueFilter carry a null branch
//     at all rather than relying on {$lte: n} alone;
//   - the four scalar req_* fields and req_custom_keys are present as
//     real values on BOTH paths, which is what makes them, rather than
//     the resource_requests subdocument, the safe basis for the fit
//     predicate.
func TestUndeclaredJobMatchesNullEqualityOnBothWritePaths(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	rawDB := rawDatabase(t, uri)
	ctx := context.Background()

	const queue = "null-shape-proof"

	inserted := newMongoHashFixture("inserted", queue, time.Now().UTC().Add(-time.Hour))
	replaced := newMongoHashFixture("replaced", queue, time.Now().UTC().Add(-time.Hour))

	for _, j := range []*job.Job{inserted, replaced} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	// Round-trip the second one through the ReplaceOne write path.
	stored, err := s.GetJob(ctx, replaced.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if err = s.UpdateJob(ctx, stored); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	col := rawDB.Collection("dispatch_jobs")
	ids := bson.M{"$in": []string{inserted.ID.String(), replaced.ID.String()}}

	// The two shapes really are different, or the rest proves nothing.
	var insertedDoc, replacedDoc bson.M

	if err = col.FindOne(ctx, bson.M{"_id": inserted.ID.String()}).Decode(&insertedDoc); err != nil {
		t.Fatalf("read inserted doc: %v", err)
	}

	if err = col.FindOne(ctx, bson.M{"_id": replaced.ID.String()}).Decode(&replacedDoc); err != nil {
		t.Fatalf("read replaced doc: %v", err)
	}

	if v, ok := insertedDoc["resource_requests"]; !ok || v != nil {
		t.Fatalf("inserted resource_requests = %#v (present=%t), want present-and-null", v, ok)
	}

	if v, ok := replacedDoc["resource_requests"]; ok {
		t.Fatalf("replaced resource_requests = %#v, want key absent", v)
	}

	// One equality clause, both shapes.
	count, err := col.CountDocuments(ctx, bson.M{"_id": ids, "resource_requests": nil})
	if err != nil {
		t.Fatalf("count null-equality: %v", err)
	}

	if count != 2 {
		t.Errorf("{resource_requests: nil} matched %d/2 documents; a single equality against "+
			"null must cover both the present-and-null and the absent shape", count)
	}

	// $exists:false sees only the ReplaceOne shape — the trap.
	count, err = col.CountDocuments(ctx,
		bson.M{"_id": ids, "resource_requests": bson.M{"$exists": false}})
	if err != nil {
		t.Fatalf("count $exists:false: %v", err)
	}

	if count != 1 {
		t.Errorf("{resource_requests: {$exists: false}} matched %d/2 documents, want 1; "+
			"if this ever matches both, the write paths converged", count)
	}

	// Range operators are type-bracketed: null is outside the numeric
	// bracket, so {$lte: n} matches neither a null nor an absent field.
	// This is exactly why dequeueFilter pairs each scalar comparison with
	// a null branch.
	count, err = col.CountDocuments(ctx,
		bson.M{"_id": ids, "resource_requests": bson.M{"$lte": bson.M{}}})
	if err != nil {
		t.Fatalf("count type-bracketed range: %v", err)
	}

	if count != 0 {
		t.Errorf("a range operator matched %d/2 null-or-absent documents, want 0", count)
	}

	// And the scalars the fit predicate actually compares are real values
	// on both write paths.
	for name, doc := range map[string]bson.M{"inserted": insertedDoc, "replaced": replacedDoc} {
		for _, field := range []string{
			"req_cpu_milli", "req_memory_bytes", "req_disk_bytes", "req_gpu_milli",
		} {
			v, ok := doc[field]
			if !ok {
				t.Errorf("%s document is missing %s; the fit predicate compares it numerically", name, field)

				continue
			}

			if got := toInt64(v); got != 0 {
				t.Errorf("%s document %s = %v, want 0", name, field, v)
			}
		}

		if v, ok := doc["req_custom_keys"]; !ok || v != "" {
			t.Errorf("%s document req_custom_keys = %#v (present=%t), want present and empty", name, v, ok)
		}
	}
}

// TestDequeueClaimsDocumentsWrittenBeforeResourceFieldsExisted is what
// makes the filter's null branches load-bearing.
//
// A job written by a build that predates the req_* fields carries none of
// them, and a job carrying none of them declares no requirement at all —
// it must stay claimable by every worker, exactly like the freshly
// enqueued undeclared job. Mongo does not agree by default: range
// operators are type-bracketed, so {$lte: 0} rejects an absent field, and
// $split raises outright on a null input rather than reading it as the
// empty list. Either would strand every pre-existing job in the
// collection the moment a resource-aware worker started polling — a queue
// that simply stops draining, with nothing in the system reporting why.
//
// The document is degraded with the raw driver because no Go path can
// produce it: toJobModel always populates all five fields.
func TestDequeueClaimsDocumentsWrittenBeforeResourceFieldsExisted(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	rawDB := rawDatabase(t, uri)
	ctx := context.Background()

	const queue = "pre-resource-fields"

	legacy := newMongoHashFixture("legacy", queue, time.Now().UTC().Add(-time.Hour))
	if err := s.EnqueueJob(ctx, legacy); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := rawDB.Collection("dispatch_jobs").UpdateByID(ctx, legacy.ID.String(),
		bson.M{"$unset": bson.M{
			"req_cpu_milli":     "",
			"req_memory_bytes":  "",
			"req_disk_bytes":    "",
			"req_gpu_milli":     "",
			"req_custom_keys":   "",
			"resource_requests": "",
		}}); err != nil {
		t.Fatalf("strip resource fields: %v", err)
	}

	// An exhausted, resource-aware worker: every dimension present and
	// zero, no custom resources offered.
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{
			resource.CPU:    0,
			resource.Memory: 0,
			resource.Disk:   0,
			resource.GPU:    0,
		},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 1 || got[0].Name != "legacy" {
		t.Fatalf("claimed %v, want [legacy]: a document with no req_* fields declares "+
			"no requirement and must fit any budget", mongoJobNames(got))
	}
}

// TestDequeueClaimsCustomKeySupersetJobsUnderRealDocuments is a
// belt-and-braces check on the $setIsSubset containment test against
// documents that also carry canonical dimensions, which is the shape a
// substring formulation gets wrong in production but not in the suite's
// custom-key-only fixtures.
func TestDequeueClaimsCustomKeySupersetJobsUnderRealDocuments(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()

	const queue = "custom-subset-mixed"

	base := time.Now().UTC().Add(-time.Hour)

	multi := newMongoHashFixture("needs-fpga-and-tpu", queue, base)
	multi.Resources = resource.Set{
		resource.CPU:    2 * resource.MilliScale,
		resource.Memory: storetest.GiB,
		"fpga":          1,
		"tpu":           1,
	}

	prefix := newMongoHashFixture("needs-fpga-large", queue, base.Add(time.Minute))
	prefix.Resources = resource.Set{resource.Memory: storetest.GiB, "fpga-large": 1}

	for _, j := range []*job.Job{multi, prefix} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	got, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      10,
		Budget:     resource.Set{resource.CPU: 4 * resource.MilliScale, resource.Memory: 4 * storetest.GiB},
		CustomKeys: []string{"fpga", "nvme", "tpu"},
	})
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	if len(got) != 1 || got[0].Name != "needs-fpga-and-tpu" {
		t.Fatalf("claimed %v, want [needs-fpga-and-tpu]: containment is a subset test, "+
			"and \"fpga\" must not match a worker offering only \"fpga-large\"", mongoJobNames(got))
	}
}

func newMongoHashFixture(name, queue string, runAt time.Time) *job.Job {
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

func mongoJobNames(jobs []*job.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Name)
	}

	return out
}
