package jobtest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// GiB is one gibibyte in bytes, the unit the memory and disk cases use.
const GiB = int64(1) << 30

// RunDequeueSuite runs the resource-aware dequeue conformance suite
// against a backend.
//
// newStore is called once per subtest and receives that subtest's
// *testing.T, so a backend that stands up a container or opens a
// database can register teardown on the T that owns it. Closing over the
// parent T instead would hold every subtest's resources open until the
// whole suite finished, and would turn a setup t.Fatalf into a FailNow on
// a parent test.
//
// newStore may return the same underlying store on every call. Each case
// enqueues onto its own queue and asserts only on the jobs it created, so
// cases cannot interfere — which matters because starting a fresh
// Postgres, Mongo, or Redis container per subtest would dominate the
// runtime of the whole suite.
func RunDequeueSuite(t *testing.T, newStore func(t *testing.T) job.Store) {
	t.Helper()

	cases := []struct {
		name string
		fn   func(*testing.T, job.Store)
	}{
		{"ZeroBudgetSelectsEverything", testZeroBudgetSelectsEverything},
		{"MemoryBudgetFilters", testMemoryBudgetFilters},
		{"CPUBudgetFilters", testCPUBudgetFilters},
		{"DiskBudgetFilters", testDiskBudgetFilters},
		{"GPUBudgetFilters", testGPUBudgetFilters},
		{"AbsentBudgetKeyIsUnconstrained", testAbsentBudgetKeyIsUnconstrained},
		{"AbsentCustomKeysAreUnconstrained", testAbsentCustomKeysAreUnconstrained},
		{"ExplicitZeroBudgetKeyStillFilters", testExplicitZeroBudgetKeyStillFilters},
		{"ZeroRequirementAlwaysFits", testZeroRequirementAlwaysFits},
		{"ExactFitIsClaimable", testExactFitIsClaimable},
		{"CustomKeyContainmentFilters", testCustomKeyContainmentFilters},
		{"CustomKeyPrefixDoesNotFalselyMatch", testCustomKeyPrefixDoesNotFalselyMatch},
		{"CustomKeySubsetOfOfferedKeysIsClaimable", testCustomKeySubsetIsClaimable},
		{"PriorityOrderingPreservedWithinBudget", testPriorityOrderingPreservedWithinBudget},
		{"PreferHashesSortsFirstButNeverFilters", testPreferHashesSortsFirstButNeverFilters},
		{"ReservedForRestrictsToOneJob", testReservedForRestrictsToOneJob},
		{"ClaimIsAtomicUnderConcurrency", testClaimIsAtomicUnderConcurrency},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

// ──────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────

// runAtBase is the anchor every fixture's RunAt is expressed against. It
// is an hour in the past so every job is ready immediately, and every
// case that asserts ordering derives its RunAt from this anchor with an
// explicit offset. Ordering is therefore a property of the data, never of
// how long the backend took to answer the previous call.
func runAtBase() time.Time {
	return time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
}

// option mutates a fixture before it is enqueued.
type option func(*job.Job)

// withPriority sets the job's scheduling priority.
func withPriority(p int) option {
	return func(j *job.Job) { j.Priority = p }
}

// withRunAtOffset moves the job's RunAt forward from the shared anchor.
// Offsets must stay under an hour so the job remains ready to run.
func withRunAtOffset(d time.Duration) option {
	return func(j *job.Job) { j.RunAt = runAtBase().Add(d) }
}

// withHash sets the locality signal PreferHashes matches against.
func withHash(h string) option {
	return func(j *job.Job) { j.PrimaryInputHash = h }
}

// newJob builds a pending job that is ready to run now, on the given
// queue, requiring res. name is echoed in every failure message, so it
// should describe the job's role in the case.
func newJob(name, queue string, res resource.Set, opts ...option) *job.Job {
	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      queue,
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      runAtBase(),
		Resources:  res,
	}

	for _, opt := range opts {
		opt(j)
	}

	return j
}

// ──────────────────────────────────────────────────
// Assertions
// ──────────────────────────────────────────────────

func mustEnqueue(t *testing.T, s job.Store, jobs ...*job.Job) {
	t.Helper()

	for _, j := range jobs {
		if err := s.EnqueueJob(context.Background(), j); err != nil {
			t.Fatalf("EnqueueJob(%s): %v", j.Name, err)
		}
	}
}

func mustDequeue(t *testing.T, s job.Store, opts job.DequeueOpts) []*job.Job {
	t.Helper()

	got, err := s.DequeueJobs(context.Background(), opts)
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}

	return got
}

func names(jobs []*job.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Name)
	}

	return out
}

// wantExactly asserts the claimed set is exactly want, ignoring order.
func wantExactly(t *testing.T, got []*job.Job, want ...string) {
	t.Helper()

	seen := make(map[string]int, len(got))
	for _, j := range got {
		seen[j.Name]++
	}

	for _, w := range want {
		switch n := seen[w]; {
		case n == 0:
			t.Errorf("job %q was not claimed; claimed set = %v, want %v", w, names(got), want)
		case n > 1:
			t.Errorf("job %q claimed %d times; claimed set = %v", w, n, names(got))
		}

		delete(seen, w)
	}

	for extra := range seen {
		t.Errorf("job %q was claimed but does not fit; claimed set = %v, want %v",
			extra, names(got), want)
	}
}

// wantOrder asserts the claimed jobs are exactly want, in that order.
func wantOrder(t *testing.T, got []*job.Job, want ...string) {
	t.Helper()

	gotNames := names(got)
	if len(gotNames) != len(want) {
		t.Fatalf("claimed %v, want %v", gotNames, want)
	}

	for i := range want {
		if gotNames[i] != want[i] {
			t.Fatalf("claimed %v, want %v (differs at index %d)", gotNames, want, i)
		}
	}
}

// wantStillClaimable proves a job the predicate rejected was left alone
// rather than claimed and put back. A backend that claims first and
// filters afterwards can pass a set assertion while still having burned a
// write on the job it rejected; here the job must come back on the very
// next unconstrained call.
func wantStillClaimable(t *testing.T, s job.Store, queue string, want ...string) {
	t.Helper()

	got := mustDequeue(t, s, job.DequeueOpts{Queues: []string{queue}, Limit: 100})
	wantExactly(t, got, want...)
}

// ──────────────────────────────────────────────────
// Cases
// ──────────────────────────────────────────────────

// testZeroBudgetSelectsEverything is the backward-compatibility
// guarantee and the most important case in the suite. A caller that
// declares no budget must see exactly what the two-argument call used to
// return, including jobs whose requirements no worker could ever satisfy.
// Anything else silently strands work the moment this option ships.
func testZeroBudgetSelectsEverything(t *testing.T, s job.Store) {
	const queue = "fit-zero-budget"

	undeclared := newJob("undeclared", queue, nil, withRunAtOffset(0))
	small := newJob("small", queue, resource.Set{resource.Memory: GiB}, withRunAtOffset(time.Minute))
	huge := newJob("huge", queue, resource.Set{
		resource.CPU:    64 * resource.MilliScale,
		resource.Memory: 512 * GiB,
		resource.Disk:   4096 * GiB,
		resource.GPU:    8 * resource.MilliScale,
		"fpga":          2,
	}, withRunAtOffset(2*time.Minute))

	mustEnqueue(t, s, undeclared, small, huge)

	opts := job.DequeueOpts{Queues: []string{queue}, Limit: 10}
	if !opts.IsUnbounded() {
		t.Fatalf("DequeueOpts%+v.IsUnbounded() = false, want true", opts)
	}

	wantExactly(t, mustDequeue(t, s, opts), "undeclared", "small", "huge")
}

func testMemoryBudgetFilters(t *testing.T, s job.Store) {
	runDimensionCase(t, s, "fit-memory", resource.Memory, 4*GiB, 2*GiB, 8*GiB)
}

func testCPUBudgetFilters(t *testing.T, s job.Store) {
	runDimensionCase(t, s, "fit-cpu", resource.CPU,
		4*resource.MilliScale, 2*resource.MilliScale, 8*resource.MilliScale)
}

func testDiskBudgetFilters(t *testing.T, s job.Store) {
	runDimensionCase(t, s, "fit-disk", resource.Disk, 100*GiB, 10*GiB, 400*GiB)
}

func testGPUBudgetFilters(t *testing.T, s job.Store) {
	runDimensionCase(t, s, "fit-gpu", resource.GPU,
		2*resource.MilliScale, resource.MilliScale, 4*resource.MilliScale)
}

// runDimensionCase proves one dimension filters on its own. Each
// dimension gets its own case because every backend stores the four as
// four separate indexed columns, and a copy-paste slip that compares
// req_memory_bytes twice and req_disk_bytes never would otherwise show up
// only in production.
func runDimensionCase(t *testing.T, s job.Store, queue, key string, budget, fitting, exceeding int64) {
	t.Helper()

	fits := newJob("fits", queue, resource.Set{key: fitting}, withRunAtOffset(0))
	exceeds := newJob("exceeds", queue, resource.Set{key: exceeding}, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, fits, exceeds)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{key: budget},
	})

	wantExactly(t, got, "fits")
	wantStillClaimable(t, s, queue, "exceeds")
}

// testAbsentBudgetKeyIsUnconstrained pins the inversion of
// resource.Set.Fits that the store-side predicate deliberately makes: an
// absent budget key means "not constrained", not "zero available".
//
// A worker that declares only memory must still claim GPU-requiring
// jobs. Otherwise adding a dimension to one worker's config would
// silently strand work on every worker whose config had not been updated
// yet, and the failure would look like a queue that stopped draining for
// no reason.
func testAbsentBudgetKeyIsUnconstrained(t *testing.T, s job.Store) {
	const queue = "fit-absent-key"

	gpuHeavy := newJob("gpu-heavy", queue, resource.Set{
		resource.Memory: GiB,
		resource.GPU:    8 * resource.MilliScale,
	}, withRunAtOffset(0))

	// The declared dimension must keep filtering. An implementation that
	// read "absent key is unconstrained" as "any missing key disables the
	// predicate" would claim this one too.
	tooBig := newJob("too-big", queue, resource.Set{
		resource.Memory: 64 * GiB,
		resource.GPU:    resource.MilliScale,
	}, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, gpuHeavy, tooBig)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{resource.Memory: 4 * GiB},
	})

	wantExactly(t, got, "gpu-heavy")
}

// testAbsentCustomKeysAreUnconstrained applies the absent-key rule to
// the custom dimension, and closes the discontinuity a backend is most
// likely to introduce here.
//
// A caller that declares a budget but no custom keys must keep claiming
// custom-key jobs. The tempting implementation — matching the job's
// stored key list against an offered list that happens to be empty —
// excludes every custom-key job the moment any budget is set, so a caller
// would go from claiming everything to stranding all specialised work by
// adding a memory budget. Backends must skip the containment clause
// entirely when the offer is empty.
func testAbsentCustomKeysAreUnconstrained(t *testing.T, s job.Store) {
	const queue = "fit-absent-custom"

	needsFPGA := newJob("needs-fpga", queue, resource.Set{
		resource.Memory: GiB,
		"fpga":          1,
	}, withRunAtOffset(0))
	plain := newJob("plain", queue, resource.Set{resource.Memory: GiB}, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, needsFPGA, plain)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{resource.Memory: 4 * GiB},
	})

	wantExactly(t, got, "needs-fpga", "plain")
}

// testExplicitZeroBudgetKeyStillFilters is the other half of the absent
// key rule. A key present with the value zero is a real constraint — a
// worker with no free memory — and must exclude anything needing more
// than zero of it. A backend that decides "unbounded" by asking whether
// the budget is all zeros (resource.Set.IsZero) instead of whether the
// key is present would hand an exhausted worker the whole queue.
func testExplicitZeroBudgetKeyStillFilters(t *testing.T, s job.Store) {
	const queue = "fit-explicit-zero"

	needsMemory := newJob("needs-memory", queue, resource.Set{resource.Memory: 1}, withRunAtOffset(0))
	needsNothing := newJob("needs-nothing", queue, nil, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, needsMemory, needsNothing)

	opts := job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{resource.Memory: 0},
	}

	if opts.IsUnbounded() {
		t.Fatalf("DequeueOpts with an explicit zero memory budget reports IsUnbounded() = true")
	}

	wantExactly(t, mustDequeue(t, s, opts), "needs-nothing")
	wantStillClaimable(t, s, queue, "needs-memory")
}

// testZeroRequirementAlwaysFits covers the job every deployment has most
// of: one that declares nothing. It must be claimable under any budget,
// including a budget of zero on every dimension.
//
// The two fixtures differ only in their write path, and that difference
// is not cosmetic. Mongo's insert path writes resource_requests as an
// explicit BSON null while its update path omits the key entirely, so a
// predicate that tests only one of the two shapes passes here on a
// freshly enqueued job and drops every job that has ever been updated —
// which, in production, is every job that was ever retried.
func testZeroRequirementAlwaysFits(t *testing.T, s job.Store) {
	ctx := context.Background()

	const queue = "fit-zero-requirement"

	fresh := newJob("never-updated", queue, nil, withRunAtOffset(0))
	updated := newJob("updated-after-enqueue", queue, nil, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, fresh, updated)

	stored, err := s.GetJob(ctx, updated.ID)
	if err != nil {
		t.Fatalf("GetJob(%s): %v", updated.Name, err)
	}

	if err = s.UpdateJob(ctx, stored); err != nil {
		t.Fatalf("UpdateJob(%s): %v", updated.Name, err)
	}

	// An exhausted worker: every dimension present, every one at zero.
	got := mustDequeue(t, s, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{
			resource.CPU:    0,
			resource.Memory: 0,
			resource.Disk:   0,
			resource.GPU:    0,
		},
	})

	wantExactly(t, got, "never-updated", "updated-after-enqueue")
}

// testExactFitIsClaimable pins the comparison as requirement <= budget,
// not <. A job needing exactly the free capacity must be claimable, or
// the last slot on every worker is silently unusable — a rounding error
// that costs a fixed fraction of the fleet forever.
func testExactFitIsClaimable(t *testing.T, s job.Store) {
	const queue = "fit-exact"

	exact := newJob("exact", queue, resource.Set{
		resource.CPU:    2 * resource.MilliScale,
		resource.Memory: 4 * GiB,
	}, withRunAtOffset(0))

	// One byte over the same budget. If this is claimed the comparison is
	// the wrong way round; if "exact" is dropped the comparison is <.
	overByOne := newJob("over-by-one", queue, resource.Set{
		resource.CPU:    2 * resource.MilliScale,
		resource.Memory: 4*GiB + 1,
	}, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, exact, overByOne)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
		Budget: resource.Set{
			resource.CPU:    2 * resource.MilliScale,
			resource.Memory: 4 * GiB,
		},
	})

	wantExactly(t, got, "exact")
}

// testCustomKeyContainmentFilters proves a job needing a custom key the
// caller does not offer is not claimed, and that offering a key is what
// makes it claimable. Only key containment is tested at dequeue; the
// quantity is settled locally after the claim.
func testCustomKeyContainmentFilters(t *testing.T, s job.Store) {
	const queue = "fit-custom-containment"

	plain := newJob("plain", queue, resource.Set{resource.Memory: GiB}, withRunAtOffset(0))
	needsTPU := newJob("needs-tpu", queue, resource.Set{
		resource.Memory: GiB,
		"tpu":           1,
	}, withRunAtOffset(time.Minute))
	needsFPGA := newJob("needs-fpga", queue, resource.Set{
		resource.Memory: GiB,
		"fpga":          1,
	}, withRunAtOffset(2*time.Minute))

	mustEnqueue(t, s, plain, needsTPU, needsFPGA)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      10,
		Budget:     resource.Set{resource.Memory: 8 * GiB},
		CustomKeys: []string{"tpu"},
	})

	// The quantity is deliberately not compared: needs-tpu asks for one
	// TPU and the caller offered the key without a count.
	wantExactly(t, got, "plain", "needs-tpu")
	wantStillClaimable(t, s, queue, "needs-fpga")
}

// testCustomKeyPrefixDoesNotFalselyMatch is why
// resource.EncodeCustomKeys wraps its output in leading and trailing
// separators. A caller offering "fpga-large" must not claim a job needing
// "fpga", and a caller offering "fpga" must not claim a job needing
// "fpga-large".
//
// A backend that implements containment as a bare LIKE '%fpga%' — or as
// strings.Contains on the unwrapped list — passes every other custom-key
// case in this suite and fails this one. Both directions are checked in a
// single call, because a backend can get one right by accident.
func testCustomKeyPrefixDoesNotFalselyMatch(t *testing.T, s job.Store) {
	const queue = "fit-custom-prefix"

	needsFPGA := newJob("needs-fpga", queue, resource.Set{"fpga": 1}, withRunAtOffset(0))
	needsFPGALarge := newJob("needs-fpga-large", queue,
		resource.Set{"fpga-large": 1}, withRunAtOffset(time.Minute))

	mustEnqueue(t, s, needsFPGA, needsFPGALarge)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues:     []string{queue},
		Limit:      10,
		CustomKeys: []string{"fpga-large"},
	})

	wantExactly(t, got, "needs-fpga-large")
	wantStillClaimable(t, s, queue, "needs-fpga")
}

// testCustomKeySubsetIsClaimable pins containment as a genuine subset
// test rather than a substring one.
//
// Both the job's required keys and the caller's offered keys are stored
// sorted, so a backend tempted to write `offered LIKE '%' || required ||
// '%'` gets the single-key cases right and then drops a job needing
// {fpga, tpu} from a caller offering {fpga, nvme, tpu}, because the
// interleaved key breaks the contiguous run. That failure strands
// precisely the specialised job that is hardest to place elsewhere.
//
// A portable exact formulation for SQL backends: strip each offered key
// from the stored list with nested REPLACE calls — one per offered key,
// built in Go since the offered set is a parameter — always replacing
// ",key," with ",", and require that what remains is "" or ",".
func testCustomKeySubsetIsClaimable(t *testing.T, s job.Store) {
	const (
		superset = "fit-custom-superset"
		partial  = "fit-custom-partial"
	)

	both := newJob("needs-fpga-and-tpu", superset, resource.Set{
		"fpga": 1,
		"tpu":  1,
	}, withRunAtOffset(0))

	mustEnqueue(t, s, both)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues:     []string{superset},
		Limit:      10,
		CustomKeys: []string{"fpga", "nvme", "tpu"},
	})

	wantExactly(t, got, "needs-fpga-and-tpu")

	// The other half: offering some of what a job needs is not enough.
	half := newJob("needs-both-offered-one", partial, resource.Set{
		"fpga": 1,
		"tpu":  1,
	}, withRunAtOffset(0))

	mustEnqueue(t, s, half)

	none := mustDequeue(t, s, job.DequeueOpts{
		Queues:     []string{partial},
		Limit:      10,
		CustomKeys: []string{"fpga"},
	})

	wantExactly(t, none)
	wantStillClaimable(t, s, partial, "needs-both-offered-one")
}

// testPriorityOrderingPreservedWithinBudget proves the fit predicate did
// not cost the queue its ordering, and — through the oversized fixture —
// that the predicate runs inside the claim rather than over the rows the
// claim returned.
//
// The oversized job carries the highest priority and does not fit. With a
// limit of four, a backend that claims the top four rows and then drops
// the ones that do not fit returns three jobs and leaves the low-priority
// one behind, so the length assertion alone catches claim-then-filter.
func testPriorityOrderingPreservedWithinBudget(t *testing.T, s job.Store) {
	const queue = "fit-priority-order"

	oversized := newJob("oversized", queue, resource.Set{resource.Memory: 64 * GiB},
		withPriority(100), withRunAtOffset(0))
	high := newJob("high", queue, resource.Set{resource.Memory: GiB},
		withPriority(9), withRunAtOffset(time.Minute))
	midEarly := newJob("mid-early", queue, resource.Set{resource.Memory: GiB},
		withPriority(5), withRunAtOffset(2*time.Minute))
	midLate := newJob("mid-late", queue, resource.Set{resource.Memory: GiB},
		withPriority(5), withRunAtOffset(3*time.Minute))
	low := newJob("low", queue, resource.Set{resource.Memory: GiB},
		withPriority(1), withRunAtOffset(4*time.Minute))

	mustEnqueue(t, s, oversized, high, midEarly, midLate, low)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues: []string{queue},
		Limit:  4,
		Budget: resource.Set{resource.Memory: 4 * GiB},
	})

	wantOrder(t, got, "high", "mid-early", "mid-late", "low")
	wantStillClaimable(t, s, queue, "oversized")
}

// testPreferHashesSortsFirstButNeverFilters covers the locality signal.
//
// A job whose PrimaryInputHash the caller already has staged sorts ahead
// of its equals, but it never displaces a higher-priority job and it
// never excludes anything. Both halves matter: a locality signal that
// could reorder across priority bands would let a steady stream of
// locally cached work starve the high-priority job the pool exists to run
// first, and a locality signal that filtered would strand every job whose
// inputs happen to be cold.
func testPreferHashesSortsFirstButNeverFilters(t *testing.T, s job.Store) {
	const (
		queue = "fit-prefer-hashes"
		local = "blake3:cached-locally"
	)

	urgent := newJob("urgent-remote", queue, resource.Set{resource.Memory: GiB},
		withPriority(5), withRunAtOffset(0), withHash("blake3:elsewhere"))
	early := newJob("early-remote", queue, resource.Set{resource.Memory: GiB},
		withPriority(1), withRunAtOffset(time.Minute), withHash("blake3:also-elsewhere"))
	mid := newJob("mid-unhashed", queue, resource.Set{resource.Memory: GiB},
		withPriority(1), withRunAtOffset(2*time.Minute))
	late := newJob("late-local", queue, resource.Set{resource.Memory: GiB},
		withPriority(1), withRunAtOffset(3*time.Minute), withHash(local))

	mustEnqueue(t, s, urgent, early, mid, late)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues:       []string{queue},
		Limit:        10,
		Budget:       resource.Set{resource.Memory: 4 * GiB},
		PreferHashes: []string{local},
	})

	// urgent outranks late-local on priority even though late-local is the
	// one already staged; late-local then jumps its own priority band; the
	// remaining two keep RunAt order. Nothing is filtered out.
	wantOrder(t, got, "urgent-remote", "late-local", "early-remote", "mid-unhashed")
}

// testReservedForRestrictsToOneJob proves a targeted claim returns that
// job and nothing else — and that it is still subject to the budget. A
// reservation that could bypass the fit test would reintroduce exactly
// the overcommit the predicate exists to prevent, through the one code
// path a scheduler is most likely to use for a large job.
func testReservedForRestrictsToOneJob(t *testing.T, s job.Store) {
	const queue = "fit-reserved"

	first := newJob("first", queue, resource.Set{resource.Memory: GiB}, withRunAtOffset(0))
	target := newJob("target", queue, resource.Set{resource.Memory: GiB}, withRunAtOffset(time.Minute))
	third := newJob("third", queue, resource.Set{resource.Memory: GiB}, withRunAtOffset(2*time.Minute))

	mustEnqueue(t, s, first, target, third)

	got := mustDequeue(t, s, job.DequeueOpts{
		Queues:      []string{queue},
		Limit:       10,
		Budget:      resource.Set{resource.Memory: 4 * GiB},
		ReservedFor: &target.ID,
	})

	wantExactly(t, got, "target")

	// Reserved, but the caller has no room for it.
	none := mustDequeue(t, s, job.DequeueOpts{
		Queues:      []string{queue},
		Limit:       10,
		Budget:      resource.Set{resource.Memory: 0},
		ReservedFor: &third.ID,
	})

	wantExactly(t, none)
	wantStillClaimable(t, s, queue, "first", "third")
}

// testClaimIsAtomicUnderConcurrency proves the predicate did not cost the
// claim its atomicity — the one property the whole store contract rests
// on, since a job handed to two workers is run twice.
//
// The assertion is an invariant, not a timing guess: whichever way the
// goroutines interleave, every job must end up claimed exactly once. A
// correct backend can never violate that, so a correct backend can never
// flake here. A backend that selects candidates and then updates them in
// a separate statement violates it whenever two scans overlap, which is
// what makes the case worth running.
func testClaimIsAtomicUnderConcurrency(t *testing.T, s job.Store) {
	const (
		queue    = "fit-concurrent"
		jobCount = 20
		claimers = 4
	)

	mine := make(map[id.JobID]string, jobCount)

	for i := range jobCount {
		j := newJob(fmt.Sprintf("concurrent-%d", i), queue,
			resource.Set{resource.Memory: GiB}, withRunAtOffset(time.Duration(i)*time.Second))

		mustEnqueue(t, s, j)

		mine[j.ID] = j.Name
	}

	var (
		mu     sync.Mutex
		claims = make(map[id.JobID]int, jobCount)
		wg     sync.WaitGroup
	)

	errCh := make(chan error, claimers)

	for range claimers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Every claimer asks for the whole batch under a budget that
			// admits every job, so the only thing that can stop a job being
			// claimed exactly once is the backend's own locking.
			got, err := s.DequeueJobs(context.Background(), job.DequeueOpts{
				Queues: []string{queue},
				Limit:  jobCount,
				Budget: resource.Set{resource.Memory: 4 * GiB},
			})
			if err != nil {
				errCh <- err

				return
			}

			mu.Lock()
			defer mu.Unlock()

			for _, j := range got {
				claims[j.ID]++
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent DequeueJobs: %v", err)
	}

	for jobID, name := range mine {
		switch n := claims[jobID]; {
		case n == 0:
			t.Errorf("job %s was never claimed", name)
		case n > 1:
			t.Errorf("job %s claimed %d times, want exactly 1 — the claim is not atomic", name, n)
		}
	}
}
