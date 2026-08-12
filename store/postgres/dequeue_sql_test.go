package postgres

import (
	"strings"
	"testing"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// The conformance suite that actually proves this backend correct lives
// behind //go:build integration and needs Docker. These tests deliberately
// do not, so `go test ./store/postgres/...` on a machine with no container
// runtime still checks the shape of the statement rather than reporting a
// green package that ran nothing.
//
// They assert the compiled SQL, not behaviour — the database is the only
// thing that can answer for behaviour, and dequeue_test.go asks it.

// TestBuildDequeueQueryUnboundedEmitsOriginalStatement is the
// backward-compatibility guarantee in its narrowest form: opts that
// constrain nothing must compile to exactly the statement that shipped
// before DequeueOpts existed, with no fit predicate at all. A worker not
// using the resource model claims everything, jobs declaring custom
// resources included.
func TestBuildDequeueQueryUnboundedEmitsOriginalStatement(t *testing.T) {
	opts := job.DequeueOpts{Queues: []string{"default"}, Limit: 10}

	if !opts.IsUnbounded() {
		t.Fatalf("DequeueOpts%+v.IsUnbounded() = false, want true", opts)
	}

	query, args := buildDequeueQuery(opts)

	for _, banned := range []string{"req_", "REPLACE", "primary_input_hash"} {
		if strings.Contains(query, banned) {
			t.Errorf("unbounded dequeue emitted %q:\n%s", banned, query)
		}
	}

	if !strings.Contains(query, "ORDER BY priority DESC, run_at ASC\n") {
		t.Errorf("unbounded dequeue lost the original ordering:\n%s", query)
	}

	// Queues and limit, nothing else.
	if len(args) != 2 {
		t.Fatalf("unbounded dequeue bound %d args, want 2: %v", len(args), args)
	}

	if args[1] != 10 {
		t.Errorf("limit bound as %v, want 10", args[1])
	}
}

// TestBuildDequeueQueryAppliesLocalityToUnboundedOpts pins the split
// IsUnbounded exists to make: it governs FILTERING only. Opts carrying
// nothing but PreferHashes are unbounded, so no fit predicate is emitted —
// and the locality term is applied anyway. A backend that derived "should
// I order?" from IsUnbounded would silently drop the signal here.
func TestBuildDequeueQueryAppliesLocalityToUnboundedOpts(t *testing.T) {
	opts := job.DequeueOpts{
		Queues:       []string{"default"},
		Limit:        4,
		PreferHashes: []string{"blake3:staged"},
	}

	if !opts.IsUnbounded() {
		t.Fatal("opts carrying only PreferHashes report IsUnbounded() = false")
	}

	query, _ := buildDequeueQuery(opts)

	if strings.Contains(query, "req_") {
		t.Errorf("PreferHashes emitted a fit predicate — locality must never filter:\n%s", query)
	}

	if !strings.Contains(query, "COALESCE(primary_input_hash = ANY(") {
		t.Errorf("locality term missing from unbounded opts:\n%s", query)
	}
}

// TestBuildDequeueQueryOrdersLocalityBelowPriority pins the one ordering
// mistake that would not show up as a wrong answer, only as starvation:
// priority must come first. Locality above it would let a steady stream of
// low-priority jobs with staged inputs beat a high-priority job with cold
// ones.
//
// It also pins that the ordering is applied to the inner candidate SELECT
// as well as the outer one, so the LIMIT truncates an ordered set.
func TestBuildDequeueQueryOrdersLocalityBelowPriority(t *testing.T) {
	query, _ := buildDequeueQuery(job.DequeueOpts{
		Queues:       []string{"default"},
		Limit:        2,
		PreferHashes: []string{"blake3:staged"},
	})

	const want = "priority DESC, COALESCE(primary_input_hash = ANY($2), FALSE) DESC, run_at ASC"

	if n := strings.Count(query, want); n != 2 {
		t.Fatalf("ordering %q appears %d times, want 2 (inner candidate SELECT and outer SELECT):\n%s",
			want, n, query)
	}

	// The LIMIT must sit after the inner ORDER BY, or it truncates an
	// unordered set and the ordering above is decoration.
	orderAt := strings.Index(query, want)
	limitAt := strings.Index(query, "LIMIT ")

	if orderAt > limitAt {
		t.Errorf("inner LIMIT precedes the inner ORDER BY:\n%s", query)
	}
}

// TestBuildDequeueQueryBindsEveryValue is the injection check. Every value
// the caller controls — queue names, budgets, custom keys, the reserved
// id, hashes, the limit — must reach Postgres as a bind parameter. The
// only thing concatenated into the statement is a column name from
// budgetColumns, all of which are compile-time constants.
func TestBuildDequeueQueryBindsEveryValue(t *testing.T) {
	reserved := id.NewJobID()

	query, args := buildDequeueQuery(job.DequeueOpts{
		Queues:       []string{"q'; DROP TABLE dispatch_jobs; --"},
		Limit:        3,
		Budget:       resource.Set{resource.Memory: 4 << 30},
		CustomKeys:   []string{"fpga'); --"},
		PreferHashes: []string{"blake3:x"},
		ReservedFor:  &reserved,
	})

	for _, hostile := range []string{"DROP TABLE", "fpga", reserved.String(), "blake3:x"} {
		if strings.Contains(query, hostile) {
			t.Errorf("value %q was interpolated into the statement:\n%s", hostile, query)
		}
	}

	// queues, reserved id, memory budget, separator, one custom key,
	// prefer hashes, limit.
	if len(args) != 7 {
		t.Fatalf("bound %d args, want 7: %v", len(args), args)
	}
}

// TestBuildDequeueQueryBudgetPredicate pins the three rules a budget
// comparison has to get right at once: only declared keys are compared, a
// declared zero is still a real constraint, and the comparison is <= so an
// exact fit is claimable.
func TestBuildDequeueQueryBudgetPredicate(t *testing.T) {
	query, _ := buildDequeueQuery(job.DequeueOpts{
		Queues: []string{"default"},
		Limit:  1,
		Budget: resource.Set{resource.Memory: 4 << 30, resource.GPU: 0},
	})

	for _, want := range []string{"req_memory_bytes <= $", "req_gpu_milli <= $"} {
		if !strings.Contains(query, want) {
			t.Errorf("missing %q:\n%s", want, query)
		}
	}

	// CPU and disk were never declared, so they are unconstrained — not
	// compared against zero.
	for _, banned := range []string{"req_cpu_milli", "req_disk_bytes"} {
		if strings.Contains(query, banned) {
			t.Errorf("undeclared dimension %q was constrained:\n%s", banned, query)
		}
	}

	if strings.Contains(query, "req_memory_bytes < $") &&
		!strings.Contains(query, "req_memory_bytes <= $") {
		t.Error("budget compared with < rather than <=; an exact fit must be claimable")
	}
}

// TestBuildDequeueQueryCustomKeysAreASubsetTest pins containment as
// nested REPLACE rather than LIKE. One REPLACE per offered key, each
// stripping ",key," and putting the separator back, with the surviving
// string required to be empty or a lone separator.
//
// The LIKE formulation this replaces passes every single-key case in the
// conformance suite and then silently strands multi-key jobs, so the shape
// is worth pinning here as well as behaviourally.
func TestBuildDequeueQueryCustomKeysAreASubsetTest(t *testing.T) {
	query, args := buildDequeueQuery(job.DequeueOpts{
		Queues:     []string{"default"},
		Limit:      1,
		CustomKeys: []string{"tpu", "fpga", "nvme"},
	})

	if strings.Contains(query, "LIKE") {
		t.Errorf("custom-key containment used LIKE — that is a substring test:\n%s", query)
	}

	if n := strings.Count(query, "REPLACE("); n != 3 {
		t.Errorf("emitted %d REPLACE calls for 3 offered keys:\n%s", n, query)
	}

	if !strings.Contains(query, "IN ('', $") {
		t.Errorf("subset test does not end in the empty-or-separator check:\n%s", query)
	}

	// Keys are bound wrapped in separators, which is what stops ",fpga,"
	// matching a job that needs ",fpga-large,".
	for _, want := range []string{",fpga,", ",nvme,", ",tpu,"} {
		if !hasArg(args, want) {
			t.Errorf("offered key %q not bound wrapped in separators: %v", want, args)
		}
	}
}

// TestBuildDequeueQueryBoundedWithNoCustomKeysExcludesCustomJobs is the
// half of the empty-offer rule a backend gets wrong. Bounded opts with an
// empty offer are a resource-aware worker with no custom resources, so a
// job requiring an fpga must not be claimable — the predicate still has to
// be emitted, with no REPLACE wrapping it.
func TestBuildDequeueQueryBoundedWithNoCustomKeysExcludesCustomJobs(t *testing.T) {
	query, _ := buildDequeueQuery(job.DequeueOpts{
		Queues: []string{"default"},
		Limit:  1,
		Budget: resource.Set{resource.Memory: 4 << 30},
	})

	if strings.Contains(query, "REPLACE(") {
		t.Errorf("an empty offer emitted a REPLACE:\n%s", query)
	}

	if !strings.Contains(query, "req_custom_keys IN ('', $") {
		t.Errorf("bounded opts with no offered keys must still exclude custom-key jobs:\n%s", query)
	}
}

func hasArg(args []any, want string) bool {
	for _, a := range args {
		if s, ok := a.(string); ok && s == want {
			return true
		}
	}

	return false
}
