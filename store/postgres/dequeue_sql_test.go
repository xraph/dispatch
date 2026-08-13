package postgres

import (
	"strconv"
	"strings"
	"testing"
	"time"

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

// argFor returns the value the statement reads at the assignment starting
// with prefix, by resolving the $N it names against the args slice.
//
// Postgres numbers its placeholders, so the SQLite bind-order hazard does
// not exist here — but the equivalent one does: buildDequeueQuery's bind
// closure derives each number from len(args) at the moment it is called,
// so a helper that writes its text and appends its values in different
// orders would emit a number naming somebody else's value. Reading the
// number back out of the finished statement is what catches that.
func argFor(t *testing.T, query string, args []any, prefix string) any {
	t.Helper()

	i := strings.Index(query, prefix)
	if i < 0 {
		t.Fatalf("statement has no %q assignment:\n%s", prefix, query)
	}

	rest := query[i+len(prefix):]
	if rest == "" || rest[0] != '$' {
		t.Fatalf("%q is not read from a bind parameter:\n%s", prefix, query)
	}

	end := 1
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}

	n, err := strconv.Atoi(rest[1:end])
	if err != nil {
		t.Fatalf("%q names an unparseable placeholder %q", prefix, rest[:end])
	}

	if n < 1 || n > len(args) {
		t.Fatalf("%q names $%d but only %d args were bound: %v", prefix, n, len(args), args)
	}

	return args[n-1]
}

// TestBuildDequeueQueryGrantsLeaseInTheClaim pins the two halves of the
// grant: it is absent unless the caller asks for it, and when present it
// is part of the claiming UPDATE's SET clause rather than a second
// statement — which is what makes a claimed job always carry a lease
// (see job.DequeueOpts.LeaseUntil).
//
// The epoch is pinned as `lease_epoch + 1` specifically. Binding a
// computed successor instead would need a prior read, and the read is
// what the single statement exists to avoid.
func TestBuildDequeueQueryGrantsLeaseInTheClaim(t *testing.T) {
	worker := id.NewWorkerID()

	// Deliberately NOT UTC. lease_expires_at is a timestamptz and the
	// driver would convert either way, but every other timestamp this
	// package writes is normalized before it is bound, and a caller that
	// hands over a wall-clock time in its own zone is the ordinary case.
	until := time.Date(2026, 8, 12, 12, 1, 30, 0, time.FixedZone("UTC-5", -5*60*60))

	base := job.DequeueOpts{
		Queues: []string{"default"},
		Limit:  3,
		Budget: resource.Set{resource.Memory: 4 << 30},
	}

	grantOpts := base
	grantOpts.WorkerID = worker
	grantOpts.LeaseUntil = until

	t.Run("no grant leaves the lease columns alone", func(t *testing.T) {
		query, _ := buildDequeueQuery(base)

		for _, banned := range []string{"worker_id", "lease_epoch", "lease_expires_at"} {
			if strings.Contains(query, banned) {
				t.Errorf("opts granting no lease still wrote %q:\n%s", banned, query)
			}
		}
	})

	t.Run("grant rides in the SET clause", func(t *testing.T) {
		query, args := buildDequeueQuery(grantOpts)

		setEnd := strings.Index(query, "WHERE id IN (")
		if setEnd < 0 {
			t.Fatalf("statement lost its claim shape:\n%s", query)
		}

		// Every lease assignment must fall inside the UPDATE's SET clause,
		// which is the part of the text before the candidate subquery.
		for _, want := range []string{"worker_id = $", "lease_epoch = lease_epoch + 1", "lease_expires_at = $"} {
			at := strings.Index(query, want)
			if at < 0 {
				t.Errorf("granting opts did not emit %q:\n%s", want, query)

				continue
			}

			if at > setEnd {
				t.Errorf("%q is outside the claiming SET clause:\n%s", want, query)
			}
		}

		if got := argFor(t, query, args, "worker_id = "); got != worker.String() {
			t.Errorf("worker_id reads %v, want %s", got, worker)
		}

		got, ok := argFor(t, query, args, "lease_expires_at = ").(time.Time)
		if !ok {
			t.Fatalf("lease_expires_at was not bound as a time.Time: %v", args)
		}

		if !got.Equal(until) {
			t.Errorf("lease_expires_at reads %v, want %v", got, until)
		}

		if got.Location() != time.UTC {
			t.Errorf("lease_expires_at bound in %v, want UTC", got.Location())
		}
	})
}
