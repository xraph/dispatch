package sqlite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// These tests assert the compiled statement, not behaviour —
// dequeue_test.go's conformance run asks the database for behaviour. They
// exist because two properties of this statement are invisible to the
// shared suite on SQLite:
//
//   - `?` binds POSITIONALLY, so the args slice must be built in the same
//     order the placeholders appear in the text. Get that wrong and a
//     budget is compared against a queue name. Nothing in the suite reads
//     the statement, and a mis-binding shows up as a wrong answer only for
//     the option combinations the suite happens to exercise.
//   - The candidate SELECT's ORDER BY is what makes the LIMIT truncate an
//     ORDERED set. Re-measured against the current 21-case suite:
//     removing it entirely fails exactly ONE case,
//     LocalityDecidesWhichRowsSurviveATightLimit. The other twenty pass,
//     because SQLite answers the scan from idx_dispatch_jobs_dequeue_res,
//     whose leading key order happens to match priority DESC, run_at ASC,
//     so the right rows come back for the wrong reason.
//     TestBuildDequeueQueryOrdersLocalityBelowPriority is the pin that
//     does not depend on which index the planner chose.

// render substitutes each bind parameter into the statement in order, so
// a test can read the finished SQL the way SQLite reads it. It is a test
// helper only: the production path never interpolates a value.
func render(t *testing.T, query string, args []any) string {
	t.Helper()

	if n := strings.Count(query, "?"); n != len(args) {
		t.Fatalf("statement has %d placeholders but %d args were bound:\n%s\n%v",
			n, len(args), query, args)
	}

	var b strings.Builder

	i := 0

	for _, r := range query {
		if r != '?' {
			b.WriteRune(r)

			continue
		}

		switch v := args[i].(type) {
		case string:
			b.WriteString("'" + strings.ReplaceAll(v, "'", "''") + "'")
		case time.Time:
			b.WriteString("'" + v.Format(time.RFC3339Nano) + "'")
		default:
			fmt.Fprintf(&b, "%v", v)
		}

		i++
	}

	return b.String()
}

func fixedNow() time.Time {
	return time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
}

// TestBuildDequeueQueryUnboundedEmitsOriginalStatement is the
// backward-compatibility guarantee in its narrowest form: opts that
// constrain nothing must compile to the statement that shipped before
// DequeueOpts existed, with no fit predicate at all. A worker not using
// the resource model claims everything, jobs declaring custom resources
// included.
func TestBuildDequeueQueryUnboundedEmitsOriginalStatement(t *testing.T) {
	opts := job.DequeueOpts{Queues: []string{"default"}, Limit: 10}

	if !opts.IsUnbounded() {
		t.Fatalf("DequeueOpts%+v.IsUnbounded() = false, want true", opts)
	}

	query, args := buildDequeueQuery(opts, fixedNow())

	for _, banned := range []string{"req_", "REPLACE", "primary_input_hash"} {
		if strings.Contains(query, banned) {
			t.Errorf("unbounded dequeue emitted %q:\n%s", banned, query)
		}
	}

	if !strings.Contains(query, "ORDER BY priority DESC, run_at ASC\n") {
		t.Errorf("unbounded dequeue lost the original ordering:\n%s", query)
	}

	// started_at, updated_at, one queue, run_at, limit.
	if len(args) != 5 {
		t.Fatalf("unbounded dequeue bound %d args, want 5: %v", len(args), args)
	}

	if args[4] != 10 {
		t.Errorf("limit bound as %v, want 10", args[4])
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

	query, _ := buildDequeueQuery(opts, fixedNow())

	if strings.Contains(query, "req_") {
		t.Errorf("PreferHashes emitted a fit predicate — locality must never filter:\n%s", query)
	}

	if !strings.Contains(query, "COALESCE(primary_input_hash IN (") {
		t.Errorf("locality term missing from unbounded opts:\n%s", query)
	}
}

// TestBuildDequeueQueryOrdersLocalityBelowPriority pins two things at
// once.
//
// First, priority comes before locality. The reverse would not show up as
// a wrong answer, only as starvation: a steady stream of low-priority
// jobs with staged inputs beating a high-priority job with cold ones.
//
// Second, the ORDER BY sits inside the candidate SELECT and before its
// LIMIT, so the LIMIT truncates an ordered set. That one is the reason
// this file exists — see the note at the top: with the ORDER BY deleted,
// every conformance case still passes on SQLite.
func TestBuildDequeueQueryOrdersLocalityBelowPriority(t *testing.T) {
	query, args := buildDequeueQuery(job.DequeueOpts{
		Queues:       []string{"default"},
		Limit:        2,
		PreferHashes: []string{"blake3:staged"},
	}, fixedNow())

	const want = "priority DESC, COALESCE(primary_input_hash IN (?), 0) DESC, run_at ASC"

	if n := strings.Count(query, want); n != 1 {
		t.Fatalf("ordering %q appears %d times, want 1:\n%s", want, n, query)
	}

	orderAt := strings.Index(query, "ORDER BY ")
	limitAt := strings.Index(query, "LIMIT ")

	if orderAt < 0 || limitAt < 0 || orderAt > limitAt {
		t.Fatalf("the candidate LIMIT must follow its ORDER BY, or it truncates "+
			"an unordered set and the ordering is decoration:\n%s", query)
	}

	// And the locality term reads the hash the caller staged, not something
	// bound out of position.
	if got := render(t, query, args); !strings.Contains(got,
		"COALESCE(primary_input_hash IN ('blake3:staged'), 0) DESC") {
		t.Errorf("locality term bound the wrong value:\n%s", got)
	}
}

// TestBuildDequeueQueryBindsInTextualOrder is the SQLite-specific
// hazard. Postgres numbers its placeholders, so buildDequeueQuery could
// bind in any order there and still be correct. Here `?` is positional:
// the nth value bound is read by the nth `?` in the text, so the builders
// must bind exactly as they write.
//
// Rendering the statement is the only way to see that, and a swap is
// silent otherwise — a budget compared against a queue name is a
// perfectly valid SQLite expression.
//
// The granting case is the one that most needs this. buildLeaseGrant
// binds BETWEEN updated_at and the queue list, which is the middle of the
// sequence rather than either end, so getting it wrong shifts every
// later value by two: the queue list would read the worker id and the
// expiry, and the statement would still run.
func TestBuildDequeueQueryBindsInTextualOrder(t *testing.T) {
	reserved := id.NewJobID()
	worker := id.NewWorkerID()
	now := fixedNow()
	until := now.Add(90 * time.Second)
	stamp := "'" + now.Format(time.RFC3339Nano) + "'"
	leaseStamp := "'" + until.Format(time.RFC3339Nano) + "'"

	base := job.DequeueOpts{
		Queues:       []string{"alpha", "beta"},
		Limit:        3,
		Budget:       resource.Set{resource.Memory: 4 << 30, resource.GPU: 0},
		CustomKeys:   []string{"tpu", "fpga"},
		PreferHashes: []string{"blake3:staged"},
		ReservedFor:  &reserved,
	}

	// Every case asserts the whole sequence, not just its own addition: a
	// mis-bound grant corrupts the values AFTER it, so the queue list and
	// the limit are the assertions that actually catch it.
	common := []string{
		"AND queue IN ('alpha','beta')",
		"AND run_at <= " + stamp,
		"AND id = '" + reserved.String() + "'",
		"AND req_memory_bytes <= 4294967296",
		"AND req_gpu_milli <= 0",
		"REPLACE(REPLACE(req_custom_keys, ',fpga,', ','), ',tpu,', ',') IN ('', ',')",
		"COALESCE(primary_input_hash IN ('blake3:staged'), 0) DESC",
		"LIMIT 3",
	}

	grantOpts := base
	grantOpts.WorkerID = worker
	grantOpts.LeaseUntil = until

	tests := []struct {
		name   string
		opts   job.DequeueOpts
		want   []string
		banned []string
	}{
		{
			name: "no grant",
			opts: base,
			want: append([]string{
				"SET state = 'running', started_at = " + stamp + ", updated_at = " + stamp + "\n",
			}, common...),
			// A caller that did not ask for a lease must not have one
			// written, and the statement is where that is decided.
			banned: []string{"worker_id", "lease_epoch", "lease_expires_at"},
		},
		{
			name: "grant",
			opts: grantOpts,
			want: append([]string{
				"SET state = 'running', started_at = " + stamp + ", updated_at = " + stamp + ",",
				"worker_id = '" + worker.String() + "'",
				"lease_epoch = lease_epoch + 1",
				"lease_expires_at = " + leaseStamp,
			}, common...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args := buildDequeueQuery(tt.opts, now)
			got := render(t, query, args)

			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("rendered statement is missing %q — a value was bound out of "+
						"position:\n%s", want, got)
				}
			}

			for _, banned := range tt.banned {
				if strings.Contains(got, banned) {
					t.Errorf("rendered statement contains %q for opts that grant no "+
						"lease:\n%s", banned, got)
				}
			}
		})
	}
}

// TestBuildDequeueQueryBindsEveryValue is the injection check. Every
// value the caller controls — queue names, budgets, custom keys, the
// reserved id, hashes, the limit — must reach SQLite as a bind parameter.
// The only things concatenated into the statement are column names from
// budgetColumns, all compile-time constants.
func TestBuildDequeueQueryBindsEveryValue(t *testing.T) {
	reserved := id.NewJobID()

	query, args := buildDequeueQuery(job.DequeueOpts{
		Queues:       []string{"q'; DROP TABLE dispatch_jobs; --"},
		Limit:        3,
		Budget:       resource.Set{resource.Memory: 4 << 30},
		CustomKeys:   []string{"fpga'); --"},
		PreferHashes: []string{"blake3:x"},
		ReservedFor:  &reserved,
	}, fixedNow())

	for _, hostile := range []string{"DROP TABLE", "fpga", reserved.String(), "blake3:x"} {
		if strings.Contains(query, hostile) {
			t.Errorf("value %q was interpolated into the statement:\n%s", hostile, query)
		}
	}

	// started_at, updated_at, queue, run_at, reserved id, memory budget,
	// one custom key + its separator, the closing separator, hash, limit.
	if len(args) != 11 {
		t.Fatalf("bound %d args, want 11: %v", len(args), args)
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
	}, fixedNow())

	for _, want := range []string{"req_memory_bytes <= ?", "req_gpu_milli <= ?"} {
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
}

// TestBuildDequeueQueryCustomKeysAreASubsetTest pins containment as
// nested REPLACE rather than LIKE or GLOB. One REPLACE per offered key,
// each stripping ",key," and putting the separator back, with the
// surviving string required to be empty or a lone separator.
//
// The substring formulation this replaces passes every single-key case in
// the conformance suite — including the prefix collision — and then
// silently strands multi-key jobs, so the shape is worth pinning here as
// well as behaviourally.
func TestBuildDequeueQueryCustomKeysAreASubsetTest(t *testing.T) {
	query, args := buildDequeueQuery(job.DequeueOpts{
		Queues:     []string{"default"},
		Limit:      1,
		CustomKeys: []string{"tpu", "fpga", "nvme"},
	}, fixedNow())

	for _, banned := range []string{"LIKE", "GLOB", "INSTR"} {
		if strings.Contains(query, banned) {
			t.Errorf("custom-key containment used %s — that is a substring test:\n%s",
				banned, query)
		}
	}

	if n := strings.Count(query, "REPLACE("); n != 3 {
		t.Errorf("emitted %d REPLACE calls for 3 offered keys:\n%s", n, query)
	}

	// Keys are bound wrapped in separators, which is what stops ",fpga,"
	// matching a job that needs ",fpga-large,".
	rendered := render(t, query, args)
	if !strings.Contains(rendered,
		"REPLACE(REPLACE(REPLACE(req_custom_keys, ',fpga,', ','), ',nvme,', ','), ',tpu,', ',') IN ('', ',')") {
		t.Errorf("subset test is not the nested-REPLACE recipe:\n%s", rendered)
	}
}

// TestBuildDequeueQueryBoundedWithNoCustomKeysExcludesCustomJobs is the
// half of the empty-offer rule a backend gets wrong. Bounded opts with an
// empty offer are a resource-aware worker with no custom resources, so a
// job requiring an fpga must not be claimable — the predicate still has to
// be emitted, with no REPLACE wrapping it.
//
// It doubles as the empty-IN-list guard: SQLite's `IN ()` is a syntax
// error, so an offer of no keys must not expand into one.
func TestBuildDequeueQueryBoundedWithNoCustomKeysExcludesCustomJobs(t *testing.T) {
	opts := job.DequeueOpts{
		Queues: []string{"default"},
		Limit:  1,
		Budget: resource.Set{resource.Memory: 4 << 30},
	}

	if opts.IsUnbounded() {
		t.Fatal("opts carrying a memory budget report IsUnbounded() = true")
	}

	query, args := buildDequeueQuery(opts, fixedNow())

	if strings.Contains(query, "REPLACE(") {
		t.Errorf("an empty custom-key offer emitted a REPLACE:\n%s", query)
	}

	if got := render(t, query, args); !strings.Contains(got, "req_custom_keys IN ('', ',')") {
		t.Errorf("bounded opts with no offered keys must still exclude custom "+
			"requirements:\n%s", got)
	}

	if strings.Contains(query, "IN ()") {
		t.Errorf("emitted an empty IN list, which SQLite rejects as a syntax error:\n%s", query)
	}
}
