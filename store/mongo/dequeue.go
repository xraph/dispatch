package mongo

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// preferredField is the name the locality flag is computed into before
// the candidate sort. It is prefixed so it can never collide with a
// stored field, and it never leaves the aggregation — the $project at the
// end of the pipeline keeps only _id.
const preferredField = "__dispatch_preferred"

// dequeueBudgetFields maps each canonical dimension the fit predicate
// compares to the scalar BSON field holding it.
//
// These are exactly the dimensions job.DequeueOpts.Allows loops over, and
// exactly the fields jobModel declares for the purpose. The comparison
// deliberately does NOT read the full-fidelity resource_requests
// subdocument: BSON comparison of a subdocument member would have to
// contend with the member being absent on every job that does not declare
// that dimension, and with the numeric type the driver happened to write.
// The scalars are plain int64s that both write paths always emit.
var dequeueBudgetFields = []struct {
	key   string
	field string
}{
	{resource.CPU, "req_cpu_milli"},
	{resource.Memory, "req_memory_bytes"},
	{resource.Disk, "req_disk_bytes"},
	{resource.GPU, "req_gpu_milli"},
}

// dequeueFilter compiles opts into the query that decides WHICH jobs may
// be claimed. It is the BSON expression of job.DequeueOpts.Allows and
// must answer identically for every job.
//
// The same filter is used for the candidate read and, with _id pinned,
// for each claiming FindOneAndUpdate.
func dequeueFilter(opts job.DequeueOpts, t time.Time) bson.M {
	filter := bson.M{
		"state":  bson.M{"$in": []string{string(job.StatePending), string(job.StateRetrying)}},
		"queue":  bson.M{"$in": opts.Queues},
		"run_at": bson.M{"$lte": t},
	}

	// Unbounded opts emit the original query verbatim: a caller that does
	// not use the resource model claims everything, including jobs
	// declaring custom resources it could not possibly satisfy. Anything
	// else strands work the day this option ships. PreferHashes is
	// deliberately not consulted here — it orders, it never filters.
	if opts.IsUnbounded() {
		return filter
	}

	if opts.ReservedFor != nil {
		filter["_id"] = opts.ReservedFor.String()
	}

	conjuncts := make([]bson.M, 0, len(dequeueBudgetFields)+1)

	// An absent budget key is unconstrained, not zero, so only declared
	// dimensions produce a comparison. A key present with the value zero
	// is a real constraint and still emits one — that is an exhausted
	// worker, which must claim nothing that needs the dimension.
	//
	// The test is requirement <= budget: a job needing exactly the free
	// capacity is claimable, or the last slot on every worker is
	// permanently unusable.
	for _, dim := range dequeueBudgetFields {
		budget, declared := opts.Budget[dim.key]
		if !declared {
			continue
		}

		// The null branch is the Mongo-specific half. Range operators are
		// type-bracketed: {$lte: 0} matches numbers only, so it rejects a
		// document where the field is null AND one where it is absent —
		// and absent is exactly the shape of any job written before these
		// scalar fields existed, which declares no requirement at all and
		// must stay claimable. A plain equality against nil covers both
		// of those shapes in one clause, which is why there is no
		// $exists test here.
		//
		// None of the req_* fields are indexed, so this $or costs nothing
		// the planner would otherwise have had: the index serves
		// queue/state/priority/run_at and every resource clause is a
		// residual filter either way.
		conjuncts = append(conjuncts, bson.M{"$or": []bson.M{
			{dim.field: bson.M{"$lte": budget}},
			{dim.field: nil},
		}})
	}

	conjuncts = append(conjuncts, customKeyFilter(opts))

	filter["$and"] = conjuncts

	return filter
}

// customKeyFilter renders custom-resource containment as a genuine SUBSET
// test.
//
// req_custom_keys holds resource.EncodeCustomKeys' output — the sorted
// required keys wrapped in leading and trailing separators, e.g.
// ",fpga,tpu," — or "" when the job needs none. The obvious formulation,
// a substring match of the stored list inside the offered one, passes
// every single-key case including the prefix collision and then silently
// strands a job needing {fpga,tpu} from a caller offering {fpga,nvme,tpu},
// because the interleaved key breaks the contiguous run. The job it
// strands is the specialised one that is hardest to place anywhere else.
//
// Mongo can state the real thing instead: split the stored list back into
// keys and ask $setIsSubset. That is exact by construction — no ordering
// assumption, no prefix hazard, no arithmetic on separators. The SQL
// backends need nested REPLACE only because they have no set operator.
//
// $ifNull guards the split: $split raises on a non-string input, so a
// document written before req_custom_keys existed would fail the whole
// query rather than being read as "requires nothing". $filter then drops
// the empty elements the wrapping separators produce.
//
// An empty offer reaches here only for opts that are bounded some other
// way — IsUnbounded already returned above — so it correctly means "this
// worker has no custom resources": $setIsSubset then admits jobs
// requiring none and rejects every other.
func customKeyFilter(opts job.DequeueOpts) bson.M {
	required := bson.M{"$filter": bson.M{
		"input": bson.M{"$split": bson.A{
			bson.M{"$ifNull": bson.A{"$req_custom_keys", ""}},
			resource.CustomKeySep,
		}},
		"cond": bson.M{"$ne": bson.A{"$$this", ""}},
	}}

	offered := opts.OfferedCustomKeys()
	if offered == nil {
		offered = []string{}
	}

	// $literal, because a bare array in an aggregation expression has its
	// elements evaluated — a resource key beginning with "$" would
	// otherwise be read as a field path.
	return bson.M{"$expr": bson.M{"$setIsSubset": bson.A{required, bson.M{"$literal": offered}}}}
}

// preferredHashes returns the locality hashes worth matching on: deduped,
// and with the empty string dropped.
//
// job.DequeueOpts.Prefers reports false for a job with no
// PrimaryInputHash, so an empty string in the caller's list must not turn
// every unhashed job into a preferred one.
func preferredHashes(opts job.DequeueOpts) []string {
	if len(opts.PreferHashes) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(opts.PreferHashes))
	out := make([]string, 0, len(opts.PreferHashes))

	for _, h := range opts.PreferHashes {
		if h == "" {
			continue
		}

		if _, dup := seen[h]; dup {
			continue
		}

		seen[h] = struct{}{}

		out = append(out, h)
	}

	return out
}

// preferredExpr computes 1 for a job the caller already has staged and 0
// for every other, so a descending sort on it puts preferred first.
//
// It is a computed 0/1 rather than a sort on primary_input_hash itself,
// which is the trap Mongo sets here: findAndModify and $sort both take
// field paths, so "preferred first" reads as though it could be spelled
// {primary_input_hash: -1}. That sorts by hash VALUE, which has nothing
// to do with whether the caller staged it — any job whose hash happens to
// collate above the staged one outranks it — and it also decides the
// no-signal case by BSON collation order rather than by the contract,
// since null sorts below strings ascending and therefore above them
// descending.
//
// $ifNull is belt-and-braces on top of that: $in already evaluates to
// false for a missing or null field path, so the guard is not what makes
// the null case lose — the computed flag is. It is kept because it makes
// "unknown means not preferred" explicit at the point of decision rather
// than a property of $in a later reader has to re-derive.
func preferredExpr(hashes []string) bson.M {
	return bson.M{"$cond": bson.A{
		bson.M{"$in": bson.A{
			bson.M{"$ifNull": bson.A{"$primary_input_hash", ""}},
			bson.M{"$literal": hashes},
		}},
		1,
		0,
	}}
}
