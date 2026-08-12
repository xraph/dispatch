package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch/job"
)

// Where the fit predicate lives, and why it is not in Lua.
//
// Redis has no query language, so the predicate had two possible homes:
// Go, or a Lua script running on the server. Lua would move less data
// over the wire; it would also be a SECOND implementation of
// job.DequeueOpts.Allows/Less, written in a language with no tests of its
// own, in a repository whose whole resource-model track exists to stop the
// five backends drifting apart — the shared resource/codec.go and the
// shared storetest conformance suite are both there for exactly that
// reason. A Lua re-statement of "absent budget key is unconstrained",
// "requirement <= budget", "custom keys are a subset test, not a
// substring one" and "locality ranks below priority" is the single
// highest-drift-risk thing this track could contain.
//
// So the predicate stays in Go and is job.DequeueOpts' own code, called
// directly — the same choice store/memory makes, and for the same reason:
// one expression of the contract, zero drift. What Redis pays for that is
// reading candidate entities it may then discard.
//
// The CLAIM is a different question from the predicate, and it does NOT
// move to Go. See claimCandidates.

// maxDequeueRounds bounds the retry loop that runs when every candidate
// this call selected was claimed by a competing worker first. Without a
// bound, a busy queue could keep a caller scanning indefinitely; with it,
// a fully contended call returns empty and the pool simply polls again.
// Mirrors store/mongo, which composes its claim the same way.
const maxDequeueRounds = 3

// dequeueScanBatch is how many job entities one pipelined read fetches.
// The scan reads every pending member of the queue index, so it is issued
// as pipelined GETs in batches rather than as one GET per round trip. A
// pipeline (not MGET) is deliberate: go-redis splits a pipeline across
// cluster nodes by slot, whereas a multi-key MGET spanning slots is a
// CROSSSLOT error.
const dequeueScanBatch = 256

// dequeueCandidate is one job that passed the fit predicate, together
// with the queue index member that has to be won to claim it.
type dequeueCandidate struct {
	id    string
	queue string
	job   *job.Job
}

// DequeueJobs atomically claims up to opts.Limit ready jobs from
// opts.Queues that fit opts, sets them to running, and returns them
// ordered by priority descending, then locality-preferred first, then
// RunAt ascending.
//
// The call is a candidate scan followed by an exclusive claim:
//
//   - dequeueCandidates reads the queue index, decodes each entity,
//     applies job.DequeueOpts.Allows, sorts the SURVIVORS with
//     job.DequeueOpts.Less and only then truncates to Limit. Order
//     precedes truncation, over the whole eligible set — a scan that took
//     Limit members first and sorted within them would hand a worker with
//     a small limit arbitrary low-priority work forever, which is what
//     storetest's LimitTruncatesAfterOrdering pins.
//   - claimCandidates wins each survivor by removing it from the queue
//     index, which is what makes the claim exclusive.
//
// A job that does not fit is never removed from the index and never
// written to: it stays pending and untouched for the next worker that
// does have room. That is the whole point of the predicate — a job
// claimed and then requeued would bounce between small workers, delaying
// exactly the job that is hardest to place.
//
// A non-positive Limit claims nothing, matching the SQL backends' LIMIT 0.
// A worker computing zero free slots must claim zero jobs, never the
// whole queue.
//
// Empty opts.Queues claims nothing here, which is this backend's existing
// "all queues" behaviour: the queue index is one sorted set per queue
// name and there has never been a cross-queue index to scan. The
// conformance suite never exercises empty Queues, and no caller in this
// repository sends it.
func (s *Store) DequeueJobs(ctx context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	if opts.Limit <= 0 {
		return nil, nil
	}

	for range maxDequeueRounds {
		candidates, err := s.dequeueCandidates(ctx, opts)
		if err != nil {
			return nil, err
		}

		if len(candidates) == 0 {
			return nil, nil
		}

		claimed, err := s.claimCandidates(ctx, candidates)
		if err != nil {
			return nil, err
		}

		if len(claimed) > 0 {
			return claimed, nil
		}
	}

	return nil, nil
}

// dequeueCandidates returns the top opts.Limit eligible jobs across
// opts.Queues, already in contract order.
//
// It scans every member of each queue's sorted set rather than taking the
// first Limit by score. The score is a legacy ordering hint — a float
// packing negated priority and RunAt into one number — and nothing here
// trusts it: the contract order includes a locality term the score cannot
// express, and the float's RunAt component loses resolution as priority
// grows. Ordering is decided by job.DequeueOpts.Less over decoded jobs,
// so the score only ever affects which entities happen to be read first,
// never which are returned.
//
// The cost of that is one GET per pending member per call, pipelined in
// batches. It is the price of expressing the predicate once, in Go, and
// it is bounded by the depth of the queues the caller named.
func (s *Store) dequeueCandidates(ctx context.Context, opts job.DequeueOpts) ([]dequeueCandidate, error) {
	t := now()
	candidates := make([]dequeueCandidate, 0, opts.Limit)

	for _, q := range opts.Queues {
		ids, err := s.rdb.ZRange(ctx, queueKey(q), 0, -1).Result()
		if err != nil && !isRedisNil(err) {
			return nil, fmt.Errorf("dispatch/redis: dequeue scan %q: %w", q, err)
		}

		for start := 0; start < len(ids); start += dequeueScanBatch {
			end := min(start+dequeueScanBatch, len(ids))
			batch := ids[start:end]

			entities, readErr := s.readJobEntities(ctx, batch)
			if readErr != nil {
				return nil, readErr
			}

			for i, e := range entities {
				if e == nil {
					continue // indexed but gone, or unreadable
				}

				// The index is not a state filter: EnqueueJob adds every
				// job it writes, including one handed to it already
				// running, and ReclaimExpiredLeases re-adds jobs it
				// returned to pending. Only a ready job may be claimed.
				if st := job.State(e.State); st != job.StatePending && st != job.StateRetrying {
					continue
				}

				if !e.RunAt.IsZero() && e.RunAt.After(t) {
					continue
				}

				j, convErr := fromJobEntity(e)
				if convErr != nil {
					continue
				}

				// IsUnbounded skips the fit predicate entirely: a caller
				// not using the resource model claims everything,
				// including jobs declaring custom resources it could not
				// possibly satisfy. It governs FILTERING only —
				// PreferHashes still orders below, even here.
				if !opts.IsUnbounded() && !opts.Allows(j) {
					continue
				}

				candidates = append(candidates, dequeueCandidate{id: batch[i], queue: q, job: j})
			}
		}
	}

	// Order THEN truncate, across every queue named, exactly as
	// store/memory does: priority descending, then locality-preferred
	// before not — a tiebreak strictly within a priority band, never
	// above it — then RunAt ascending.
	//
	// Do not "optimize" this by truncating first. A measured caveat for
	// whoever tries: swapping these two statements alone does NOT fail
	// storetest's LimitTruncatesAfterOrdering, because ZRange happens to
	// return members in score order and the score happens to encode
	// priority. The suite only catches the swap once the scan order is
	// also perturbed — reversing the batch loop makes it fail immediately
	// with [prio-1 prio-0]. So the safety here rests on this sort, not on
	// the index, and the test would not warn you if you leaned on the
	// index instead.
	sort.SliceStable(candidates, func(i, k int) bool {
		return opts.Less(candidates[i].job, candidates[k].job)
	})

	if len(candidates) > opts.Limit {
		candidates = candidates[:opts.Limit]
	}

	return candidates, nil
}

// readJobEntities fetches one batch of job entities by ID, returning a
// slice positionally aligned with ids and holding nil where the entity
// was missing or could not be decoded.
//
// The read is a pipeline of GETs rather than one MGET so it stays correct
// against a clustered client, where the job keys of one queue are spread
// over many slots.
func (s *Store) readJobEntities(ctx context.Context, ids []string) ([]*jobEntity, error) {
	pipe := s.rdb.Pipeline()

	cmds := make([]*goredis.StringCmd, len(ids))
	for i, jID := range ids {
		cmds[i] = pipe.Get(ctx, jobKey(jID))
	}

	// A missing key makes Exec report goredis.Nil for the batch as a
	// whole; the per-command results below distinguish the misses, and a
	// job that vanished between the index read and this one is simply not
	// a candidate.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("dispatch/redis: dequeue read entities: %w", err)
	}

	out := make([]*jobEntity, len(ids))

	for i, cmd := range cmds {
		raw, err := cmd.Bytes()
		if err != nil {
			continue
		}

		var e jobEntity
		if json.Unmarshal(raw, &e) != nil {
			continue
		}

		out[i] = &e
	}

	return out, nil
}

// claimCandidates takes exclusive ownership of each candidate and returns
// the ones this caller won, still in candidate order.
//
// THE CLAIM IS WHERE ATOMICITY LIVES, and it is unchanged in kind from
// what this store did before the predicate existed. Removal from the
// queue's sorted set is the claim: ZREM is a single Redis command, so the
// server executes it indivisibly, and of any number of workers racing for
// one member exactly one gets a reply of 1. Everyone else gets 0 and
// moves on empty-handed — never a second claim of the same job. That is
// the same guarantee ZPopMin gave (also one atomic command, also
// removal-is-the-claim), and it interoperates with the ZPopMin that
// DequeueLeased still uses: a pop and a rem of the same member cannot
// both succeed.
//
// ZREM rather than ZPopMin because ZPopMin chooses its own members by
// score, which would mean popping jobs this caller has already decided do
// not fit and then having to put them back — the claim-then-requeue the
// predicate exists to prevent. ZREM lets Go nominate exactly the jobs that
// passed.
//
// Only after winning the removal is the entity read and rewritten as
// running. Between those two steps the job is reachable by no other
// dequeue path, so the read-modify-write needs no compare-and-set of its
// own; this is the identical window the previous ZPopMin implementation
// had, and a crash inside it leaves the job exactly as a crash after
// ZPopMin did.
func (s *Store) claimCandidates(ctx context.Context, candidates []dequeueCandidate) ([]*job.Job, error) {
	pipe := s.rdb.Pipeline()

	rems := make([]*goredis.IntCmd, len(candidates))
	for i, c := range candidates {
		rems[i] = pipe.ZRem(ctx, queueKey(c.queue), c.id)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("dispatch/redis: dequeue claim: %w", err)
	}

	t := now()
	claimed := make([]*job.Job, 0, len(candidates))

	for i, c := range candidates {
		won, err := rems[i].Result()
		if err != nil || won != 1 {
			continue // another worker removed it first
		}

		key := jobKey(c.id)

		var e jobEntity
		if getErr := s.getEntity(ctx, key, &e); getErr != nil {
			continue // won the index entry but the entity is gone
		}

		// Re-read rather than reusing the scanned copy, so the blob
		// written back is built on the freshest state. The state check is
		// belt-and-braces: winning the ZREM already excludes every other
		// dequeue path, so this only fires if some other writer moved the
		// job out of pending while it sat in the index.
		if st := job.State(e.State); st != job.StatePending && st != job.StateRetrying {
			continue
		}

		e.State = string(job.StateRunning)
		e.StartedAt = &t
		e.UpdatedAt = t

		if setErr := s.setEntity(ctx, key, &e); setErr != nil {
			return nil, fmt.Errorf("dispatch/redis: dequeue update: %w", setErr)
		}

		j, convErr := fromJobEntity(&e)
		if convErr != nil {
			return nil, convErr
		}

		claimed = append(claimed, j)
	}

	return claimed, nil
}
