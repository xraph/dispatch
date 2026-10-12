package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// commitEnqueue is the tail every enqueue shares: write the job, nudge
// the local worker pool so an in-process enqueue is picked up at once
// instead of after the idle poll backoff, and tell extensions.
func (eng *Engine) commitEnqueue(ctx context.Context, j *job.Job) error {
	if err := eng.jobStore.EnqueueJob(ctx, j); err != nil {
		return err
	}

	if eng.pool != nil {
		eng.pool.Wake()
	}

	eng.extensions.EmitJobEnqueued(ctx, j)

	return nil
}

// enqueuePrepared enqueues a job somebody else built, ID and all. It is
// the dlq.Enqueuer the engine hands its DLQ service, so a replay goes
// through the same tail as EnqueueRaw.
//
// The job's Resources were resolved when the failed job was first
// enqueued and are not resolved again: the estimator and the definition
// may have changed since, and a replay runs what failed. They are only
// checked against today's fleet ceiling, with the same check Resolve
// runs, because the fleet may have shrunk since and a job no worker can
// run must fail loudly here rather than sit pending forever.
//
// Like EnqueueRaw, it does not ask whether a handler is registered for
// the job's name. EnqueueRaw never has: the worker that claims the job
// may run a build this process does not.
func (eng *Engine) enqueuePrepared(ctx context.Context, j *job.Job) error {
	if len(j.Resources) > 0 {
		if err := resource.CheckSchedulable(j.Resources, eng.MaxWorkerCapacity(ctx)); err != nil {
			return err
		}
	}

	return eng.commitEnqueue(ctx, j)
}

// ReplayDLQ turns a dead letter entry back into work: a new pending job
// carrying everything the failed one ran with, under a fresh ID.
//
// The entry is claimed before the job is enqueued (dlq.Service.ReplayEntry),
// so of two concurrent replays exactly one creates a job and the other
// gets an error wrapping dispatch.ErrDLQAlreadyReplayed, as does a replay
// of an entry whose job was already retried. A failed enqueue releases
// the claim. Unknown entries return dispatch.ErrDLQNotFound, and a job
// too big for every worker wraps resource.ErrUnschedulable.
//
// One OperatorAction of kind ext.ActionDLQReplayed is emitted on success,
// naming the entry, the failed job and the new one.
func (eng *Engine) ReplayDLQ(ctx context.Context, entryID id.DLQID) (*job.Job, error) {
	entry, err := eng.dlqService.DLQStore().GetDLQ(ctx, entryID)
	if err != nil {
		return nil, err
	}

	j, err := eng.dlqService.ReplayEntry(ctx, entry)
	if err != nil {
		return nil, err
	}

	eng.extensions.EmitOperatorAction(ctx, ext.Action{
		Kind:     ext.ActionDLQReplayed,
		JobID:    entry.JobID,
		NewJobID: j.ID,
		DLQID:    entryID,
	})

	return j, nil
}

// defaultReplayAllLimit is how many entries ReplayAllDLQ looks at when
// the caller sets no limit. It is the cap the REST handler has always
// listed with.
const defaultReplayAllLimit = 1000

// replayAllPageSize is how many entries ReplayAllDLQ reads per page.
const replayAllPageSize = 100

// maxReplayAllErrors caps ReplayAllResult.Errors. Failed keeps the full
// count; the messages are for a person to read, and a thousand copies of
// one store error tell them nothing the first few do not.
const maxReplayAllErrors = 20

// ReplayAllOpts selects what ReplayAllDLQ replays.
type ReplayAllOpts struct {
	// Queue limits the replay to one queue. Empty means every queue.
	Queue string
	// Limit caps how many entries are tried. Zero or less means 1000.
	Limit int
}

// ReplayAllResult counts what ReplayAllDLQ did with each entry it tried.
type ReplayAllResult struct {
	// Replayed is how many entries became new jobs.
	Replayed int
	// Conflicts is how many were claimed by somebody else between the
	// listing and their own replay.
	Conflicts int
	// Failed is how many could not be replayed for any other reason.
	// Each of those entries was released and can be replayed again.
	Failed int
	// Errors holds a message for the first failures, "<entry id>: <error>",
	// at most 20 of them.
	Errors []string
}

// ReplayAllDLQ replays every entry nobody has replayed yet, newest first,
// up to opts.Limit, each one exactly as ReplayDLQ would. It is not a
// transaction: entries are claimed one at a time, so a concurrent replay
// of one of them shows up as a Conflict, and a failure partway leaves the
// earlier replays in place.
//
// It returns an error only when the entries cannot be listed or ctx ends,
// together with what it had done by then. One OperatorAction records the
// committed replay count, including when a later read fails. A completed
// operation with no replays also emits a zero count. An interrupted operation
// that changed no entries emits nothing.
func (eng *Engine) ReplayAllDLQ(ctx context.Context, opts ReplayAllOpts) (ReplayAllResult, error) {
	var res ReplayAllResult

	pl, ok := eng.dlqService.DLQStore().(dlq.PageLister)
	if !ok {
		return res, errors.New("dispatch: dlq store does not support paged listing")
	}

	completed := false
	defer func() {
		if completed || res.Replayed > 0 {
			eng.extensions.EmitOperatorAction(ctx, ext.Action{
				Kind:  ext.ActionDLQReplayed,
				Count: int64(res.Replayed),
			})
		}
	}()

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultReplayAllLimit
	}

	unreplayed := false
	cursor := ""
	tried := 0

	for tried < limit {
		page, err := pl.ListDLQPage(ctx, dlq.PageOpts{
			Queue:    opts.Queue,
			Replayed: &unreplayed,
			Cursor:   cursor,
			Limit:    min(limit-tried, replayAllPageSize),
		})
		if err != nil {
			return res, fmt.Errorf("replay all dlq: list: %w", err)
		}

		for _, entry := range page.Entries {
			if err := ctx.Err(); err != nil {
				return res, err
			}

			tried++

			_, replayErr := eng.dlqService.ReplayEntry(ctx, entry)
			switch {
			case replayErr == nil:
				res.Replayed++
			case errors.Is(replayErr, dispatch.ErrDLQAlreadyReplayed):
				res.Conflicts++
			default:
				res.Failed++
				if len(res.Errors) < maxReplayAllErrors {
					res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", entry.ID, replayErr))
				}
			}
		}

		if page.NextCursor == "" {
			break
		}

		cursor = page.NextCursor
	}

	completed = true
	return res, nil
}

// DeleteDLQ removes one dead letter entry for good. Unknown entries
// return dispatch.ErrDLQNotFound. One OperatorAction of kind
// ext.ActionDLQDeleted is emitted on success.
func (eng *Engine) DeleteDLQ(ctx context.Context, entryID id.DLQID) error {
	if err := eng.dlqService.DLQStore().DeleteDLQ(ctx, entryID); err != nil {
		return err
	}

	eng.extensions.EmitOperatorAction(ctx, ext.Action{
		Kind:  ext.ActionDLQDeleted,
		DLQID: entryID,
	})

	return nil
}

// errNoPurgeCutoff refuses a purge or purge count with a zero cutoff.
var errNoPurgeCutoff = errors.New("dispatch: dlq purge needs a non-zero before time")

// PurgeDLQ deletes every dead letter entry that failed strictly before
// before, and returns how many went. A zero before is refused: it matches
// nothing, so it can only be a caller that forgot to set it. One
// OperatorAction of kind ext.ActionDLQPurged, with Count set, is emitted
// on success, including when nothing matched. A store that reports partial
// progress with an error retains that count and emits it before returning.
func (eng *Engine) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	if before.IsZero() {
		return 0, errNoPurgeCutoff
	}

	n, err := eng.dlqService.DLQStore().PurgeDLQ(ctx, before)
	if err == nil || n > 0 {
		eng.extensions.EmitOperatorAction(ctx, ext.Action{
			Kind:  ext.ActionDLQPurged,
			Count: n,
		})
	}

	return n, err
}

// CountDLQPurge is PurgeDLQ's dry run: how many entries a purge with the
// same before would delete, counted on the same boundary. It changes
// nothing and emits nothing. A zero before is refused, as PurgeDLQ
// refuses it.
func (eng *Engine) CountDLQPurge(ctx context.Context, before time.Time) (int64, error) {
	if before.IsZero() {
		return 0, errNoPurgeCutoff
	}

	pl, ok := eng.dlqService.DLQStore().(dlq.PageLister)
	if !ok {
		return 0, errors.New("dispatch: dlq store does not support filtered counts")
	}

	return pl.CountDLQEntries(ctx, dlq.CountOpts{FailedBefore: before})
}
