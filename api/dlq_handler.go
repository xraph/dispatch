// Package api provides HTTP handlers for the Dispatch API.
package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// listDLQ answers a page of entries, oldest failure first, as a bare JSON
// array.
func (a *API) listDLQ(ctx forge.Context, req *ListDLQRequest) (*ListDLQResponse, error) {
	entries, err := a.eng.DLQService().DLQStore().ListDLQ(ctx.Context(), dlq.ListOpts{
		Limit:  defaultLimit(req.Limit),
		Offset: req.Offset,
		Queue:  req.Queue,
	})
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}

	return &ListDLQResponse{Entries: nonNil(entries)}, nil
}

func (a *API) getDLQ(ctx forge.Context, _ *GetDLQRequest) (*dlq.Entry, error) {
	entryID, err := id.ParseDLQID(ctx.Param("entryId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid DLQ entry ID: %v", err))
	}

	entry, err := a.eng.DLQService().DLQStore().GetDLQ(ctx.Context(), entryID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusOK, entry)
}

// replayDLQ turns an entry into a new pending job through the engine,
// which claims the entry first. An entry already replayed, or a job no
// worker can run, answers 409.
func (a *API) replayDLQ(ctx forge.Context, _ *ReplayDLQRequest) (*job.Job, error) {
	entryID, err := id.ParseDLQID(ctx.Param("entryId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid DLQ entry ID: %v", err))
	}

	j, err := a.eng.ReplayDLQ(ctx.Context(), entryID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusCreated, j)
}

// maxReplayAll is the most entries one replay-all request may try. It is
// the cap the route has always listed with.
const maxReplayAll = 1000

// replayAllDLQ replays every unreplayed entry, newest first, optionally
// in one queue and up to a limit. Entries somebody else claims meanwhile
// count as conflicts; errors counts every other failure.
func (a *API) replayAllDLQ(ctx forge.Context, req *ReplayAllDLQRequest) (*ReplayAllDLQResponse, error) {
	if req.Limit < 0 || req.Limit > maxReplayAll {
		return nil, forge.BadRequest(fmt.Sprintf("limit must be from 0 to %d, and 0 means %d", maxReplayAll, maxReplayAll))
	}

	res, err := a.eng.ReplayAllDLQ(ctx.Context(), engine.ReplayAllOpts{
		Queue: req.Queue,
		Limit: req.Limit,
	})
	if err != nil {
		return nil, fmt.Errorf("replay all dlq: %w", err)
	}

	messages := res.Errors
	if messages == nil {
		messages = []string{}
	}

	return nil, ctx.JSON(http.StatusOK, ReplayAllDLQResponse{
		Replayed:      int64(res.Replayed),
		Conflicts:     int64(res.Conflicts),
		Errors:        int64(res.Failed),
		ErrorMessages: messages,
	})
}

// defaultPurgeAge is how far back a purge reaches when the request names
// no cutoff. Before the route took one, it was the only cutoff there was.
const defaultPurgeAge = 30 * 24 * time.Hour

// purgeDLQ deletes the entries that failed before the cutoff, or with
// dry_run counts them and deletes nothing.
func (a *API) purgeDLQ(ctx forge.Context, req *PurgeDLQRequest) (*PurgeDLQResponse, error) {
	before, ok := ctx.Context().Value(purgeAuditCutoffKey{}).(time.Time)
	var err error
	if !ok {
		before, err = purgeCutoff(req, time.Now().UTC())
	}
	if err != nil {
		return nil, err
	}

	resp := PurgeDLQResponse{DryRun: req.DryRun, Before: before}
	if req.DryRun {
		n, countErr := a.eng.CountDLQPurge(ctx.Context(), before)
		if countErr != nil {
			return nil, fmt.Errorf("count dlq purge: %w", countErr)
		}
		resp.Matched = n
	} else {
		n, purgeErr := a.eng.PurgeDLQ(ctx.Context(), before)
		if purgeErr != nil {
			return nil, fmt.Errorf("purge dlq: %w", purgeErr)
		}
		resp.Matched = n
		resp.Purged = n
	}

	return nil, ctx.JSON(http.StatusOK, resp)
}

// purgeCutoff reads the purge cutoff from req: before as an RFC 3339
// time, or older_than as a positive Go duration back from now, or 30 days
// back when neither is given. Anything else is a 400.
func purgeCutoff(req *PurgeDLQRequest, now time.Time) (time.Time, error) {
	switch {
	case req.Before != "" && req.OlderThan != "":
		return time.Time{}, forge.BadRequest("give before or older_than, not both")

	case req.Before != "":
		before, err := time.Parse(time.RFC3339, req.Before)
		if err != nil {
			return time.Time{}, forge.BadRequest(fmt.Sprintf("invalid before, want an RFC 3339 time: %v", err))
		}
		if before.IsZero() {
			return time.Time{}, forge.BadRequest("before must not be the zero time")
		}
		return before.UTC(), nil

	case req.OlderThan != "":
		age, err := time.ParseDuration(req.OlderThan)
		if err != nil {
			return time.Time{}, forge.BadRequest(fmt.Sprintf("invalid older_than, want a duration such as 72h: %v", err))
		}
		if age <= 0 {
			return time.Time{}, forge.BadRequest("older_than must be positive")
		}
		return now.Add(-age), nil

	default:
		return now.Add(-defaultPurgeAge), nil
	}
}

// deleteDLQ removes one entry for good.
func (a *API) deleteDLQ(ctx forge.Context, _ *DeleteDLQRequest) (*struct{}, error) {
	entryID, err := id.ParseDLQID(ctx.Param("entryId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid DLQ entry ID: %v", err))
	}

	if delErr := a.eng.DeleteDLQ(ctx.Context(), entryID); delErr != nil {
		return nil, mapStoreError(delErr)
	}

	return nil, ctx.NoContent(http.StatusNoContent)
}

func (a *API) dlqCount(ctx forge.Context) error {
	count, err := a.eng.DLQService().DLQStore().CountDLQ(ctx.Context())
	if err != nil {
		return fmt.Errorf("count dlq: %w", err)
	}

	return ctx.JSON(http.StatusOK, DLQCountResponse{Count: count})
}
