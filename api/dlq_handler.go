package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

func (a *API) listDLQ(ctx forge.Context, req *ListDLQRequest) ([]*dlq.Entry, error) {
	entries, err := a.eng.DLQService().DLQStore().ListDLQ(ctx.Context(), dlq.ListOpts{
		Limit:  defaultLimit(req.Limit),
		Offset: req.Offset,
		Queue:  req.Queue,
	})
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}

	return entries, ctx.JSON(http.StatusOK, entries)
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

	return entry, ctx.JSON(http.StatusOK, entry)
}

func (a *API) replayDLQ(ctx forge.Context, _ *ReplayDLQRequest) (*job.Job, error) {
	entryID, err := id.ParseDLQID(ctx.Param("entryId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid DLQ entry ID: %v", err))
	}

	j, err := a.eng.DLQService().Replay(ctx.Context(), entryID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return j, ctx.JSON(http.StatusCreated, j)
}

func (a *API) purgeDLQ(ctx forge.Context) error {
	// Purge entries older than 30 days.
	before := time.Now().UTC().Add(-30 * 24 * time.Hour)

	count, err := a.eng.DLQService().DLQStore().PurgeDLQ(ctx.Context(), before)
	if err != nil {
		return fmt.Errorf("purge dlq: %w", err)
	}

	return ctx.JSON(http.StatusOK, PurgeDLQResponse{Purged: count})
}

func (a *API) dlqCount(ctx forge.Context) error {
	count, err := a.eng.DLQService().DLQStore().CountDLQ(ctx.Context())
	if err != nil {
		return fmt.Errorf("count dlq: %w", err)
	}

	return ctx.JSON(http.StatusOK, DLQCountResponse{Count: count})
}
