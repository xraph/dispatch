// Package api provides HTTP handlers for the Dispatch API.
package api

import (
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// listCrons answers a page of entries, oldest first, as a bare JSON array.
func (a *API) listCrons(ctx forge.Context, req *ListCronsRequest) (*ListCronsResponse, error) {
	cs, ok := a.eng.Dispatcher().Store().(cron.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement cron.Store")
	}

	entries, err := cs.ListCrons(ctx.Context())
	if err != nil {
		return nil, fmt.Errorf("list crons: %w", err)
	}

	// Apply basic pagination.
	limit := defaultLimit(req.Limit)
	offset := min(max(req.Offset, 0), len(entries))
	end := min(offset+limit, len(entries))

	return &ListCronsResponse{Entries: nonNil(entries[offset:end])}, nil
}

func (a *API) getCron(ctx forge.Context, _ *GetCronRequest) (*cron.Entry, error) {
	cronID, err := id.ParseCronID(ctx.Param("cronId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid cron ID: %v", err))
	}

	cs, ok := a.eng.Dispatcher().Store().(cron.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement cron.Store")
	}

	entry, err := cs.GetCron(ctx.Context(), cronID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusOK, entry)
}

// enableCron turns an entry on through the engine, which computes the
// next fire time from now. A schedule that never fires answers 409.
func (a *API) enableCron(ctx forge.Context, _ *EnableCronRequest) (*cron.Entry, error) {
	cronID, err := id.ParseCronID(ctx.Param("cronId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid cron ID: %v", err))
	}

	entry, err := a.eng.EnableCron(ctx.Context(), cronID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusOK, entry)
}

// disableCron turns an entry off through the engine.
func (a *API) disableCron(ctx forge.Context, _ *DisableCronRequest) (*cron.Entry, error) {
	cronID, err := id.ParseCronID(ctx.Param("cronId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid cron ID: %v", err))
	}

	entry, err := a.eng.DisableCron(ctx.Context(), cronID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusOK, entry)
}

// deleteCron removes an entry through the engine.
func (a *API) deleteCron(ctx forge.Context, _ *DeleteCronRequest) (*struct{}, error) {
	cronID, err := id.ParseCronID(ctx.Param("cronId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid cron ID: %v", err))
	}

	if delErr := a.eng.DeleteCron(ctx.Context(), cronID); delErr != nil {
		return nil, mapStoreError(delErr)
	}

	return nil, ctx.NoContent(http.StatusNoContent)
}

// triggerCron enqueues the entry's job now. The schedule is left alone,
// and a disabled entry can be triggered too.
func (a *API) triggerCron(ctx forge.Context, _ *TriggerCronRequest) (*job.Job, error) {
	cronID, err := id.ParseCronID(ctx.Param("cronId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid cron ID: %v", err))
	}

	j, err := a.eng.TriggerCron(ctx.Context(), cronID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusCreated, j)
}
