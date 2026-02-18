package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
)

func (a *API) listCrons(ctx forge.Context, req *ListCronsRequest) ([]*cron.Entry, error) {
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
	offset := req.Offset
	if offset > len(entries) {
		offset = len(entries)
	}
	end := offset + limit
	if end > len(entries) {
		end = len(entries)
	}
	page := entries[offset:end]

	return page, ctx.JSON(http.StatusOK, page)
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

	return entry, ctx.JSON(http.StatusOK, entry)
}

func (a *API) enableCron(ctx forge.Context, _ *EnableCronRequest) (*cron.Entry, error) {
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

	entry.Enabled = true
	entry.UpdatedAt = time.Now().UTC()
	if updateErr := cs.UpdateCronEntry(ctx.Context(), entry); updateErr != nil {
		return nil, fmt.Errorf("enable cron: %w", updateErr)
	}

	return entry, ctx.JSON(http.StatusOK, entry)
}

func (a *API) disableCron(ctx forge.Context, _ *DisableCronRequest) (*cron.Entry, error) {
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

	entry.Enabled = false
	entry.UpdatedAt = time.Now().UTC()
	if updateErr := cs.UpdateCronEntry(ctx.Context(), entry); updateErr != nil {
		return nil, fmt.Errorf("disable cron: %w", updateErr)
	}

	return entry, ctx.JSON(http.StatusOK, entry)
}

func (a *API) deleteCron(ctx forge.Context, _ *DeleteCronRequest) (*struct{}, error) {
	cronID, err := id.ParseCronID(ctx.Param("cronId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid cron ID: %v", err))
	}

	cs, ok := a.eng.Dispatcher().Store().(cron.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement cron.Store")
	}

	if delErr := cs.DeleteCron(ctx.Context(), cronID); delErr != nil {
		return nil, mapStoreError(delErr)
	}

	return nil, ctx.NoContent(http.StatusNoContent)
}
