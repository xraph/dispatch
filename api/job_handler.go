package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

func (a *API) listJobs(ctx forge.Context, req *ListJobsRequest) ([]*job.Job, error) {
	state := jobStateFromString(req.State)

	js, ok := a.eng.Dispatcher().Store().(job.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement job.Store")
	}

	jobs, err := js.ListJobsByState(ctx.Context(), state, job.ListOpts{
		Limit:  defaultLimit(req.Limit),
		Offset: req.Offset,
		Queue:  req.Queue,
	})
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}

	return jobs, ctx.JSON(http.StatusOK, jobs)
}

func (a *API) getJob(ctx forge.Context, _ *GetJobRequest) (*job.Job, error) {
	jobID, err := id.ParseJobID(ctx.Param("jobId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid job ID: %v", err))
	}

	js, ok := a.eng.Dispatcher().Store().(job.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement job.Store")
	}

	j, err := js.GetJob(ctx.Context(), jobID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return j, ctx.JSON(http.StatusOK, j)
}

func (a *API) cancelJob(ctx forge.Context, _ *CancelJobRequest) (*struct{}, error) {
	jobID, err := id.ParseJobID(ctx.Param("jobId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid job ID: %v", err))
	}

	js, ok := a.eng.Dispatcher().Store().(job.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement job.Store")
	}

	j, err := js.GetJob(ctx.Context(), jobID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	if j.State != job.StatePending && j.State != job.StateRetrying {
		return nil, forge.BadRequest(fmt.Sprintf("can only cancel pending or retrying jobs, current state: %s", j.State))
	}

	now := time.Now().UTC()
	j.State = job.StateCancelled
	j.CompletedAt = &now
	if updateErr := js.UpdateJob(ctx.Context(), j); updateErr != nil {
		return nil, fmt.Errorf("cancel job: %w", updateErr)
	}

	return nil, ctx.NoContent(http.StatusNoContent)
}

func (a *API) jobCounts(ctx forge.Context) error {
	js, ok := a.eng.Dispatcher().Store().(job.Store)
	if !ok {
		return fmt.Errorf("store does not implement job.Store")
	}

	c := ctx.Context()

	states := []job.State{
		job.StatePending,
		job.StateRunning,
		job.StateCompleted,
		job.StateFailed,
		job.StateRetrying,
		job.StateCancelled,
	}

	resp := JobCountsResponse{}
	for _, state := range states {
		count, err := js.CountJobs(c, job.CountOpts{State: state})
		if err != nil {
			return fmt.Errorf("count jobs (%s): %w", state, err)
		}
		switch state {
		case job.StatePending:
			resp.Pending = count
		case job.StateRunning:
			resp.Running = count
		case job.StateCompleted:
			resp.Completed = count
		case job.StateFailed:
			resp.Failed = count
		case job.StateRetrying:
			resp.Retrying = count
		case job.StateCancelled:
			resp.Cancelled = count
		}
	}

	return ctx.JSON(http.StatusOK, resp)
}

// mapStoreError converts dispatch sentinel errors to forge HTTP errors.
func mapStoreError(err error) error {
	if err == nil {
		return nil
	}
	if isNotFound(err) {
		return forge.NotFound(err.Error())
	}
	return err
}

func isNotFound(err error) bool {
	return errors.Is(err, dispatch.ErrJobNotFound) ||
		errors.Is(err, dispatch.ErrRunNotFound) ||
		errors.Is(err, dispatch.ErrDLQNotFound) ||
		errors.Is(err, dispatch.ErrWorkflowNotFound) ||
		errors.Is(err, dispatch.ErrEventNotFound) ||
		errors.Is(err, dispatch.ErrCronNotFound)
}
