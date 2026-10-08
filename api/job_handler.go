// Package api provides HTTP handlers for the Dispatch API.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/workflow"
)

// allJobStates is every job state, for a list with no state filter.
var allJobStates = []job.State{
	job.StatePending,
	job.StateRunning,
	job.StateCompleted,
	job.StateFailed,
	job.StateRetrying,
	job.StateCancelled,
}

// listJobs answers a page of jobs, oldest first, as a bare JSON array. With
// no state it lists every state; an unknown state is a 400.
func (a *API) listJobs(ctx forge.Context, req *ListJobsRequest) (*ListJobsResponse, error) {
	states := allJobStates
	if req.State != "" {
		state := jobStateFromString(req.State)
		if state == "" {
			return nil, forge.BadRequest(fmt.Sprintf("unknown job state %q", req.State))
		}
		states = []job.State{state}
	}

	js, ok := a.eng.Dispatcher().Store().(job.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement job.Store")
	}

	jobs, err := listJobsInStates(ctx.Context(), js, states, req.Queue, defaultLimit(req.Limit), max(req.Offset, 0))
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}

	return &ListJobsResponse{Jobs: nonNil(jobs)}, nil
}

// listJobsInStates pages through the jobs in states, oldest first. The
// store lists one state at a time, so for several it takes the first
// offset+limit of each, merges them in the store's order (created_at, then
// ID), and cuts the page from the merge.
func listJobsInStates(ctx context.Context, js job.Store, states []job.State, queue string, limit, offset int) ([]*job.Job, error) {
	if len(states) == 1 {
		return js.ListJobsByState(ctx, states[0], job.ListOpts{Limit: limit, Offset: offset, Queue: queue})
	}

	var merged []*job.Job
	for _, state := range states {
		part, err := js.ListJobsByState(ctx, state, job.ListOpts{Limit: offset + limit, Queue: queue})
		if err != nil {
			return nil, err
		}
		merged = append(merged, part...)
	}

	sort.Slice(merged, func(i, k int) bool {
		if !merged[i].CreatedAt.Equal(merged[k].CreatedAt) {
			return merged[i].CreatedAt.Before(merged[k].CreatedAt)
		}
		return merged[i].ID.String() < merged[k].ID.String()
	})

	if offset >= len(merged) {
		return nil, nil
	}
	merged = merged[offset:]
	if len(merged) > limit {
		merged = merged[:limit]
	}

	return merged, nil
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

	return nil, ctx.JSON(http.StatusOK, j)
}

// cancelJob cancels a pending, retrying or running job through the
// engine. A running job's worker stops when it next touches its lease.
// Any other state answers 409.
func (a *API) cancelJob(ctx forge.Context, _ *CancelJobRequest) (*struct{}, error) {
	jobID, err := id.ParseJobID(ctx.Param("jobId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid job ID: %v", err))
	}

	if _, cancelErr := a.eng.CancelJob(ctx.Context(), jobID); cancelErr != nil {
		return nil, mapStoreError(cancelErr)
	}

	return nil, ctx.NoContent(http.StatusNoContent)
}

// retryJob puts a failed job back to pending through the engine, which
// claims the job's dead letter entry first. Any other state, or an entry
// already replayed, answers 409.
func (a *API) retryJob(ctx forge.Context, _ *RetryJobRequest) (*struct{}, error) {
	jobID, err := id.ParseJobID(ctx.Param("jobId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid job ID: %v", err))
	}

	if _, retryErr := a.eng.RetryJob(ctx.Context(), jobID); retryErr != nil {
		return nil, mapStoreError(retryErr)
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

// mapStoreError converts dispatch sentinel errors to forge HTTP errors:
// not found answers 404, an operator action the current state refuses
// answers 409, and a workflow runner that has shut down answers 503.
// Anything else passes through and answers 500.
func mapStoreError(err error) error {
	switch {
	case err == nil:
		return nil
	case isNotFound(err):
		return forge.NotFound(err.Error())
	case isConflict(err):
		return forge.NewHTTPError(http.StatusConflict, err.Error())
	case errors.Is(err, workflow.ErrRunnerShutdown):
		return forge.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	default:
		return err
	}
}

// isConflict reports a refusal that comes from the state of things, not
// from the request: the job, run or schedule is in a state that does not
// allow the action, the entry was already replayed, or no worker in the
// fleet is big enough for the job.
func isConflict(err error) bool {
	return errors.Is(err, dispatch.ErrInvalidState) ||
		errors.Is(err, dispatch.ErrDLQAlreadyReplayed) ||
		errors.Is(err, resource.ErrUnschedulable)
}

func isNotFound(err error) bool {
	return errors.Is(err, dispatch.ErrJobNotFound) ||
		errors.Is(err, dispatch.ErrRunNotFound) ||
		errors.Is(err, dispatch.ErrDLQNotFound) ||
		errors.Is(err, dispatch.ErrWorkflowNotFound) ||
		errors.Is(err, dispatch.ErrEventNotFound) ||
		errors.Is(err, dispatch.ErrCronNotFound)
}
