// Package api provides HTTP handlers for the Dispatch API.
package api

import (
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

func (a *API) stats(ctx forge.Context) error {
	c := ctx.Context()

	js, ok := a.eng.Dispatcher().Store().(job.Store)
	if !ok {
		return forge.InternalError(fmt.Errorf("store does not implement job.Store"))
	}
	ws, ok := a.eng.Dispatcher().Store().(workflow.Store)
	if !ok {
		return forge.InternalError(fmt.Errorf("store does not implement workflow.Store"))
	}

	// Job counts.
	var jobCounts JobCountsResponse
	for _, state := range []job.State{
		job.StatePending, job.StateRunning, job.StateCompleted,
		job.StateFailed, job.StateRetrying, job.StateCancelled,
	} {
		count, err := js.CountJobs(c, job.CountOpts{State: state})
		if err != nil {
			return err
		}
		switch state {
		case job.StatePending:
			jobCounts.Pending = count
		case job.StateRunning:
			jobCounts.Running = count
		case job.StateCompleted:
			jobCounts.Completed = count
		case job.StateFailed:
			jobCounts.Failed = count
		case job.StateRetrying:
			jobCounts.Retrying = count
		case job.StateCancelled:
			jobCounts.Cancelled = count
		}
	}

	// DLQ count.
	dlqCount, err := a.eng.DLQService().DLQStore().CountDLQ(c)
	if err != nil {
		return err
	}

	// Workflow counts.
	var wfCounts WorkflowCounts
	for _, state := range []workflow.RunState{
		workflow.RunStateRunning, workflow.RunStateCompleted, workflow.RunStateFailed,
	} {
		runs, listErr := ws.ListRuns(c, workflow.ListOpts{State: state, Limit: 0})
		if listErr != nil {
			return listErr
		}
		switch state {
		case workflow.RunStateRunning:
			wfCounts.Running = len(runs)
		case workflow.RunStateCompleted:
			wfCounts.Completed = len(runs)
		case workflow.RunStateFailed:
			wfCounts.Failed = len(runs)
		}
	}

	return ctx.JSON(http.StatusOK, StatsResponse{
		Jobs:      jobCounts,
		DLQCount:  dlqCount,
		Workflows: wfCounts,
	})
}
