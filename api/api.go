// Package api provides HTTP handlers for the Dispatch API.
package api

import (
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// API wires all Forge-style HTTP handlers together for the dispatch system.
type API struct {
	eng    *engine.Engine
	router forge.Router

	handlerOnce sync.Once
	handler     http.Handler
}

// New creates an API from a dispatch Engine.
func New(eng *engine.Engine, router forge.Router) *API {
	return &API{eng: eng, router: router}
}

// Handler returns the fully assembled http.Handler with all routes. The
// routes are registered on the first call; later calls return the same
// handler. A route forge refuses is a programming error, so Handler
// panics with forge's reason rather than serve an api with a hole in it.
func (a *API) Handler() http.Handler {
	a.handlerOnce.Do(func() {
		if a.router == nil {
			a.router = forge.NewRouter()
		}
		if err := a.RegisterRoutes(a.router); err != nil {
			panic(fmt.Sprintf("dispatch api: %v", err))
		}
		a.handler = a.router.Handler()
	})

	return a.handler
}

// RegisterRoutes registers all dispatch API routes into the given Forge router
// with full OpenAPI metadata. It returns every route forge refused, each
// named by method and path. Forge refuses a handler it cannot bind, or a
// path that collides with one already registered, and that route would
// otherwise answer 404.
func (a *API) RegisterRoutes(router forge.Router) error {
	return errors.Join(
		a.registerJobRoutes(router),
		a.registerWorkflowRoutes(router),
		a.registerDLQRoutes(router),
		a.registerCronRoutes(router),
		a.registerStatsRoutes(router),
	)
}

// routes registers one group's routes and keeps the error forge returns
// for each, named by method and path.
type routes struct {
	g    forge.Router
	errs []error
}

// group starts a /v1 group with the given OpenAPI tag.
func group(router forge.Router, tag string) *routes {
	return &routes{g: router.Group("/v1", forge.WithGroupTags(tag))}
}

func (r *routes) get(path string, handler any, opts ...forge.RouteOption) {
	r.keep(http.MethodGet, path, r.g.GET(path, handler, opts...))
}

func (r *routes) post(path string, handler any, opts ...forge.RouteOption) {
	r.keep(http.MethodPost, path, r.g.POST(path, handler, opts...))
}

func (r *routes) delete(path string, handler any, opts ...forge.RouteOption) {
	r.keep(http.MethodDelete, path, r.g.DELETE(path, handler, opts...))
}

func (r *routes) keep(method, path string, err error) {
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s /v1%s: %w", method, path, err))
	}
}

func (r *routes) err() error {
	return errors.Join(r.errs...)
}

// registerJobRoutes registers job management routes.
func (a *API) registerJobRoutes(router forge.Router) error {
	r := group(router, "jobs")

	r.get("/jobs", a.listJobs,
		forge.WithSummary("List jobs"),
		forge.WithDescription("Returns jobs filtered by state and queue."),
		forge.WithOperationID("listJobs"),
		forge.WithRequestSchema(ListJobsRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Job list", []*job.Job{}),
		forge.WithErrorResponses(),
	)

	r.get("/jobs/:jobId", a.getJob,
		forge.WithSummary("Get job"),
		forge.WithDescription("Returns details of a specific job."),
		forge.WithOperationID("getJob"),
		forge.WithRequestSchema(GetJobRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Job details", &job.Job{}),
		forge.WithErrorResponses(),
	)

	r.post("/jobs/:jobId/cancel", a.cancelJob,
		forge.WithSummary("Cancel job"),
		forge.WithDescription("Cancels a pending, retrying or running job. The worker cancels a running handler's context on its next lease renewal."),
		forge.WithOperationID("cancelJob"),
		forge.WithRequestSchema(CancelJobRequest{}),
		forge.WithNoContentResponse(),
		conflictResponse("The job is completed, failed or already cancelled"),
		forge.WithErrorResponses(),
	)

	r.post("/jobs/:jobId/retry", a.retryJob,
		forge.WithSummary("Retry job"),
		forge.WithDescription("Retries a failed job by resetting it to pending state. Claims the job's DLQ entry, if it has one, so the entry cannot also be replayed."),
		forge.WithOperationID("retryJob"),
		forge.WithRequestSchema(RetryJobRequest{}),
		forge.WithNoContentResponse(),
		conflictResponse("The job is not failed, or its DLQ entry was already replayed"),
		forge.WithErrorResponses(),
	)

	r.get("/jobs/counts", a.jobCounts,
		forge.WithSummary("Job counts"),
		forge.WithDescription("Returns job counts grouped by state."),
		forge.WithOperationID("jobCounts"),
		forge.WithResponseSchema(http.StatusOK, "Job counts", JobCountsResponse{}),
		forge.WithErrorResponses(),
	)

	return r.err()
}

// registerWorkflowRoutes registers workflow management routes.
func (a *API) registerWorkflowRoutes(router forge.Router) error {
	r := group(router, "workflows")

	r.get("/workflows", a.listWorkflowNames,
		forge.WithSummary("List workflows"),
		forge.WithDescription("Returns the names of all registered workflows."),
		forge.WithOperationID("listWorkflows"),
		forge.WithResponseSchema(http.StatusOK, "Workflow names", ListWorkflowNamesResponse{}),
		forge.WithErrorResponses(),
	)

	r.get("/workflows/runs", a.listWorkflowRuns,
		forge.WithSummary("List workflow runs"),
		forge.WithDescription("Returns workflow runs filtered by state."),
		forge.WithOperationID("listWorkflowRuns"),
		forge.WithRequestSchema(ListWorkflowRunsRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Workflow runs", []*workflow.Run{}),
		forge.WithErrorResponses(),
	)

	r.get("/workflows/runs/:runId", a.getWorkflowRun,
		forge.WithSummary("Get workflow run"),
		forge.WithDescription("Returns details of a specific workflow run."),
		forge.WithOperationID("getWorkflowRun"),
		forge.WithRequestSchema(GetWorkflowRunRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Workflow run details", &workflow.Run{}),
		forge.WithErrorResponses(),
	)

	r.get("/workflows/runs/:runId/replay", a.planWorkflowReplay,
		forge.WithSummary("Plan workflow replay"),
		forge.WithDescription("Reports what replaying the run from a step would do, without changing anything."),
		forge.WithOperationID("planWorkflowReplay"),
		forge.WithRequestSchema(PlanWorkflowReplayRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Replay plan", &workflow.ReplayPlan{}),
		conflictResponse("The step has no checkpoint, or the run's version is not registered"),
		forge.WithErrorResponses(),
	)

	r.post("/workflows/runs/:runId/replay", a.replayWorkflow,
		forge.WithSummary("Replay workflow run"),
		forge.WithDescription("Re-runs a finished run from a step on its own version. Answers once the replay has started; the run continues in the background."),
		forge.WithOperationID("replayWorkflowRun"),
		forge.WithRequestSchema(ReplayWorkflowRequest{}),
		forge.WithResponseSchema(http.StatusAccepted, "Replay started", &workflow.ReplayPlan{}),
		conflictResponse("The run is running, the step has no checkpoint, or the run's version is not registered"),
		forge.WithResponseSchema(http.StatusServiceUnavailable, "The workflow runner has shut down", ErrorResponse{}),
		forge.WithErrorResponses(),
	)

	return r.err()
}

// registerDLQRoutes registers dead letter queue management routes.
func (a *API) registerDLQRoutes(router forge.Router) error {
	r := group(router, "dlq")

	r.get("/dlq", a.listDLQ,
		forge.WithSummary("List DLQ entries"),
		forge.WithDescription("Returns dead letter queue entries."),
		forge.WithOperationID("listDLQ"),
		forge.WithRequestSchema(ListDLQRequest{}),
		forge.WithResponseSchema(http.StatusOK, "DLQ entries", []*dlq.Entry{}),
		forge.WithErrorResponses(),
	)

	r.get("/dlq/:entryId", a.getDLQ,
		forge.WithSummary("Get DLQ entry"),
		forge.WithDescription("Returns details of a specific DLQ entry."),
		forge.WithOperationID("getDLQ"),
		forge.WithRequestSchema(GetDLQRequest{}),
		forge.WithResponseSchema(http.StatusOK, "DLQ entry details", &dlq.Entry{}),
		forge.WithErrorResponses(),
	)

	r.post("/dlq/:entryId/replay", a.replayDLQ,
		forge.WithSummary("Replay DLQ entry"),
		forge.WithDescription("Re-enqueues a DLQ entry as a new pending job. An entry is replayed at most once."),
		forge.WithOperationID("dispatchReplayDLQ"),
		forge.WithRequestSchema(ReplayDLQRequest{}),
		forge.WithCreatedResponse(&job.Job{}),
		conflictResponse("The entry was already replayed, or no worker can run the job"),
		forge.WithErrorResponses(),
	)

	r.delete("/dlq/:entryId", a.deleteDLQ,
		forge.WithSummary("Delete DLQ entry"),
		forge.WithDescription("Permanently removes one DLQ entry."),
		forge.WithOperationID("deleteDLQ"),
		forge.WithRequestSchema(DeleteDLQRequest{}),
		forge.WithNoContentResponse(),
		forge.WithErrorResponses(),
	)

	r.post("/dlq/replay-all", a.replayAllDLQ,
		forge.WithSummary("Replay all DLQ entries"),
		forge.WithDescription("Re-enqueues unreplayed DLQ entries as new pending jobs, newest first, optionally in one queue and up to a limit."),
		forge.WithOperationID("replayAllDLQ"),
		forge.WithRequestSchema(ReplayAllDLQRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Replay result", ReplayAllDLQResponse{}),
		forge.WithErrorResponses(),
	)

	r.post("/dlq/purge", a.purgeDLQ,
		forge.WithSummary("Purge DLQ"),
		forge.WithDescription("Removes DLQ entries that failed before a cutoff: before, older_than, or 30 days ago when neither is given. dry_run counts them instead."),
		forge.WithOperationID("purgeDLQ"),
		forge.WithRequestSchema(PurgeDLQRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Purge result", PurgeDLQResponse{}),
		forge.WithErrorResponses(),
	)

	r.get("/dlq/count", a.dlqCount,
		forge.WithSummary("DLQ count"),
		forge.WithDescription("Returns the total number of DLQ entries."),
		forge.WithOperationID("dlqCount"),
		forge.WithResponseSchema(http.StatusOK, "DLQ count", DLQCountResponse{}),
		forge.WithErrorResponses(),
	)

	return r.err()
}

// registerCronRoutes registers cron management routes.
func (a *API) registerCronRoutes(router forge.Router) error {
	r := group(router, "crons")

	r.get("/crons", a.listCrons,
		forge.WithSummary("List cron entries"),
		forge.WithDescription("Returns all registered cron entries."),
		forge.WithOperationID("listCrons"),
		forge.WithRequestSchema(ListCronsRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Cron entries", []*cron.Entry{}),
		forge.WithErrorResponses(),
	)

	r.get("/crons/:cronId", a.getCron,
		forge.WithSummary("Get cron entry"),
		forge.WithDescription("Returns details of a specific cron entry."),
		forge.WithOperationID("getCron"),
		forge.WithRequestSchema(GetCronRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Cron entry details", &cron.Entry{}),
		forge.WithErrorResponses(),
	)

	r.post("/crons/:cronId/enable", a.enableCron,
		forge.WithSummary("Enable cron entry"),
		forge.WithDescription("Enables a cron entry. Its next run is computed from now, so it does not fire a catch-up."),
		forge.WithOperationID("enableCron"),
		forge.WithRequestSchema(EnableCronRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Enabled cron entry", &cron.Entry{}),
		conflictResponse("The schedule never fires"),
		forge.WithErrorResponses(),
	)

	r.post("/crons/:cronId/disable", a.disableCron,
		forge.WithSummary("Disable cron entry"),
		forge.WithDescription("Disables a cron entry so it no longer fires."),
		forge.WithOperationID("disableCron"),
		forge.WithRequestSchema(DisableCronRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Disabled cron entry", &cron.Entry{}),
		forge.WithErrorResponses(),
	)

	r.delete("/crons/:cronId", a.deleteCron,
		forge.WithSummary("Delete cron entry"),
		forge.WithDescription("Permanently removes a cron entry."),
		forge.WithOperationID("deleteCron"),
		forge.WithRequestSchema(DeleteCronRequest{}),
		forge.WithNoContentResponse(),
		forge.WithErrorResponses(),
	)

	r.post("/crons/:cronId/trigger", a.triggerCron,
		forge.WithSummary("Trigger cron entry"),
		forge.WithDescription("Enqueues the entry's job now. The schedule is left alone, and a disabled entry can be triggered too."),
		forge.WithOperationID("triggerCron"),
		forge.WithRequestSchema(TriggerCronRequest{}),
		forge.WithCreatedResponse(&job.Job{}),
		conflictResponse("No worker can run the job"),
		forge.WithErrorResponses(),
	)

	return r.err()
}

// conflictResponse documents the 409 an operator route answers when the
// current state refuses the action. WithErrorResponses does not list 409.
func conflictResponse(description string) forge.RouteOption {
	return forge.WithResponseSchema(http.StatusConflict, description, ErrorResponse{})
}

// registerStatsRoutes registers aggregate statistics routes.
func (a *API) registerStatsRoutes(router forge.Router) error {
	r := group(router, "stats")

	r.get("/stats", a.stats,
		forge.WithSummary("Dispatch stats"),
		forge.WithDescription("Returns aggregate statistics for jobs, workflows, and DLQ."),
		forge.WithOperationID("dispatchStats"),
		forge.WithResponseSchema(http.StatusOK, "Dispatch statistics", StatsResponse{}),
		forge.WithErrorResponses(),
	)

	return r.err()
}
