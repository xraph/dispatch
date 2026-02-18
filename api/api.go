package api

import (
	"net/http"

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
}

// New creates an API from a dispatch Engine.
func New(eng *engine.Engine, router forge.Router) *API {
	return &API{eng: eng, router: router}
}

// Handler returns the fully assembled http.Handler with all routes.
func (a *API) Handler() http.Handler {
	if a.router == nil {
		a.router = forge.NewRouter()
	}
	a.RegisterRoutes(a.router)
	return a.router.Handler()
}

// RegisterRoutes registers all dispatch API routes into the given Forge router
// with full OpenAPI metadata.
func (a *API) RegisterRoutes(router forge.Router) {
	a.registerJobRoutes(router)
	a.registerWorkflowRoutes(router)
	a.registerDLQRoutes(router)
	a.registerCronRoutes(router)
	a.registerStatsRoutes(router)
}

// registerJobRoutes registers job management routes.
func (a *API) registerJobRoutes(router forge.Router) {
	g := router.Group("/v1", forge.WithGroupTags("jobs"))

	_ = g.GET("/jobs", a.listJobs,
		forge.WithSummary("List jobs"),
		forge.WithDescription("Returns jobs filtered by state and queue."),
		forge.WithOperationID("listJobs"),
		forge.WithRequestSchema(ListJobsRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Job list", []*job.Job{}),
		forge.WithErrorResponses(),
	)

	_ = g.GET("/jobs/:jobId", a.getJob,
		forge.WithSummary("Get job"),
		forge.WithDescription("Returns details of a specific job."),
		forge.WithOperationID("getJob"),
		forge.WithResponseSchema(http.StatusOK, "Job details", &job.Job{}),
		forge.WithErrorResponses(),
	)

	_ = g.POST("/jobs/:jobId/cancel", a.cancelJob,
		forge.WithSummary("Cancel job"),
		forge.WithDescription("Cancels a pending or retrying job."),
		forge.WithOperationID("cancelJob"),
		forge.WithNoContentResponse(),
		forge.WithErrorResponses(),
	)

	_ = g.GET("/jobs/counts", a.jobCounts,
		forge.WithSummary("Job counts"),
		forge.WithDescription("Returns job counts grouped by state."),
		forge.WithOperationID("jobCounts"),
		forge.WithResponseSchema(http.StatusOK, "Job counts", JobCountsResponse{}),
		forge.WithErrorResponses(),
	)
}

// registerWorkflowRoutes registers workflow management routes.
func (a *API) registerWorkflowRoutes(router forge.Router) {
	g := router.Group("/v1", forge.WithGroupTags("workflows"))

	_ = g.GET("/workflows", a.listWorkflowNames,
		forge.WithSummary("List workflows"),
		forge.WithDescription("Returns the names of all registered workflows."),
		forge.WithOperationID("listWorkflows"),
		forge.WithResponseSchema(http.StatusOK, "Workflow names", ListWorkflowNamesResponse{}),
		forge.WithErrorResponses(),
	)

	_ = g.GET("/workflows/runs", a.listWorkflowRuns,
		forge.WithSummary("List workflow runs"),
		forge.WithDescription("Returns workflow runs filtered by state."),
		forge.WithOperationID("listWorkflowRuns"),
		forge.WithRequestSchema(ListWorkflowRunsRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Workflow runs", []*workflow.Run{}),
		forge.WithErrorResponses(),
	)

	_ = g.GET("/workflows/runs/:runId", a.getWorkflowRun,
		forge.WithSummary("Get workflow run"),
		forge.WithDescription("Returns details of a specific workflow run."),
		forge.WithOperationID("getWorkflowRun"),
		forge.WithResponseSchema(http.StatusOK, "Workflow run details", &workflow.Run{}),
		forge.WithErrorResponses(),
	)
}

// registerDLQRoutes registers dead letter queue management routes.
func (a *API) registerDLQRoutes(router forge.Router) {
	g := router.Group("/v1", forge.WithGroupTags("dlq"))

	_ = g.GET("/dlq", a.listDLQ,
		forge.WithSummary("List DLQ entries"),
		forge.WithDescription("Returns dead letter queue entries."),
		forge.WithOperationID("listDLQ"),
		forge.WithRequestSchema(ListDLQRequest{}),
		forge.WithResponseSchema(http.StatusOK, "DLQ entries", []*dlq.Entry{}),
		forge.WithErrorResponses(),
	)

	_ = g.GET("/dlq/:entryId", a.getDLQ,
		forge.WithSummary("Get DLQ entry"),
		forge.WithDescription("Returns details of a specific DLQ entry."),
		forge.WithOperationID("getDLQ"),
		forge.WithResponseSchema(http.StatusOK, "DLQ entry details", &dlq.Entry{}),
		forge.WithErrorResponses(),
	)

	_ = g.POST("/dlq/:entryId/replay", a.replayDLQ,
		forge.WithSummary("Replay DLQ entry"),
		forge.WithDescription("Re-enqueues a DLQ entry as a new pending job."),
		forge.WithOperationID("replayDLQ"),
		forge.WithCreatedResponse(&job.Job{}),
		forge.WithErrorResponses(),
	)

	_ = g.POST("/dlq/purge", a.purgeDLQ,
		forge.WithSummary("Purge DLQ"),
		forge.WithDescription("Removes old DLQ entries."),
		forge.WithOperationID("purgeDLQ"),
		forge.WithResponseSchema(http.StatusOK, "Purge result", PurgeDLQResponse{}),
		forge.WithErrorResponses(),
	)

	_ = g.GET("/dlq/count", a.dlqCount,
		forge.WithSummary("DLQ count"),
		forge.WithDescription("Returns the total number of DLQ entries."),
		forge.WithOperationID("dlqCount"),
		forge.WithResponseSchema(http.StatusOK, "DLQ count", DLQCountResponse{}),
		forge.WithErrorResponses(),
	)
}

// registerCronRoutes registers cron management routes.
func (a *API) registerCronRoutes(router forge.Router) {
	g := router.Group("/v1", forge.WithGroupTags("crons"))

	_ = g.GET("/crons", a.listCrons,
		forge.WithSummary("List cron entries"),
		forge.WithDescription("Returns all registered cron entries."),
		forge.WithOperationID("listCrons"),
		forge.WithRequestSchema(ListCronsRequest{}),
		forge.WithResponseSchema(http.StatusOK, "Cron entries", []*cron.Entry{}),
		forge.WithErrorResponses(),
	)

	_ = g.GET("/crons/:cronId", a.getCron,
		forge.WithSummary("Get cron entry"),
		forge.WithDescription("Returns details of a specific cron entry."),
		forge.WithOperationID("getCron"),
		forge.WithResponseSchema(http.StatusOK, "Cron entry details", &cron.Entry{}),
		forge.WithErrorResponses(),
	)

	_ = g.POST("/crons/:cronId/enable", a.enableCron,
		forge.WithSummary("Enable cron entry"),
		forge.WithDescription("Enables a disabled cron entry."),
		forge.WithOperationID("enableCron"),
		forge.WithResponseSchema(http.StatusOK, "Enabled cron entry", &cron.Entry{}),
		forge.WithErrorResponses(),
	)

	_ = g.POST("/crons/:cronId/disable", a.disableCron,
		forge.WithSummary("Disable cron entry"),
		forge.WithDescription("Disables a cron entry so it no longer fires."),
		forge.WithOperationID("disableCron"),
		forge.WithResponseSchema(http.StatusOK, "Disabled cron entry", &cron.Entry{}),
		forge.WithErrorResponses(),
	)

	_ = g.DELETE("/crons/:cronId", a.deleteCron,
		forge.WithSummary("Delete cron entry"),
		forge.WithDescription("Permanently removes a cron entry."),
		forge.WithOperationID("deleteCron"),
		forge.WithNoContentResponse(),
		forge.WithErrorResponses(),
	)
}

// registerStatsRoutes registers aggregate statistics routes.
func (a *API) registerStatsRoutes(router forge.Router) {
	g := router.Group("/v1", forge.WithGroupTags("stats"))

	_ = g.GET("/stats", a.stats,
		forge.WithSummary("Dispatch stats"),
		forge.WithDescription("Returns aggregate statistics for jobs, workflows, and DLQ."),
		forge.WithOperationID("dispatchStats"),
		forge.WithResponseSchema(http.StatusOK, "Dispatch statistics", StatsResponse{}),
		forge.WithErrorResponses(),
	)
}
