package dashboard

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/a-h/templ"

	"github.com/xraph/forge/extensions/dashboard/contributor"

	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dashboard/components"
	"github.com/xraph/dispatch/dashboard/pages"
	"github.com/xraph/dispatch/dashboard/settings"
	"github.com/xraph/dispatch/dashboard/widgets"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

var _ contributor.LocalContributor = (*Contributor)(nil)

// Contributor implements the dashboard LocalContributor interface for dispatch.
type Contributor struct {
	manifest    *contributor.Manifest
	engine      *engine.Engine
	apiBasePath string
}

// New creates a new dispatch dashboard contributor.
// apiBasePath is the URL prefix for the dispatch API routes (e.g. "/dispatch").
func New(manifest *contributor.Manifest, eng *engine.Engine, apiBasePath string) *Contributor {
	if apiBasePath == "" {
		apiBasePath = "/dispatch"
	}
	return &Contributor{
		manifest:    manifest,
		engine:      eng,
		apiBasePath: apiBasePath,
	}
}

// Manifest returns the contributor manifest.
func (c *Contributor) Manifest() *contributor.Manifest { return c.manifest }

// RenderPage renders a page for the given route.
func (c *Contributor) RenderPage(ctx context.Context, route string, params contributor.Params) (templ.Component, error) {
	if c.engine == nil {
		return components.EmptyState("alert-circle", "Engine not initialized", "The Dispatch engine is not available. Please check extension configuration."), nil
	}

	// Normalize route.
	route = strings.TrimRight(route, "/")
	if route == "" {
		route = "/"
	}

	comp, err := c.renderPageRoute(ctx, route, params)
	if err != nil {
		return nil, err
	}

	// Wrap every page in the PathRewriter so bare hx-get paths (e.g. "/crons/detail")
	// are rewritten to the fully-qualified dashboard extension path at runtime,
	// and API paths (e.g. "/v1/jobs/...") are prefixed with the API base path.
	pagesBase := params.BasePath + "/ext/" + c.manifest.Name + "/pages"
	return templ.ComponentFunc(func(tCtx context.Context, w io.Writer) error {
		return components.PathRewriter(pagesBase, c.apiBasePath).Render(templ.WithChildren(tCtx, comp), w)
	}), nil
}

// renderPageRoute dispatches to the correct page renderer based on the route.
func (c *Contributor) renderPageRoute(ctx context.Context, route string, params contributor.Params) (templ.Component, error) {
	switch route {
	case "/":
		return c.renderOverview(ctx)
	case "/jobs":
		return c.renderJobs(ctx, params)
	case "/jobs/detail":
		return c.renderJobDetail(ctx, params)
	case "/workflows":
		return c.renderWorkflows(ctx, params)
	case "/workflows/detail":
		return c.renderWorkflowDetail(ctx, params)
	case "/dlq":
		return c.renderDLQ(ctx, params)
	case "/dlq/detail":
		return c.renderDLQDetail(ctx, params)
	case "/queues":
		return c.renderQueues(ctx)
	case "/queues/detail":
		return c.renderQueueDetail(ctx, params)
	case "/handlers":
		return c.renderHandlers(ctx)
	case "/workers":
		return c.renderWorkers(ctx)
	case "/workers/detail":
		return c.renderWorkerDetail(ctx, params)
	case "/crons":
		return c.renderCrons(ctx, params)
	case "/crons/detail":
		return c.renderCronDetail(ctx, params)
	default:
		return components.EmptyState("alert-circle", "Page not found", "The requested page '"+route+"' does not exist in the Dispatch dashboard."), nil
	}
}

// RenderWidget renders a widget by ID.
func (c *Contributor) RenderWidget(ctx context.Context, widgetID string) (templ.Component, error) {
	if c.engine == nil {
		return nil, contributor.ErrWidgetNotFound
	}

	switch widgetID {
	case "dispatch-stats":
		return c.renderStatsWidget(ctx)
	case "dispatch-recent-jobs":
		return c.renderRecentJobsWidget(ctx)
	case "dispatch-dlq":
		return c.renderDLQCountWidget(ctx)
	case "dispatch-queue-activity":
		return c.renderQueueActivityWidget(ctx)
	case "dispatch-cluster-health":
		return c.renderClusterHealthWidget(ctx)
	case "dispatch-cron-schedule":
		return c.renderCronScheduleWidget(ctx)
	default:
		return nil, contributor.ErrWidgetNotFound
	}
}

// RenderSettings renders a settings panel by ID.
func (c *Contributor) RenderSettings(_ context.Context, settingID string) (templ.Component, error) {
	switch settingID {
	case "dispatch-config":
		return c.renderConfigSettings()
	default:
		return nil, contributor.ErrSettingNotFound
	}
}

// --- Page Renderers ---

func (c *Contributor) renderOverview(ctx context.Context) (templ.Component, error) {
	js, jsOK := resolveJobStore(c.engine)
	ws, wsOK := resolveWorkflowStore(c.engine)
	ds, dsOK := resolveDLQStore(c.engine)
	cs, csOK := resolveCronStore(c.engine)

	var jc jobCounts
	if jsOK {
		jc = fetchJobCounts(ctx, js)
	}
	var wc workflowCounts
	if wsOK {
		wc = fetchWorkflowCounts(ctx, ws)
	}
	var dlqCount int64
	if dsOK {
		dlqCount = fetchDLQCount(ctx, ds)
	}
	var cronCount int
	if csOK {
		cronCount = len(fetchCronEntries(ctx, cs))
	}

	// Worker count.
	var workerCount int64
	if clusterStore, clusterOK := resolveClusterStore(c.engine); clusterOK {
		workers := fetchWorkers(ctx, clusterStore)
		workerCount = int64(len(workers))
	}

	// Recent jobs (running + completed + failed).
	var recentJobs []*job.Job
	if jsOK {
		for _, state := range []job.State{job.StateRunning, job.StateCompleted, job.StateFailed} {
			jobs, _ := fetchJobsByState(ctx, js, state, 5, 0) //nolint:errcheck // best-effort dashboard data
			recentJobs = append(recentJobs, jobs...)
		}
		if len(recentJobs) > 10 {
			recentJobs = recentJobs[:10]
		}
	}

	// Recent workflow runs.
	var recentRuns []*workflow.Run
	if wsOK {
		recentRuns = fetchRecentWorkflowRuns(ctx, ws, 10)
	}

	return pages.OverviewPage(jc.Pending, jc.Running, jc.Completed, jc.Failed, jc.Retrying, jc.Cancelled,
		int64(wc.Total), dlqCount, int64(cronCount), workerCount, recentJobs, recentRuns), nil
}

func (c *Contributor) renderJobs(ctx context.Context, params contributor.Params) (templ.Component, error) {
	js, ok := resolveJobStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	stateFilter := params.QueryParams["state"]
	queueFilter := params.QueryParams["queue"]
	nameFilter := params.QueryParams["name"]
	limit := parseIntParam(params.QueryParams, "limit", 20)
	offset := parseIntParam(params.QueryParams, "offset", 0)

	var jobs []*job.Job
	var total int64

	if stateFilter != "" {
		state := job.State(stateFilter)
		var err error
		jobs, err = fetchJobsByState(ctx, js, state, limit, offset)
		if err != nil {
			return components.EmptyState("alert-circle", "Error loading jobs", err.Error()), nil
		}
		total, _ = js.CountJobs(ctx, job.CountOpts{State: state, Queue: queueFilter}) //nolint:errcheck // best-effort dashboard data
	} else {
		// Fetch from all states.
		for _, state := range []job.State{job.StateRunning, job.StatePending, job.StateRetrying, job.StateFailed, job.StateCompleted, job.StateCancelled} {
			items, _ := fetchJobsByState(ctx, js, state, limit, offset) //nolint:errcheck // best-effort dashboard data
			jobs = append(jobs, items...)
		}
		jc := fetchJobCounts(ctx, js)
		total = jc.Total
		if len(jobs) > limit {
			jobs = jobs[:limit]
		}
	}

	// Client-side name filtering.
	if nameFilter != "" {
		filtered := make([]*job.Job, 0, len(jobs))
		lower := strings.ToLower(nameFilter)
		for _, j := range jobs {
			if strings.Contains(strings.ToLower(j.Name), lower) {
				filtered = append(filtered, j)
			}
		}
		jobs = filtered
	}

	pg := NewPaginationMeta(total, limit, offset)
	return pages.JobsPage(jobs, stateFilter, queueFilter, nameFilter, pg), nil
}

func (c *Contributor) renderJobDetail(ctx context.Context, params contributor.Params) (templ.Component, error) {
	js, ok := resolveJobStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	idStr := params.QueryParams["id"]
	if idStr == "" {
		return nil, contributor.ErrPageNotFound
	}

	jobID, err := id.ParseJobID(idStr)
	if err != nil {
		return nil, contributor.ErrPageNotFound
	}

	j, err := js.GetJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("dashboard: resolve job: %w", err)
	}

	return pages.JobDetailPage(j), nil
}

func (c *Contributor) renderWorkflows(ctx context.Context, params contributor.Params) (templ.Component, error) {
	ws, ok := resolveWorkflowStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	stateFilter := params.QueryParams["state"]
	nameFilter := params.QueryParams["name"]
	limit := parseIntParam(params.QueryParams, "limit", 20)
	offset := parseIntParam(params.QueryParams, "offset", 0)

	var state workflow.RunState
	if stateFilter != "" {
		state = workflow.RunState(stateFilter)
	}

	runs, err := fetchWorkflowRuns(ctx, ws, state, limit, offset)
	if err != nil {
		return components.EmptyState("alert-circle", "Error loading workflows", err.Error()), nil
	}

	// Client-side name filtering.
	if nameFilter != "" {
		filtered := make([]*workflow.Run, 0, len(runs))
		lower := strings.ToLower(nameFilter)
		for _, r := range runs {
			if strings.Contains(strings.ToLower(r.Name), lower) {
				filtered = append(filtered, r)
			}
		}
		runs = filtered
	}

	// Get registered workflow names.
	var names []string
	if c.engine.WorkflowRunner() != nil && c.engine.WorkflowRunner().Registry() != nil {
		names = c.engine.WorkflowRunner().Registry().Names()
	}

	wc := fetchWorkflowCounts(ctx, ws)
	pg := NewPaginationMeta(int64(wc.Total), limit, offset)

	return pages.WorkflowsPage(runs, names, stateFilter, nameFilter, pg), nil
}

func (c *Contributor) renderWorkflowDetail(ctx context.Context, params contributor.Params) (templ.Component, error) {
	ws, ok := resolveWorkflowStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	idStr := params.QueryParams["id"]
	if idStr == "" {
		return nil, contributor.ErrPageNotFound
	}

	runID, err := id.ParseRunID(idStr)
	if err != nil {
		return nil, contributor.ErrPageNotFound
	}

	r, err := ws.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("dashboard: resolve workflow run: %w", err)
	}

	checkpoints, _ := ws.ListCheckpoints(ctx, runID) //nolint:errcheck // best-effort dashboard data
	childRuns, _ := ws.ListChildRuns(ctx, runID)     //nolint:errcheck // best-effort dashboard data

	return pages.WorkflowDetailPage(r, checkpoints, childRuns), nil
}

func (c *Contributor) renderDLQ(ctx context.Context, params contributor.Params) (templ.Component, error) {
	ds, ok := resolveDLQStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	queueFilter := params.QueryParams["queue"]
	nameFilter := params.QueryParams["name"]
	limit := parseIntParam(params.QueryParams, "limit", 20)
	offset := parseIntParam(params.QueryParams, "offset", 0)

	entries, err := fetchDLQEntries(ctx, ds, queueFilter, limit, offset)
	if err != nil {
		return components.EmptyState("alert-circle", "Error loading DLQ", err.Error()), nil
	}

	// Client-side name filtering.
	if nameFilter != "" {
		filtered := make([]*dlq.Entry, 0, len(entries))
		lower := strings.ToLower(nameFilter)
		for _, e := range entries {
			if strings.Contains(strings.ToLower(e.JobName), lower) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}

	total := fetchDLQCount(ctx, ds)
	pg := NewPaginationMeta(total, limit, offset)

	return pages.DLQPage(entries, queueFilter, nameFilter, pg), nil
}

func (c *Contributor) renderDLQDetail(ctx context.Context, params contributor.Params) (templ.Component, error) {
	ds, ok := resolveDLQStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	idStr := params.QueryParams["id"]
	if idStr == "" {
		return nil, contributor.ErrPageNotFound
	}

	entryID, err := id.ParseDLQID(idStr)
	if err != nil {
		return nil, contributor.ErrPageNotFound
	}

	entry, err := ds.GetDLQ(ctx, entryID)
	if err != nil {
		return nil, fmt.Errorf("dashboard: resolve dlq entry: %w", err)
	}

	return pages.DLQDetailPage(entry), nil
}

func (c *Contributor) renderCrons(ctx context.Context, params contributor.Params) (templ.Component, error) {
	cs, ok := resolveCronStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	searchFilter := params.QueryParams["search"]
	limit := parseIntParam(params.QueryParams, "limit", 20)
	offset := parseIntParam(params.QueryParams, "offset", 0)

	entries := fetchCronEntries(ctx, cs)

	// Client-side search filtering.
	if searchFilter != "" {
		filtered := make([]*cron.Entry, 0, len(entries))
		lower := strings.ToLower(searchFilter)
		for _, e := range entries {
			if strings.Contains(strings.ToLower(e.Name), lower) ||
				strings.Contains(strings.ToLower(e.JobName), lower) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}

	total := int64(len(entries))

	// Manual pagination.
	if offset < len(entries) {
		end := offset + limit
		if end > len(entries) {
			end = len(entries)
		}
		entries = entries[offset:end]
	} else {
		entries = nil
	}

	pg := NewPaginationMeta(total, limit, offset)
	return pages.CronsPage(entries, searchFilter, pg), nil
}

func (c *Contributor) renderCronDetail(ctx context.Context, params contributor.Params) (templ.Component, error) {
	cs, ok := resolveCronStore(c.engine)
	if !ok {
		return components.EmptyState("database", "No store configured", "The Dispatch dashboard requires a database store."), nil
	}

	idStr := params.QueryParams["id"]
	if idStr == "" {
		return nil, contributor.ErrPageNotFound
	}

	cronID, err := id.ParseCronID(idStr)
	if err != nil {
		return nil, contributor.ErrPageNotFound
	}

	entry, err := cs.GetCron(ctx, cronID)
	if err != nil {
		return nil, fmt.Errorf("dashboard: resolve cron entry: %w", err)
	}

	return pages.CronDetailPage(entry), nil
}

// --- Widget Renderers ---

func (c *Contributor) renderStatsWidget(ctx context.Context) (templ.Component, error) {
	js, ok := resolveJobStore(c.engine)
	if !ok {
		return nil, contributor.ErrWidgetNotFound
	}
	jc := fetchJobCounts(ctx, js)
	var wfTotal int64
	if ws, wsOK := resolveWorkflowStore(c.engine); wsOK {
		wc := fetchWorkflowCounts(ctx, ws)
		wfTotal = int64(wc.Total)
	}
	return widgets.StatsWidget(jc.Pending, jc.Running, jc.Completed, jc.Failed, wfTotal), nil
}

func (c *Contributor) renderRecentJobsWidget(ctx context.Context) (templ.Component, error) {
	js, ok := resolveJobStore(c.engine)
	if !ok {
		return nil, contributor.ErrWidgetNotFound
	}

	var recentJobs []*job.Job
	for _, state := range []job.State{job.StateRunning, job.StateCompleted, job.StateFailed} {
		items, _ := fetchJobsByState(ctx, js, state, 5, 0) //nolint:errcheck // best-effort dashboard data
		recentJobs = append(recentJobs, items...)
	}
	if len(recentJobs) > 10 {
		recentJobs = recentJobs[:10]
	}

	return widgets.RecentJobsWidget(recentJobs), nil
}

// --- Page Renderers (new) ---

func (c *Contributor) renderQueues(ctx context.Context) (templ.Component, error) {
	queueInfos := fetchQueueInfo(ctx, c.engine)
	concurrency := 0
	if c.engine.Dispatcher() != nil {
		concurrency = c.engine.Dispatcher().Config().Concurrency
	}
	return pages.QueuesPage(queueInfos, concurrency), nil
}

func (c *Contributor) renderQueueDetail(ctx context.Context, params contributor.Params) (templ.Component, error) {
	name := params.QueryParams["name"]
	if name == "" {
		return nil, contributor.ErrPageNotFound
	}

	queueInfo := fetchSingleQueueInfo(ctx, c.engine, name)

	// Fetch jobs in this queue.
	js, jsOK := resolveJobStore(c.engine)
	limit := parseIntParam(params.QueryParams, "limit", 20)
	offset := parseIntParam(params.QueryParams, "offset", 0)

	var jobs []*job.Job
	var total int64
	if jsOK {
		for _, state := range []job.State{job.StateRunning, job.StatePending, job.StateRetrying, job.StateFailed, job.StateCompleted} {
			items, _ := js.ListJobsByState(ctx, state, job.ListOpts{ //nolint:errcheck // best-effort dashboard data
				Limit:  limit,
				Offset: offset,
				Queue:  name,
			})
			jobs = append(jobs, items...)
		}
		total, _ = js.CountJobs(ctx, job.CountOpts{Queue: name}) //nolint:errcheck // best-effort dashboard data
		if len(jobs) > limit {
			jobs = jobs[:limit]
		}
	}

	pg := NewPaginationMeta(total, limit, offset)
	return pages.QueueDetailPage(queueInfo, jobs, pg), nil
}

func (c *Contributor) renderHandlers(_ context.Context) (templ.Component, error) {
	jobNames := fetchJobHandlerNames(c.engine)
	wfNames := fetchWorkflowNames(c.engine)
	return pages.HandlersPage(jobNames, wfNames), nil
}

func (c *Contributor) renderWorkers(ctx context.Context) (templ.Component, error) {
	cs, ok := resolveClusterStore(c.engine)
	if !ok {
		return components.EmptyState("server", "No cluster store", "Cluster store is not configured. Workers are only tracked in clustered mode."), nil
	}
	workers := fetchWorkers(ctx, cs)
	currentWorkerID := c.engine.WorkerID()
	return pages.WorkersPage(workers, currentWorkerID), nil
}

func (c *Contributor) renderWorkerDetail(ctx context.Context, params contributor.Params) (templ.Component, error) {
	cs, ok := resolveClusterStore(c.engine)
	if !ok {
		return components.EmptyState("server", "No cluster store", "Cluster store is not configured."), nil
	}

	idStr := params.QueryParams["id"]
	if idStr == "" {
		return nil, contributor.ErrPageNotFound
	}

	workerID, err := id.ParseWorkerID(idStr)
	if err != nil {
		return nil, contributor.ErrPageNotFound
	}

	w := fetchWorkerByID(ctx, cs, workerID)
	if w == nil {
		return components.EmptyState("server", "Worker not found", "The worker with ID '"+idStr+"' was not found in the cluster."), nil
	}

	currentWorkerID := c.engine.WorkerID()
	return pages.WorkerDetailPage(w, currentWorkerID), nil
}

// --- Widget Renderers (new) ---

func (c *Contributor) renderDLQCountWidget(ctx context.Context) (templ.Component, error) {
	var dlqCount int64
	if ds, ok := resolveDLQStore(c.engine); ok {
		dlqCount = fetchDLQCount(ctx, ds)
	}
	var failedJobs int64
	if js, ok := resolveJobStore(c.engine); ok {
		failedJobs, _ = js.CountJobs(ctx, job.CountOpts{State: job.StateFailed}) //nolint:errcheck // best-effort dashboard data
	}
	return widgets.DLQCountWidget(dlqCount, failedJobs), nil
}

func (c *Contributor) renderQueueActivityWidget(ctx context.Context) (templ.Component, error) {
	queueInfos := fetchQueueInfo(ctx, c.engine)
	return widgets.QueueActivityWidget(queueInfos), nil
}

func (c *Contributor) renderClusterHealthWidget(ctx context.Context) (templ.Component, error) {
	cs, ok := resolveClusterStore(c.engine)
	if !ok {
		return widgets.ClusterHealthWidget(nil, ""), nil
	}
	workers := fetchWorkers(ctx, cs)
	var leaderHostname string
	for _, w := range workers {
		if w.IsLeader {
			leaderHostname = w.Hostname
			break
		}
	}
	return widgets.ClusterHealthWidget(workers, leaderHostname), nil
}

func (c *Contributor) renderCronScheduleWidget(ctx context.Context) (templ.Component, error) {
	cs, ok := resolveCronStore(c.engine)
	if !ok {
		return widgets.CronScheduleWidget(nil), nil
	}
	entries := fetchCronEntries(ctx, cs)
	// Limit to at most 10 entries for widget display.
	if len(entries) > 10 {
		entries = entries[:10]
	}
	return widgets.CronScheduleWidget(entries), nil
}

// --- Settings Renderer ---

func (c *Contributor) renderConfigSettings() (templ.Component, error) {
	s := settings.DispatcherSettings{}
	if c.engine != nil && c.engine.Dispatcher() != nil {
		cfg := c.engine.Dispatcher().Config()
		s.Concurrency = cfg.Concurrency
		s.Queues = cfg.Queues
		s.PollInterval = cfg.PollInterval.String()
		s.ShutdownTimeout = cfg.ShutdownTimeout.String()
		s.HeartbeatInterval = cfg.HeartbeatInterval.String()
		s.StaleJobThreshold = cfg.StaleJobThreshold.String()
	}
	s.JobHandlers = fetchJobHandlerNames(c.engine)
	s.WorkflowHandlers = fetchWorkflowNames(c.engine)
	return settings.ConfigPanel(s), nil
}
