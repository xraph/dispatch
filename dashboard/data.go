package dashboard

import (
	"context"
	"strconv"
	"time"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dashboard/pages"
	"github.com/xraph/dispatch/dashboard/shared"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// PaginationMeta is an alias for shared.PaginationMeta.
type PaginationMeta = shared.PaginationMeta

// NewPaginationMeta is a convenience re-export.
var NewPaginationMeta = shared.NewPaginationMeta

// --- Helper Functions ---

func parseIntParam(params map[string]string, key string, defaultVal int) int {
	v, ok := params[key]
	if !ok || v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return defaultVal
	}
	return n
}

// --- Job Data ---

type jobCounts struct {
	Pending   int64
	Running   int64
	Completed int64
	Failed    int64
	Retrying  int64
	Cancelled int64
	Total     int64
}

func fetchJobCounts(ctx context.Context, js job.Store) jobCounts {
	var c jobCounts
	c.Pending, _ = js.CountJobs(ctx, job.CountOpts{State: job.StatePending})
	c.Running, _ = js.CountJobs(ctx, job.CountOpts{State: job.StateRunning})
	c.Completed, _ = js.CountJobs(ctx, job.CountOpts{State: job.StateCompleted})
	c.Failed, _ = js.CountJobs(ctx, job.CountOpts{State: job.StateFailed})
	c.Retrying, _ = js.CountJobs(ctx, job.CountOpts{State: job.StateRetrying})
	c.Cancelled, _ = js.CountJobs(ctx, job.CountOpts{State: job.StateCancelled})
	c.Total = c.Pending + c.Running + c.Completed + c.Failed + c.Retrying + c.Cancelled
	return c
}

func fetchJobsByState(ctx context.Context, js job.Store, state job.State, limit, offset int) ([]*job.Job, error) {
	return js.ListJobsByState(ctx, state, job.ListOpts{
		Limit:  limit,
		Offset: offset,
	})
}

// --- Workflow Data ---

type workflowCounts struct {
	Running   int
	Completed int
	Failed    int
	Total     int
}

func fetchWorkflowCounts(ctx context.Context, ws workflow.Store) workflowCounts {
	var c workflowCounts
	running, _ := ws.ListRuns(ctx, workflow.ListOpts{State: workflow.RunStateRunning, Limit: 0})
	completed, _ := ws.ListRuns(ctx, workflow.ListOpts{State: workflow.RunStateCompleted, Limit: 0})
	failed, _ := ws.ListRuns(ctx, workflow.ListOpts{State: workflow.RunStateFailed, Limit: 0})
	c.Running = len(running)
	c.Completed = len(completed)
	c.Failed = len(failed)
	c.Total = c.Running + c.Completed + c.Failed
	return c
}

func fetchWorkflowRuns(ctx context.Context, ws workflow.Store, state workflow.RunState, limit, offset int) ([]*workflow.Run, error) {
	return ws.ListRuns(ctx, workflow.ListOpts{
		State:  state,
		Limit:  limit,
		Offset: offset,
	})
}

func fetchRecentWorkflowRuns(ctx context.Context, ws workflow.Store, limit int) []*workflow.Run {
	runs, _ := ws.ListRuns(ctx, workflow.ListOpts{Limit: limit})
	return runs
}

// --- DLQ Data ---

func fetchDLQEntries(ctx context.Context, ds dlq.Store, queue string, limit, offset int) ([]*dlq.Entry, error) {
	return ds.ListDLQ(ctx, dlq.ListOpts{
		Limit:  limit,
		Offset: offset,
		Queue:  queue,
	})
}

func fetchDLQCount(ctx context.Context, ds dlq.Store) int64 {
	count, _ := ds.CountDLQ(ctx)
	return count
}

// --- Cron Data ---

func fetchCronEntries(ctx context.Context, cs cron.Store) []*cron.Entry {
	entries, _ := cs.ListCrons(ctx)
	return entries
}

// --- Queue Data ---

func fetchQueueInfo(_ context.Context, eng *engine.Engine) []pages.QueueInfo {
	var queues []string
	if eng.Dispatcher() != nil {
		queues = eng.Dispatcher().Config().Queues
	}
	if len(queues) == 0 {
		queues = []string{"default"}
	}

	infos := make([]pages.QueueInfo, 0, len(queues))
	for _, q := range queues {
		info := pages.QueueInfo{Name: q}
		if eng.QueueManager() != nil {
			info.Active = eng.QueueManager().ActiveCount(q)
			if cfg, ok := eng.QueueManager().QueueConfig(q); ok {
				info.HasConfig = true
				info.MaxConcurrency = cfg.MaxConcurrency
				info.RateLimit = cfg.RateLimit
				info.RateBurst = cfg.RateBurst
			}
		}
		infos = append(infos, info)
	}
	return infos
}

func fetchSingleQueueInfo(_ context.Context, eng *engine.Engine, name string) pages.QueueInfo {
	info := pages.QueueInfo{Name: name}
	if eng.QueueManager() != nil {
		info.Active = eng.QueueManager().ActiveCount(name)
		if cfg, ok := eng.QueueManager().QueueConfig(name); ok {
			info.HasConfig = true
			info.MaxConcurrency = cfg.MaxConcurrency
			info.RateLimit = cfg.RateLimit
			info.RateBurst = cfg.RateBurst
		}
	}
	return info
}

// --- Handler Data ---

func fetchJobHandlerNames(eng *engine.Engine) []string {
	if eng.Registry() == nil {
		return nil
	}
	return eng.Registry().Names()
}

func fetchWorkflowNames(eng *engine.Engine) []string {
	if eng.WorkflowRunner() == nil || eng.WorkflowRunner().Registry() == nil {
		return nil
	}
	return eng.WorkflowRunner().Registry().Names()
}

// --- Store Resolution ---

func resolveJobStore(eng *engine.Engine) (job.Store, bool) {
	if eng == nil || eng.Dispatcher() == nil || eng.Dispatcher().Store() == nil {
		return nil, false
	}
	js, ok := eng.Dispatcher().Store().(job.Store)
	return js, ok
}

func resolveWorkflowStore(eng *engine.Engine) (workflow.Store, bool) {
	if eng == nil || eng.Dispatcher() == nil || eng.Dispatcher().Store() == nil {
		return nil, false
	}
	ws, ok := eng.Dispatcher().Store().(workflow.Store)
	return ws, ok
}

func resolveDLQStore(eng *engine.Engine) (dlq.Store, bool) {
	if eng == nil || eng.DLQService() == nil {
		return nil, false
	}
	return eng.DLQService().DLQStore(), true
}

func resolveCronStore(eng *engine.Engine) (cron.Store, bool) {
	cs := eng.CronStore()
	return cs, cs != nil
}

// --- Time Formatting ---

func formatTimeAgo(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		m := int(d.Minutes())
		if m == 1 {
			return "1m ago"
		}
		return strconv.Itoa(m) + "m ago"
	case d < 24*time.Hour:
		h := int(d.Hours())
		if h == 1 {
			return "1h ago"
		}
		return strconv.Itoa(h) + "h ago"
	default:
		days := int(d.Hours() / 24)
		if days == 1 {
			return "1d ago"
		}
		return strconv.Itoa(days) + "d ago"
	}
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	if d < time.Second {
		return strconv.Itoa(int(d.Milliseconds())) + "ms"
	}
	if d < time.Minute {
		return strconv.FormatFloat(d.Seconds(), 'f', 1, 64) + "s"
	}
	return d.Truncate(time.Second).String()
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// --- Cluster / Worker Data ---

func resolveClusterStore(eng *engine.Engine) (cluster.Store, bool) {
	if eng == nil {
		return nil, false
	}
	cs := eng.ClusterStore()
	return cs, cs != nil
}

func fetchWorkers(ctx context.Context, cs cluster.Store) []*cluster.Worker {
	workers, _ := cs.ListWorkers(ctx)
	return workers
}

func fetchWorkerByID(ctx context.Context, cs cluster.Store, workerID id.WorkerID) *cluster.Worker {
	workers, _ := cs.ListWorkers(ctx)
	for _, w := range workers {
		if w.ID == workerID {
			return w
		}
	}
	return nil
}
