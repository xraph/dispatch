package contract

import (
	"context"
	"slices"
	"strings"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/job"
)

type NameInput struct {
	Name string `json:"name"`
}
type QueueSettings struct {
	MaxConcurrency     int     `json:"maxConcurrency"`
	RateLimit          float64 `json:"rateLimit"`
	RateBurst          int     `json:"rateBurst"`
	EffectiveRateBurst *int    `json:"effectiveRateBurst"`
}
type QueueRow struct {
	Name                string              `json:"name"`
	Counts              map[job.State]int64 `json:"counts"`
	Total               int64               `json:"total"`
	PolledByThisProcess bool                `json:"polledByThisProcess"`
	LocalSettings       *QueueSettings      `json:"localSettings"`
	LocalActiveCount    *int                `json:"localActiveCount"`
}
type QueueDetail struct {
	QueueRow
	AsOf string `json:"asOf"`
}
type QueuesPage struct {
	Page[QueueRow]
	WorkerDiscoveryEnabled bool `json:"workerDiscoveryEnabled"`
}

func projectQueueLocal(deps Deps, name string) QueueRow {
	row := QueueRow{Name: name, PolledByThisProcess: slices.Contains(deps.Engine.Inspect().Pool.Queues, name)}
	manager := deps.Engine.QueueManager()
	if manager == nil {
		return row
	}
	if cfg, ok := manager.QueueConfig(name); ok {
		row.LocalSettings = &QueueSettings{MaxConcurrency: cfg.MaxConcurrency, RateLimit: cfg.RateLimit, RateBurst: cfg.RateBurst}
		if cfg.RateLimit > 0 {
			burst := max(cfg.RateBurst, 1)
			row.LocalSettings.EffectiveRateBurst = &burst
		}
		active := manager.ActiveCount(name)
		row.LocalActiveCount = &active
	}
	return row
}
func readQueue(ctx context.Context, deps Deps, name string) (QueueRow, error) {
	counts, err := countJobs(ctx, deps, name)
	if err != nil {
		return QueueRow{}, err
	}
	row := projectQueueLocal(deps, name)
	row.Counts = counts.Counts
	row.Total = counts.Total
	return row, nil
}
func queuesListHandler(deps Deps) func(context.Context, EmptyInput, fc.Principal) (QueuesPage, error) {
	return handle(deps, "queues.list", false, func(ctx context.Context, _ EmptyInput, _ fc.Principal) (QueuesPage, error) {
		names := map[string]bool{}
		for _, name := range deps.Engine.Inspect().Pool.Queues {
			if name != "" {
				names[name] = true
			}
		}
		if manager := deps.Engine.QueueManager(); manager != nil {
			for _, name := range manager.QueueNames() {
				if name != "" {
					names[name] = true
				}
			}
		}
		cluster := deps.Engine.ClusterStore()
		if cluster != nil {
			workers, err := cluster.ListWorkers(ctx)
			if err != nil {
				return QueuesPage{}, err
			}
			for _, worker := range workers {
				for _, name := range worker.Queues {
					if name != "" {
						names[name] = true
					}
				}
			}
		}
		ordered := make([]string, 0, len(names))
		for name := range names {
			ordered = append(ordered, name)
		}
		slices.Sort(ordered)
		rows := make([]QueueRow, 0, len(ordered))
		for _, name := range ordered {
			row, err := readQueue(ctx, deps, name)
			if err != nil {
				return QueuesPage{}, err
			}
			rows = append(rows, row)
		}
		return QueuesPage{Page: newPage(rows, "", true, time.Now()), WorkerDiscoveryEnabled: cluster != nil}, nil
	})
}
func queuesGetHandler(deps Deps) func(context.Context, NameInput, fc.Principal) (QueueDetail, error) {
	return handle(deps, "queues.get", false, func(ctx context.Context, input NameInput, _ fc.Principal) (QueueDetail, error) {
		if strings.TrimSpace(input.Name) == "" {
			return QueueDetail{}, badRequest("name must identify a queue")
		}
		row, err := readQueue(ctx, deps, input.Name)
		if err != nil {
			return QueueDetail{}, err
		}
		return QueueDetail{QueueRow: row, AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
