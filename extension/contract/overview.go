package contract

import (
	"context"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/workflow"
)

type CronSummary struct {
	Enabled  int `json:"enabled"`
	Disabled int `json:"disabled"`
}
type WorkerSummary struct {
	Enabled     bool      `json:"enabled"`
	Recent      int       `json:"recent"`
	Silent      int       `json:"silent"`
	Unknown     int       `json:"unknown"`
	LeaderID    *string   `json:"leaderId"`
	SilentAfter *Duration `json:"silentAfter"`
}
type OverviewSummary struct {
	Jobs                  JobCounts                   `json:"jobs"`
	Runs                  map[workflow.RunState]int64 `json:"runs"`
	UnreplayedDeadLetters int64                       `json:"unreplayedDeadLetters"`
	Crons                 CronSummary                 `json:"crons"`
	Workers               WorkerSummary               `json:"workers"`
	AsOf                  string                      `json:"asOf"`
}

func overviewSummaryHandler(deps Deps) func(context.Context, EmptyInput, fc.Principal) (OverviewSummary, error) {
	return handle(deps, "overview.summary", false, func(ctx context.Context, _ EmptyInput, _ fc.Principal) (OverviewSummary, error) {
		jobs, err := countJobs(ctx, deps, "")
		if err != nil {
			return OverviewSummary{}, err
		}
		out := OverviewSummary{Jobs: jobs, Runs: map[workflow.RunState]int64{}}
		for _, state := range []workflow.RunState{workflow.RunStateRunning, workflow.RunStateCompleted, workflow.RunStateFailed} {
			n, countErr := deps.Store.CountRuns(ctx, workflow.CountRunsOpts{State: state})
			if countErr != nil {
				return OverviewSummary{}, countErr
			}
			out.Runs[state] = n
		}
		replayed := false
		out.UnreplayedDeadLetters, err = deps.Store.CountDLQEntries(ctx, dlq.CountOpts{Replayed: &replayed})
		if err != nil {
			return OverviewSummary{}, err
		}
		crons, err := deps.Store.ListCrons(ctx)
		if err != nil {
			return OverviewSummary{}, err
		}
		for _, entry := range crons {
			if entry.Enabled {
				out.Crons.Enabled++
			} else {
				out.Crons.Disabled++
			}
		}
		workers, err := readWorkers(ctx, deps)
		if err != nil {
			return OverviewSummary{}, err
		}
		out.Workers = WorkerSummary{Enabled: workers.Enabled, LeaderID: workers.LeaderID, SilentAfter: workers.SilentAfter}
		for _, worker := range workers.Items {
			switch worker.HeartbeatStatus {
			case "recent":
				out.Workers.Recent++
			case "silent":
				out.Workers.Silent++
			default:
				out.Workers.Unknown++
			}
		}
		out.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
		return out, nil
	})
}
