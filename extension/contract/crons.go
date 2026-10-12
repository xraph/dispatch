package contract

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

type EmptyInput struct{}
type CronRow struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Schedule       string   `json:"schedule"`
	JobName        string   `json:"jobName"`
	Queue          *string  `json:"queue"`
	EffectiveQueue string   `json:"effectiveQueue"`
	ScopeAppID     *string  `json:"scopeAppId"`
	ScopeOrgID     *string  `json:"scopeOrgId"`
	Enabled        bool     `json:"enabled"`
	CreatedAt      *string  `json:"createdAt"`
	UpdatedAt      *string  `json:"updatedAt"`
	LastRunAt      *string  `json:"lastRunAt"`
	NextRunAt      *string  `json:"nextRunAt"`
	LockedBy       *string  `json:"lockedBy"`
	LockedUntil    *string  `json:"lockedUntil"`
	NextFires      []string `json:"nextFires"`
	Location       *string  `json:"location"`
	ScheduleError  *string  `json:"scheduleError"`
}
type CronDetail struct {
	CronRow
	Payload Payload `json:"payload"`
	AsOf    string  `json:"asOf"`
}
type CronRunResult struct {
	CronID string `json:"cronId"`
	Job    JobRow `json:"job"`
	AsOf   string `json:"asOf"`
}

func projectCron(entry *cron.Entry, at time.Time) CronRow {
	queue := entry.Queue
	if queue == "" {
		queue = job.DefaultOptions().Queue
	}
	row := CronRow{ID: entry.ID.String(), Name: entry.Name, Schedule: entry.Schedule, JobName: entry.JobName, Queue: nullable(entry.Queue),
		EffectiveQueue: queue, ScopeAppID: nullable(entry.ScopeAppID), ScopeOrgID: nullable(entry.ScopeOrgID), Enabled: entry.Enabled,
		CreatedAt: timestamp(entry.CreatedAt), UpdatedAt: timestamp(entry.UpdatedAt), LastRunAt: timestampPtr(entry.LastRunAt),
		NextRunAt: timestampPtr(entry.NextRunAt), LockedBy: nullable(entry.LockedBy), LockedUntil: timestampPtr(entry.LockedUntil), NextFires: []string{}}
	fires, location, err := cron.NextFires(entry.Schedule, at, 5)
	if err != nil {
		row.ScheduleError = nullable("Stored schedule cannot be evaluated.")
		return row
	}
	row.Location = nullable(location.String())
	for _, fire := range fires {
		row.NextFires = append(row.NextFires, fire.UTC().Format(time.RFC3339Nano))
	}
	if len(fires) == 0 {
		row.ScheduleError = nullable("Schedule has no fire time in the next five years.")
	}
	return row
}
func projectCronDetail(entry *cron.Entry, at time.Time) CronDetail {
	return CronDetail{CronRow: projectCron(entry, at), Payload: projectPayload(entry.Payload, false), AsOf: at.UTC().Format(time.RFC3339Nano)}
}
func parseCronID(raw string) (id.CronID, error) {
	parsed, err := id.ParseCronID(raw)
	if err != nil || parsed.IsNil() {
		return id.CronID{}, badRequest("id must be a cron ID")
	}
	return parsed, nil
}
func cronsListHandler(deps Deps) func(context.Context, EmptyInput, fc.Principal) (Page[CronRow], error) {
	return handle(deps, "crons.list", false, func(ctx context.Context, _ EmptyInput, _ fc.Principal) (Page[CronRow], error) {
		entries, err := deps.Store.ListCrons(ctx)
		if err != nil {
			return Page[CronRow]{}, err
		}
		at := time.Now()
		rows := make([]CronRow, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, projectCron(entry, at))
		}
		slices.SortFunc(rows, func(a, b CronRow) int {
			if c := strings.Compare(a.Name, b.Name); c != 0 {
				return c
			}
			return strings.Compare(a.ID, b.ID)
		})
		return newPage(rows, "", true, at), nil
	})
}
func cronsGetHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (CronDetail, error) {
	return handle(deps, "crons.get", false, func(ctx context.Context, input IDInput, _ fc.Principal) (CronDetail, error) {
		cronID, err := parseCronID(input.ID)
		if err != nil {
			return CronDetail{}, err
		}
		entry, err := deps.Store.GetCron(ctx, cronID)
		if err != nil {
			return CronDetail{}, err
		}
		return projectCronDetail(entry, time.Now()), nil
	})
}
func cronToggleHandler(deps Deps, enabled bool) func(context.Context, IDInput, fc.Principal) (CronDetail, error) {
	intent := "crons.disable"
	if enabled {
		intent = "crons.enable"
	}
	return handle(deps, intent, true, func(ctx context.Context, input IDInput, _ fc.Principal) (CronDetail, error) {
		cronID, err := parseCronID(input.ID)
		if err != nil {
			return CronDetail{}, err
		}
		var entry *cron.Entry
		if enabled {
			current, readErr := deps.Store.GetCron(ctx, cronID)
			if readErr != nil {
				return CronDetail{}, readErr
			}
			if projectCron(current, time.Now()).ScheduleError != nil {
				return CronDetail{}, cronStateConflict(current)
			}
			entry, err = deps.Engine.EnableCron(ctx, cronID)
		} else {
			entry, err = deps.Engine.DisableCron(ctx, cronID)
		}
		if errors.Is(err, dispatch.ErrInvalidState) {
			current, readErr := deps.Store.GetCron(ctx, cronID)
			if readErr != nil {
				return CronDetail{}, readErr
			}
			return CronDetail{}, cronStateConflict(current)
		}
		if err != nil {
			return CronDetail{}, err
		}
		return projectCronDetail(entry, time.Now()), nil
	})
}
func cronStateConflict(entry *cron.Entry) error {
	state := "disabled"
	if entry.Enabled {
		state = "enabled"
	}
	return &fc.Error{Code: fc.CodeConflict, Message: "the stored cron schedule cannot be enabled", Details: map[string]any{"state": state}}
}
func cronsDeleteHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (DeletedResult, error) {
	return handle(deps, "crons.delete", true, func(ctx context.Context, input IDInput, _ fc.Principal) (DeletedResult, error) {
		cronID, err := parseCronID(input.ID)
		if err != nil {
			return DeletedResult{}, err
		}
		if err := deps.Engine.DeleteCron(ctx, cronID); err != nil {
			return DeletedResult{}, err
		}
		return DeletedResult{ID: cronID.String(), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
func cronsRunNowHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (CronRunResult, error) {
	return handle(deps, "crons.runNow", true, func(ctx context.Context, input IDInput, _ fc.Principal) (CronRunResult, error) {
		cronID, err := parseCronID(input.ID)
		if err != nil {
			return CronRunResult{}, err
		}
		j, err := deps.Engine.TriggerCron(ctx, cronID)
		if err != nil {
			return CronRunResult{}, err
		}
		return CronRunResult{CronID: cronID.String(), Job: projectJob(j), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
