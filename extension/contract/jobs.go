package contract

import (
	"context"
	"errors"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

var jobStates = []job.State{job.StatePending, job.StateRunning, job.StateCompleted, job.StateFailed, job.StateRetrying, job.StateCancelled}

type IDInput struct {
	ID string `json:"id"`
}

type JobsListInput struct {
	States     []job.State `json:"states"`
	Queue      string      `json:"queue"`
	NamePrefix string      `json:"namePrefix"`
	ScopeAppID string      `json:"scopeAppId"`
	ScopeOrgID string      `json:"scopeOrgId"`
	Cursor     string      `json:"cursor"`
	Limit      int         `json:"limit"`
}

type QueueInput struct {
	Queue string `json:"queue"`
}

type JobCounts struct {
	Counts map[job.State]int64 `json:"counts"`
	Total  int64               `json:"total"`
	AsOf   string              `json:"asOf"`
}

func validJobState(state job.State) bool {
	for _, value := range jobStates {
		if value == state {
			return true
		}
	}
	return false
}

func jobsListHandler(deps Deps) func(context.Context, JobsListInput, fc.Principal) (Page[JobRow], error) {
	return handle(deps, "jobs.list", false, func(ctx context.Context, input JobsListInput, _ fc.Principal) (Page[JobRow], error) {
		limit, err := pageLimit(input.Limit)
		if err != nil {
			return Page[JobRow]{}, err
		}
		for _, state := range input.States {
			if !validJobState(state) {
				return Page[JobRow]{}, badRequest("unknown job state")
			}
		}
		page, err := deps.Store.ListJobs(ctx, job.ListJobsOpts{States: input.States, Queue: input.Queue, NamePrefix: input.NamePrefix,
			ScopeAppID: input.ScopeAppID, ScopeOrgID: input.ScopeOrgID, Cursor: input.Cursor, Limit: limit})
		if err != nil {
			return Page[JobRow]{}, err
		}
		items := make([]JobRow, 0, len(page.Jobs))
		for _, j := range page.Jobs {
			items = append(items, projectJob(j))
		}
		return newPage(items, page.NextCursor, page.Complete, time.Now()), nil
	})
}

func countJobs(ctx context.Context, deps Deps, queue string) (JobCounts, error) {
	out := JobCounts{Counts: make(map[job.State]int64, len(jobStates))}
	for _, state := range jobStates {
		n, err := deps.Store.CountJobs(ctx, job.CountOpts{Queue: queue, State: state})
		if err != nil {
			return JobCounts{}, err
		}
		out.Counts[state] = n
		out.Total += n
	}
	out.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
	return out, nil
}

func jobsCountsHandler(deps Deps) func(context.Context, QueueInput, fc.Principal) (JobCounts, error) {
	return handle(deps, "jobs.counts", false, func(ctx context.Context, input QueueInput, _ fc.Principal) (JobCounts, error) {
		return countJobs(ctx, deps, input.Queue)
	})
}

func parseJobID(raw string) (id.JobID, error) {
	parsed, err := id.ParseJobID(raw)
	if err != nil || parsed.IsNil() {
		return id.JobID{}, badRequest("id must be a job ID")
	}
	return parsed, nil
}

func jobLinks(ctx context.Context, deps Deps, jobID id.JobID) (JobArtifactLinks, error) {
	svc := deps.Engine.Artifacts()
	out := JobArtifactLinks{Enabled: svc.Enabled(), Links: []ArtifactLinkRow{}}
	if !out.Enabled {
		return out, nil
	}
	links, err := svc.Store().ListLinks(ctx, artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()})
	if err != nil {
		return out, err
	}
	for _, link := range links {
		out.Links = append(out.Links, ArtifactLinkRow{ArtifactID: link.ArtifactID.String(), Role: string(link.Role),
			Name: link.Name, Attempt: link.Attempt, CreatedAt: timestamp(link.CreatedAt)})
	}
	return out, nil
}

func jobsGetHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (JobDetail, error) {
	return handle(deps, "jobs.get", false, func(ctx context.Context, input IDInput, _ fc.Principal) (JobDetail, error) {
		jobID, err := parseJobID(input.ID)
		if err != nil {
			return JobDetail{}, err
		}
		j, err := deps.Store.GetJob(ctx, jobID)
		if err != nil {
			return JobDetail{}, err
		}
		links, err := jobLinks(ctx, deps, jobID)
		if err != nil {
			return JobDetail{}, err
		}
		var deadLetterID *string
		entry, err := deps.Store.GetDLQByJobID(ctx, jobID)
		if err != nil && !errors.Is(err, dispatch.ErrDLQNotFound) {
			return JobDetail{}, err
		}
		if err == nil {
			raw := entry.ID.String()
			deadLetterID = &raw
		}
		effectiveTTL := j.LeaseTTL
		if effectiveTTL <= 0 {
			effectiveTTL = deps.Engine.Inspect().Pool.DefaultLeaseTTL
		}
		return JobDetail{JobRow: projectJob(j), Payload: projectPayload(j.Payload, false), LastError: nullable(j.LastError),
			HeartbeatAt: timestampPtr(j.HeartbeatAt), Timeout: duration(j.Timeout), LeaseEpoch: j.LeaseEpoch,
			LeaseExpiresAt: timestampPtr(j.LeaseExpiresAt), LeaseTTL: duration(j.LeaseTTL), EffectiveLeaseTTL: duration(effectiveTTL),
			EvictCount: j.EvictCount, Resources: resourceValues(j.Resources), ResourceLimits: resourceValues(j.ResourceLimits),
			ResourceClass: nullable(j.ResourceClass), InputBytes: j.InputBytes, PrimaryInputHash: nullable(j.PrimaryInputHash),
			ArtifactBindings: projectPayload(j.ArtifactBindings, false), Artifacts: links, DLQEntryID: deadLetterID,
			AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}

type JobAction struct {
	Job  JobRow `json:"job"`
	AsOf string `json:"asOf"`
}

func jobActionHandler(deps Deps, intent string, action func(context.Context, id.JobID) (*job.Job, error)) func(context.Context, IDInput, fc.Principal) (JobAction, error) {
	return handle(deps, intent, true, func(ctx context.Context, input IDInput, _ fc.Principal) (JobAction, error) {
		jobID, err := parseJobID(input.ID)
		if err != nil {
			return JobAction{}, err
		}
		j, err := action(ctx, jobID)
		if errors.Is(err, dispatch.ErrInvalidState) || errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
			current, readErr := deps.Store.GetJob(ctx, jobID)
			if readErr != nil {
				return JobAction{}, readErr
			}
			conflict := stateConflict(string(current.State))
			if errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
				conflict = &fc.Error{Code: fc.CodeConflict, Message: "the dead letter has already been replayed", Details: map[string]any{"state": string(current.State)}}
			}
			return JobAction{}, conflict
		}
		if err != nil {
			return JobAction{}, err
		}
		return JobAction{Job: projectJob(j), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
