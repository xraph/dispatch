package contract

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/forge"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
)

type DLQListInput struct {
	Queue      string `json:"queue"`
	NamePrefix string `json:"namePrefix"`
	ScopeAppID string `json:"scopeAppId"`
	ScopeOrgID string `json:"scopeOrgId"`
	Replayed   *bool  `json:"replayed"`
	Cursor     string `json:"cursor"`
	Limit      int    `json:"limit"`
}
type DLQCountsInput struct {
	Queue    string `json:"queue"`
	Replayed *bool  `json:"replayed"`
}
type CountResult struct {
	Count int64  `json:"count"`
	AsOf  string `json:"asOf"`
}
type BeforeInput struct {
	Before string `json:"before"`
}
type DLQPurgeResult struct {
	Before      string    `json:"before"`
	Count       int64     `json:"count"`
	Interrupted bool      `json:"interrupted"`
	Failure     *fc.Error `json:"failure"`
	AsOf        string    `json:"asOf"`
}
type DeletedResult struct {
	ID   string `json:"id"`
	AsOf string `json:"asOf"`
}
type DLQReplayResult struct {
	EntryID string `json:"entryId"`
	Job     JobRow `json:"job"`
	AsOf    string `json:"asOf"`
}
type DLQReplayAllInput struct {
	Queue string `json:"queue"`
	Limit int    `json:"limit"`
}
type DLQBulkResult struct {
	Replayed    int       `json:"replayed"`
	Conflicts   int       `json:"conflicts"`
	Errors      int       `json:"errors"`
	Limit       int       `json:"limit"`
	Interrupted bool      `json:"interrupted"`
	Failure     *fc.Error `json:"failure"`
	AsOf        string    `json:"asOf"`
}

func parseDLQID(raw string) (id.DLQID, error) {
	parsed, err := id.ParseDLQID(raw)
	if err != nil || parsed.IsNil() {
		return id.DLQID{}, badRequest("id must be a dead letter ID")
	}
	return parsed, nil
}
func parseBefore(raw string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || parsed.IsZero() {
		return time.Time{}, badRequest("before must be a non-zero RFC 3339 timestamp")
	}
	return parsed.UTC(), nil
}
func dlqListHandler(deps Deps) func(context.Context, DLQListInput, fc.Principal) (Page[DLQRow], error) {
	return handle(deps, "dlq.list", false, func(ctx context.Context, input DLQListInput, _ fc.Principal) (Page[DLQRow], error) {
		limit, err := pageLimit(input.Limit)
		if err != nil {
			return Page[DLQRow]{}, err
		}
		page, err := deps.Store.ListDLQPage(ctx, dlq.PageOpts{Queue: input.Queue, NamePrefix: input.NamePrefix, ScopeAppID: input.ScopeAppID,
			ScopeOrgID: input.ScopeOrgID, Replayed: input.Replayed, Cursor: input.Cursor, Limit: limit})
		if err != nil {
			return Page[DLQRow]{}, err
		}
		items := make([]DLQRow, 0, len(page.Entries))
		for _, e := range page.Entries {
			items = append(items, projectDLQ(e))
		}
		return newPage(items, page.NextCursor, page.Complete, time.Now()), nil
	})
}
func dlqGetHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (DLQDetail, error) {
	return handle(deps, "dlq.get", false, func(ctx context.Context, input IDInput, _ fc.Principal) (DLQDetail, error) {
		entryID, err := parseDLQID(input.ID)
		if err != nil {
			return DLQDetail{}, err
		}
		e, err := deps.Store.GetDLQ(ctx, entryID)
		if err != nil {
			return DLQDetail{}, err
		}
		return DLQDetail{DLQRow: projectDLQ(e), Payload: projectPayload(e.Payload, false), Priority: e.Priority, Timeout: duration(e.Timeout),
			LeaseTTL: duration(e.LeaseTTL), ArtifactBindings: projectPayload(e.ArtifactBindings, false), Resources: resourceValues(e.Resources),
			ResourceLimits: resourceValues(e.ResourceLimits), ResourceClass: nullable(e.ResourceClass), InputBytes: e.InputBytes,
			PrimaryInputHash: nullable(e.PrimaryInputHash), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
func dlqCountsHandler(deps Deps) func(context.Context, DLQCountsInput, fc.Principal) (CountResult, error) {
	return handle(deps, "dlq.counts", false, func(ctx context.Context, input DLQCountsInput, _ fc.Principal) (CountResult, error) {
		n, err := deps.Store.CountDLQEntries(ctx, dlq.CountOpts{Queue: input.Queue, Replayed: input.Replayed})
		if err != nil {
			return CountResult{}, err
		}
		return CountResult{Count: n, AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
func dlqPurgeHandler(deps Deps, preview bool) func(context.Context, BeforeInput, fc.Principal) (DLQPurgeResult, error) {
	intent := "dlq.purge"
	if preview {
		intent = "dlq.purgePreview"
	}
	return handle(deps, intent, !preview, func(ctx context.Context, input BeforeInput, _ fc.Principal) (DLQPurgeResult, error) {
		before, err := parseBefore(input.Before)
		if err != nil {
			return DLQPurgeResult{}, err
		}
		var n int64
		if preview {
			n, err = deps.Engine.CountDLQPurge(ctx, before)
		} else {
			n, err = deps.Engine.PurgeDLQ(ctx, before)
		}
		if preview && err != nil {
			return DLQPurgeResult{}, err
		}
		out := DLQPurgeResult{Before: before.Format(time.RFC3339Nano), Count: n, Interrupted: err != nil, AsOf: time.Now().UTC().Format(time.RFC3339Nano)}
		if err != nil {
			mapped := deps.mapError(intent, err)
			var failure *fc.Error
			if errors.As(mapped, &failure) {
				out.Failure = failure
			}
		}
		return out, nil
	})
}
func dlqReplayHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (DLQReplayResult, error) {
	return handle(deps, "dlq.replay", true, func(ctx context.Context, input IDInput, _ fc.Principal) (DLQReplayResult, error) {
		entryID, err := parseDLQID(input.ID)
		if err != nil {
			return DLQReplayResult{}, err
		}
		j, err := deps.Engine.ReplayDLQ(ctx, entryID)
		if errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
			return DLQReplayResult{}, stateConflict("replayed")
		}
		if err != nil {
			return DLQReplayResult{}, err
		}
		return DLQReplayResult{EntryID: entryID.String(), Job: projectJob(j), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
func dlqDeleteHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (DeletedResult, error) {
	return handle(deps, "dlq.delete", true, func(ctx context.Context, input IDInput, _ fc.Principal) (DeletedResult, error) {
		entryID, err := parseDLQID(input.ID)
		if err != nil {
			return DeletedResult{}, err
		}
		if err := deps.Engine.DeleteDLQ(ctx, entryID); err != nil {
			return DeletedResult{}, err
		}
		return DeletedResult{ID: entryID.String(), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
func dlqReplayAllHandler(deps Deps) func(context.Context, DLQReplayAllInput, fc.Principal) (DLQBulkResult, error) {
	return handle(deps, "dlq.replayAll", true, func(ctx context.Context, input DLQReplayAllInput, _ fc.Principal) (DLQBulkResult, error) {
		if input.Limit < 0 {
			return DLQBulkResult{}, badRequest("limit must not be negative")
		}
		limit := input.Limit
		if limit == 0 || limit > 1000 {
			limit = 1000
		}
		result, err := deps.Engine.ReplayAllDLQ(ctx, engine.ReplayAllOpts{Queue: input.Queue, Limit: limit})
		out := DLQBulkResult{Replayed: result.Replayed, Conflicts: result.Conflicts, Errors: result.Failed, Limit: limit,
			Interrupted: err != nil, AsOf: time.Now().UTC().Format(time.RFC3339Nano)}
		if len(result.Errors) > 0 && deps.Logger != nil {
			deps.Logger.Error("dispatch/contract: bulk replay entries failed", forge.F("intent", "dlq.replayAll"), forge.F("errors", result.Errors))
		}
		if err != nil {
			mapped := deps.mapError("dlq.replayAll", err)
			var failure *fc.Error
			if errors.As(mapped, &failure) {
				out.Failure = failure
			}
		}
		// Earlier writes remain committed when a later read fails. Return their counts
		// with a safe failure so the transport still invalidates the affected views.
		return out, nil
	})
}
