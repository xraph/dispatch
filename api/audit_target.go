package api

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/security"
)

type purgeAuditCutoffKey struct{}

func auditTarget(ctx forge.Context, path string) (string, error) {
	switch {
	case strings.Contains(path, ":jobId"):
		return security.ResourceTarget(id.PrefixJob, ctx.Param("jobId"))
	case strings.Contains(path, ":runId"):
		return security.ResourceTarget(id.PrefixRun, ctx.Param("runId"))
	case strings.Contains(path, ":entryId"):
		return security.ResourceTarget(id.PrefixDLQ, ctx.Param("entryId"))
	case strings.Contains(path, ":cronId"):
		return security.ResourceTarget(id.PrefixCron, ctx.Param("cronId"))
	case path == "/dlq/replay-all":
		limit := 1000
		if raw := ctx.Query("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				return "invalid-target", err
			}
			if parsed != 0 {
				limit = parsed
			}
		}
		return security.BulkTarget(ctx.Query("queue"), limit, time.Time{}, false)
	case path == "/dlq/purge":
		dryRun := false
		if raw := ctx.Query("dry_run"); raw != "" {
			parsed, err := strconv.ParseBool(raw)
			if err != nil {
				return "invalid-target", err
			}
			dryRun = parsed
		}
		before, err := purgeCutoff(&PurgeDLQRequest{Before: ctx.Query("before"), OlderThan: ctx.Query("older_than")}, time.Now().UTC())
		if err != nil {
			return "invalid-target", err
		}
		// The handler consumes this exact cutoff rather than recomputing relative time.
		ctx.WithContext(context.WithValue(ctx.Context(), purgeAuditCutoffKey{}, before))
		return security.BulkTarget("", 0, before, dryRun)
	default:
		return "installation", nil
	}
}
