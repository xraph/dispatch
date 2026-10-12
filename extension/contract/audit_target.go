package contract

import (
	"strings"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/security"
)

func auditResourceTarget(intent, raw string) (string, error) {
	switch {
	case strings.HasPrefix(intent, "jobs.") || intent == "artifacts.forJob":
		return security.ResourceTarget(id.PrefixJob, raw)
	case strings.HasPrefix(intent, "workflows."):
		return security.ResourceTarget(id.PrefixRun, raw)
	case strings.HasPrefix(intent, "dlq."):
		return security.ResourceTarget(id.PrefixDLQ, raw)
	case strings.HasPrefix(intent, "crons."):
		return security.ResourceTarget(id.PrefixCron, raw)
	case strings.HasPrefix(intent, "workers."):
		return security.ResourceTarget(id.PrefixWorker, raw)
	case strings.HasPrefix(intent, "artifacts."):
		return security.ResourceTarget(id.PrefixArtifact, raw)
	default:
		return "unresolved", nil
	}
}
func auditInputTarget(intent string, input any) (string, error) {
	switch value := input.(type) {
	case IDInput:
		return auditResourceTarget(intent, value.ID)
	case WorkflowReplayInput:
		return auditResourceTarget(intent, value.ID)
	case WorkflowReplayCommandInput:
		return auditResourceTarget(intent, value.ID)
	case BeforeInput:
		before, err := parseBefore(value.Before)
		if err != nil {
			return "invalid-target", err
		}
		return security.BulkTarget("", 0, before, intent == "dlq.purgePreview")
	case DLQReplayAllInput:
		limit := value.Limit
		if limit == 0 || limit > 1000 {
			limit = 1000
		}
		return security.BulkTarget(value.Queue, limit, time.Time{}, false)
	case NameInput:
		return security.CreationTarget("queue-selector", value.Name, "")
	case HandlerInput:
		return security.CreationTarget("handler-selector", value.Name, "")
	default:
		return "installation", nil
	}
}
