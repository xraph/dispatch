package dwp

import (
	"encoding/json"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/stream"
)

func auditOperation(frame *Frame) (security.Operation, error) {
	op := security.DWPOperation(frame.Method)
	op.Target = "installation"
	if op.Action == "" || frame.Method == MethodStats {
		return op, nil
	}
	// Decode only typed selectors. Payload, input, token and arbitrary fields
	// cannot enter the security envelope.
	var selector struct {
		JobID   string `json:"job_id"`
		RunID   string `json:"run_id"`
		Name    string `json:"name"`
		Queue   string `json:"queue"`
		Channel string `json:"channel"`
		PeerID  string `json:"peer_id"`
	}
	if err := json.Unmarshal(frame.Data, &selector); err != nil {
		op.Target = "invalid-target"
		return op, err
	}
	var err error
	switch frame.Method {
	case MethodJobGet, MethodJobCancel:
		op.Target, err = security.ResourceTarget(id.PrefixJob, selector.JobID)
	case MethodWorkflowGet, MethodWorkflowTimeline:
		op.Target, err = security.ResourceTarget(id.PrefixRun, selector.RunID)
	case MethodJobEnqueue, MethodFederationEnqueue:
		queue := selector.Queue
		if queue == "" {
			queue = "default"
		}
		op.Target, err = security.CreationTarget("job-selector", selector.Name, queue)
	case MethodWorkflowStart:
		op.Target, err = security.CreationTarget("workflow-selector", selector.Name, "")
	case MethodWorkflowEvent, MethodFederationEvent:
		op.Target, err = security.CreationTarget("event-selector", selector.Name, "")
	case MethodFederationHeartbeat:
		op.Target, err = security.CreationTarget("peer-selector", selector.PeerID, "")
	case MethodSubscribe, MethodUnsubscribe:
		err = stream.ValidateTopic(selector.Channel)
		if err == nil {
			op.Target, err = security.CreationTarget("subscription-selector", selector.Channel, "")
		} else {
			op.Target = "invalid-target"
		}
	}
	return op, err
}
