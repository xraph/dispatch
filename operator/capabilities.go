package operator

import (
	"context"
	"errors"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

type CapabilitiesInput struct {
	durable.Key
	BuildID      string `json:"build_id"`
	WorkflowType string `json:"workflow_type"`
}
type Capabilities struct {
	Runtime string          `json:"runtime"`
	Actions map[string]bool `json:"actions"`
}

// Capabilities is an observation of current policy. Every operation rechecks it.
func (s *Service) Capabilities(ctx context.Context, p security.Principal, in CapabilitiesInput) (Capabilities, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := Capabilities{Runtime: "unavailable", Actions: map[string]bool{}}
	if in.RunID != "" {
		e, err := s.commandRun(ctx, p, ReadExecution, in.Key, in.BuildID)
		if err != nil {
			return out, err
		}
		in.WorkflowType = e.WorkflowType
		in.BuildID = e.BuildID
	} else if err := s.check(ctx, p, Discover, durable.Key{Namespace: in.Namespace}); err != nil {
		return out, err
	}
	worker, err := s.worker(in.Namespace, in.BuildID)
	if err != nil {
		return out, nil
	}
	out.Runtime = "available"
	actions := []string{StartWorkflow, SignalStartWorkflow}
	if in.RunID != "" {
		actions = []string{SignalWorkflow, CancelWorkflow, QueryWorkflow}
	}
	for _, action := range actions {
		var err error
		if action == SignalStartWorkflow {
			if !worker.SupportsSignalStartOutcome() {
				out.Actions[action] = false
				continue
			}
			identity := durable.Key{Namespace: in.Namespace, WorkflowID: in.WorkflowID}
			for _, required := range []string{SignalStartWorkflow, SignalWorkflow} {
				if err = s.checkFacts(ctx, p, required, identity, "", ""); err != nil {
					break
				}
			}
			if err == nil {
				err = s.checkFacts(ctx, p, StartWorkflow, identity, in.WorkflowType, in.BuildID)
			}
		} else {
			err = s.checkFacts(ctx, p, action, in.Key, in.WorkflowType, in.BuildID)
		}
		if errors.Is(err, security.ErrForbidden) {
			out.Actions[action] = false
			continue
		}
		if err != nil {
			return out, err
		}
		out.Actions[action] = true
	}
	return out, nil
}
