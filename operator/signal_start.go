package operator

import (
	"context"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

type SignalStartInput struct {
	Start StartInput `json:"start"`
	Name  string     `json:"name"`
	Input []byte     `json:"input,omitempty"`
}

func (s *Service) SignalStart(ctx context.Context, p security.Principal, in SignalStartInput) (Acceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	r := durable.SignalWithStartRequest{Start: in.Start.request(), Name: in.Name, Input: in.Input}
	if r.Validate() != nil || !validCommandID(r.Start.RequestID) || len(r.Start.Input) > maxStartInputBytes {
		return Acceptance{}, durable.ErrInvalid
	}
	// These grants cover either atomic branch, including a concurrent open run.
	// Run-only authority cannot stand in for a workflow-identity grant.
	key := durable.Key{Namespace: r.Start.Namespace, WorkflowID: r.Start.WorkflowID}
	for _, action := range []string{SignalStartWorkflow, SignalWorkflow} {
		if err := s.checkFacts(ctx, p, action, key, "", ""); err != nil {
			return Acceptance{}, err
		}
	}
	if err := s.checkFacts(ctx, p, StartWorkflow, key, r.Start.WorkflowType, r.Start.BuildID); err != nil {
		return Acceptance{}, err
	}
	w, err := s.worker(key.Namespace, r.Start.BuildID)
	if err != nil {
		return Acceptance{}, err
	}
	outcome, err := w.SignalWithStartOutcome(commandContext(ctx, p, r.Start.RequestID), r)
	if err != nil {
		return Acceptance{}, commandError(err)
	}
	if outcome.Recovered {
		// A recovered receipt can point at an older run than today's open execution.
		// Reauthorize its persisted facts before disclosing that acceptance.
		for _, action := range []string{SignalStartWorkflow, SignalWorkflow} {
			if _, err = s.commandRun(ctx, p, action, outcome.Receipt.Key, r.Start.BuildID); err != nil {
				return Acceptance{}, err
			}
		}
	}
	out := accepted(outcome.Receipt.Key, r.Start.RequestID, "accepted", outcome.Receipt.Receipt)
	out.Started = outcome.Receipt.Started
	return out, nil
}
