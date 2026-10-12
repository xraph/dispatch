package durable

import "context"

// LegacyAttempt retains evidence before a nontransactional legacy command runs.
// An empty OutcomeID means the result is unconfirmed, including after restart.
type LegacyAttempt struct {
	Attempt   Delivery
	OutcomeID string
}
type LegacyOutcome struct {
	AttemptID string // the accepted attempt delivery ID
	Audit     SecurityAudit
}
type LegacyAttemptList struct {
	InstallationID string
	Namespace      string
	After          string
	Limit          int
}

func (r LegacyAttemptList) Validate() error {
	if !DeliveryIdentifier(r.InstallationID) || !DeliveryIdentifier(r.Namespace) || r.Limit < 1 || r.Limit > MaxDeliveryBatch || (r.After != "" && !DeliveryIdentifier(r.After)) {
		return ErrInvalid
	}
	return nil
}
func (o LegacyOutcome) Validate(attempt, outcome Delivery) error {
	if o.AttemptID != attempt.ID || outcome.InstallationID != attempt.InstallationID || outcome.Namespace != attempt.Namespace || outcome.Action != attempt.Action || outcome.Target != attempt.Target || outcome.Metadata != attempt.Metadata || (outcome.Outcome != "returned_success" && outcome.Outcome != "returned_error") {
		return ErrRequestConflict
	}
	return nil
}

// LegacyAuditStore atomically pairs attempts/outcomes with their outbox intents.
// It does not make the intervening legacy mutation atomic with either write.
type LegacyAuditStore interface {
	BeginLegacyAttempt(context.Context, SecurityAudit) (LegacyAttempt, error)
	CompleteLegacyAttempt(context.Context, LegacyOutcome) error
	UnresolvedLegacyAttempts(context.Context, LegacyAttemptList) ([]LegacyAttempt, error)
}
