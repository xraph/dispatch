package memory

import (
	"context"
	"sort"

	"github.com/xraph/dispatch/durable"
)

func (m *Store) BeginLegacyAttempt(ctx context.Context, a durable.SecurityAudit) (durable.LegacyAttempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.Outcome != "attempted" {
		return durable.LegacyAttempt{}, durable.ErrInvalid
	}
	d, err := m.appendSecurityAudit(ctx, a)
	if err != nil {
		return durable.LegacyAttempt{}, err
	}
	if old, ok := m.legacyAttempts[d.ID]; ok {
		return old, nil
	}
	attempt := durable.LegacyAttempt{Attempt: d}
	if m.legacyAttempts == nil {
		m.legacyAttempts = make(map[string]durable.LegacyAttempt)
	}
	m.legacyAttempts[d.ID] = attempt
	return attempt, nil
}
func (m *Store) CompleteLegacyAttempt(ctx context.Context, o durable.LegacyOutcome) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	attempt, ok := m.legacyAttempts[o.AttemptID]
	if !ok {
		return durable.ErrNotFound
	}
	n, ok := m.namespaces[o.Audit.Namespace]
	if !ok {
		return durable.ErrNotFound
	}
	d, err := o.Audit.Delivery(n)
	if err != nil {
		return err
	}
	if validateErr := o.Validate(attempt.Attempt, d); validateErr != nil {
		return validateErr
	}
	if attempt.OutcomeID != "" && attempt.OutcomeID != d.ID {
		return durable.ErrRequestConflict
	}
	for id, other := range m.legacyAttempts {
		if id != o.AttemptID && other.OutcomeID == d.ID {
			return durable.ErrRequestConflict
		}
	}
	if _, err = m.appendSecurityAudit(ctx, o.Audit); err != nil {
		return err
	}
	attempt.OutcomeID = d.ID
	m.legacyAttempts[o.AttemptID] = attempt
	return nil
}
func (m *Store) UnresolvedLegacyAttempts(ctx context.Context, r durable.LegacyAttemptList) ([]durable.LegacyAttempt, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := []durable.LegacyAttempt{}
	for id, a := range m.legacyAttempts {
		if a.Attempt.InstallationID == r.InstallationID && a.Attempt.Namespace == r.Namespace && id > r.After && a.OutcomeID == "" {
			result = append(result, a)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Attempt.ID < result[j].Attempt.ID })
	if len(result) > r.Limit {
		result = result[:r.Limit]
	}
	return result, nil
}
