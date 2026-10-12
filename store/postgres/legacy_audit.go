package postgres

import (
	"context"
	"database/sql"

	"github.com/xraph/dispatch/durable"
)

func (s *Store) BeginLegacyAttempt(ctx context.Context, a durable.SecurityAudit) (durable.LegacyAttempt, error) {
	if a.Outcome != "attempted" {
		return durable.LegacyAttempt{}, durable.ErrInvalid
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.LegacyAttempt{}, err
	}
	defer s.rollbackExecution(tx)
	n, ok, err := lockedAuditNamespace(ctx, tx, a.Namespace)
	if err != nil {
		return durable.LegacyAttempt{}, err
	}
	if !ok {
		return durable.LegacyAttempt{}, durable.ErrNotFound
	}
	d, err := a.Delivery(n)
	if err != nil {
		return durable.LegacyAttempt{}, err
	}
	if insertErr := insertDelivery(ctx, tx, d); insertErr != nil {
		return durable.LegacyAttempt{}, insertErr
	}
	_, err = tx.Exec(ctx, `INSERT INTO dispatch_legacy_audit_attempts(attempt_id,installation_id,namespace) VALUES($1,$2,$3) ON CONFLICT(attempt_id) DO NOTHING`, d.ID, d.InstallationID, d.Namespace)
	if err != nil {
		return durable.LegacyAttempt{}, err
	}
	var outcome sql.NullString
	err = tx.QueryRow(ctx, `SELECT outcome_id FROM dispatch_legacy_audit_attempts WHERE attempt_id=$1`, d.ID).Scan(&outcome)
	if err != nil {
		return durable.LegacyAttempt{}, err
	}
	return durable.LegacyAttempt{Attempt: d, OutcomeID: outcome.String}, tx.Commit()
}
func (s *Store) CompleteLegacyAttempt(ctx context.Context, o durable.LegacyOutcome) error {
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer s.rollbackExecution(tx)
	n, ok, err := lockedAuditNamespace(ctx, tx, o.Audit.Namespace)
	if err != nil {
		return err
	}
	if !ok {
		return durable.ErrNotFound
	}
	d, err := o.Audit.Delivery(n)
	if err != nil {
		return err
	}
	var outcome sql.NullString
	err = tx.QueryRow(ctx, `SELECT outcome_id FROM dispatch_legacy_audit_attempts WHERE attempt_id=$1 AND installation_id=$2 AND namespace=$3 FOR UPDATE`, o.AttemptID, d.InstallationID, d.Namespace).Scan(&outcome)
	if isNoRows(err) {
		return durable.ErrNotFound
	}
	if err != nil {
		return err
	}
	record, err := scanDelivery(tx.QueryRow(ctx, `SELECT `+outboxColumns+` FROM dispatch_durable_outbox WHERE id=$1`, o.AttemptID))
	if err != nil {
		return err
	}
	if validateErr := o.Validate(record.Delivery, d); validateErr != nil {
		return validateErr
	}
	if outcome.Valid && outcome.String != d.ID {
		return durable.ErrRequestConflict
	}
	if insertErr := insertDelivery(ctx, tx, d); insertErr != nil {
		return insertErr
	}
	if _, err = tx.Exec(ctx, `UPDATE dispatch_legacy_audit_attempts SET outcome_id=$2 WHERE attempt_id=$1`, o.AttemptID, d.ID); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) UnresolvedLegacyAttempts(ctx context.Context, r durable.LegacyAttemptList) ([]durable.LegacyAttempt, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pgdb.Query(ctx, `SELECT `+outboxColumns+` FROM dispatch_durable_outbox WHERE id IN (SELECT attempt_id FROM dispatch_legacy_audit_attempts WHERE installation_id=$1 AND namespace=$2 AND attempt_id>$3 AND outcome_id IS NULL) ORDER BY id LIMIT $4`, r.InstallationID, r.Namespace, r.After, r.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []durable.LegacyAttempt{}
	for rows.Next() {
		d, scanErr := scanDelivery(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, durable.LegacyAttempt{Attempt: d.Delivery})
	}
	return result, rows.Err()
}
