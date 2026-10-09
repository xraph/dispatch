package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

const outboxColumns = `envelope,owner,epoch,attempts,lease_until,next_attempt_at,accepted_at,delivered_at,receipt,error_category`

func scanDelivery(row interface{ Scan(...any) error }) (durable.DeliveryRecord, error) {
	var d durable.DeliveryRecord
	var envelope, receipt []byte
	var lease, delivered sql.NullTime
	err := row.Scan(&envelope, &d.Owner, &d.Epoch, &d.Attempts, &lease, &d.NextAttemptAt, &d.AcceptedAt, &delivered, &receipt, &d.ErrorCategory)
	if err != nil {
		return d, err
	}
	if decodeErr := json.Unmarshal(envelope, &d.Delivery); decodeErr != nil {
		return d, decodeErr
	}
	if verifyErr := d.Delivery.Verify(); verifyErr != nil {
		return d, verifyErr
	}
	if lease.Valid {
		d.LeaseUntil = lease.Time
	}
	if delivered.Valid {
		d.DeliveredAt = delivered.Time
	}
	if len(receipt) > 0 {
		err = json.Unmarshal(receipt, &d.Receipt)
	}
	return d, err
}
func insertDelivery(ctx context.Context, tx driver.Tx, d durable.Delivery) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `INSERT INTO dispatch_durable_outbox(id,installation_id,namespace,destination,schema_version,app_id,tenant_id,workflow_id,run_id,source_kind,source_id,sequence,fingerprint,envelope) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(id) DO NOTHING`, d.ID, d.InstallationID, d.Namespace, string(d.Destination), d.SchemaVersion, d.AppID, d.TenantID, d.WorkflowID, d.RunID, d.SourceKind, d.SourceID, d.Sequence, d.Fingerprint, data)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count > 0 {
		return err
	}
	var fingerprint string
	err = tx.QueryRow(ctx, `SELECT fingerprint FROM dispatch_durable_outbox WHERE id=$1`, d.ID).Scan(&fingerprint)
	if err != nil {
		return err
	}
	if fingerprint != d.Fingerprint {
		return durable.ErrRequestConflict
	}
	return nil
}
func (s *Store) AppendSecurityAudit(ctx context.Context, a durable.SecurityAudit) (durable.Delivery, error) {
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.Delivery{}, err
	}
	defer s.rollbackExecution(tx)
	n, ok, err := lockedAuditNamespace(ctx, tx, a.Namespace)
	if err != nil {
		return durable.Delivery{}, err
	}
	if !ok {
		return durable.Delivery{}, durable.ErrNotFound
	}
	d, err := a.Delivery(n)
	if err != nil {
		return durable.Delivery{}, err
	}
	if err := insertDelivery(ctx, tx, d); err != nil {
		return durable.Delivery{}, err
	}
	return d, tx.Commit()
}
func (s *Store) ClaimDeliveries(ctx context.Context, r durable.DeliveryClaim) ([]durable.DeliveryRecord, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer s.rollbackExecution(tx)
	rows, err := tx.Query(ctx, `SELECT `+outboxColumns+` FROM dispatch_durable_outbox WHERE installation_id=$1 AND destination=$2 AND delivered_at IS NULL AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY next_attempt_at,id LIMIT $3 FOR UPDATE SKIP LOCKED`, r.InstallationID, string(r.Destination), r.Limit)
	if err != nil {
		return nil, err
	}
	result := []durable.DeliveryRecord{}
	for rows.Next() {
		d, scanErr := scanDelivery(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, scanErr
		}
		result = append(result, d)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	for i := range result {
		d := &result[i]
		if d.Epoch == math.MaxInt64 || d.Attempts == math.MaxInt64 {
			return nil, durable.ErrInvalid
		}
		d.Owner = r.Owner
		d.Epoch++
		d.Attempts++
		d.LeaseUntil = now.Add(r.LeaseDuration)
		if _, err = tx.Exec(ctx, `UPDATE dispatch_durable_outbox SET owner=$2,epoch=$3,attempts=$4,lease_until=$5 WHERE id=$1`, d.Delivery.ID, d.Owner, d.Epoch, d.Attempts, d.LeaseUntil); err != nil {
			return nil, err
		}
	}
	return result, tx.Commit()
}
func (s *Store) mutateDelivery(ctx context.Context, t durable.DeliveryToken, fn func(driver.Tx, durable.DeliveryRecord, time.Time) error) error {
	if err := t.Validate(); err != nil {
		return err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer s.rollbackExecution(tx)
	d, err := scanDelivery(tx.QueryRow(ctx, `SELECT `+outboxColumns+` FROM dispatch_durable_outbox WHERE id=$1 AND installation_id=$2 AND destination=$3 FOR UPDATE`, t.ID, t.InstallationID, string(t.Destination)))
	if isNoRows(err) {
		return durable.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return err
	}
	if d.Owner != t.Owner || d.Epoch != t.Epoch || !d.DeliveredAt.IsZero() || !d.LeaseUntil.After(now) {
		return durable.ErrLeaseLost
	}
	if err := fn(tx, d, now); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) RenewDelivery(ctx context.Context, t durable.DeliveryToken, ttl time.Duration) (time.Time, error) {
	if err := durable.ValidateDeliveryRenewal(t, ttl); err != nil {
		return time.Time{}, err
	}
	var until time.Time
	err := s.mutateDelivery(ctx, t, func(tx driver.Tx, _ durable.DeliveryRecord, now time.Time) error {
		until = now.Add(ttl)
		_, err := tx.Exec(ctx, `UPDATE dispatch_durable_outbox SET lease_until=$2 WHERE id=$1`, t.ID, until)
		return err
	})
	return until, err
}
func (s *Store) AcknowledgeDelivery(ctx context.Context, t durable.DeliveryToken, r durable.SinkReceipt) error {
	return s.mutateDelivery(ctx, t, func(tx driver.Tx, d durable.DeliveryRecord, now time.Time) error {
		if err := r.Verify(d.Delivery); err != nil {
			return err
		}
		data, err := json.Marshal(r)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE dispatch_durable_outbox SET delivered_at=$2,receipt=$3,error_category='' WHERE id=$1`, t.ID, now, data)
		return err
	})
}
func (s *Store) RetryDelivery(ctx context.Context, r durable.DeliveryRetry) error {
	if err := r.Validate(); err != nil {
		return err
	}
	return s.mutateDelivery(ctx, r.Token, func(tx driver.Tx, _ durable.DeliveryRecord, now time.Time) error {
		_, err := tx.Exec(ctx, `UPDATE dispatch_durable_outbox SET next_attempt_at=$2,error_category=$3,owner='',lease_until=NULL WHERE id=$1`, r.Token.ID, now.Add(r.Delay), r.Category)
		return err
	})
}
func (s *Store) DeliveryStatus(ctx context.Context, r durable.DeliveryStatusRequest) (durable.DeliveryStatus, error) {
	var result durable.DeliveryStatus
	if err := r.Validate(); err != nil {
		return result, err
	}
	var oldest sql.NullTime
	err := s.pgdb.QueryRow(ctx, `SELECT count(*),min(accepted_at) FROM dispatch_durable_outbox WHERE installation_id=$1 AND destination=$2 AND delivered_at IS NULL`, r.InstallationID, string(r.Destination)).Scan(&result.Pending, &oldest)
	if err != nil {
		return result, err
	}
	if oldest.Valid {
		result.OldestAcceptedAt = oldest.Time
	}
	rows, err := s.pgdb.Query(ctx, `SELECT `+outboxColumns+` FROM dispatch_durable_outbox WHERE installation_id=$1 AND destination=$2 AND id>$3 ORDER BY id LIMIT $4`, r.InstallationID, string(r.Destination), r.After, r.Limit)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	result.Records = []durable.DeliveryRecord{}
	for rows.Next() {
		d, scanErr := scanDelivery(rows)
		if scanErr != nil {
			return result, scanErr
		}
		result.Records = append(result.Records, d)
	}
	return result, rows.Err()
}
