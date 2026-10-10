package postgres

import (
	"context"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

var _ durable.NamespaceStore = (*Store)(nil)
var _ durable.OutboxStore = (*Store)(nil)

const namespaceColumns = `installation_id,namespace,app_id,tenant_id,require_audit,require_hooks,schema_version,coverage_started_at,writer_protocol`

func scanNamespace(row interface{ Scan(...any) error }) (durable.NamespaceRecord, error) {
	var n durable.NamespaceRecord
	err := row.Scan(&n.InstallationID, &n.Namespace, &n.AppID, &n.TenantID, &n.RequireAudit, &n.RequireHooks, &n.SchemaVersion, &n.CoverageStartedAt, &n.WriterProtocol)
	return n, err
}
func (s *Store) RegisterNamespace(ctx context.Context, c durable.NamespaceConfig) (durable.NamespaceRecord, error) {
	if err := c.Validate(); err != nil {
		return durable.NamespaceRecord{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.NamespaceRecord{}, err
	}
	defer s.rollbackExecution(tx)
	// No execution rows are read or locked by this dedicated transaction.
	if _, err = tx.Exec(ctx, `DO $$ BEGIN IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'durable audit requires READ COMMITTED'; END IF; END $$`); err != nil {
		return durable.NamespaceRecord{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(dispatch_audit_lock_key($1))`, c.Namespace); err != nil {
		return durable.NamespaceRecord{}, err
	}
	old, err := scanNamespace(tx.QueryRow(ctx, `SELECT `+namespaceColumns+` FROM dispatch_durable_namespaces WHERE namespace=$1`, c.Namespace))
	if err == nil {
		if old.NamespaceConfig != c {
			return durable.NamespaceRecord{}, durable.ErrRequestConflict
		}
		return old, tx.Commit()
	}
	if !isNoRows(err) {
		return durable.NamespaceRecord{}, err
	}
	n, err := scanNamespace(tx.QueryRow(ctx, `INSERT INTO dispatch_durable_namespaces(installation_id,namespace,app_id,tenant_id,require_audit,require_hooks,schema_version) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING `+namespaceColumns, c.InstallationID, c.Namespace, c.AppID, c.TenantID, c.RequireAudit, c.RequireHooks, c.SchemaVersion))
	if err != nil {
		return durable.NamespaceRecord{}, err
	}
	return n, tx.Commit()
}
func (s *Store) GetNamespace(ctx context.Context, installation, namespace string) (durable.NamespaceRecord, error) {
	if !durable.DeliveryIdentifier(installation) || !durable.DeliveryIdentifier(namespace) {
		return durable.NamespaceRecord{}, durable.ErrInvalid
	}
	n, err := scanNamespace(s.pgdb.QueryRow(ctx, `SELECT `+namespaceColumns+` FROM dispatch_durable_namespaces WHERE installation_id=$1 AND namespace=$2`, installation, namespace))
	if isNoRows(err) {
		err = durable.ErrNotFound
	}
	return n, err
}
func (s *Store) ListNamespaces(ctx context.Context, r durable.NamespaceList) ([]durable.NamespaceRecord, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pgdb.Query(ctx, `SELECT `+namespaceColumns+` FROM dispatch_durable_namespaces WHERE installation_id=$1 AND namespace COLLATE "C">$2 COLLATE "C" ORDER BY namespace COLLATE "C" LIMIT $3`, r.InstallationID, r.After, r.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []durable.NamespaceRecord{}
	for rows.Next() {
		n, scanErr := scanNamespace(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

// lockAuditMutation must precede execution/identity/task locks and authoritative
// clock reads. Activation can wait long enough to expire a grant or deadline.
// Registration touches only catalog rows, and child mutations stay in one
// namespace. Helpers and database triggers retain the same lock as a fallback.
func lockAuditMutation(ctx context.Context, tx driver.Tx, namespace string) error {
	_, err := tx.Exec(ctx, `SELECT dispatch_retirement_writer_lock($1,1)`, namespace)
	return normalizeExecutionError(err)
}
func lockedAuditNamespace(ctx context.Context, tx driver.Tx, namespace string) (durable.NamespaceRecord, bool, error) {
	if err := lockAuditMutation(ctx, tx, namespace); err != nil {
		return durable.NamespaceRecord{}, false, err
	}
	n, err := scanNamespace(tx.QueryRow(ctx, `SELECT `+namespaceColumns+` FROM dispatch_durable_namespaces WHERE namespace=$1`, namespace))
	if isNoRows(err) {
		return n, false, nil
	}
	return n, err == nil, err
}
