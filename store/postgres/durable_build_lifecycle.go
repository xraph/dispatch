package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

var _ durable.LifecycleStore = (*Store)(nil)

func readBuildAdmission(ctx context.Context, tx driver.Tx, target durable.BuildTarget) (durable.BuildAdmission, error) {
	b := durable.BuildAdmission{BuildTarget: target}
	var identity []byte
	err := tx.QueryRow(ctx, `SELECT state,epoch,version,cutoff_epoch,changed_at,COALESCE(query_identity,'{}'::jsonb) FROM dispatch_build_lifecycle WHERE namespace=$1 AND build_id=$2`, target.Namespace, target.BuildID).Scan(&b.State, &b.Epoch, &b.Version, &b.CutoffEpoch, &b.ChangedAt, &identity)
	if isNoRows(err) {
		err = durable.ErrNotFound
	}
	if err == nil && (json.Unmarshal(identity, &b.QueryIdentity) != nil || (b.QueryIdentity != (durable.BuildQueryIdentity{}) && b.QueryIdentity.Validate() != nil)) {
		return b, durable.ErrInvalid
	}
	return b, err
}
func lockLifecycleTarget(ctx context.Context, tx driver.Tx, target durable.NamespaceTarget) (durable.NamespaceRecord, error) {
	if _, err := tx.Exec(ctx, `SELECT dispatch_retirement_coordinator_lock($1,1)`, target.Namespace); err != nil {
		return durable.NamespaceRecord{}, normalizeExecutionError(err)
	}
	n, err := scanNamespace(tx.QueryRow(ctx, `SELECT `+namespaceColumns+` FROM dispatch_durable_namespaces WHERE namespace=$1 AND installation_id=$2`, target.Namespace, target.InstallationID))
	if isNoRows(err) {
		err = durable.ErrNotFound
	}
	return n, err
}
func checkRetirementEnrollment(ctx context.Context, tx driver.Tx, namespace string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dispatch_retirement_namespaces WHERE namespace=$1)`, namespace).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return durable.ErrWriterCompatibility
	}
	return nil
}
func (s *Store) RegisterBuild(ctx context.Context, r durable.RegisterBuildRequest) (result durable.LifecycleReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if r.Identity != nil {
		identity := *r.Identity
		r.Identity = &identity
	}
	if err := r.Validate(); err != nil {
		return result, err
	}
	digest, err := durable.Fingerprint(string(durable.OperationRegisterBuild), r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	n, err := lockLifecycleTarget(ctx, tx, r.NamespaceTarget)
	if err != nil {
		return result, err
	}
	q := durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: durable.OperationRegisterBuild, RequestID: r.RequestID, RequestDigest: digest}
	if saved, readErr := readLifecycleReceipt(ctx, tx, q); !errors.Is(readErr, durable.ErrNotFound) {
		return saved, readErr
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, r.Namespace); checkErr != nil {
		return result, checkErr
	}

	prior, readErr := readBuildAdmission(ctx, tx, r.BuildTarget)
	exists := readErr == nil
	if readErr != nil && !errors.Is(readErr, durable.ErrNotFound) {
		return result, readErr
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	b, err := durable.RegisterBuildIdentity(prior, exists, r, now)
	if err != nil {
		return result, err
	}
	var identity []byte
	if b.QueryIdentity != (durable.BuildQueryIdentity{}) {
		identity, err = json.Marshal(b.QueryIdentity)
		if err != nil {
			return result, err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_build_lifecycle(namespace,build_id,state,epoch,version,changed_at,query_identity) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(namespace,build_id) DO UPDATE SET query_identity=EXCLUDED.query_identity,version=EXCLUDED.version,changed_at=EXCLUDED.changed_at`, r.Namespace, r.BuildID, b.State, b.Epoch, b.Version, b.ChangedAt, identity); err != nil {
		return result, err
	}

	result = durable.LifecycleReceipt{NamespaceTarget: r.NamespaceTarget, Operation: q.Operation, RequestID: r.RequestID, RequestDigest: digest, CommandDigest: r.CommandDigest, ResponseVersion: 1, AcceptedAt: now, Build: &b}
	if checkErr := saveLifecycleReceipt(ctx, tx, n, &result); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	return result, tx.Commit()
}
func (s *Store) InspectBuildLifecycle(ctx context.Context, target durable.BuildTarget) (durable.BuildLifecycleFacts, error) {
	if err := target.Validate(); err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	defer s.rollbackExecution(tx)
	if _, err = lockLifecycleTarget(ctx, tx, target.NamespaceTarget); err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, target.Namespace); checkErr != nil {
		return durable.BuildLifecycleFacts{}, checkErr
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	facts, err := buildLifecycleFacts(ctx, tx, target, now)
	if err != nil {
		return facts, err
	}
	return facts, tx.Commit()
}
func buildLifecycleFacts(ctx context.Context, tx driver.Tx, target durable.BuildTarget, now time.Time) (durable.BuildLifecycleFacts, error) {
	var f durable.BuildLifecycleFacts
	b, err := readBuildAdmission(ctx, tx, target)
	if err != nil {
		return f, err
	}
	f.Admission = b
	f.ObservationVersion.BuildVersion = b.Version
	f.ObservationVersion.ObservedAt = now
	if checkErr := tx.QueryRow(ctx, `SELECT version FROM dispatch_retirement_namespaces WHERE namespace=$1`, target.Namespace).Scan(&f.ObservationVersion.CompatibilityVersion); checkErr != nil {
		return f, checkErr
	}
	err = tx.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM dispatch_executions WHERE namespace=$1 AND build_id=$2 AND state='running'),
 (SELECT count(*) FROM dispatch_execution_tasks t JOIN dispatch_executions e USING(namespace,workflow_id,run_id) WHERE t.namespace=$1 AND e.build_id=$2 AND NOT t.done),
 (SELECT count(*) FROM dispatch_execution_tasks t JOIN dispatch_executions e USING(namespace,workflow_id,run_id) WHERE t.namespace=$1 AND e.build_id=$2 AND NOT t.done AND t.lease_kind='async'),
 (SELECT count(*) FROM dispatch_executions WHERE namespace=$1 AND build_id=$2 AND state='running' AND run_available_at>$3),
 (SELECT count(*) FROM dispatch_child_deliveries d WHERE namespace=$1 AND NOT done AND CASE WHEN kind IN ('close','cancel') THEN (SELECT e.build_id FROM dispatch_executions e WHERE e.namespace=d.namespace AND e.workflow_id=d.target_workflow_id AND e.first_run_id=d.target_run_id ORDER BY e.run_number DESC LIMIT 1) ELSE target_build_id END=$2),
 (SELECT count(*) FROM dispatch_child_executions c JOIN dispatch_executions p ON (p.namespace,p.workflow_id,p.run_id)=(c.namespace,c.parent_workflow_id,c.parent_run_id) WHERE c.namespace=$1 AND p.build_id=$2 AND EXISTS(SELECT 1 FROM dispatch_executions e WHERE e.namespace=c.namespace AND e.workflow_id=c.child_workflow_id AND e.first_run_id=c.child_run_id AND e.state='running')) +
 (SELECT count(*) FROM dispatch_child_deliveries d JOIN dispatch_executions p ON (p.namespace,p.workflow_id,p.run_id)=(d.namespace,d.source_workflow_id,d.source_run_id) WHERE d.namespace=$1 AND p.build_id=$2 AND NOT d.done AND d.kind='cancel' AND COALESCE(d.message->>'cancellation_id','')<>'')`, target.Namespace, target.BuildID, now).Scan(&f.Blockers.OpenExecutions, &f.Blockers.PendingTasks, &f.Blockers.AsyncCallbacks, &f.Blockers.DelayedRuns, &f.Blockers.PendingChildDeliveries, &f.Blockers.ChildObligations)
	if err != nil {
		return f, err
	}
	retained, bindings, err := queryBindings(ctx, tx, target)
	if err != nil {
		return f, err
	}
	now, err = executionTime(ctx, tx)
	if err != nil {
		return f, err
	}
	f.ObservationVersion.ObservedAt = now
	f.QueryRetention = durable.QueryRetention(b, retained, bindings, now)
	return f, nil
}
func (s *Store) BeginBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	return s.mutateBuildLifecycle(ctx, r, durable.OperationBeginRetirement)
}
func (s *Store) FinalizeBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	return s.mutateBuildLifecycle(ctx, r, durable.OperationFinalizeRetirement)
}
func (s *Store) AbortBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	return s.mutateBuildLifecycle(ctx, r, durable.OperationAbortRetirement)
}
func (s *Store) mutateBuildLifecycle(ctx context.Context, r durable.BuildRetirementRequest, operation durable.LifecycleOperation) (result durable.LifecycleReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return result, err
	}
	digest, err := durable.Fingerprint(string(operation), r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	n, err := lockLifecycleTarget(ctx, tx, r.NamespaceTarget)
	if err != nil {
		return result, err
	}
	q := durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: operation, RequestID: r.RequestID, RequestDigest: digest}
	if saved, readErr := readLifecycleReceipt(ctx, tx, q); !errors.Is(readErr, durable.ErrNotFound) {
		return saved, readErr
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, r.Namespace); checkErr != nil {
		return result, checkErr
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	facts, err := buildLifecycleFacts(ctx, tx, r.BuildTarget, now)
	if err != nil {
		return result, err
	}
	if operation == durable.OperationFinalizeRetirement && facts.Blockers.Empty() && facts.QueryRetention.RetainedExecutions > 0 && facts.QueryRetention.VerifiedBindings == 0 {
		return result, durable.ErrQueryRetention
	}
	b, err := durable.TransitionBuild(facts.Admission, r, operation, facts.Blockers, now)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `UPDATE dispatch_build_lifecycle SET state=$3,epoch=$4,version=$5,cutoff_epoch=$6,changed_at=$7 WHERE namespace=$1 AND build_id=$2`, r.Namespace, r.BuildID, b.State, b.Epoch, b.Version, b.CutoffEpoch, b.ChangedAt); err != nil {
		return result, err
	}
	result = durable.LifecycleReceipt{NamespaceTarget: r.NamespaceTarget, Operation: operation, RequestID: r.RequestID, RequestDigest: digest, ResponseVersion: 1, AcceptedAt: now, Build: &b}
	if checkErr := saveLifecycleReceipt(ctx, tx, n, &result); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	return result, tx.Commit()
}

func admitExecution(ctx context.Context, tx driver.Tx, e, source *durable.Execution, kind, command string) error {
	var installation string
	err := tx.QueryRow(ctx, `SELECT n.installation_id FROM dispatch_retirement_namespaces r JOIN dispatch_durable_namespaces n USING(namespace) WHERE namespace=$1`, e.Namespace).Scan(&installation)
	if isNoRows(err) {
		e.AdmissionEpoch = 0
		return nil
	}
	if err != nil {
		return err
	}
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: installation, Namespace: e.Namespace}, BuildID: e.BuildID}
	b, err := readBuildAdmission(ctx, tx, target)
	if errors.Is(err, durable.ErrNotFound) {
		b = durable.BuildAdmission{BuildTarget: target, State: "unregistered"}
	} else if err != nil {
		return err
	}
	epoch, err := durable.AdmissionEpoch(b, source, kind, command)
	if err != nil {
		return err
	}
	e.AdmissionEpoch = epoch
	return nil
}
