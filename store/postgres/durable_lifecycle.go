package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

var _ durable.RetirementEnrollmentStore = (*Store)(nil)

func (s *Store) InspectCompatibility(ctx context.Context, target durable.NamespaceTarget) (durable.CompatibilityFacts, error) {
	if err := target.Validate(); err != nil {
		return durable.CompatibilityFacts{}, err
	}
	var f durable.CompatibilityFacts
	f.NamespaceTarget = target
	err := s.pgdb.QueryRow(ctx, `SELECT r.namespace IS NOT NULL,COALESCE(r.schema_version,0),COALESCE(r.writer_protocol,0),COALESCE(r.version,0),COALESCE(r.enrolled_at,'0001-01-01'::timestamptz),clock_timestamp(),CASE WHEN to_regclass('dispatch_query_runtimes') IS NOT NULL AND to_regprocedure('dispatch_query_build_guard()') IS NOT NULL AND to_regprocedure('dispatch_query_binding_guard()') IS NOT NULL THEN 1 ELSE 0 END FROM dispatch_durable_namespaces n LEFT JOIN dispatch_retirement_namespaces r USING(namespace) WHERE n.namespace=$1 AND n.installation_id=$2`, target.Namespace, target.InstallationID).Scan(&f.Enrolled, &f.SchemaVersion, &f.WriterProtocol, &f.Version, &f.EnrolledAt, &f.ObservedAt, &f.QueryRetentionSchemaVersion)
	if isNoRows(err) {
		err = durable.ErrNotFound
	}
	return f, err
}

func readLifecycleReceipt(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) driver.Row
}, q durable.LifecycleReceiptLookup) (durable.LifecycleReceipt, error) {
	var r durable.LifecycleReceipt
	var data []byte
	err := db.QueryRow(ctx, `SELECT r.response FROM dispatch_lifecycle_receipts r JOIN dispatch_durable_namespaces n USING(namespace) WHERE r.namespace=$1 AND n.installation_id=$2 AND r.operation=$3 AND r.request_id=$4`, q.Namespace, q.InstallationID, string(q.Operation), q.RequestID).Scan(&data)
	if isNoRows(err) {
		return r, durable.ErrNotFound
	}
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(data, &r); err != nil {
		return r, durable.ErrInvalid
	}
	return r, r.Match(q)
}

func (s *Store) LookupLifecycleReceipt(ctx context.Context, q durable.LifecycleReceiptLookup) (durable.LifecycleReceipt, error) {
	if err := q.Validate(); err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return readLifecycleReceipt(ctx, s.pgdb, q)
}

func (s *Store) EnrollRetirement(ctx context.Context, r durable.RetirementEnrollmentRequest) (result durable.LifecycleReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return result, err
	}
	digest, err := durable.Fingerprint(string(durable.OperationEnrollRetirement), r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	if _, err = tx.Exec(ctx, `SELECT dispatch_retirement_coordinator_lock($1,1)`, r.Namespace); err != nil {
		return result, err
	}
	n, err := scanNamespace(tx.QueryRow(ctx, `SELECT `+namespaceColumns+` FROM dispatch_durable_namespaces WHERE namespace=$1 AND installation_id=$2`, r.Namespace, r.InstallationID))
	if isNoRows(err) {
		return result, durable.ErrNotFound
	}
	if err != nil {
		return result, err
	}
	q := durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: durable.OperationEnrollRetirement, RequestID: r.RequestID, RequestDigest: digest}
	if saved, readErr := readLifecycleReceipt(ctx, tx, q); !errors.Is(readErr, durable.ErrNotFound) {
		return saved, readErr
	}
	if !n.RequireAudit {
		return result, durable.ErrInvalid
	}
	var exists bool
	if checkErr := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dispatch_retirement_namespaces WHERE namespace=$1)`, r.Namespace).Scan(&exists); checkErr != nil {
		return result, checkErr
	}
	if exists {
		return result, durable.ErrRequestConflict
	}
	builds, err := historicalBuilds(ctx, tx, r.Namespace)
	if err != nil {
		return result, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	for _, build := range builds {
		if _, err = tx.Exec(ctx, `INSERT INTO dispatch_build_lifecycle(namespace,build_id,state,epoch,version,changed_at) VALUES($1,$2,'accepting',1,1,$3)`, r.Namespace, build, now); err != nil {
			return result, err
		}
	}
	// Work-row triggers may have re-entered the shared lock under our exclusive
	// lock. Insert the floor directly under the already-held exclusive owner.
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_retirement_namespaces(namespace,schema_version,writer_protocol,version,enrolled_at) VALUES($1,1,1,1,$2)`, r.Namespace, now); err != nil {
		return result, err
	}
	setDigest, err := durable.Fingerprint("retirement-historical-builds.v1", builds)
	if err != nil {
		return result, err
	}
	facts := durable.CompatibilityFacts{NamespaceTarget: r.NamespaceTarget, Enrolled: true, SchemaVersion: 1, WriterProtocol: 1, Version: 1, EnrolledAt: now, ObservedAt: now}
	result = durable.LifecycleReceipt{NamespaceTarget: r.NamespaceTarget, Operation: q.Operation, RequestID: r.RequestID, RequestDigest: digest, ResponseVersion: 1, AcceptedAt: now, Enrollment: &durable.RetirementEnrollment{Compatibility: facts, HistoricalBuildCount: int64(len(builds)), HistoricalBuildDigest: setDigest}}
	if checkErr := saveLifecycleReceipt(ctx, tx, n, &result); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	return result, tx.Commit()
}

func saveLifecycleReceipt(ctx context.Context, tx driver.Tx, n durable.NamespaceRecord, r *durable.LifecycleReceipt) error {
	d, err := durable.NewDelivery(n, durable.DestinationChronicle, durable.LifecycleDeliverySource(ctx, *r))
	if err != nil {
		return err
	}
	r.DeliveryID = d.ID
	if checkErr := insertDelivery(ctx, tx, d); checkErr != nil {
		return checkErr
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO dispatch_lifecycle_receipts(namespace,operation,request_id,request_digest,command_digest,response_version,response,accepted_at,delivery_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.Namespace, string(r.Operation), r.RequestID, r.RequestDigest, r.CommandDigest, r.ResponseVersion, data, r.AcceptedAt, r.DeliveryID)
	return err
}

func historicalBuilds(ctx context.Context, tx driver.Tx, namespace string) ([]string, error) {
	// Foreign keys cover owner existence. Explicit anti-joins also reject
	// inconsistent imported state rather than losing an orphan in a join.
	var invalid bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dispatch_execution_tasks t LEFT JOIN dispatch_executions e USING(namespace,workflow_id,run_id) WHERE t.namespace=$1 AND e.run_id IS NULL) OR EXISTS(SELECT 1 FROM dispatch_child_executions c LEFT JOIN dispatch_executions p ON (p.namespace,p.workflow_id,p.run_id)=(c.namespace,c.parent_workflow_id,c.parent_run_id) LEFT JOIN dispatch_executions e ON (e.namespace,e.workflow_id,e.run_id)=(c.namespace,c.child_workflow_id,c.child_run_id) WHERE c.namespace=$1 AND (p.run_id IS NULL OR e.run_id IS NULL OR c.start_request->>'namespace' IS DISTINCT FROM c.namespace OR c.start_request->>'workflow_id' IS DISTINCT FROM c.child_workflow_id OR c.start_request->>'run_id' IS DISTINCT FROM c.child_run_id OR c.start_request->>'build_id' IS DISTINCT FROM e.build_id))`, namespace).Scan(&invalid)
	if err != nil {
		return nil, err
	}
	if invalid {
		return nil, durable.ErrInvalid
	}
	if checkErr := validateHistoricalLineage(ctx, tx, namespace); checkErr != nil {
		return nil, checkErr
	}
	rows, err := tx.Query(ctx, `SELECT build_id FROM dispatch_executions WHERE namespace=$1 UNION SELECT target_build_id FROM dispatch_child_deliveries WHERE namespace=$1`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	builds := []string{}
	for rows.Next() {
		var build string
		if checkErr := rows.Scan(&build); checkErr != nil {
			return nil, checkErr
		}
		if durable.ValidateBuildID(build) != nil {
			return nil, durable.ErrInvalid
		}
		builds = append(builds, build)
	}
	if checkErr := rows.Err(); checkErr != nil {
		return nil, checkErr
	}
	sort.Strings(builds)
	return builds, nil
}

func validateHistoricalLineage(ctx context.Context, tx driver.Tx, namespace string) error {
	var invalid bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dispatch_executions e LEFT JOIN dispatch_executions first ON (first.namespace,first.workflow_id,first.run_id)=(e.namespace,e.workflow_id,e.first_run_id) LEFT JOIN dispatch_executions previous ON (previous.namespace,previous.workflow_id,previous.run_id)=(e.namespace,e.workflow_id,e.previous_run_id) LEFT JOIN dispatch_executions next ON (next.namespace,next.workflow_id,next.run_id)=(e.namespace,e.workflow_id,e.next_run_id) WHERE e.namespace=$1 AND (first.run_id IS NULL OR (e.previous_run_id<>'' AND (previous.run_id IS NULL OR previous.next_run_id<>e.run_id OR previous.run_number<>e.run_number-1)) OR (e.next_run_id<>'' AND (next.run_id IS NULL OR next.previous_run_id<>e.run_id OR next.run_number<>e.run_number+1)))) OR EXISTS(SELECT 1 FROM dispatch_child_deliveries d LEFT JOIN dispatch_executions source ON (source.namespace,source.workflow_id,source.run_id)=(d.namespace,d.source_workflow_id,d.source_run_id) LEFT JOIN dispatch_executions target ON (target.namespace,target.workflow_id,target.run_id)=(d.namespace,d.target_workflow_id,d.target_run_id) WHERE d.namespace=$1 AND (source.run_id IS NULL OR target.run_id IS NULL OR d.target_build_id<>target.build_id))`, namespace).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid {
		return durable.ErrInvalid
	}
	rows, err := tx.Query(ctx, `SELECT c.namespace,c.parent_workflow_id,c.parent_run_id,c.command_id,c.start_request,c.parent_queue,c.parent_close_policy,e.workflow_type FROM dispatch_child_executions c JOIN dispatch_executions e ON (e.namespace,e.workflow_id,e.run_id)=(c.namespace,c.child_workflow_id,c.child_run_id) WHERE c.namespace=$1`, namespace)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var child durable.ChildExecution
		var data []byte
		var workflowType string
		if err := rows.Scan(&child.Parent.Namespace, &child.Parent.WorkflowID, &child.Parent.RunID, &child.CommandID, &data, &child.ParentQueue, &child.ParentClosePolicy, &workflowType); err != nil {
			return err
		}
		if json.Unmarshal(data, &child.Start) != nil || child.Validate(child.Parent) != nil || child.Start.WorkflowType != workflowType {
			return durable.ErrInvalid
		}
	}
	return rows.Err()
}
