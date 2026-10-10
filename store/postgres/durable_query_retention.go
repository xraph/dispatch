package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

var _ durable.QueryRuntimeStore = (*Store)(nil)

func readQueryBinding(ctx context.Context, tx driver.Tx, target durable.QueryRuntimeTarget) (durable.QueryRuntimeBinding, error) {
	var b durable.QueryRuntimeBinding
	var data []byte
	err := tx.QueryRow(ctx, `SELECT binding FROM dispatch_query_runtimes WHERE namespace=$1 AND runtime_id=$2`, target.Namespace, target.RuntimeID).Scan(&data)
	if isNoRows(err) {
		return b, durable.ErrNotFound
	}
	if err != nil {
		return b, err
	}
	if json.Unmarshal(data, &b) != nil || b.Validate() != nil {
		return b, durable.ErrInvalid
	}
	if b.QueryRuntimeTarget != target {
		return b, durable.ErrRequestConflict
	}
	return b, nil
}
func queryBindings(ctx context.Context, tx driver.Tx, target durable.BuildTarget) (int64, []durable.QueryRuntimeBinding, error) {
	var retained int64
	if checkErr := tx.QueryRow(ctx, `SELECT count(*) FROM dispatch_executions WHERE namespace=$1 AND build_id=$2`, target.Namespace, target.BuildID).Scan(&retained); checkErr != nil {
		return 0, nil, checkErr
	}
	rows, err := tx.Query(ctx, `SELECT binding FROM dispatch_query_runtimes WHERE namespace=$1 AND build_id=$2 ORDER BY runtime_id`, target.Namespace, target.BuildID)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	bindings := []durable.QueryRuntimeBinding{}
	for rows.Next() {
		var data []byte
		var b durable.QueryRuntimeBinding
		if checkErr := rows.Scan(&data); checkErr != nil {
			return 0, nil, checkErr
		}
		if json.Unmarshal(data, &b) != nil || b.Validate() != nil || b.BuildTarget != target {
			return 0, nil, durable.ErrInvalid
		}
		bindings = append(bindings, b)
	}
	return retained, bindings, rows.Err()
}
func (s *Store) InspectQueryRetention(ctx context.Context, target durable.BuildTarget) (durable.QueryRetentionFacts, error) {
	var result durable.QueryRetentionFacts
	if checkErr := target.Validate(); checkErr != nil {
		return result, checkErr
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	if _, err = lockLifecycleTarget(ctx, tx, target.NamespaceTarget); err != nil {
		return result, err
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, target.Namespace); checkErr != nil {
		return result, checkErr
	}
	build, err := readBuildAdmission(ctx, tx, target)
	if err != nil {
		return result, err
	}
	retained, bindings, err := queryBindings(ctx, tx, target)
	if err != nil {
		return result, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	return durable.QueryRetention(build, retained, bindings, now), nil
}
func (s *Store) ListQueryRuntimes(ctx context.Context, r durable.QueryRuntimeList) (durable.QueryRuntimePage, error) {
	var page durable.QueryRuntimePage
	if checkErr := r.Validate(); checkErr != nil {
		return page, checkErr
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return page, err
	}
	defer s.rollbackExecution(tx)
	if _, err = lockLifecycleTarget(ctx, tx, r.NamespaceTarget); err != nil {
		return page, err
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, r.Namespace); checkErr != nil {
		return page, checkErr
	}
	rows, err := tx.Query(ctx, `SELECT binding FROM dispatch_query_runtimes WHERE namespace=$1 AND runtime_id>$2 AND ($3='' OR instance_id=$3) AND ($4='' OR build_id=$4) ORDER BY runtime_id LIMIT $5`, r.Namespace, r.After, r.InstanceID, r.BuildID, r.Limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	page.Items = []durable.QueryRuntimeBinding{}
	for rows.Next() {
		var data []byte
		var b durable.QueryRuntimeBinding
		if checkErr := rows.Scan(&data); checkErr != nil {
			return page, checkErr
		}
		if json.Unmarshal(data, &b) != nil || b.Validate() != nil || b.NamespaceTarget != r.NamespaceTarget {
			return page, durable.ErrInvalid
		}
		page.Items = append(page.Items, b)
	}
	if checkErr := rows.Err(); checkErr != nil {
		return page, checkErr
	}
	rows.Close()
	if len(page.Items) > r.Limit {
		page.Items = page.Items[:r.Limit]
		page.Next = page.Items[len(page.Items)-1].RuntimeID
	}
	page.ObservedAt, err = executionTime(ctx, tx)
	return page, err
}
func (s *Store) mutateQueryRuntime(ctx context.Context, target durable.QueryRuntimeTarget, operation durable.LifecycleOperation, id, digest, command string, settlement *durable.QueryRemovalAbortEvidence, apply func(driver.Tx, durable.BuildAdmission, durable.QueryRuntimeBinding, bool, time.Time) (durable.QueryRuntimeBinding, error)) (result durable.LifecycleReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	n, err := lockLifecycleTarget(ctx, tx, target.NamespaceTarget)
	if err != nil {
		return result, err
	}
	q := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: operation, RequestID: id, RequestDigest: digest}
	if saved, readErr := readLifecycleReceipt(ctx, tx, q); !errors.Is(readErr, durable.ErrNotFound) {
		return saved, readErr
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, target.Namespace); checkErr != nil {
		return result, checkErr
	}
	build, err := readBuildAdmission(ctx, tx, target.BuildTarget)
	if err != nil {
		return result, err
	}
	prior, err := readQueryBinding(ctx, tx, target)
	exists := err == nil
	if err != nil && !errors.Is(err, durable.ErrNotFound) {
		return result, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	next, err := apply(tx, build, prior, exists, now)
	if err != nil {
		return result, err
	}
	if checkErr := next.Validate(); checkErr != nil {
		return result, checkErr
	}
	data, err := json.Marshal(next)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_query_runtimes(namespace,build_id,runtime_id,instance_id,state,version,binding) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(namespace,runtime_id) DO UPDATE SET state=EXCLUDED.state,version=EXCLUDED.version,binding=EXCLUDED.binding`, next.Namespace, next.BuildID, next.RuntimeID, next.InstanceID, next.State, next.Version, data); err != nil {
		return result, err
	}
	result = durable.LifecycleReceipt{NamespaceTarget: target.NamespaceTarget, Operation: operation, RequestID: id, RequestDigest: digest, CommandDigest: command, ResponseVersion: 1, AcceptedAt: next.ChangedAt, QueryRuntime: &next, QueryAbort: settlement}
	if checkErr := saveLifecycleReceipt(ctx, tx, n, &result); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	return result, tx.Commit()
}
func (s *Store) RegisterQueryRuntime(ctx context.Context, r durable.RegisterQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	digest, err := durable.Fingerprint(string(durable.OperationRegisterQueryRuntime), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return s.mutateQueryRuntime(ctx, r.Identity.QueryRuntimeTarget, durable.OperationRegisterQueryRuntime, r.RequestID, digest, r.CommandDigest, nil, func(_ driver.Tx, build durable.BuildAdmission, _ durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if exists {
			return durable.QueryRuntimeBinding{}, durable.ErrRequestConflict
		}
		if build.QueryIdentity != r.Identity.BuildIdentity || build.QueryIdentity.Validate() != nil {
			return durable.QueryRuntimeBinding{}, durable.ErrQueryRetention
		}
		return durable.QueryRuntimeBinding{QueryRuntimeIdentity: r.Identity, State: durable.QueryRuntimeActive, Version: 1, CreatedAt: now, ChangedAt: now}, nil
	})
}
func (s *Store) RecordQueryRuntimeVerification(ctx context.Context, r durable.VerifyQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	operation := durable.OperationVerifyQueryRuntime
	digest, err := durable.Fingerprint(string(operation), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return s.mutateQueryRuntime(ctx, r.QueryRuntimeTarget, operation, r.RequestID, digest, r.CommandDigest, nil, func(_ driver.Tx, _ durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		return durable.VerifyQueryBinding(b, r, now)
	})
}
func (s *Store) AbortQueryRuntimeRemoval(ctx context.Context, r durable.AbortQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	operation := durable.OperationAbortQueryRemoval
	digest, err := durable.Fingerprint(string(operation), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return s.mutateQueryRuntime(ctx, r.QueryRuntimeTarget, operation, r.RequestID, digest, r.CommandDigest, &durable.QueryRemovalAbortEvidence{Fence: r.Fence, Settlement: r.Settlement}, func(_ driver.Tx, _ durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		return durable.AbortQueryRemoval(b, r, now)
	})
}
func (s *Store) BeginQueryRuntimeRemoval(ctx context.Context, r durable.BeginQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	digest, err := durable.Fingerprint(string(durable.OperationBeginQueryRemoval), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return s.mutateQueryRuntime(ctx, r.QueryRuntimeTarget, durable.OperationBeginQueryRemoval, r.RequestID, digest, "", nil, func(tx driver.Tx, build durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, _ time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		retained, bindings, readErr := queryBindings(ctx, tx, r.BuildTarget)
		if readErr != nil {
			return b, readErr
		}
		now, readErr := executionTime(ctx, tx)
		if readErr != nil {
			return b, readErr
		}
		return durable.ReserveQueryRemoval(b, r, build, retained, bindings, now)
	})
}
func (s *Store) FinishQueryRuntimeRemoval(ctx context.Context, r durable.FinishQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	digest, err := durable.Fingerprint(string(durable.OperationFinishQueryRemoval), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return s.mutateQueryRuntime(ctx, r.Fence.Candidate.QueryRuntimeTarget, durable.OperationFinishQueryRemoval, r.RequestID, digest, "", nil, func(_ driver.Tx, _ durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		return durable.FinishQueryRemoval(b, r, now)
	})
}
func (s *Store) CheckQueryRuntimeRemoval(ctx context.Context, f durable.QueryRemovalFence) (durable.QueryRemovalFacts, error) {
	var result durable.QueryRemovalFacts
	if checkErr := f.Candidate.Validate(); checkErr != nil {
		return result, checkErr
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	if _, err = lockLifecycleTarget(ctx, tx, f.Candidate.NamespaceTarget); err != nil {
		return result, err
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, f.Candidate.Namespace); checkErr != nil {
		return result, checkErr
	}
	candidate, err := readQueryBinding(ctx, tx, f.Candidate.QueryRuntimeTarget)
	if err != nil {
		return result, err
	}
	build, err := readBuildAdmission(ctx, tx, f.Candidate.BuildTarget)
	if err != nil {
		return result, err
	}
	retained, bindings, err := queryBindings(ctx, tx, f.Candidate.BuildTarget)
	if err != nil {
		return result, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	if checkErr := durable.CheckQueryRemoval(candidate, f, build, retained, bindings, now); checkErr != nil {
		return result, checkErr
	}
	return durable.QueryRemovalFacts{Fence: f, CheckedAt: now}, nil
}
