package postgres

import (
	"context"
	"errors"

	"github.com/xraph/dispatch/durable"
)

var _ durable.WorkerDrainStore = (*Store)(nil)

func (s *Store) RequestWorkerDrain(ctx context.Context, r durable.WorkerDrainRequest) (result durable.LifecycleReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return result, err
	}
	digest, err := durable.Fingerprint(string(durable.OperationRequestWorkerDrain), r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	namespace, err := lockLifecycleTarget(ctx, tx, r.NamespaceTarget)
	if err != nil {
		return result, err
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: durable.OperationRequestWorkerDrain, RequestID: r.RequestID, RequestDigest: digest}
	if saved, readErr := readLifecycleReceipt(ctx, tx, lookup); !errors.Is(readErr, durable.ErrNotFound) {
		return saved, readErr
	}
	if checkErr := checkRetirementEnrollment(ctx, tx, r.Namespace); checkErr != nil {
		return result, checkErr
	}
	if _, readErr := readBuildAdmission(ctx, tx, r.BuildTarget); readErr != nil {
		return result, readErr
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return result, err
	}
	if !r.Deadline.After(now) {
		return result, durable.ErrInvalid
	}
	result = durable.LifecycleReceipt{NamespaceTarget: r.NamespaceTarget, Operation: lookup.Operation, RequestID: r.RequestID, RequestDigest: digest, CommandDigest: r.CommandDigest, ResponseVersion: 1, AcceptedAt: now, WorkerDrain: &r}
	if saveErr := saveLifecycleReceipt(ctx, tx, namespace, &result); saveErr != nil {
		return durable.LifecycleReceipt{}, saveErr
	}
	return result, tx.Commit()
}
