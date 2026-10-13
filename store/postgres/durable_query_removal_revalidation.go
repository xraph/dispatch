package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

var _ durable.QueryRemovalRevalidationStore = (*Store)(nil)

func readGuardJSON(ctx context.Context, tx driver.Tx, table, key, namespace, id string, out any) error {
	var data []byte
	// The table and key are private constants at each call site.
	err := tx.QueryRow(ctx, "SELECT data FROM "+table+" WHERE namespace=$1 AND "+key+"=$2", namespace, id).Scan(&data)
	if isNoRows(err) {
		return durable.ErrNotFound
	}
	if err != nil {
		return err
	}
	if json.Unmarshal(data, out) != nil {
		return durable.ErrInvalid
	}
	return nil
}
func readGuardMaterial(ctx context.Context, tx driver.Tx, namespace, id string) (durable.QueryRemovalRevalidationMaterial, error) {
	var m durable.QueryRemovalRevalidationMaterial
	err := readGuardJSON(ctx, tx, "dispatch_query_removal_receipts", "request_id", namespace, id, &m)
	if err == nil {
		err = m.Validate()
	}
	return m, err
}
func readGuardState(ctx context.Context, tx driver.Tx, namespace, id string) (g durable.QueryRemovalGuard, cur durable.QueryRemovalCurrent, initial, current durable.QueryRemovalRevalidationMaterial, resultErr error) {
	if err := readGuardJSON(ctx, tx, "dispatch_query_removal_guards", "guard_id", namespace, id, &g); err != nil {
		return g, cur, initial, current, err
	}
	if err := readGuardJSON(ctx, tx, "dispatch_query_removal_current", "guard_id", namespace, id, &cur); err != nil {
		return g, cur, initial, current, err
	}
	var err error
	initial, err = readGuardMaterial(ctx, tx, namespace, g.InitialRequestID)
	if err != nil {
		return g, cur, initial, current, err
	}
	root, err := durable.QueryRemovalRoot(initial.Request, initial.Candidate)
	if err != nil || root.GuardID != g.GuardID || root.RootDigest != g.RootDigest || root.OriginalDigest != g.OriginalDigest || root.ExternalScopeDigest != g.ExternalScopeDigest || root.CandidateDigest != g.CandidateDigest || root.ProcessDigest != g.ProcessDigest {
		return g, cur, initial, current, durable.ErrInvalid
	}
	current, err = readGuardMaterial(ctx, tx, namespace, cur.RequestID)
	if err != nil {
		return g, cur, initial, current, err
	}
	if cur.GuardID != g.GuardID || cur.RootDigest != g.RootDigest || current.MaterialDigest != cur.MaterialDigest || current.GuardID != g.GuardID || current.RootDigest != g.RootDigest || !current.Request.Evidence.ObservedAt.Equal(cur.ObservedAt) {
		return g, cur, initial, current, durable.ErrInvalid
	}
	if g.State == "closed" {
		var closed durable.QueryRemovalGuardCloseReceipt
		if guardCheckErr := readGuardJSON(ctx, tx, "dispatch_query_removal_closes", "request_id", namespace, g.CloseRequestID, &closed); guardCheckErr != nil {
			return g, cur, initial, current, guardCheckErr
		}
		if closed.ReceiptDigest != g.CloseDigest || closed.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: initial.Candidate.QueryRuntimeTarget, RequestID: g.CloseRequestID, RequestDigest: closed.RequestDigest}) != nil {
			return g, cur, initial, current, durable.ErrInvalid
		}
	} else if g.State != "active" {
		return g, cur, initial, current, durable.ErrInvalid
	}
	return g, cur, initial, current, nil
}
func queryGuardFacts(ctx context.Context, tx driver.Tx, r durable.QueryRemovalRevalidationRequest, now *time.Time) (candidateResult durable.QueryRuntimeBinding, buildResult durable.BuildAdmission, survivorResult durable.QueryRuntimeBinding, retainedResult int64, resultErr error) {
	var candidate, survivor durable.QueryRuntimeBinding
	var build durable.BuildAdmission
	var retained int64
	begin, err := readLifecycleReceipt(ctx, tx, durable.LifecycleReceiptLookup{NamespaceTarget: r.Original.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: r.Original.BeginRequestID, RequestDigest: r.Original.BeginRequestDigest})
	if err != nil {
		return candidate, build, survivor, retained, err
	}
	candidate, err = readQueryBinding(ctx, tx, r.Original.QueryRuntimeTarget)
	if err != nil {
		return candidate, build, survivor, retained, err
	}
	build, err = readBuildAdmission(ctx, tx, r.Original.BuildTarget)
	if err != nil {
		return candidate, build, survivor, retained, err
	}
	lookup, required, lookupErr := durable.QueryRemovalFinalizationLookup(r, begin, build)
	if lookupErr != nil {
		return candidate, build, survivor, retained, lookupErr
	}
	if required {
		finalized, finalErr := readLifecycleReceipt(ctx, tx, lookup)
		if finalErr != nil {
			return candidate, build, survivor, retained, finalErr
		}
		if finalizationErr := durable.ValidateQueryRemovalFinalization(lookup, finalized, build); finalizationErr != nil {
			return candidate, build, survivor, retained, finalizationErr
		}
	}
	if r.Survivor.RuntimeID != "" {
		survivor, err = readQueryBinding(ctx, tx, r.Survivor)
		if err != nil {
			return candidate, build, survivor, retained, err
		}
	}
	if guardCheckErr := tx.QueryRow(ctx, `SELECT count(*) FROM dispatch_executions WHERE namespace=$1 AND build_id=$2`, r.Original.Namespace, r.Original.BuildID).Scan(&retained); guardCheckErr != nil {
		return candidate, build, survivor, retained, guardCheckErr
	}
	sample, sampleErr := queryGuardTime(ctx, tx)
	*now = sample
	err = sampleErr
	if err == nil {
		err = durable.ValidateQueryRemovalFacts(r, begin, candidate, build, survivor, retained, *now)
	}
	return candidate, build, survivor, retained, err
}
func saveGuardDelivery(ctx context.Context, tx driver.Tx, n durable.NamespaceRecord, kind, id string, at time.Time) (string, error) {
	d, err := durable.NewDelivery(n, durable.DestinationChronicle, durable.QueryRemovalDeliverySource(ctx, n.Namespace, kind, id, at))
	if err != nil {
		return "", err
	}
	return d.ID, insertDelivery(ctx, tx, d)
}

// RevalidateQueryRuntimeRemoval retains fresh proof and durable pins atomically.
func (s *Store) RevalidateQueryRuntimeRemoval(ctx context.Context, r durable.QueryRemovalRevalidationRequest) (result durable.QueryRemovalRevalidationReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	r = r.Clone()
	if err := r.Validate(); err != nil {
		return result, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.revalidation.v1", r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	n, err := lockLifecycleTarget(ctx, tx, r.Original.NamespaceTarget)
	if err != nil {
		return result, err
	}
	if saved, readErr := readGuardMaterial(ctx, tx, r.Original.Namespace, r.RequestID); !errors.Is(readErr, durable.ErrNotFound) {
		if readErr != nil {
			return result, readErr
		}
		if guardMatchErr := saved.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: r.Original.QueryRuntimeTarget, RequestID: r.RequestID, RequestDigest: digest}); guardMatchErr != nil {
			return result, guardMatchErr
		}
		return saved.Compact()
	}
	if guardCheckErr := checkRetirementEnrollment(ctx, tx, r.Original.Namespace); guardCheckErr != nil {
		return result, guardCheckErr
	}
	var now time.Time
	candidate, build, survivor, retained, err := queryGuardFacts(ctx, tx, r, &now)
	if err != nil {
		return result, err
	}
	g, err := durable.QueryRemovalRoot(r, candidate.QueryRuntimeIdentity)
	if err != nil {
		return result, err
	}
	prior, cur, initial, _, readErr := readGuardState(ctx, tx, r.Original.Namespace, g.GuardID)
	exists := readErr == nil
	if readErr != nil && !errors.Is(readErr, durable.ErrNotFound) {
		return result, readErr
	}
	// A root without its current or initial row is corruption, never a new guard.
	if errors.Is(readErr, durable.ErrNotFound) && prior.GuardID != "" {
		return result, durable.ErrInvalid
	}
	if exists {
		if prior.State != "active" || prior.RootDigest != g.RootDigest || r.ExpectedCurrentDigest != cur.MaterialDigest || !r.Evidence.ObservedAt.After(cur.ObservedAt) || !durable.QueryRemovalCompatibleBuild(initial.Build, build) {
			return result, durable.ErrQueryRemovalGuard
		}
		g = prior
	} else if r.ExpectedCurrentDigest != "" {
		return result, durable.ErrRevisionConflict
	}
	// Authority reads may wait; time is sampled again after every required read.
	begin, err := readLifecycleReceipt(ctx, tx, durable.LifecycleReceiptLookup{NamespaceTarget: r.Original.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: r.Original.BeginRequestID, RequestDigest: r.Original.BeginRequestDigest})
	if err != nil {
		return result, err
	}
	now, err = queryGuardTime(ctx, tx)
	if err != nil {
		return result, err
	}
	if guardCheckErr := durable.ValidateQueryRemovalFacts(r, begin, candidate, build, survivor, retained, now); guardCheckErr != nil {
		return result, guardCheckErr
	}
	material, err := durable.NewQueryRemovalMaterial(r, g, candidate, build, survivor, retained, now)
	if err != nil {
		return result, err
	}
	material.DeliveryID, err = saveGuardDelivery(ctx, tx, n, "revalidate", r.RequestID, now)
	if err != nil {
		return result, err
	}
	if guardCheckErr := material.Seal(); guardCheckErr != nil {
		return result, guardCheckErr
	}
	cur = durable.QueryRemovalCurrent{GuardID: g.GuardID, RootDigest: g.RootDigest, RequestID: r.RequestID, MaterialDigest: material.MaterialDigest, ObservedAt: r.Evidence.ObservedAt}
	if durable.QueryRemovalSize(g, 8192) != nil || durable.QueryRemovalSize(cur, 1024) != nil {
		return result, durable.ErrInvalid
	}
	result, err = material.Compact()
	if err != nil {
		return result, err
	}
	if !exists {
		data, marshalErr := json.Marshal(g)
		if marshalErr != nil {
			return result, marshalErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO dispatch_query_removal_guards(namespace,guard_id,state,data) VALUES($1,$2,$3,$4)`, n.Namespace, g.GuardID, g.State, data); err != nil {
			return result, err
		}
	}
	data, err := json.Marshal(material)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_query_removal_receipts(namespace,request_id,guard_id,candidate_id,survivor_id,build_id,removal_epoch,material_digest,delivery_id,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, n.Namespace, r.RequestID, g.GuardID, candidate.RuntimeID, survivor.RuntimeID, build.BuildID, candidate.RemovalEpoch, material.MaterialDigest, material.DeliveryID, data); err != nil {
		return result, err
	}
	data, err = json.Marshal(cur)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_query_removal_current(namespace,guard_id,request_id,material_digest,data) VALUES($1,$2,$3,$4,$5) ON CONFLICT(namespace,guard_id) DO UPDATE SET request_id=EXCLUDED.request_id,material_digest=EXCLUDED.material_digest,data=EXCLUDED.data`, n.Namespace, g.GuardID, r.RequestID, material.MaterialDigest, data); err != nil {
		return result, err
	}
	return result, tx.Commit()
}

// LookupQueryRemovalRevalidation recovers one immutable material row.
func (s *Store) LookupQueryRemovalRevalidation(ctx context.Context, q durable.QueryRemovalRevalidationLookup) (durable.QueryRemovalRevalidationMaterial, error) {
	if err := q.Validate(); err != nil {
		return durable.QueryRemovalRevalidationMaterial{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.QueryRemovalRevalidationMaterial{}, err
	}
	defer s.rollbackExecution(tx)
	if _, err = lockLifecycleTarget(ctx, tx, q.NamespaceTarget); err != nil {
		return durable.QueryRemovalRevalidationMaterial{}, err
	}
	m, err := readGuardMaterial(ctx, tx, q.Namespace, q.RequestID)
	if err == nil {
		err = m.Match(q)
	}
	return m, err
}

// BindQueryRemovalCommit records an invocation only while current proof is fresh.
func (s *Store) BindQueryRemovalCommit(ctx context.Context, r durable.QueryRemovalCommitRequest) (result durable.QueryRemovalCommitReceipt, resultErr error) {
	r.Permission.AcceptedAt = durable.Timestamp(r.Permission.AcceptedAt)
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return result, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.commit.v1", r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	n, err := lockLifecycleTarget(ctx, tx, r.Guard.NamespaceTarget)
	if err != nil {
		return result, err
	}
	if readErr := readGuardJSON(ctx, tx, "dispatch_query_removal_commits", "request_id", r.Guard.Namespace, r.RequestID, &result); !errors.Is(readErr, durable.ErrNotFound) {
		if readErr != nil {
			return result, readErr
		}
		return result, result.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: r.Guard.QueryRuntimeTarget, RequestID: r.RequestID, RequestDigest: digest})
	}
	if guardCheckErr := checkRetirementEnrollment(ctx, tx, r.Guard.Namespace); guardCheckErr != nil {
		return result, guardCheckErr
	}
	g, cur, initial, current, err := readGuardState(ctx, tx, r.Guard.Namespace, r.Guard.GuardID)
	if err != nil {
		return result, err
	}
	permission, err := readGuardMaterial(ctx, tx, r.Guard.Namespace, r.Permission.RevalidationRequestID)
	if err != nil {
		return result, err
	}
	if r.Guard.Match(g, initial) != nil || g.State != "active" || r.ExpectedCurrentDigest != cur.MaterialDigest || r.CurrentRequestID != cur.RequestID || permission.GuardID != g.GuardID || permission.MaterialDigest != r.Permission.RevalidationDigest || r.Permission.ExternalScope != initial.Request.ExternalScope || r.Permission.AcceptedAt.Before(permission.AcceptedAt) || !r.Permission.AcceptedAt.Before(permission.ValidUntil) {
		return result, durable.ErrQueryRemovalGuard
	}
	var now time.Time
	_, _, _, _, err = queryGuardFacts(ctx, tx, current.Request, &now)
	if err != nil {
		return result, err
	}
	if r.Permission.AcceptedAt.After(now) {
		return result, durable.ErrQueryRemovalGuard
	}
	result = durable.QueryRemovalCommitReceipt{Request: r, RequestDigest: digest, AcceptedAt: now}
	result.DeliveryID, err = saveGuardDelivery(ctx, tx, n, "commit", r.RequestID, now)
	if err != nil {
		return result, err
	}
	if guardCheckErr := result.Seal(); guardCheckErr != nil {
		return result, guardCheckErr
	}
	data, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_query_removal_commits(namespace,request_id,guard_id,delivery_id,data) VALUES($1,$2,$3,$4,$5)`, n.Namespace, r.RequestID, g.GuardID, result.DeliveryID, data); err != nil {
		return result, err
	}
	return result, tx.Commit()
}

// CloseQueryRemovalGuard retains the permanent cutoff and releases its pins.
func (s *Store) CloseQueryRemovalGuard(ctx context.Context, r durable.QueryRemovalGuardCloseRequest) (result durable.QueryRemovalGuardCloseReceipt, resultErr error) {
	r.Closure.CutoffAt = durable.Timestamp(r.Closure.CutoffAt)
	r.Closure.TerminalAt = durable.Timestamp(r.Closure.TerminalAt)
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return result, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.close.v1", r)
	if err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	n, err := lockLifecycleTarget(ctx, tx, r.Guard.NamespaceTarget)
	if err != nil {
		return result, err
	}
	if readErr := readGuardJSON(ctx, tx, "dispatch_query_removal_closes", "request_id", r.Guard.Namespace, r.RequestID, &result); !errors.Is(readErr, durable.ErrNotFound) {
		if readErr != nil {
			return result, readErr
		}
		return result, result.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: r.Guard.QueryRuntimeTarget, RequestID: r.RequestID, RequestDigest: digest})
	}
	if guardCheckErr := checkRetirementEnrollment(ctx, tx, r.Guard.Namespace); guardCheckErr != nil {
		return result, guardCheckErr
	}
	g, _, initial, _, err := readGuardState(ctx, tx, r.Guard.Namespace, r.Guard.GuardID)
	if err != nil {
		return result, err
	}
	begin, err := readLifecycleReceipt(ctx, tx, durable.LifecycleReceiptLookup{NamespaceTarget: r.Guard.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: initial.Request.Original.BeginRequestID, RequestDigest: initial.Request.Original.BeginRequestDigest})
	if err != nil {
		return result, err
	}
	if guardCheckErr := initial.Request.Original.MatchOriginal(begin); guardCheckErr != nil {
		return result, guardCheckErr
	}
	now, err := queryGuardTime(ctx, tx)
	if err != nil {
		return result, err
	}
	c := r.Closure
	if r.Guard.Match(g, initial) != nil || g.State != "active" || c.ExternalScope != initial.Request.ExternalScope || c.CutoffAt.Before(initial.AcceptedAt) || c.TerminalAt.Before(initial.AcceptedAt) || c.CutoffAt.After(now) || c.TerminalAt.After(now) {
		return result, durable.ErrQueryRemovalGuard
	}
	result = durable.QueryRemovalGuardCloseReceipt{Request: r, RequestDigest: digest, AcceptedAt: now}
	result.DeliveryID, err = saveGuardDelivery(ctx, tx, n, "guard.close", r.RequestID, now)
	if err != nil {
		return result, err
	}
	if guardCheckErr := result.Seal(); guardCheckErr != nil {
		return result, guardCheckErr
	}
	data, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dispatch_query_removal_closes(namespace,request_id,guard_id,delivery_id,data) VALUES($1,$2,$3,$4,$5)`, n.Namespace, r.RequestID, g.GuardID, result.DeliveryID, data); err != nil {
		return result, err
	}
	g.State = "closed"
	g.CloseRequestID = r.RequestID
	g.CloseDigest = result.ReceiptDigest
	data, err = json.Marshal(g)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `UPDATE dispatch_query_removal_guards SET state='closed',data=$3 WHERE namespace=$1 AND guard_id=$2`, n.Namespace, g.GuardID, data); err != nil {
		return result, err
	}
	return result, tx.Commit()
}

// LookupQueryRemovalGuardReceipt performs exact nonissuing command recovery.
func (s *Store) LookupQueryRemovalGuardReceipt(ctx context.Context, q durable.QueryRemovalGuardReceiptLookup) (durable.QueryRemovalGuardReceipt, error) {
	if err := q.Validate(); err != nil {
		return durable.QueryRemovalGuardReceipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.QueryRemovalGuardReceipt{}, err
	}
	defer s.rollbackExecution(tx)
	if _, err = lockLifecycleTarget(ctx, tx, q.NamespaceTarget); err != nil {
		return durable.QueryRemovalGuardReceipt{}, err
	}
	switch q.Kind {
	case "commit":
		var r durable.QueryRemovalCommitReceipt
		err = readGuardJSON(ctx, tx, "dispatch_query_removal_commits", "request_id", q.Namespace, q.RequestID, &r)
		if err == nil {
			err = r.Match(q.QueryRemovalRevalidationLookup)
		}
		return durable.QueryRemovalGuardReceipt{Commit: &r}, err
	case "close":
		var r durable.QueryRemovalGuardCloseReceipt
		err = readGuardJSON(ctx, tx, "dispatch_query_removal_closes", "request_id", q.Namespace, q.RequestID, &r)
		if err == nil {
			err = r.Match(q.QueryRemovalRevalidationLookup)
		}
		return durable.QueryRemovalGuardReceipt{Close: &r}, err
	default:
		return durable.QueryRemovalGuardReceipt{}, durable.ErrInvalid
	}
}

// CheckQueryRemovalCommit reports historical acceptance and exact current state.
func (s *Store) CheckQueryRemovalCommit(ctx context.Context, q durable.QueryRemovalCommitLookup) (result durable.QueryRemovalCommitStatus, resultErr error) {
	if err := q.Validate(); err != nil {
		return result, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer s.rollbackExecution(tx)
	if _, err = lockLifecycleTarget(ctx, tx, q.NamespaceTarget); err != nil {
		return result, err
	}
	if guardCheckErr := readGuardJSON(ctx, tx, "dispatch_query_removal_commits", "request_id", q.Namespace, q.RequestID, &result.Receipt); guardCheckErr != nil {
		return result, guardCheckErr
	}
	if guardCheckErr := result.Receipt.Match(q.QueryRemovalRevalidationLookup); guardCheckErr != nil {
		return result, guardCheckErr
	}
	result.Guard, result.Current, _, _, err = readGuardState(ctx, tx, q.Namespace, result.Receipt.Request.Guard.GuardID)
	if err != nil {
		return result, err
	}
	result.ObservedAt, err = queryGuardTime(ctx, tx)
	return result, err
}

func queryGuardTime(ctx context.Context, tx driver.Tx) (time.Time, error) {
	at, err := executionTime(ctx, tx)
	return durable.Timestamp(at), err
}
