package memory

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

var _ durable.QueryRemovalRevalidationStore = (*Store)(nil)

type queryGuardKey struct{ namespace, id string }

func (m *Store) guardFacts(r durable.QueryRemovalRevalidationRequest, now *time.Time) (candidateResult durable.QueryRuntimeBinding, buildResult durable.BuildAdmission, survivorResult durable.QueryRuntimeBinding, retainedResult int64, resultErr error) {
	begin, ok := m.lifecycleReceipts[lifecycleReceiptKey{r.Original.Namespace, durable.OperationBeginQueryRemoval, r.Original.BeginRequestID}]
	if !ok {
		return durable.QueryRuntimeBinding{}, durable.BuildAdmission{}, durable.QueryRuntimeBinding{}, 0, durable.ErrNotFound
	}
	candidate := m.queryRuntimes[queryRuntimeKey{r.Original.Namespace, r.Original.RuntimeID}]
	build := m.buildAdmissions[r.Original.BuildTarget]
	lookup, required, lookupErr := durable.QueryRemovalFinalizationLookup(r, begin, build)
	if lookupErr != nil {
		return candidate, build, durable.QueryRuntimeBinding{}, 0, lookupErr
	}
	if required {
		finalized := m.lifecycleReceipts[lifecycleReceiptKey{r.Original.Namespace, durable.OperationFinalizeRetirement, lookup.RequestID}]
		if finalErr := durable.ValidateQueryRemovalFinalization(lookup, finalized, build); finalErr != nil {
			return candidate, build, durable.QueryRuntimeBinding{}, 0, finalErr
		}
	}
	var survivor durable.QueryRuntimeBinding
	if r.Survivor.RuntimeID != "" {
		survivor = m.queryRuntimes[queryRuntimeKey{r.Survivor.Namespace, r.Survivor.RuntimeID}]
	}
	var retained int64
	for key, e := range m.executions {
		if key.Namespace == r.Original.Namespace && e.execution.BuildID == r.Original.BuildID {
			retained++
		}
	}
	*now = durable.Timestamp(time.Now())
	err := durable.ValidateQueryRemovalFacts(r, begin, candidate, build, survivor, retained, *now)
	return candidate, build, survivor, retained, err
}
func (m *Store) guardState(namespace, id string) (durable.QueryRemovalGuard, durable.QueryRemovalCurrent, durable.QueryRemovalRevalidationMaterial, error) {
	key := queryGuardKey{namespace, id}
	g, ok := m.queryRemovalGuards[key]
	if !ok {
		return g, durable.QueryRemovalCurrent{}, durable.QueryRemovalRevalidationMaterial{}, durable.ErrNotFound
	}
	cur, ok := m.queryRemovalCurrent[key]
	if !ok {
		return g, cur, durable.QueryRemovalRevalidationMaterial{}, durable.ErrInvalid
	}
	initial, ok := m.queryRemovalMaterials[queryGuardKey{namespace, g.InitialRequestID}]
	if !ok || initial.Validate() != nil {
		return g, cur, initial, durable.ErrInvalid
	}
	root, err := durable.QueryRemovalRoot(initial.Request, initial.Candidate)
	if err != nil || root.RootDigest != g.RootDigest || root.GuardID != g.GuardID || root.OriginalDigest != g.OriginalDigest || root.ExternalScopeDigest != g.ExternalScopeDigest || root.CandidateDigest != g.CandidateDigest || root.ProcessDigest != g.ProcessDigest {
		return g, cur, initial, durable.ErrInvalid
	}
	current, ok := m.queryRemovalMaterials[queryGuardKey{namespace, cur.RequestID}]
	if !ok || current.Validate() != nil || cur.GuardID != g.GuardID || cur.RootDigest != g.RootDigest || current.MaterialDigest != cur.MaterialDigest || current.GuardID != g.GuardID || current.RootDigest != g.RootDigest || !current.Request.Evidence.ObservedAt.Equal(cur.ObservedAt) {
		return g, cur, initial, durable.ErrInvalid
	}
	if g.State == "closed" {
		closeReceipt, exists := m.queryRemovalCloses[queryGuardKey{namespace, g.CloseRequestID}]
		if !exists || closeReceipt.ReceiptDigest != g.CloseDigest || closeReceipt.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: initial.Candidate.QueryRuntimeTarget, RequestID: g.CloseRequestID, RequestDigest: closeReceipt.RequestDigest}) != nil {
			return g, cur, initial, durable.ErrInvalid
		}
	} else if g.State != "active" {
		return g, cur, initial, durable.ErrInvalid
	}
	return g, cur, initial, nil
}
func (m *Store) guardDelivery(ctx context.Context, namespace, kind, id string, at time.Time) (string, error) {
	d, err := durable.NewDelivery(m.namespaces[namespace], durable.DestinationChronicle, durable.QueryRemovalDeliverySource(ctx, namespace, kind, id, at))
	return d.ID, err
}

// RevalidateQueryRuntimeRemoval atomically protects all selected associations.
func (m *Store) RevalidateQueryRuntimeRemoval(ctx context.Context, r durable.QueryRemovalRevalidationRequest) (durable.QueryRemovalRevalidationReceipt, error) {
	r = r.Clone()
	if err := r.Validate(); err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.revalidation.v1", r)
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Original.Namespace, func(c *Store) (durable.QueryRemovalRevalidationReceipt, error) {
		key := queryGuardKey{r.Original.Namespace, r.RequestID}
		if saved, ok := c.queryRemovalMaterials[key]; ok {
			if matchErr := saved.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: r.Original.QueryRuntimeTarget, RequestID: r.RequestID, RequestDigest: digest}); matchErr != nil {
				return durable.QueryRemovalRevalidationReceipt{}, matchErr
			}
			return saved.Compact()
		}
		if checkErr := c.checkLifecycleTarget(r.Original.NamespaceTarget); checkErr != nil {
			return durable.QueryRemovalRevalidationReceipt{}, checkErr
		}
		now := durable.Timestamp(time.Now())
		candidate, build, survivor, retained, checkErr := c.guardFacts(r, &now)
		if checkErr != nil {
			return durable.QueryRemovalRevalidationReceipt{}, checkErr
		}
		g, checkErr := durable.QueryRemovalRoot(r, candidate.QueryRuntimeIdentity)
		if checkErr != nil {
			return durable.QueryRemovalRevalidationReceipt{}, checkErr
		}
		gkey := queryGuardKey{r.Original.Namespace, g.GuardID}
		if _, exists := c.queryRemovalGuards[gkey]; exists {
			prior, cur, initial, stateErr := c.guardState(r.Original.Namespace, g.GuardID)
			if stateErr != nil {
				return durable.QueryRemovalRevalidationReceipt{}, stateErr
			}
			if prior.State != "active" || prior.RootDigest != g.RootDigest || r.ExpectedCurrentDigest != cur.MaterialDigest || !r.Evidence.ObservedAt.After(cur.ObservedAt) || !durable.QueryRemovalCompatibleBuild(initial.Build, build) {
				return durable.QueryRemovalRevalidationReceipt{}, durable.ErrQueryRemovalGuard
			}
			g = prior
		} else if _, corrupt := c.queryRemovalCurrent[gkey]; corrupt {
			return durable.QueryRemovalRevalidationReceipt{}, durable.ErrInvalid
		} else if r.ExpectedCurrentDigest != "" {
			return durable.QueryRemovalRevalidationReceipt{}, durable.ErrRevisionConflict
		}
		now = durable.Timestamp(time.Now())
		if _, _, _, _, checkErr = c.guardFacts(r, &now); checkErr != nil {
			return durable.QueryRemovalRevalidationReceipt{}, checkErr
		}
		material, checkErr := durable.NewQueryRemovalMaterial(r, g, candidate, build, survivor, retained, now)
		if checkErr != nil {
			return durable.QueryRemovalRevalidationReceipt{}, checkErr
		}
		material.DeliveryID, checkErr = c.guardDelivery(ctx, r.Original.Namespace, "revalidate", r.RequestID, now)
		if checkErr != nil {
			return durable.QueryRemovalRevalidationReceipt{}, checkErr
		}
		if sealErr := material.Seal(); sealErr != nil {
			return durable.QueryRemovalRevalidationReceipt{}, sealErr
		}
		cur := durable.QueryRemovalCurrent{GuardID: g.GuardID, RootDigest: g.RootDigest, RequestID: r.RequestID, MaterialDigest: material.MaterialDigest, ObservedAt: r.Evidence.ObservedAt}
		if durable.QueryRemovalSize(g, 8192) != nil || durable.QueryRemovalSize(cur, 1024) != nil {
			return durable.QueryRemovalRevalidationReceipt{}, durable.ErrInvalid
		}
		compact, checkErr := material.Compact()
		if checkErr != nil {
			return compact, checkErr
		}
		c.queryRemovalGuards[gkey] = g
		c.queryRemovalCurrent[gkey] = cur
		c.queryRemovalMaterials[key] = material
		return compact, nil
	})
}

// LookupQueryRemovalRevalidation recovers authenticated material without renewal.
func (m *Store) LookupQueryRemovalRevalidation(ctx context.Context, q durable.QueryRemovalRevalidationLookup) (durable.QueryRemovalRevalidationMaterial, error) {
	if err := q.Validate(); err != nil {
		return durable.QueryRemovalRevalidationMaterial{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.QueryRemovalRevalidationMaterial{}, err
	}
	saved, ok := m.queryRemovalMaterials[queryGuardKey{q.Namespace, q.RequestID}]
	if !ok {
		return saved, durable.ErrNotFound
	}
	return saved.Clone(), saved.Match(q)
}

// BindQueryRemovalCommit records one external permission after fresh native checks.
func (m *Store) BindQueryRemovalCommit(ctx context.Context, r durable.QueryRemovalCommitRequest) (durable.QueryRemovalCommitReceipt, error) {
	r.Permission.AcceptedAt = durable.Timestamp(r.Permission.AcceptedAt)
	if err := r.Validate(); err != nil {
		return durable.QueryRemovalCommitReceipt{}, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.commit.v1", r)
	if err != nil {
		return durable.QueryRemovalCommitReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Guard.Namespace, func(c *Store) (durable.QueryRemovalCommitReceipt, error) {
		key := queryGuardKey{r.Guard.Namespace, r.RequestID}
		if saved, ok := c.queryRemovalCommits[key]; ok {
			return saved, saved.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: r.Guard.QueryRuntimeTarget, RequestID: r.RequestID, RequestDigest: digest})
		}
		if checkErr := c.checkLifecycleTarget(r.Guard.NamespaceTarget); checkErr != nil {
			return durable.QueryRemovalCommitReceipt{}, checkErr
		}
		g, cur, initial, checkErr := c.guardState(r.Guard.Namespace, r.Guard.GuardID)
		if checkErr != nil {
			return durable.QueryRemovalCommitReceipt{}, checkErr
		}
		current := c.queryRemovalMaterials[queryGuardKey{r.Guard.Namespace, cur.RequestID}]
		permission, ok := c.queryRemovalMaterials[queryGuardKey{r.Guard.Namespace, r.Permission.RevalidationRequestID}]
		now := durable.Timestamp(time.Now())
		if !ok || permission.Validate() != nil || r.Guard.Match(g, initial) != nil || g.State != "active" || r.ExpectedCurrentDigest != cur.MaterialDigest || r.CurrentRequestID != cur.RequestID || permission.GuardID != g.GuardID || permission.MaterialDigest != r.Permission.RevalidationDigest || r.Permission.ExternalScope != initial.Request.ExternalScope || r.Permission.AcceptedAt.Before(permission.AcceptedAt) || !r.Permission.AcceptedAt.Before(permission.ValidUntil) || r.Permission.AcceptedAt.After(now) {
			return durable.QueryRemovalCommitReceipt{}, durable.ErrQueryRemovalGuard
		}
		if _, _, _, _, checkErr = c.guardFacts(current.Request, &now); checkErr != nil {
			return durable.QueryRemovalCommitReceipt{}, checkErr
		}
		result := durable.QueryRemovalCommitReceipt{Request: r, RequestDigest: digest, AcceptedAt: now}
		result.DeliveryID, checkErr = c.guardDelivery(ctx, r.Guard.Namespace, "commit", r.RequestID, now)
		if checkErr != nil {
			return result, checkErr
		}
		if guardCheckErr := result.Seal(); guardCheckErr != nil {
			return result, guardCheckErr
		}
		c.queryRemovalCommits[key] = result
		return result, nil
	})
}

// CloseQueryRemovalGuard releases pins only after authenticated complete cutoff.
func (m *Store) CloseQueryRemovalGuard(ctx context.Context, r durable.QueryRemovalGuardCloseRequest) (durable.QueryRemovalGuardCloseReceipt, error) {
	r.Closure.CutoffAt = durable.Timestamp(r.Closure.CutoffAt)
	r.Closure.TerminalAt = durable.Timestamp(r.Closure.TerminalAt)
	if err := r.Validate(); err != nil {
		return durable.QueryRemovalGuardCloseReceipt{}, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.close.v1", r)
	if err != nil {
		return durable.QueryRemovalGuardCloseReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Guard.Namespace, func(c *Store) (durable.QueryRemovalGuardCloseReceipt, error) {
		key := queryGuardKey{r.Guard.Namespace, r.RequestID}
		if saved, ok := c.queryRemovalCloses[key]; ok {
			return saved, saved.Match(durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: r.Guard.QueryRuntimeTarget, RequestID: r.RequestID, RequestDigest: digest})
		}
		if checkErr := c.checkLifecycleTarget(r.Guard.NamespaceTarget); checkErr != nil {
			return durable.QueryRemovalGuardCloseReceipt{}, checkErr
		}
		g, _, initial, checkErr := c.guardState(r.Guard.Namespace, r.Guard.GuardID)
		if checkErr != nil {
			return durable.QueryRemovalGuardCloseReceipt{}, checkErr
		}
		now := durable.Timestamp(time.Now())
		closure := r.Closure
		if r.Guard.Match(g, initial) != nil || g.State != "active" || closure.ExternalScope != initial.Request.ExternalScope || closure.CutoffAt.Before(initial.AcceptedAt) || closure.TerminalAt.Before(initial.AcceptedAt) || closure.CutoffAt.After(now) || closure.TerminalAt.After(now) {
			return durable.QueryRemovalGuardCloseReceipt{}, durable.ErrQueryRemovalGuard
		}
		begin := c.lifecycleReceipts[lifecycleReceiptKey{initial.Candidate.Namespace, durable.OperationBeginQueryRemoval, initial.Request.Original.BeginRequestID}]
		if originalErr := initial.Request.Original.MatchOriginal(begin); originalErr != nil {
			return durable.QueryRemovalGuardCloseReceipt{}, originalErr
		}
		result := durable.QueryRemovalGuardCloseReceipt{Request: r, RequestDigest: digest, AcceptedAt: now}
		result.DeliveryID, checkErr = c.guardDelivery(ctx, r.Guard.Namespace, "guard.close", r.RequestID, now)
		if checkErr != nil {
			return result, checkErr
		}
		if guardCheckErr := result.Seal(); guardCheckErr != nil {
			return result, guardCheckErr
		}
		g.State = "closed"
		g.CloseRequestID = r.RequestID
		g.CloseDigest = result.ReceiptDigest
		c.queryRemovalCloses[key] = result
		c.queryRemovalGuards[queryGuardKey{r.Guard.Namespace, g.GuardID}] = g
		return result, nil
	})
}

// LookupQueryRemovalGuardReceipt recovers one immutable command before host probes.
func (m *Store) LookupQueryRemovalGuardReceipt(ctx context.Context, q durable.QueryRemovalGuardReceiptLookup) (durable.QueryRemovalGuardReceipt, error) {
	if err := q.Validate(); err != nil {
		return durable.QueryRemovalGuardReceipt{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.QueryRemovalGuardReceipt{}, err
	}
	key := queryGuardKey{q.Namespace, q.RequestID}
	switch q.Kind {
	case "commit":
		r, ok := m.queryRemovalCommits[key]
		if !ok {
			return durable.QueryRemovalGuardReceipt{}, durable.ErrNotFound
		}
		return durable.QueryRemovalGuardReceipt{Commit: &r}, r.Match(q.QueryRemovalRevalidationLookup)
	case "close":
		r, ok := m.queryRemovalCloses[key]
		if !ok {
			return durable.QueryRemovalGuardReceipt{}, durable.ErrNotFound
		}
		return durable.QueryRemovalGuardReceipt{Close: &r}, r.Match(q.QueryRemovalRevalidationLookup)
	default:
		return durable.QueryRemovalGuardReceipt{}, durable.ErrInvalid
	}
}

// CheckQueryRemovalCommit reports saved commitment and current guard observations.
func (m *Store) CheckQueryRemovalCommit(ctx context.Context, q durable.QueryRemovalCommitLookup) (durable.QueryRemovalCommitStatus, error) {
	if err := q.Validate(); err != nil {
		return durable.QueryRemovalCommitStatus{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.QueryRemovalCommitStatus{}, err
	}
	r, ok := m.queryRemovalCommits[queryGuardKey{q.Namespace, q.RequestID}]
	if !ok {
		return durable.QueryRemovalCommitStatus{}, durable.ErrNotFound
	}
	if err := r.Match(q.QueryRemovalRevalidationLookup); err != nil {
		return durable.QueryRemovalCommitStatus{}, err
	}
	g, cur, _, err := m.guardState(q.Namespace, r.Request.Guard.GuardID)
	return durable.QueryRemovalCommitStatus{Receipt: r, Guard: g, Current: cur, ObservedAt: durable.Timestamp(time.Now())}, err
}

func (m *Store) guardQueryMutation(prior, next durable.QueryRuntimeBinding) error {
	for key, material := range m.queryRemovalMaterials {
		if key.namespace != next.Namespace || material.Candidate.RuntimeID != next.RuntimeID && material.Survivor.RuntimeID != next.RuntimeID {
			continue
		}
		g, ok := m.queryRemovalGuards[queryGuardKey{key.namespace, material.GuardID}]
		if !ok {
			return durable.ErrInvalid
		}
		if g.State == "active" {
			if material.Candidate.RuntimeID == next.RuntimeID && prior != next {
				return durable.ErrQueryRemovalGuard
			}
			if material.Survivor.RuntimeID == next.RuntimeID && (next.State != durable.QueryRuntimeActive || next.QueryRuntimeIdentity != material.Survivor) {
				return durable.ErrQueryRemovalGuard
			}
		} else if material.Candidate.RuntimeID == next.RuntimeID && material.RemovalEpoch == prior.RemovalEpoch && prior.State == durable.QueryRuntimeRemoving && (next.State == durable.QueryRuntimeRemoved || next.State == durable.QueryRuntimeActive) {
			disposition := "all_positive"
			if next.State == durable.QueryRuntimeActive {
				disposition = "settled_without_deletion"
			}
			closeReceipt, exists := m.queryRemovalCloses[queryGuardKey{key.namespace, g.CloseRequestID}]
			if !exists || closeReceipt.ReceiptDigest != g.CloseDigest || closeReceipt.Request.Closure.Disposition != disposition {
				return durable.ErrQueryRemovalGuard
			}
		}
	}
	return nil
}
func (m *Store) guardBuildMutation(prior, next durable.BuildAdmission) error {
	for key, material := range m.queryRemovalMaterials {
		if key.namespace != next.Namespace || material.Build.BuildID != next.BuildID {
			continue
		}
		g, ok := m.queryRemovalGuards[queryGuardKey{key.namespace, material.GuardID}]
		if !ok {
			return durable.ErrInvalid
		}
		if g.State == "active" && !durable.QueryRemovalCompatibleBuild(prior, next) {
			return durable.ErrQueryRemovalGuard
		}
	}
	return nil
}
