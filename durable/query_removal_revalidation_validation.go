package durable

import (
	"context"
	"slices"
	"time"
)

// QueryRemovalRoot creates a compact root over the complete immutable association.
func QueryRemovalRoot(r QueryRemovalRevalidationRequest, candidate QueryRuntimeIdentity) (QueryRemovalGuard, error) {
	original, err := Fingerprint("dispatch.query-removal.original.v1", r.Original)
	if err != nil {
		return QueryRemovalGuard{}, err
	}
	scope, err := Fingerprint("dispatch.query-removal.scope.v1", r.ExternalScope)
	if err != nil {
		return QueryRemovalGuard{}, err
	}
	identity, err := Fingerprint("dispatch.query-removal.candidate.v1", candidate)
	if err != nil {
		return QueryRemovalGuard{}, err
	}
	process := r.Evidence.Process
	process.ObservedAt = time.Time{}
	process.ValidUntil = time.Time{}
	processDigest, err := Fingerprint("dispatch.query-removal.process.v1", process)
	if err != nil {
		return QueryRemovalGuard{}, err
	}
	id, err := Fingerprint("dispatch.query-removal.guard-key.v1", struct{ Original, Scope string }{original, scope})
	if err != nil {
		return QueryRemovalGuard{}, err
	}
	g := QueryRemovalGuard{GuardID: id, OriginalDigest: original, ExternalScopeDigest: scope, CandidateDigest: identity, ProcessDigest: processDigest, State: "active", InitialRequestID: r.RequestID}
	root, err := Fingerprint("dispatch.query-removal.guard.v1", struct{ Original, Scope, Candidate, Process string }{original, scope, identity, processDigest})
	g.RootDigest = root
	return g, err
}

// MatchOriginal resolves the exact historical Begin before checking current state.
func (r QueryRemovalOriginal) MatchOriginal(receipt LifecycleReceipt) error {
	q := LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: OperationBeginQueryRemoval, RequestID: r.BeginRequestID, RequestDigest: r.BeginRequestDigest}
	if err := receipt.Match(q); err != nil {
		return err
	}
	if receipt.QueryRuntime == nil {
		return ErrInvalid
	}
	f := receipt.QueryRuntime.Removal
	digest, err := Fingerprint("query_runtime.removal_fence.v1", f)
	if err != nil || digest != r.FenceDigest || f.Candidate.QueryRuntimeTarget != r.QueryRuntimeTarget || f.CandidateStateVersion != r.CandidateStateVersion || f.RemovalEpoch != r.RemovalEpoch {
		return ErrQueryRemovalGuard
	}
	return nil
}

// QueryRemovalCompatibleBuild permits only unchanged catalogs or final retirement.
func QueryRemovalCompatibleBuild(prior, next BuildAdmission) bool {
	if prior == next {
		return true
	}
	return prior.BuildTarget == next.BuildTarget && prior.QueryIdentity == next.QueryIdentity && prior.Epoch == next.Epoch && prior.CutoffEpoch == next.CutoffEpoch && prior.State == BuildRetiring && next.State == BuildRetired && next.Version == prior.Version+1
}

// ValidateQueryRemovalFacts checks exact native rows at the post-lock clock.
func ValidateQueryRemovalFacts(r QueryRemovalRevalidationRequest, begin LifecycleReceipt, candidate QueryRuntimeBinding, build BuildAdmission, survivor QueryRuntimeBinding, retained int64, now time.Time) error {
	if r.Validate() != nil || r.Original.MatchOriginal(begin) != nil {
		return ErrInvalid
	}
	f := begin.QueryRuntime.Removal
	if candidate.QueryRuntimeIdentity != f.Candidate || candidate.State != QueryRuntimeRemoving || candidate.Version != f.CandidateStateVersion || candidate.RemovalEpoch != f.RemovalEpoch || candidate.Removal != f {
		return ErrQueryRemovalGuard
	}
	if build.BuildTarget != candidate.BuildTarget || build.QueryIdentity != candidate.BuildIdentity || build.Epoch != f.BuildEpoch || ((build.Version != f.BuildVersion || build.State != f.BuildState) && (f.BuildState != BuildRetiring || build.State != BuildRetired || build.Version != f.BuildVersion+1)) {
		return ErrQueryRemovalGuard
	}
	if (build.State == BuildRetiring || build.State == BuildRetired) && (build.Epoch < 2 || build.CutoffEpoch != build.Epoch-1) {
		return ErrQueryRemovalGuard
	}
	e := r.Evidence
	p := e.Process
	if e.ObservedAt.IsZero() || e.ObservedAt.After(now) || !e.ValidUntil.After(now) || e.ValidUntil.Sub(e.ObservedAt) > time.Minute || e.ValidUntil.Sub(e.ObservedAt) > build.QueryIdentity.MaximumProofValidity {
		return ErrQueryRemovalGuard
	}
	if !slices.Contains(e.ExcludedInstances, candidate.InstanceID) {
		return ErrQueryRemovalGuard
	}
	for _, id := range []string{p.HostID, p.ProcessID, p.DescriptorID, p.RegistrationID, p.DrainRuntimeID, p.DrainOperationID} {
		if !DeliveryIdentifier(id) {
			return ErrInvalid
		}
	}
	if !validHex256(p.DescriptorDigest) || p.DrainRuntimeID != candidate.RuntimeID || p.DrainDeadline.IsZero() || p.Admission != "permanently_closed" || p.Claims != "quiescent" || p.DrainOutcome != "completed" || (p.Role != "polling" && p.Role != "retained_query") {
		return ErrQueryRemovalGuard
	}
	candidateAt := now
	if e.Disposition == "effect_complete" {
		x := e.Positive
		for _, id := range []string{x.PermissionID, x.ResultID, x.CommandID, x.ResourceID, x.IncarnationID} {
			if !QueryRemovalToken(id) {
				return ErrInvalid
			}
		}
		for _, digest := range []string{x.PermissionDigest, x.ResultDigest, x.AssociationDigest, x.PreEffectDigest} {
			if !validHex256(digest) {
				return ErrInvalid
			}
		}
		if x.PreEffectAt.IsZero() || x.PreEffectAt.Before(f.AcceptedAt) || x.AcceptedAt.Before(x.PreEffectAt) || x.CompletedAt.Before(x.AcceptedAt) || x.CompletedAt.After(now) || e.PermissionAttestation == "" || e.ResultAttestation == "" || e.PreEffectAttestation == "" || e.AssociationAttestation == "" {
			return ErrQueryRemovalGuard
		}
		candidateAt = x.PreEffectAt
	}
	if p.ObservedAt.IsZero() || p.ObservedAt.After(candidateAt) || !p.ValidUntil.After(candidateAt) || p.ValidUntil.Sub(p.ObservedAt) > time.Minute || p.ValidUntil.Sub(p.ObservedAt) > build.QueryIdentity.MaximumProofValidity || e.CandidateProof.Validate(candidate.QueryRuntimeIdentity, candidateAt) != nil {
		return ErrQueryRemovalGuard
	}
	if e.CandidateProof.AcceptedAt.After(candidateAt) || e.SurvivorProof.AcceptedAt.After(now) {
		return ErrQueryRemovalGuard
	}
	if e.Disposition == "live" && (e.ValidUntil.After(p.ValidUntil) || e.ValidUntil.After(e.CandidateProof.ValidUntil)) {
		return ErrQueryRemovalGuard
	}
	if r.Survivor.RuntimeID == "" {
		if retained != 0 || build.State != BuildRetired || r.SurvivorVersion != 0 {
			return ErrQueryRetention
		}
		return nil
	}
	if survivor.QueryRuntimeTarget != r.Survivor || survivor.BuildTarget != build.BuildTarget || survivor.Version != r.SurvivorVersion || survivor.State != QueryRuntimeActive || survivor.RuntimeID == candidate.RuntimeID || slices.Contains(e.ExcludedInstances, survivor.InstanceID) || survivor.BuildIdentity != build.QueryIdentity || e.SurvivorAttestation == "" || !queryRemovalProofEqual(e.SurvivorProof, survivor.Verification) || e.SurvivorProof.Validate(survivor.QueryRuntimeIdentity, now) != nil || e.ValidUntil.After(e.SurvivorProof.ValidUntil) {
		return ErrQueryRetention
	}
	return nil
}

// NewQueryRemovalMaterial builds the accepted facts after native validation.
func NewQueryRemovalMaterial(r QueryRemovalRevalidationRequest, g QueryRemovalGuard, candidate QueryRuntimeBinding, build BuildAdmission, survivor QueryRuntimeBinding, retained int64, now time.Time) (QueryRemovalRevalidationMaterial, error) {
	digest, err := Fingerprint("dispatch.query-removal.revalidation.v1", r)
	if err != nil {
		return QueryRemovalRevalidationMaterial{}, err
	}
	return QueryRemovalRevalidationMaterial{Request: r.Clone(), RequestDigest: digest, GuardID: g.GuardID, RootDigest: g.RootDigest, Candidate: candidate.QueryRuntimeIdentity, CandidateState: candidate.State, CandidateVersion: candidate.Version, RemovalEpoch: candidate.RemovalEpoch, Build: build, RetainedExecutions: retained, Survivor: survivor.QueryRuntimeIdentity, SurvivorVersion: survivor.Version, AcceptedAt: now, ValidUntil: r.Evidence.ValidUntil}, nil
}

// Seal computes the material commitment after transactional delivery identity is set.
func (m *QueryRemovalRevalidationMaterial) Seal() error {
	m.MaterialDigest = ""
	digest, err := Fingerprint("dispatch.query-removal.material.v1", m)
	if err != nil {
		return err
	}
	m.MaterialDigest = digest
	return QueryRemovalSize(m, QueryRemovalMaterialLimit)
}

// Validate verifies retained material before it participates in authority or replay.
func (m QueryRemovalRevalidationMaterial) Validate() error {
	saved := m.MaterialDigest
	if !validHex256(saved) || !validHex256(m.GuardID) || !validHex256(m.RootDigest) || m.Request.Validate() != nil || m.Candidate.Validate() != nil || !DeliveryIdentifier(m.DeliveryID) || m.AcceptedAt.IsZero() || !m.ValidUntil.After(m.AcceptedAt) {
		return ErrInvalid
	}
	digest, err := Fingerprint("dispatch.query-removal.revalidation.v1", m.Request)
	if err != nil || digest != m.RequestDigest {
		return ErrInvalid
	}
	if m.Seal() != nil || m.MaterialDigest != saved {
		return ErrInvalid
	}
	return nil
}

// Match checks immutable command or request recovery without refreshing its interval.
func (m QueryRemovalRevalidationMaterial) Match(q QueryRemovalRevalidationLookup) error {
	if q.Validate() != nil || m.Validate() != nil {
		return ErrInvalid
	}
	if m.Candidate.QueryRuntimeTarget != q.QueryRuntimeTarget || m.Request.RequestID != q.RequestID || q.CommandDigest != "" && q.CommandDigest != m.Request.CommandDigest || q.RequestDigest != "" && q.RequestDigest != m.RequestDigest {
		return ErrRequestConflict
	}
	return nil
}

// Clone copies the authenticated excluded set for callers and staging maps.
func (m QueryRemovalRevalidationMaterial) Clone() QueryRemovalRevalidationMaterial {
	m.Request = m.Request.Clone()
	return m
}

// Compact exports digests only; retained material remains the source of full facts.
func (m QueryRemovalRevalidationMaterial) Compact() (QueryRemovalRevalidationReceipt, error) {
	if m.Validate() != nil {
		return QueryRemovalRevalidationReceipt{}, ErrInvalid
	}
	g, err := QueryRemovalRoot(m.Request, m.Candidate)
	if err != nil {
		return QueryRemovalRevalidationReceipt{}, err
	}
	b, err := Fingerprint("dispatch.query-removal.build.v1", m.Build)
	if err != nil {
		return QueryRemovalRevalidationReceipt{}, err
	}
	s, err := Fingerprint("dispatch.query-removal.survivor.v1", struct {
		Identity QueryRuntimeIdentity
		Version  int64
		Proof    QueryRuntimeVerification
	}{m.Survivor, m.SurvivorVersion, m.Request.Evidence.SurvivorProof})
	if err != nil {
		return QueryRemovalRevalidationReceipt{}, err
	}
	out := QueryRemovalRevalidationReceipt{Schema: "dispatch.query-removal.revalidation.v1", GuardID: m.GuardID, RootDigest: m.RootDigest, RequestID: m.Request.RequestID, RequestDigest: m.RequestDigest, MaterialDigest: m.MaterialDigest, OriginalDigest: g.OriginalDigest, ExternalScopeDigest: g.ExternalScopeDigest, CandidateDigest: g.CandidateDigest, BuildDigest: b, SurvivorDigest: s, ProcessDigest: g.ProcessDigest, ExclusionDigest: m.Request.ExternalScope.ExcludedInstancesDigest, CoverageDigest: m.Request.ExternalScope.CoverageDigest, Disposition: m.Request.Evidence.Disposition, AcceptedAt: m.AcceptedAt, ValidUntil: m.ValidUntil}
	return out, QueryRemovalSize(out, 4096)
}

// QueryRemovalDeliverySource binds one native command to its transactional audit intent.
func QueryRemovalDeliverySource(ctx context.Context, namespace, kind, id string, at time.Time) DeliverySource {
	source := ReceiptSourceID("dispatch.query-removal.v1", namespace, kind, id)
	return DeliverySource{Key: Key{Namespace: namespace}, Kind: "security", ID: source, OccurredAt: at, Action: "dispatch.query_runtime." + kind, Outcome: "accepted", Target: source, Metadata: AuditMetadataFromContext(ctx)}
}

// Validate checks a compact exact original guard reference.
func (r QueryRemovalGuardReference) Validate() error {
	if r.QueryRuntimeTarget.Validate() != nil || !validHex256(r.OriginalDigest) || !validHex256(r.GuardID) || !validHex256(r.RootDigest) {
		return ErrInvalid
	}
	return nil
}

// Match checks a guard reference against authenticated initial material.
func (r QueryRemovalGuardReference) Match(g QueryRemovalGuard, m QueryRemovalRevalidationMaterial) error {
	if r.Validate() != nil || m.Validate() != nil || r.GuardID != g.GuardID || r.RootDigest != g.RootDigest || r.OriginalDigest != g.OriginalDigest || r.QueryRuntimeTarget != m.Candidate.QueryRuntimeTarget || m.GuardID != g.GuardID || m.RootDigest != g.RootDigest {
		return ErrQueryRemovalGuard
	}
	return nil
}

// Validate checks a first bind's bounded typed external facts.
func (r QueryRemovalCommitRequest) Validate() error {
	p := r.Permission
	if r.Guard.Validate() != nil || !QueryRemovalToken(r.RequestID) || !validHex256(r.CommandDigest) || !validHex256(r.ExpectedCurrentDigest) || !QueryRemovalToken(r.CurrentRequestID) || p.ExternalScope.Validate() != nil {
		return ErrInvalid
	}
	for _, id := range []string{p.PermissionID, p.CommandID, p.RevalidationRequestID} {
		if !QueryRemovalToken(id) {
			return ErrInvalid
		}
	}
	for _, d := range []string{p.PermissionDigest, p.StepDigest, p.ResourceDigest, p.IncarnationDigest, p.RevalidationDigest, p.EvidenceDigest} {
		if !validHex256(d) {
			return ErrInvalid
		}
	}
	if p.AcceptedAt.IsZero() || p.Attestation == "" || QueryRemovalSize(p.Attestation, 4096) != nil {
		return ErrInvalid
	}
	return QueryRemovalSize(r, 16384)
}

// Validate checks affirmative permanent closure rather than timeout or absence.
func (r QueryRemovalGuardCloseRequest) Validate() error {
	c := r.Closure
	if r.Guard.Validate() != nil || !QueryRemovalToken(r.RequestID) || !validHex256(r.CommandDigest) || c.ExternalScope.Validate() != nil || !QueryRemovalToken(c.CutoffReceiptID) || !validHex256(c.CutoffDigest) || !validHex256(c.CoverageDigest) || !validHex256(c.TerminalDigest) || !validHex256(c.EvidenceDigest) || c.CoverageDigest != c.ExternalScope.CoverageDigest || c.CutoffAt.IsZero() || c.TerminalAt.IsZero() || (c.Disposition != "all_positive" && c.Disposition != "settled_without_deletion") || c.Attestation == "" || QueryRemovalSize(c.Attestation, 4096) != nil {
		return ErrInvalid
	}
	return QueryRemovalSize(r, 16384)
}

// Seal authenticates a saved commit's entire typed request and response.
func (r *QueryRemovalCommitReceipt) Seal() error {
	r.ReceiptDigest = ""
	d, err := Fingerprint("dispatch.query-removal.commit-receipt.v1", r)
	if err != nil {
		return err
	}
	r.ReceiptDigest = d
	return QueryRemovalSize(r, QueryRemovalCommitReceiptLimit)
}

// Seal authenticates a saved close's entire typed request and response.
func (r *QueryRemovalGuardCloseReceipt) Seal() error {
	r.ReceiptDigest = ""
	d, err := Fingerprint("dispatch.query-removal.close-receipt.v1", r)
	if err != nil {
		return err
	}
	r.ReceiptDigest = d
	return QueryRemovalSize(r, 16384)
}

// Match validates historical commit recovery before external verification.
func (r QueryRemovalCommitReceipt) Match(q QueryRemovalRevalidationLookup) error {
	d := r.ReceiptDigest
	request, err := Fingerprint("dispatch.query-removal.commit.v1", r.Request)
	if q.Validate() != nil || r.Request.Validate() != nil || err != nil || request != r.RequestDigest || r.Seal() != nil || r.ReceiptDigest != d || !DeliveryIdentifier(r.DeliveryID) || r.AcceptedAt.IsZero() {
		return ErrInvalid
	}
	if q.QueryRuntimeTarget != r.Request.Guard.QueryRuntimeTarget || q.RequestID != r.Request.RequestID || q.CommandDigest != "" && q.CommandDigest != r.Request.CommandDigest || q.RequestDigest != "" && q.RequestDigest != r.RequestDigest {
		return ErrRequestConflict
	}
	return nil
}

// Match validates historical close recovery without reissuing a cutoff.
func (r QueryRemovalGuardCloseReceipt) Match(q QueryRemovalRevalidationLookup) error {
	d := r.ReceiptDigest
	request, err := Fingerprint("dispatch.query-removal.close.v1", r.Request)
	if q.Validate() != nil || r.Request.Validate() != nil || err != nil || request != r.RequestDigest || r.Seal() != nil || r.ReceiptDigest != d || !DeliveryIdentifier(r.DeliveryID) || r.AcceptedAt.IsZero() {
		return ErrInvalid
	}
	if q.QueryRuntimeTarget != r.Request.Guard.QueryRuntimeTarget || q.RequestID != r.Request.RequestID || q.CommandDigest != "" && q.CommandDigest != r.Request.CommandDigest || q.RequestDigest != "" && q.RequestDigest != r.RequestDigest {
		return ErrRequestConflict
	}
	return nil
}

func queryRemovalProofEqual(a, b QueryRuntimeVerification) bool {
	for _, p := range []*QueryRuntimeVerification{&a, &b} {
		p.VerifiedAt = Timestamp(p.VerifiedAt)
		p.ValidUntil = Timestamp(p.ValidUntil)
		p.AcceptedAt = Timestamp(p.AcceptedAt)
	}
	return a == b
}

// QueryRemovalFinalizationLookup names the native receipt for a monotone finalization.
func QueryRemovalFinalizationLookup(r QueryRemovalRevalidationRequest, begin LifecycleReceipt, build BuildAdmission) (LifecycleReceiptLookup, bool, error) {
	if begin.QueryRuntime == nil {
		return LifecycleReceiptLookup{}, false, ErrInvalid
	}
	if begin.QueryRuntime.Removal.BuildState != BuildRetiring || build.State != BuildRetired {
		return LifecycleReceiptLookup{}, false, nil
	}
	q := LifecycleReceiptLookup{NamespaceTarget: r.Original.NamespaceTarget, Operation: OperationFinalizeRetirement, RequestID: r.Evidence.BuildFinalizationRequestID, RequestDigest: r.Evidence.BuildFinalizationRequestDigest}
	if q.Validate() != nil {
		return q, true, ErrQueryRemovalGuard
	}
	return q, true, nil
}

// ValidateQueryRemovalFinalization checks the exact native finalization association.
func ValidateQueryRemovalFinalization(q LifecycleReceiptLookup, receipt LifecycleReceipt, build BuildAdmission) error {
	if receipt.Match(q) != nil || receipt.Build == nil {
		return ErrQueryRemovalGuard
	}
	accepted := *receipt.Build
	accepted.ChangedAt = Timestamp(accepted.ChangedAt)
	build.ChangedAt = Timestamp(build.ChangedAt)
	if accepted != build {
		return ErrQueryRemovalGuard
	}
	return nil
}
