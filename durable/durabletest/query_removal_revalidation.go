package durabletest

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// QueryGuardFixture holds synthetic storage evidence, never production attestations.
type QueryGuardFixture struct {
	Request   durable.QueryRemovalRevalidationRequest
	Begin     durable.LifecycleReceipt
	Candidate durable.QueryRuntimeBinding
	Survivor  durable.QueryRuntimeBinding
}

// NewQueryGuardFixture enrolls real native rows for the guard conformance campaign.
func NewQueryGuardFixture(t *testing.T, s durable.Store) QueryGuardFixture {
	t.Helper()
	return newQueryGuardFixture(t, s, false, 30*time.Second)
}

// NewQueryGuardMaximumFixture populates all supported identity and evidence widths.
func NewQueryGuardMaximumFixture(t *testing.T, s durable.Store) QueryGuardFixture {
	t.Helper()
	return newQueryGuardFixture(t, s, true, 30*time.Second)
}

// NewQueryGuardExpiringFixture gives the original reservation a short real lifetime.
func NewQueryGuardExpiringFixture(t *testing.T, s durable.Store) QueryGuardFixture {
	t.Helper()
	return newQueryGuardFixture(t, s, false, 500*time.Millisecond)
}

func newQueryGuardFixture(t *testing.T, s durable.Store, maximum bool, lifetime time.Duration) QueryGuardFixture {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	q := queryStore(t, s)
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: fmt.Sprintf("guard-%d", time.Now().UnixNano())}, BuildID: "historic"}
	if maximum {
		target.InstallationID = guardWide(1, 256)
		target.Namespace = guardWide(2, 230) + fmt.Sprintf("%026d", time.Now().UnixNano())
		target.BuildID = guardWide(3, 512)
	}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: target.InstallationID, Namespace: target.Namespace, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	identity := QueryIdentityFixture()
	if maximum {
		identity.ConfigurationVersion = guardWide(4, 256)
		identity.ProbePolicyID = guardWide(5, 256)
		identity.VerifierID = guardWide(6, 256)
	}
	if _, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "build", Identity: &identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.BeginBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "retire", ExpectedVersion: 1, ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	candidateID, survivorID, candidateInstance, survivorInstance := "candidate", "survivor", "candidate-instance", "survivor-instance"
	if maximum {
		candidateID = guardWide(7, 256)
		survivorID = guardWide(8, 256)
		candidateInstance = guardWide(9, 256)
		survivorInstance = guardWide(10, 256)
	}
	register := func(id, instance, label string) durable.QueryRuntimeBinding {
		i := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: id}, InstanceID: instance, IdentityVersion: 1, BuildIdentity: identity}
		if _, err := q.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: i, RequestID: "register-" + label}); err != nil {
			t.Fatal(err)
		}
		proof := QueryProofFixture(i, "proof-"+label)
		proof.ValidUntil = proof.VerifiedAt.Add(lifetime)
		if maximum {
			proof.ProofID = guardWide(11, 256)
		}
		r, err := q.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: i.QueryRuntimeTarget, RequestID: "verify-" + label, ExpectedVersion: 1, Verification: proof})
		if err != nil {
			t.Fatal(err)
		}
		return *r.QueryRuntime
	}
	candidate := register(candidateID, candidateInstance, "candidate")
	survivor := register(survivorID, survivorInstance, "survivor")
	beginID := "begin"
	if maximum {
		beginID = guardWide(12, 256)
	}
	begin, err := q.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: candidate.QueryRuntimeTarget, RequestID: beginID, ExpectedVersion: candidate.Version})
	if err != nil {
		t.Fatal(err)
	}
	f := begin.QueryRuntime.Removal
	fd, err := durable.Fingerprint("query_runtime.removal_fence.v1", f)
	if err != nil {
		t.Fatal(err)
	}
	excluded := []string{candidate.InstanceID}
	if maximum {
		for i := 20; i < 83; i++ {
			excluded = append(excluded, guardWide(i, 256))
		}
		slices.Sort(excluded)
	}
	ed, err := durable.Fingerprint("dispatch.query-removal.exclusions.v1", excluded)
	if err != nil {
		t.Fatal(err)
	}
	now := durable.Timestamp(time.Now())
	until := now.Add(20 * time.Second)
	if survivor.Verification.ValidUntil.Before(until) {
		until = survivor.Verification.ValidUntil
	}
	request := durable.QueryRemovalRevalidationRequest{Original: durable.QueryRemovalOriginal{QueryRuntimeTarget: candidate.QueryRuntimeTarget, BeginRequestID: begin.RequestID, BeginRequestDigest: begin.RequestDigest, FenceDigest: fd, CandidateStateVersion: f.CandidateStateVersion, RemovalEpoch: f.RemovalEpoch}, ExternalScope: durable.QueryRemovalExternalScope{AuthorityID: "authority", OperationScopeID: "scope", AttemptID: "attempt", ProviderOperationID: "provider", CoverageDigest: strings.Repeat("1", 64), ExcludedInstancesDigest: ed, ProducerID: "producer", ProfileID: "profile", ProfileVersion: 1, VerifierID: "guard-verifier"}, RequestID: "proof", CommandDigest: strings.Repeat("2", 64), Survivor: survivor.QueryRuntimeTarget, SurvivorVersion: survivor.Version, Evidence: durable.QueryRemovalEvidence{Disposition: "live", CandidateProof: QueryProofFixture(candidate.QueryRuntimeIdentity, "candidate-proof"), SurvivorProof: survivor.Verification, Process: durable.QueryRemovalProcess{HostID: "host", ProcessID: "process", DescriptorID: "descriptor", DescriptorDigest: strings.Repeat("3", 64), RegistrationID: "registration", DrainRuntimeID: candidate.RuntimeID, DrainOperationID: "drain", DrainDeadline: now.Add(time.Minute), Role: "polling", Admission: "permanently_closed", Claims: "quiescent", DrainOutcome: "completed", ObservedAt: now, ValidUntil: until}, ExcludedInstances: excluded, ObservedAt: now, ValidUntil: until, CandidateAttestation: "candidate-attestation", SurvivorAttestation: "survivor-attestation", ProcessAttestation: "process-attestation", ScopeAttestation: "scope-attestation"}}
	if maximum {
		maximizeQueryGuard(&request)
	}
	return QueryGuardFixture{Request: request, Begin: begin, Candidate: *begin.QueryRuntime, Survivor: survivor}
}

// QueryGuardCommitFixture creates synthetic accepted-permission evidence.
func QueryGuardCommitFixture(f QueryGuardFixture, r durable.QueryRemovalRevalidationReceipt) durable.QueryRemovalCommitRequest {
	return durable.QueryRemovalCommitRequest{Guard: durable.QueryRemovalGuardReference{QueryRuntimeTarget: f.Request.Original.QueryRuntimeTarget, OriginalDigest: r.OriginalDigest, GuardID: r.GuardID, RootDigest: r.RootDigest}, RequestID: "commit", CommandDigest: strings.Repeat("4", 64), ExpectedCurrentDigest: r.MaterialDigest, CurrentRequestID: r.RequestID, Permission: durable.QueryRemovalPermission{ExternalScope: f.Request.ExternalScope, PermissionID: "permission", PermissionDigest: strings.Repeat("5", 64), CommandID: "invocation", StepDigest: strings.Repeat("6", 64), ResourceDigest: strings.Repeat("7", 64), IncarnationDigest: strings.Repeat("8", 64), RevalidationRequestID: r.RequestID, RevalidationDigest: r.MaterialDigest, AcceptedAt: r.AcceptedAt, EvidenceDigest: strings.Repeat("9", 64), Attestation: "permission-attestation"}}
}

// QueryGuardCloseFixture supplies affirmative permanent full-set closure.
func QueryGuardCloseFixture(f QueryGuardFixture, r durable.QueryRemovalRevalidationReceipt) durable.QueryRemovalGuardCloseRequest {
	return durable.QueryRemovalGuardCloseRequest{Guard: QueryGuardCommitFixture(f, r).Guard, RequestID: "close", CommandDigest: strings.Repeat("a", 64), Closure: durable.QueryRemovalClosure{ExternalScope: f.Request.ExternalScope, CutoffReceiptID: "cutoff", CutoffDigest: strings.Repeat("b", 64), CoverageDigest: f.Request.ExternalScope.CoverageDigest, Disposition: "all_positive", TerminalDigest: strings.Repeat("c", 64), CutoffAt: durable.Timestamp(time.Now()), TerminalAt: durable.Timestamp(time.Now()), EvidenceDigest: strings.Repeat("d", 64), Attestation: "closure-attestation"}}
}

// RunQueryRemovalGuard proves native protection, immutable replay and terminal closure.
func RunQueryRemovalGuard(t *testing.T, s durable.Store) {
	t.Helper()
	guard, ok := s.(durable.QueryRemovalRevalidationStore)
	if !ok {
		t.Fatal("guard capability missing")
	}
	q := queryStore(t, s)
	life, _ := lifecycleCapabilities(t, s)
	f := NewQueryGuardFixture(t, s)
	r, err := guard.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := guard.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil || again != r {
		t.Fatalf("replay: %v", err)
	}
	changed := f.Request.Clone()
	changed.Evidence.ScopeAttestation = "changed"
	if _, err = guard.RevalidateQueryRuntimeRemoval(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed replay accepted: %v", err)
	}
	if _, err = q.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "remove-survivor", ExpectedVersion: f.Survivor.Version}); err == nil {
		t.Fatal("selected survivor removal accepted")
	}
	if _, err = life.AbortBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: f.Candidate.BuildTarget, RequestID: "resume", ExpectedVersion: 2, ExpectedEpoch: 2}); err == nil {
		t.Fatal("protected build resumed")
	}
	if _, err = q.FinishQueryRuntimeRemoval(t.Context(), durable.FinishQueryRemovalRequest{Fence: f.Begin.QueryRuntime.Removal, RequestID: "finish-before-close"}); err == nil {
		t.Fatal("candidate finished before qualified close")
	}
	commit := QueryGuardCommitFixture(f, r)
	bound, err := guard.BindQueryRemovalCommit(t.Context(), commit)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := guard.BindQueryRemovalCommit(t.Context(), commit)
	if err != nil || !reflect.DeepEqual(bound, replay) {
		t.Fatalf("bind replay: %v", err)
	}
	// Refresh changes only current proof, leaving the original reservation byte-identical.
	refreshed := f.Request.Clone()
	refreshed.RequestID = "refresh"
	refreshed.ExpectedCurrentDigest = r.MaterialDigest
	refreshed.Evidence.ObservedAt = durable.Timestamp(time.Now())
	fresh, err := guard.RevalidateQueryRuntimeRemoval(t.Context(), refreshed)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.MaterialDigest == r.MaterialDigest {
		t.Fatal("refresh reused material")
	}
	page, err := q.ListQueryRuntimes(t.Context(), durable.QueryRuntimeList{NamespaceTarget: f.Candidate.NamespaceTarget, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range page.Items {
		if b.RuntimeID == f.Candidate.RuntimeID && !reflect.DeepEqual(b, f.Candidate) {
			t.Fatal("refresh changed original binding")
		}
	}
	original, err := life.LookupLifecycleReceipt(t.Context(), durable.LifecycleReceiptLookup{NamespaceTarget: f.Candidate.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: f.Begin.RequestID, RequestDigest: f.Begin.RequestDigest})
	if err != nil || !reflect.DeepEqual(original, f.Begin) {
		t.Fatal("refresh changed Begin receipt")
	}
	finalized, err := life.FinalizeBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: f.Candidate.BuildTarget, RequestID: "finalize", ExpectedVersion: 2, ExpectedEpoch: 2})
	if err != nil {
		t.Fatalf("compatible finalize refused: %v", err)
	}
	finalProof := refreshed.Clone()
	finalProof.RequestID = "finalized-proof"
	finalProof.ExpectedCurrentDigest = fresh.MaterialDigest
	finalProof.Evidence.ObservedAt = durable.Timestamp(time.Now())
	if _, err = guard.RevalidateQueryRuntimeRemoval(t.Context(), finalProof); err == nil {
		t.Fatal("missing native finalization receipt accepted")
	}
	finalProof.Evidence.BuildFinalizationRequestID = finalized.RequestID
	finalProof.Evidence.BuildFinalizationRequestDigest = finalized.RequestDigest
	if _, err = guard.RevalidateQueryRuntimeRemoval(t.Context(), finalProof); err != nil {
		t.Fatalf("exact native finalization refused: %v", err)
	}
	closeRequest := QueryGuardCloseFixture(f, r)
	closed, err := guard.CloseQueryRemovalGuard(t.Context(), closeRequest)
	if err != nil {
		t.Fatal(err)
	}
	closedAgain, err := guard.CloseQueryRemovalGuard(t.Context(), closeRequest)
	if err != nil || !reflect.DeepEqual(closed, closedAgain) {
		t.Fatalf("close replay: %v", err)
	}
	commit.RequestID = "after-close"
	if _, err = guard.BindQueryRemovalCommit(t.Context(), commit); err == nil {
		t.Fatal("closed guard bound")
	}
	refreshed.RequestID = "after-close"
	refreshed.ExpectedCurrentDigest = fresh.MaterialDigest
	refreshed.Evidence.ObservedAt = durable.Timestamp(time.Now())
	if _, err = guard.RevalidateQueryRuntimeRemoval(t.Context(), refreshed); err == nil {
		t.Fatal("closed guard refreshed")
	}
	if _, err = q.FinishQueryRuntimeRemoval(t.Context(), durable.FinishQueryRemovalRequest{Fence: f.Begin.QueryRuntime.Removal, RequestID: "finish"}); err != nil {
		t.Fatalf("qualified finish: %v", err)
	}
	material, err := guard.LookupQueryRemovalRevalidation(t.Context(), durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: f.Request.RequestID, CommandDigest: f.Request.CommandDigest})
	if err != nil {
		t.Fatal(err)
	}
	compact, err := material.Compact()
	if err != nil || compact != r {
		t.Fatal("historical material changed")
	}
	data, err := json.Marshal(compact)
	if err != nil || len(data) > 4096 {
		t.Fatal("compact receipt overflow")
	}
}

// guardWide makes distinct maximum-width legal delivery IDs using escaped ASCII.
func guardWide(n, width int) string {
	out := []byte(strings.Repeat("<", width))
	for i := 0; i < 16; i++ {
		if n&(1<<i) != 0 {
			out[i] = '&'
		}
	}
	return string(out)
}
func maximizeQueryGuard(r *durable.QueryRemovalRevalidationRequest) {
	r.RequestID = strings.Repeat("r", 256)
	r.ExternalScope.AuthorityID = strings.Repeat("a", 256)
	r.ExternalScope.OperationScopeID = strings.Repeat("s", 256)
	r.ExternalScope.AttemptID = strings.Repeat("t", 256)
	r.ExternalScope.ProviderOperationID = strings.Repeat("o", 256)
	r.ExternalScope.ProducerID = strings.Repeat("p", 256)
	r.ExternalScope.ProfileID = strings.Repeat("f", 256)
	r.ExternalScope.VerifierID = strings.Repeat("v", 256)
	e := &r.Evidence
	e.CandidateProof.ProofID = guardWide(84, 256)
	e.Process.HostID = guardWide(85, 256)
	e.Process.ProcessID = guardWide(86, 256)
	e.Process.DescriptorID = guardWide(87, 256)
	e.Process.RegistrationID = guardWide(88, 256)
	e.Process.DrainOperationID = guardWide(89, 256)
	e.Positive = durable.QueryRemovalPositiveEffect{PermissionID: strings.Repeat("p", 256), PermissionDigest: strings.Repeat("a", 64), ResultID: strings.Repeat("r", 256), ResultDigest: strings.Repeat("b", 64), CommandID: strings.Repeat("c", 256), ResourceID: strings.Repeat("s", 256), IncarnationID: strings.Repeat("i", 256), AssociationDigest: strings.Repeat("c", 64), PreEffectDigest: strings.Repeat("d", 64), PreEffectAt: e.ObservedAt, AcceptedAt: e.ObservedAt, CompletedAt: e.ObservedAt}
	for _, slot := range []*string{&e.CandidateAttestation, &e.SurvivorAttestation, &e.ProcessAttestation, &e.ScopeAttestation, &e.PermissionAttestation, &e.ResultAttestation, &e.PreEffectAttestation, &e.AssociationAttestation} {
		*slot = strings.Repeat("<", 4096)
	}
}

// RunQueryRemovalGuardMaximum persists and recovers the actual joint maximum material.
func RunQueryRemovalGuardMaximum(t *testing.T, s durable.Store) QueryGuardFixture {
	t.Helper()
	g, ok := s.(durable.QueryRemovalRevalidationStore)
	if !ok {
		t.Fatal("guard capability missing")
	}
	f := NewQueryGuardMaximumFixture(t, s)
	r, err := g.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	m, err := g.LookupQueryRemovalRevalidation(t.Context(), durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: f.Request.RequestID, CommandDigest: f.Request.CommandDigest})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Request, f.Request.Clone()) {
		t.Fatal("maximum material changed on recovery")
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("joint material=%d compact=%d excluded=%d", len(data), len(compact), len(m.Request.Evidence.ExcludedInstances))
	if len(data) > durable.QueryRemovalMaterialLimit || len(data) < 300000 || len(compact) > 4096 {
		t.Fatal("joint width contract changed")
	}
	for i := 0; i < 8; i++ {
		bad := f.Request.Clone()
		bad.RequestID = fmt.Sprintf("overflow-%d", i)
		bad.ExpectedCurrentDigest = r.MaterialDigest
		slots := []*string{&bad.Evidence.CandidateAttestation, &bad.Evidence.SurvivorAttestation, &bad.Evidence.ProcessAttestation, &bad.Evidence.ScopeAttestation, &bad.Evidence.PermissionAttestation, &bad.Evidence.ResultAttestation, &bad.Evidence.PreEffectAttestation, &bad.Evidence.AssociationAttestation}
		*slots[i] += "x"
		if _, err = g.RevalidateQueryRuntimeRemoval(t.Context(), bad); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("proof %d overflow: %v", i, err)
		}
	}
	return f
}

// RunQueryRemovalGuardExpiry exercises original expiry, first bind and saved replay.
func RunQueryRemovalGuardExpiry(t *testing.T, s durable.Store) {
	t.Helper()
	g, ok := s.(durable.QueryRemovalRevalidationStore)
	if !ok {
		t.Fatal("guard capability missing")
	}
	q := queryStore(t, s)
	f := NewQueryGuardExpiringFixture(t, s)
	r, err := g.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	commit := QueryGuardCommitFixture(f, r)
	bound, err := g.BindQueryRemovalCommit(t.Context(), commit)
	if err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(f.Begin.QueryRuntime.Removal.ValidUntil) + time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if _, err = q.CheckQueryRuntimeRemoval(t.Context(), f.Begin.QueryRuntime.Removal); err == nil {
		t.Fatal("original fence unexpectedly live")
	}
	if replay, e := g.RevalidateQueryRuntimeRemoval(t.Context(), f.Request); e != nil || replay != r {
		t.Fatalf("expired historical revalidation: %v", e)
	}
	if replay, e := g.BindQueryRemovalCommit(t.Context(), commit); e != nil || !reflect.DeepEqual(replay, bound) {
		t.Fatalf("expired saved bind: %v", e)
	}
	late := commit
	late.RequestID = "first-bind-after-expiry"
	if _, err = g.BindQueryRemovalCommit(t.Context(), late); err == nil {
		t.Fatal("expired first bind accepted")
	}
	if _, err = q.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "expired-pin", ExpectedVersion: f.Survivor.Version}); err == nil {
		t.Fatal("expiry released pin")
	}
	verified, err := q.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "fresh-survivor", ExpectedVersion: f.Survivor.Version, Verification: QueryProofFixture(f.Survivor.QueryRuntimeIdentity, "fresh-survivor")})
	if err != nil {
		t.Fatal(err)
	}
	fresh := f.Request.Clone()
	fresh.RequestID = "fresh"
	fresh.ExpectedCurrentDigest = r.MaterialDigest
	fresh.SurvivorVersion = verified.QueryRuntime.Version
	fresh.Evidence.SurvivorProof = verified.QueryRuntime.Verification
	fresh.Evidence.CandidateProof = QueryProofFixture(f.Candidate.QueryRuntimeIdentity, "fresh-candidate")
	fresh.Evidence.ObservedAt = durable.Timestamp(time.Now())
	fresh.Evidence.ValidUntil = fresh.Evidence.ObservedAt.Add(10 * time.Second)
	fresh.Evidence.Process.ObservedAt = fresh.Evidence.ObservedAt
	fresh.Evidence.Process.ValidUntil = fresh.Evidence.ValidUntil
	refreshed, err := g.RevalidateQueryRuntimeRemoval(t.Context(), fresh)
	if err != nil {
		t.Fatalf("original-expired revalidation: %v", err)
	}
	late.CurrentRequestID = refreshed.RequestID
	late.ExpectedCurrentDigest = refreshed.MaterialDigest
	if _, err = g.BindQueryRemovalCommit(t.Context(), late); err != nil {
		t.Fatalf("saved permission with fresh current proof: %v", err)
	}
	stale := fresh
	stale.RequestID = "stale-cas"
	if _, err = g.RevalidateQueryRuntimeRemoval(t.Context(), stale); err == nil {
		t.Fatal("stale current accepted")
	}
	stale.ExpectedCurrentDigest = refreshed.MaterialDigest
	if _, err = g.RevalidateQueryRuntimeRemoval(t.Context(), stale); err == nil {
		t.Fatal("nonadvancing source accepted")
	}
	other := registerQueryFixture(t, s, f.Candidate.BuildTarget, "third", "third-instance")
	fresh.RequestID = "other-survivor"
	fresh.ExpectedCurrentDigest = refreshed.MaterialDigest
	fresh.Survivor = other.QueryRuntimeTarget
	fresh.SurvivorVersion = other.Version
	fresh.Evidence.SurvivorProof = other.Verification
	fresh.Evidence.ObservedAt = durable.Timestamp(time.Now())
	if _, err = g.RevalidateQueryRuntimeRemoval(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	for _, survivor := range []durable.QueryRuntimeBinding{*verified.QueryRuntime, other} {
		if _, err = q.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: survivor.QueryRuntimeTarget, RequestID: "pinned-" + survivor.RuntimeID, ExpectedVersion: survivor.Version}); err == nil {
			t.Fatal("previous or current survivor lost pin")
		}
	}
	closeRequest := QueryGuardCloseFixture(f, r)
	closed, err := g.CloseQueryRemovalGuard(t.Context(), closeRequest)
	if err != nil {
		t.Fatal(err)
	}
	if replay, e := g.CloseQueryRemovalGuard(t.Context(), closeRequest); e != nil || !reflect.DeepEqual(replay, closed) {
		t.Fatalf("lost close response: %v", e)
	}
}

// RunQueryRemovalGuardRaces checks namespace coordination at competing boundaries.
func RunQueryRemovalGuardRaces(t *testing.T, s durable.Store) {
	t.Helper()
	g, ok := s.(durable.QueryRemovalRevalidationStore)
	if !ok {
		t.Fatal("guard capability missing")
	}
	q := queryStore(t, s)
	life, _ := lifecycleCapabilities(t, s)
	for _, kind := range []string{"survivor", "resume", "bind-close"} {
		t.Run(kind, func(t *testing.T) {
			f := NewQueryGuardFixture(t, s)
			if kind == "survivor" {
				registerQueryFixture(t, s, f.Candidate.BuildTarget, "third-racer", "third-racer-instance")
			}
			start := make(chan struct{})
			results := make(chan error, 2)
			if kind == "bind-close" {
				r, err := g.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
				if err != nil {
					t.Fatal(err)
				}
				commit := QueryGuardCommitFixture(f, r)
				closeRequest := QueryGuardCloseFixture(f, r)
				go func() { <-start; _, e := g.BindQueryRemovalCommit(t.Context(), commit); results <- e }()
				go func() { <-start; _, e := g.CloseQueryRemovalGuard(t.Context(), closeRequest); results <- e }()
				close(start)
				a, b := <-results, <-results
				if a != nil && b != nil {
					t.Fatalf("both bind/close refused: %v %v", a, b)
				}
				commit.RequestID = "after-terminal"
				if _, err = g.BindQueryRemovalCommit(t.Context(), commit); err == nil {
					t.Fatal("bind accepted after close")
				}
				return
			}
			go func() { <-start; _, e := g.RevalidateQueryRuntimeRemoval(t.Context(), f.Request); results <- e }()
			go func() {
				<-start
				var e error
				if kind == "survivor" {
					_, e = q.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "competing-removal", ExpectedVersion: f.Survivor.Version})
				} else {
					_, e = life.AbortBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: f.Candidate.BuildTarget, RequestID: "competing-resume", ExpectedVersion: 2, ExpectedEpoch: 2})
				}
				results <- e
			}()
			close(start)
			a, b := <-results, <-results
			if a == nil && b == nil {
				t.Fatal("guard and invalidator both accepted")
			}
			if a != nil && b != nil {
				t.Fatalf("neither coordinated operation accepted: %v %v", a, b)
			}
		})
	}
}

// RunQueryRemovalGuardPositiveEffect requires positive retained evidence after process expiry.
func RunQueryRemovalGuardPositiveEffect(t *testing.T, s durable.Store) {
	t.Helper()
	g, ok := s.(durable.QueryRemovalRevalidationStore)
	if !ok {
		t.Fatal("guard capability missing")
	}
	q := queryStore(t, s)
	f := NewQueryGuardExpiringFixture(t, s)
	r, err := g.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	before := durable.Timestamp(time.Now())
	timer := time.NewTimer(time.Until(f.Request.Evidence.Process.ValidUntil) + time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	verified, err := q.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "survivor-after-effect", ExpectedVersion: f.Survivor.Version, Verification: QueryProofFixture(f.Survivor.QueryRuntimeIdentity, "survivor-after-effect")})
	if err != nil {
		t.Fatal(err)
	}
	fresh := f.Request.Clone()
	fresh.RequestID = "positive-effect"
	fresh.ExpectedCurrentDigest = r.MaterialDigest
	fresh.SurvivorVersion = verified.QueryRuntime.Version
	fresh.Evidence.SurvivorProof = verified.QueryRuntime.Verification
	fresh.Evidence.ObservedAt = durable.Timestamp(time.Now())
	fresh.Evidence.ValidUntil = fresh.Evidence.ObservedAt.Add(10 * time.Second)
	if _, err = g.RevalidateQueryRuntimeRemoval(t.Context(), fresh); err == nil {
		t.Fatal("expired process proof accepted as live")
	}
	fresh.Evidence.Disposition = "effect_complete"
	fresh.Evidence.Positive = durable.QueryRemovalPositiveEffect{PermissionID: "permission", PermissionDigest: strings.Repeat("a", 64), ResultID: "positive-result", ResultDigest: strings.Repeat("b", 64), CommandID: "invocation", ResourceID: "resource", IncarnationID: "original-incarnation", AssociationDigest: strings.Repeat("c", 64), PreEffectDigest: strings.Repeat("d", 64), PreEffectAt: before, AcceptedAt: before, CompletedAt: before}
	if _, err = g.RevalidateQueryRuntimeRemoval(t.Context(), fresh); err == nil {
		t.Fatal("missing positive attestations accepted")
	}
	fresh.Evidence.PermissionAttestation = "accepted-permission"
	fresh.Evidence.ResultAttestation = "positive-result"
	fresh.Evidence.PreEffectAttestation = "quiescent-before-effect"
	fresh.Evidence.AssociationAttestation = "original-resource-incarnation"
	if _, err = g.RevalidateQueryRuntimeRemoval(t.Context(), fresh); err != nil {
		t.Fatalf("positive historical resource proof: %v", err)
	}
}

// RunQueryRemovalGuardRowBounds qualifies the other records with joint maximum identities.
func RunQueryRemovalGuardRowBounds(t *testing.T, s durable.Store) {
	t.Helper()
	g, ok := s.(durable.QueryRemovalRevalidationStore)
	if !ok {
		t.Fatal("guard capability missing")
	}
	f := NewQueryGuardMaximumFixture(t, s)
	f.Request.ExternalScope.ProfileVersion = 9223372036854775807
	r, err := g.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	commit := QueryGuardCommitFixture(f, r)
	commit.RequestID = strings.Repeat("c", 256)
	commit.Permission.PermissionID = strings.Repeat("p", 256)
	commit.Permission.CommandID = strings.Repeat("i", 256)
	commit.Permission.Attestation = strings.Repeat("x", 4094)
	dataBefore, marshalErr := json.Marshal(commit)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	t.Logf("joint commit request=%d", len(dataBefore))
	fresh := f.Request.Clone()
	fresh.RequestID = strings.Repeat("n", 256)
	fresh.ExpectedCurrentDigest = r.MaterialDigest
	fresh.Evidence.ObservedAt = durable.Timestamp(time.Now())
	current, err := g.RevalidateQueryRuntimeRemoval(t.Context(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	commit.CurrentRequestID = current.RequestID
	commit.ExpectedCurrentDigest = current.MaterialDigest
	accepted, err := g.BindQueryRemovalCommit(t.Context(), commit)
	if err != nil {
		t.Fatal(err)
	}
	status, err := g.CheckQueryRemovalCommit(t.Context(), durable.QueryRemovalCommitLookup{QueryRemovalRevalidationLookup: durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: commit.RequestID, CommandDigest: commit.CommandDigest}})
	if err != nil {
		t.Fatal(err)
	}
	closeRequest := QueryGuardCloseFixture(f, r)
	closeRequest.RequestID = strings.Repeat("z", 256)
	closeRequest.Closure.CutoffReceiptID = strings.Repeat("k", 256)
	closeRequest.Closure.Attestation = strings.Repeat("x", 4094)
	dataBefore, marshalErr = json.Marshal(closeRequest)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	t.Logf("joint close request=%d", len(dataBefore))
	closed, err := g.CloseQueryRemovalGuard(t.Context(), closeRequest)
	if err != nil {
		t.Fatal(err)
	}
	if replay, e := g.BindQueryRemovalCommit(t.Context(), commit); e != nil || !reflect.DeepEqual(replay, accepted) {
		t.Fatalf("maximum saved bind recovery: %v", e)
	}
	if replay, e := g.CloseQueryRemovalGuard(t.Context(), closeRequest); e != nil || !reflect.DeepEqual(replay, closed) {
		t.Fatalf("maximum close recovery: %v", e)
	}
	lookup := durable.QueryRemovalGuardReceiptLookup{Kind: "close", QueryRemovalRevalidationLookup: durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: closeRequest.RequestID, CommandDigest: closeRequest.CommandDigest}}
	if found, e := g.LookupQueryRemovalGuardReceipt(t.Context(), lookup); e != nil || found.Close == nil || !reflect.DeepEqual(*found.Close, closed) {
		t.Fatalf("maximum exact close lookup: %v", e)
	}
	for _, row := range []struct {
		name  string
		value any
		limit int
	}{{"guard", status.Guard, 8192}, {"current", status.Current, 1024}, {"commit", accepted, durable.QueryRemovalCommitReceiptLimit}, {"close", closed, 16384}} {
		data, e := json.Marshal(row.value)
		if e != nil || len(data) > row.limit {
			t.Fatalf("%s joint width: %d %v", row.name, len(data), e)
		}
		t.Logf("joint %s=%d cap=%d", row.name, len(data), row.limit)
	}
}
