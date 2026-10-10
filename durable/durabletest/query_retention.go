package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// QueryIdentityFixture is synthetic store conformance data, not artifact proof.
func QueryIdentityFixture() durable.BuildQueryIdentity {
	return durable.BuildQueryIdentity{ArtifactDigest: strings.Repeat("a", 64), ConfigurationDigest: strings.Repeat("b", 64), ConfigurationVersion: "fixture-v1", EnrollmentEvidenceDigest: strings.Repeat("c", 64), ProbePolicyID: "conformance", ProbePolicyVersion: 1, VerifierID: "fixture", MaximumProofValidity: time.Minute}
}
func QueryProofFixture(identity durable.QueryRuntimeIdentity, id string) durable.QueryRuntimeVerification {
	now := durable.Timestamp(time.Now())
	return durable.QueryRuntimeVerification{Identity: identity, ProofID: id, VerifierID: identity.BuildIdentity.VerifierID, EvidenceDigest: strings.Repeat("d", 64), ProbePolicyID: identity.BuildIdentity.ProbePolicyID, ProbePolicyVersion: identity.BuildIdentity.ProbePolicyVersion, VerifiedAt: now, ValidUntil: now.Add(30 * time.Second)}
}
func queryStore(t *testing.T, s durable.Store) durable.QueryRuntimeStore {
	t.Helper()
	q, ok := s.(durable.QueryRuntimeStore)
	if !ok {
		t.Fatal("query retention capability missing")
	}
	return q
}
func registerQueryFixture(t *testing.T, s durable.Store, target durable.BuildTarget, id, instance string) durable.QueryRuntimeBinding {
	t.Helper()
	q := queryStore(t, s)
	identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: id}, InstanceID: instance, IdentityVersion: 1, BuildIdentity: QueryIdentityFixture()}
	accepted, err := q.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: identity, RequestID: "register-" + id})
	if err != nil {
		t.Fatal(err)
	}
	if id == "a" {
		for _, kind := range []string{"artifact", "verifier", "policy", "future", "expired", "too-long"} {
			proof := QueryProofFixture(identity, "invalid-"+kind)
			switch kind {
			case "artifact":
				proof.Identity.BuildIdentity.ArtifactDigest = strings.Repeat("f", 64)
			case "verifier":
				proof.VerifierID = "untrusted"
			case "policy":
				proof.ProbePolicyVersion++
			case "future":
				proof.VerifiedAt = proof.VerifiedAt.Add(time.Hour)
				proof.ValidUntil = proof.VerifiedAt.Add(time.Second)
			case "expired":
				proof.VerifiedAt = proof.VerifiedAt.Add(-time.Minute)
				proof.ValidUntil = proof.VerifiedAt.Add(time.Second)
			case "too-long":
				proof.ValidUntil = proof.VerifiedAt.Add(2 * time.Minute)
			}
			if _, err = q.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, RequestID: "invalid-" + kind, ExpectedVersion: accepted.QueryRuntime.Version, Verification: proof}); !errors.Is(err, durable.ErrQueryRetention) {
				t.Fatalf("%s proof accepted: %v", kind, err)
			}
		}
	}
	verified, err := q.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, RequestID: "verify-" + id, ExpectedVersion: accepted.QueryRuntime.Version, Verification: QueryProofFixture(identity, "proof-"+id)})
	if err != nil {
		t.Fatal(err)
	}
	return *verified.QueryRuntime
}
func RunQueryRuntimeRetention(t *testing.T, s durable.Store) {
	t.Helper()
	q := queryStore(t, s)
	life, catalog := lifecycleCapabilities(t, s)
	ns := fmt.Sprintf("query-%d", time.Now().UnixNano())
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, BuildID: "historic"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	start := lifecycleStart(t, s, ns, "closed", "historic")
	task := lifecycleClaim(t, s, start)
	if _, err := s.CommitTransition(t.Context(), durable.CommitRequest{Key: start.Key, RequestID: "close", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "closed"}}, State: durable.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.BeginBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "retire", ExpectedEpoch: 1, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	capability, capabilityErr := life.InspectCompatibility(t.Context(), target.NamespaceTarget)
	if capabilityErr != nil || capability.QueryRetentionSchemaVersion != durable.QueryRetentionSchemaVersion {
		t.Fatalf("query capability missing: %+v %v", capability, capabilityErr)
	}
	final := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}
	if _, err := life.FinalizeBuildRetirement(t.Context(), final); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("unmapped history finalized: %v", err)
	}
	identity := QueryIdentityFixture()
	if _, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "identity", ExpectedVersion: 2, Identity: &identity}); err != nil {
		t.Fatal(err)
	}
	final.ExpectedVersion = 3
	if _, err := life.FinalizeBuildRetirement(t.Context(), final); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("unverified history finalized: %v", err)
	}
	a := registerQueryFixture(t, s, target, "a", "host-a")
	b := registerQueryFixture(t, s, target, "b", "host-a")
	removeA := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: a.QueryRuntimeTarget, RequestID: "remove-a", ExpectedVersion: a.Version}
	if _, err := q.BeginQueryRuntimeRemoval(t.Context(), removeA); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("same instance survived removal: %v", err)
	}
	c := registerQueryFixture(t, s, target, "c", "host-c")
	acceptedFinal, err := life.FinalizeBuildRetirement(t.Context(), final)
	if err != nil {
		t.Fatal(err)
	}
	again, err := life.FinalizeBuildRetirement(t.Context(), final)
	if err != nil || !reflect.DeepEqual(again, acceptedFinal) {
		t.Fatalf("final replay: %+v %v", again, err)
	}
	removeB, err := q.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: b.QueryRuntimeTarget, RequestID: "remove-b", ExpectedVersion: b.Version})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.FinishQueryRuntimeRemoval(t.Context(), durable.FinishQueryRemovalRequest{Fence: removeB.QueryRuntime.Removal, RequestID: "finish-b"}); err != nil {
		t.Fatal(err)
	}
	// With only independent A and C active, competing reservations cannot use
	// each other as the sole survivor.
	requests := []durable.BeginQueryRemovalRequest{removeA, {QueryRuntimeTarget: c.QueryRuntimeTarget, RequestID: "remove-c", ExpectedVersion: c.Version}}
	var wg sync.WaitGroup
	results := make([]durable.LifecycleReceipt, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Go(func() { results[i], errs[i] = q.BeginQueryRuntimeRemoval(t.Context(), requests[i]) })
	}
	wg.Wait()
	winner := -1
	for i, err := range errs {
		if err == nil {
			if winner != -1 {
				t.Fatal("both sole survivors reserved")
			}
			winner = i
		} else if !errors.Is(err, durable.ErrQueryRetention) {
			t.Fatal(err)
		}
	}
	if winner < 0 {
		t.Fatal("neither reservation accepted")
	}
	saved := results[winner]
	fence := saved.QueryRuntime.Removal
	if _, err = q.CheckQueryRuntimeRemoval(t.Context(), fence); err != nil {
		t.Fatal(err)
	}
	survivor := fence.Survivor
	verify := durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: survivor.QueryRuntimeTarget, RequestID: "refresh", ExpectedVersion: fence.SurvivorStateVersion, Verification: QueryProofFixture(survivor, "refreshed")}
	if _, err = q.RecordQueryRuntimeVerification(t.Context(), verify); err != nil {
		t.Fatal(err)
	}
	if _, err = q.CheckQueryRuntimeRemoval(t.Context(), fence); !errors.Is(err, durable.ErrQueryFence) {
		t.Fatalf("stale proof substitution allowed: %v", err)
	}
	replay, err := q.BeginQueryRuntimeRemoval(t.Context(), requests[winner])
	if err != nil || !reflect.DeepEqual(replay, saved) {
		t.Fatalf("reservation replay changed: %+v %v", replay, err)
	}
	settlement := QuerySettlementFixture(t, fence)
	abort := durable.AbortQueryRemovalRequest{Fence: fence, Settlement: settlement, VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: fence.Candidate.QueryRuntimeTarget, RequestID: "abort", ExpectedVersion: fence.CandidateStateVersion, Verification: QueryProofFixture(fence.Candidate, "exists-again")}}
	withoutSettlement := abort
	withoutSettlement.Settlement = durable.QueryRemovalSettlement{}
	if _, err = q.AbortQueryRuntimeRemoval(t.Context(), withoutSettlement); !errors.Is(err, durable.ErrQueryFence) {
		t.Fatalf("healthy probe aborted unsettled deletion: %v", err)
	}
	if _, err = q.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: survivor.QueryRuntimeTarget, RequestID: "survivor-after-unknown", ExpectedVersion: verify.ExpectedVersion + 1}); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("unsettled candidate used as survivor: %v", err)
	}
	aborted, err := q.AbortQueryRuntimeRemoval(t.Context(), abort)
	if err != nil || aborted.QueryRuntime.State != durable.QueryRuntimeActive {
		t.Fatalf("abort: %+v %v", aborted, err)
	}
	verifyLifecycleDelivery(t, s, aborted)
	next := requests[winner]
	next.RequestID = "remove-again"
	next.ExpectedVersion = aborted.QueryRuntime.Version
	reserved, err := q.BeginQueryRuntimeRemoval(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	stale := abort
	stale.RequestID = "stale-settlement"
	stale.Fence = reserved.QueryRuntime.Removal
	stale.ExpectedVersion = reserved.QueryRuntime.Version
	if _, err = q.AbortQueryRuntimeRemoval(t.Context(), stale); !errors.Is(err, durable.ErrQueryFence) {
		t.Fatalf("prior epoch settlement accepted: %v", err)
	}
	finished, err := q.FinishQueryRuntimeRemoval(t.Context(), durable.FinishQueryRemovalRequest{Fence: reserved.QueryRuntime.Removal, RequestID: "finished"})
	if err != nil || finished.QueryRuntime.State != durable.QueryRuntimeRemoved {
		t.Fatalf("finish: %+v %v", finished, err)
	}
	recoveredAbort, err := q.AbortQueryRuntimeRemoval(t.Context(), abort)
	if err != nil || !reflect.DeepEqual(recoveredAbort, aborted) {
		t.Fatalf("abort replay changed after removal: %+v %v", recoveredAbort, err)
	}
	facts, err := q.InspectQueryRetention(t.Context(), target)
	if err != nil || facts.RetainedExecutions != 1 || facts.ActiveBindings != 1 || facts.VerifiedBindings != 1 {
		t.Fatalf("retention: %+v %v", facts, err)
	}
	page, err := q.ListQueryRuntimes(t.Context(), durable.QueryRuntimeList{NamespaceTarget: target.NamespaceTarget, InstanceID: "host-a", Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Next == "" {
		t.Fatalf("bounded host bindings: %+v %v", page, err)
	}
}

// QuerySettlementFixture is synthetic store conformance evidence, not a host or
// provider settlement claim. Native qualification uses its owned controller.
func QuerySettlementFixture(t *testing.T, f durable.QueryRemovalFence) durable.QueryRemovalSettlement {
	t.Helper()
	digest, err := durable.Fingerprint("query_runtime.removal_fence.v1", f)
	if err != nil {
		t.Fatal(err)
	}
	return durable.QueryRemovalSettlement{FenceDigest: digest, VerifierID: f.Candidate.BuildIdentity.VerifierID, EvidenceDigest: strings.Repeat("e", 64), Disposition: durable.QueryRemovalNotIssued, ExternalOperationID: "fixture-" + f.RequestID, SettledAt: durable.Timestamp(time.Now())}
}

func queryRetirementFixture(t *testing.T, s durable.Store, target durable.BuildTarget, version int64) int64 {
	t.Helper()
	life, _ := lifecycleCapabilities(t, s)
	identity := QueryIdentityFixture()
	accepted, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "retained-query-identity", ExpectedVersion: version, Identity: &identity})
	if err != nil {
		t.Fatal(err)
	}
	registerQueryFixture(t, s, target, "retained-query", "query-host")
	return accepted.Build.Version
}

func RunQueryEmptyRetiredLastBinding(t *testing.T, s durable.Store) {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	q := queryStore(t, s)
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: fmt.Sprintf("empty-query-%d", time.Now().UnixNano())}, BuildID: "empty"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: target.Namespace, AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	identity := QueryIdentityFixture()
	if _, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "build", Identity: &identity}); err != nil {
		t.Fatal(err)
	}
	binding := registerQueryFixture(t, s, target, "only", "only-host")
	remove := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: binding.QueryRuntimeTarget, RequestID: "remove", ExpectedVersion: binding.Version}
	if _, err := q.BeginQueryRuntimeRemoval(t.Context(), remove); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("accepting last binding removed: %v", err)
	}
	if _, err := life.BeginBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedEpoch: 1, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.BeginQueryRuntimeRemoval(t.Context(), remove); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("retiring last binding removed: %v", err)
	}
	if _, err := life.FinalizeBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}); err != nil {
		t.Fatal(err)
	}
	accepted, err := q.BeginQueryRuntimeRemoval(t.Context(), remove)
	if err != nil || accepted.QueryRuntime.Removal.Survivor.RuntimeID != "" {
		t.Fatalf("empty retired last binding: %+v %v", accepted, err)
	}
	if _, err = q.CheckQueryRuntimeRemoval(t.Context(), accepted.QueryRuntime.Removal); err != nil {
		t.Fatal(err)
	}
	if _, err = q.FinishQueryRuntimeRemoval(t.Context(), durable.FinishQueryRemovalRequest{Fence: accepted.QueryRuntime.Removal, RequestID: "finish"}); err != nil {
		t.Fatal(err)
	}
}
