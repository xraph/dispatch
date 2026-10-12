package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RunQueryReceiptRecovery corrupts only dedicated fixture receipt payloads.
// Replace must retain the immutable lookup columns so both recovery paths run.
func RunQueryReceiptRecovery(t *testing.T, s durable.Store, replace func(*testing.T, durable.LifecycleReceipt)) {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	q := queryStore(t, s)
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: fmt.Sprintf("receipt-query-%d", time.Now().UnixNano())}, BuildID: "b"}
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
	runtime := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: "a"}, InstanceID: "host-a", IdentityVersion: 1, BuildIdentity: identity}
	register := durable.RegisterQueryRuntimeRequest{Identity: runtime, RequestID: "register"}
	registered, err := q.RegisterQueryRuntime(t.Context(), register)
	if err != nil {
		t.Fatal(err)
	}
	verify := durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: runtime.QueryRuntimeTarget, RequestID: "verify", ExpectedVersion: 1, Verification: QueryProofFixture(runtime, "proof")}
	verify.Verification.ValidUntil = verify.Verification.VerifiedAt.Add(500 * time.Millisecond)
	verified, err := q.RecordQueryRuntimeVerification(t.Context(), verify)
	if err != nil {
		t.Fatal(err)
	}
	other := runtime
	other.RuntimeID = "b"
	other.InstanceID = "host-b"
	if _, err = q.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: other, RequestID: "register-b"}); err != nil {
		t.Fatal(err)
	}
	survivorProof := QueryProofFixture(other, "survivor")
	survivorProof.ValidUntil = survivorProof.VerifiedAt.Add(500 * time.Millisecond)
	if _, err = q.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: other.QueryRuntimeTarget, RequestID: "verify-b", ExpectedVersion: 1, Verification: survivorProof}); err != nil {
		t.Fatal(err)
	}
	begin := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: runtime.QueryRuntimeTarget, RequestID: "begin", ExpectedVersion: 2}
	reserved, err := q.BeginQueryRuntimeRemoval(t.Context(), begin)
	if err != nil {
		t.Fatal(err)
	}
	settlement := QuerySettlementFixture(t, reserved.QueryRuntime.Removal)
	abort := durable.AbortQueryRemovalRequest{Fence: reserved.QueryRuntime.Removal, Settlement: settlement, VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: runtime.QueryRuntimeTarget, RequestID: "abort", ExpectedVersion: 3, Verification: QueryProofFixture(runtime, "abort-proof")}}
	abort.Verification.ValidUntil = abort.Verification.VerifiedAt.Add(500 * time.Millisecond)
	aborted, err := q.AbortQueryRuntimeRemoval(t.Context(), abort)
	if err != nil {
		t.Fatal(err)
	}
	next := begin
	next.RequestID = "begin-again"
	next.ExpectedVersion = 4
	last, err := q.BeginQueryRuntimeRemoval(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	waitDeferral(t, abort.Verification.ValidUntil)
	finish := durable.FinishQueryRemovalRequest{Fence: last.QueryRuntime.Removal, RequestID: "finish"}
	finished, err := q.FinishQueryRuntimeRemoval(t.Context(), finish)
	if err != nil {
		t.Fatalf("finish after original proof expiry: %v", err)
	}
	cases := []struct {
		name   string
		good   durable.LifecycleReceipt
		replay func() (durable.LifecycleReceipt, error)
		breaks map[string]func(*durable.LifecycleReceipt)
	}{
		{"register", registered, func() (durable.LifecycleReceipt, error) { return q.RegisterQueryRuntime(t.Context(), register) }, map[string]func(*durable.LifecycleReceipt){
			"state":   func(r *durable.LifecycleReceipt) { r.QueryRuntime.State = durable.QueryRuntimeRemoved },
			"version": func(r *durable.LifecycleReceipt) { r.QueryRuntime.Version++ }}},
		{"verify", verified, func() (durable.LifecycleReceipt, error) { return q.RecordQueryRuntimeVerification(t.Context(), verify) }, map[string]func(*durable.LifecycleReceipt){
			"missing-proof":  func(r *durable.LifecycleReceipt) { r.QueryRuntime.Verification = durable.QueryRuntimeVerification{} },
			"proof-identity": func(r *durable.LifecycleReceipt) { r.QueryRuntime.Verification.Identity.InstanceID = "other" },
			"proof-time":     func(r *durable.LifecycleReceipt) { r.QueryRuntime.Verification.AcceptedAt = time.Time{} },
			"state":          func(r *durable.LifecycleReceipt) { r.QueryRuntime.State = durable.QueryRuntimeRemoving }}},
		{"begin", reserved, func() (durable.LifecycleReceipt, error) { return q.BeginQueryRuntimeRemoval(t.Context(), begin) }, map[string]func(*durable.LifecycleReceipt){
			"missing-fence":     func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal = durable.QueryRemovalFence{} },
			"candidate-version": func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.CandidateStateVersion++ },
			"survivor-proof": func(r *durable.LifecycleReceipt) {
				r.QueryRuntime.Removal.SurvivorProof = durable.QueryRuntimeVerification{}
			},
			"same-instance":      func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.Survivor.InstanceID = runtime.InstanceID },
			"build-epoch":        func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.BuildEpoch = 0 },
			"build-version":      func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.BuildVersion = 0 },
			"build-state":        func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.BuildState = "unknown" },
			"survivor-version":   func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.SurvivorStateVersion = 0 },
			"survivor-policy":    func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.SurvivorProof.ProbePolicyVersion++ },
			"candidate-identity": func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.Candidate.InstanceID = "other" },
			"altered-fence-expiry": func(r *durable.LifecycleReceipt) {
				r.QueryRuntime.Removal.ValidUntil = r.QueryRuntime.Removal.ValidUntil.Add(-time.Microsecond)
			},
			"fence-time": func(r *durable.LifecycleReceipt) {
				r.QueryRuntime.Removal.ValidUntil = r.QueryRuntime.Removal.AcceptedAt
			},
			"state": func(r *durable.LifecycleReceipt) { r.QueryRuntime.State = durable.QueryRuntimeActive }}},
		{"finish", finished, func() (durable.LifecycleReceipt, error) { return q.FinishQueryRuntimeRemoval(t.Context(), finish) }, map[string]func(*durable.LifecycleReceipt){
			"missing-fence":  func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal = durable.QueryRemovalFence{} },
			"epoch":          func(r *durable.LifecycleReceipt) { r.QueryRuntime.RemovalEpoch++ },
			"original-proof": func(r *durable.LifecycleReceipt) { r.QueryRuntime.Removal.SurvivorProof.Identity.BuildID = "altered" },
			"state":          func(r *durable.LifecycleReceipt) { r.QueryRuntime.State = durable.QueryRuntimeRemoving }}},
		{"abort", aborted, func() (durable.LifecycleReceipt, error) { return q.AbortQueryRuntimeRemoval(t.Context(), abort) }, map[string]func(*durable.LifecycleReceipt){
			"missing-settlement": func(r *durable.LifecycleReceipt) { r.QueryAbort = nil },
			"missing-proof":      func(r *durable.LifecycleReceipt) { r.QueryRuntime.Verification = durable.QueryRuntimeVerification{} },
			"fence":              func(r *durable.LifecycleReceipt) { r.QueryAbort.Fence.BuildEpoch = 0 }}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			lookup := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: test.good.Operation, RequestID: test.good.RequestID, RequestDigest: test.good.RequestDigest}
			for name, damage := range test.breaks {
				t.Run(name, func(t *testing.T) {
					broken := test.good.Clone()
					damage(&broken)
					replace(t, broken)
					defer replace(t, test.good)
					if _, err := life.LookupLifecycleReceipt(t.Context(), lookup); !errors.Is(err, durable.ErrInvalid) {
						t.Errorf("corrupt lookup accepted: %v", err)
					}
					if _, err := test.replay(); !errors.Is(err, durable.ErrInvalid) {
						t.Errorf("corrupt mutation replay accepted: %v", err)
					}
				})
			}
			recovered, replayErr := test.replay()
			if replayErr != nil || !reflect.DeepEqual(recovered, test.good) {
				t.Fatalf("historical evidence replay: %+v %v", recovered, replayErr)
			}
		})
	}
}
