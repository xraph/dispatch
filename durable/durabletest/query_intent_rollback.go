package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RunQueryIntentRollback checks complete stored evidence around failed audit writes.
func RunQueryIntentRollback(t *testing.T, s durable.Store, snapshot func(*testing.T, durable.NamespaceTarget) any, inject func(*testing.T, string) func()) {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	q := queryStore(t, s)
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: fmt.Sprintf("query-rollback-%d", time.Now().UnixNano())}, BuildID: "b"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: target.Namespace, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	identity := QueryIdentityFixture()
	if _, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "build", Identity: &identity}); err != nil {
		t.Fatal(err)
	}
	a := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: "a"}, InstanceID: "host-a", IdentityVersion: 1, BuildIdentity: identity}
	b := a
	b.RuntimeID = "b"
	b.InstanceID = "host-b"
	for _, runtime := range []durable.QueryRuntimeIdentity{a, b} {
		if _, err := q.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: runtime, RequestID: "register-" + runtime.RuntimeID}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(op durable.LifecycleOperation, id string, request any, call func() (durable.LifecycleReceipt, error)) durable.LifecycleReceipt {
		t.Helper()
		before := snapshot(t, target.NamespaceTarget)
		restore := inject(t, durable.LifecycleAction(op))
		_, err := call()
		restore()
		if err == nil {
			t.Fatalf("%s accepted failed required intent", op)
		}
		if after := snapshot(t, target.NamespaceTarget); !reflect.DeepEqual(before, after) {
			t.Fatalf("%s changed binding/proof/version, immutable receipt or outbox after failed intent\nbefore: %#v\nafter: %#v", op, before, after)
		}
		digest, err := durable.Fingerprint(string(op), request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = life.LookupLifecycleReceipt(t.Context(), durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: op, RequestID: id, RequestDigest: digest}); !errors.Is(err, durable.ErrNotFound) {
			t.Fatalf("%s persisted failed receipt: %v", op, err)
		}
		receipt, err := call()
		if err != nil {
			t.Fatalf("%s retry: %v", op, err)
		}
		return receipt
	}
	verify := durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: a.QueryRuntimeTarget, RequestID: "verify-a", ExpectedVersion: 1, Verification: QueryProofFixture(a, "proof-a")}
	check(durable.OperationVerifyQueryRuntime, verify.RequestID, verify, func() (durable.LifecycleReceipt, error) { return q.RecordQueryRuntimeVerification(t.Context(), verify) })
	if _, err := q.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: b.QueryRuntimeTarget, RequestID: "verify-b", ExpectedVersion: 1, Verification: QueryProofFixture(b, "proof-b")}); err != nil {
		t.Fatal(err)
	}
	begin := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: a.QueryRuntimeTarget, RequestID: "begin", ExpectedVersion: 2}
	reserved := check(durable.OperationBeginQueryRemoval, begin.RequestID, begin, func() (durable.LifecycleReceipt, error) { return q.BeginQueryRuntimeRemoval(t.Context(), begin) })
	abort := durable.AbortQueryRemovalRequest{Fence: reserved.QueryRuntime.Removal, Settlement: QuerySettlementFixture(t, reserved.QueryRuntime.Removal), VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: a.QueryRuntimeTarget, RequestID: "abort", ExpectedVersion: 3, Verification: QueryProofFixture(a, "abort-proof")}}
	accepted := check(durable.OperationAbortQueryRemoval, abort.RequestID, abort, func() (durable.LifecycleReceipt, error) { return q.AbortQueryRuntimeRemoval(t.Context(), abort) })
	if accepted.QueryAbort == nil || accepted.QueryAbort.Settlement != abort.Settlement || accepted.QueryAbort.Fence != abort.Fence {
		t.Fatal("accepted abort lost immutable settlement/fence")
	}
	replay, err := q.AbortQueryRuntimeRemoval(t.Context(), abort)
	if err != nil || !reflect.DeepEqual(replay, accepted) {
		t.Fatalf("accepted abort replay: %+v %v", replay, err)
	}
}
