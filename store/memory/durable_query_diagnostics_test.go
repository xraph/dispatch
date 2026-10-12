package memory

import (
	"errors"
	"maps"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryDiagnosticRollbackAndExpiredReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New()
		start := outboxStart(t, s, "proof-diagnostic")
		target := durable.NamespaceTarget{InstallationID: "host", Namespace: start.Namespace}
		if _, err := s.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
			t.Fatal(err)
		}
		identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: target, BuildID: "v1"}, RuntimeID: "runtime"}, InstanceID: "physical", IdentityVersion: 1, BuildIdentity: durabletest.QueryIdentityFixture()}
		if _, err := s.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: identity.BuildTarget, RequestID: "identity", ExpectedVersion: 1, Identity: &identity.BuildIdentity}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: identity, RequestID: "register"}); err != nil {
			t.Fatal(err)
		}
		proof := durabletest.QueryProofFixture(identity, "proof")
		request := durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, RequestID: "verify", ExpectedVersion: 1, Verification: proof}
		priorBindings, priorReceipts, priorOutbox := maps.Clone(s.queryRuntimes), maps.Clone(s.lifecycleReceipts), maps.Clone(s.outbox)
		for _, kind := range []string{"future", "expired", "overlong"} {
			bad := request
			bad.RequestID = kind
			switch kind {
			case "future":
				bad.Verification.VerifiedAt = proof.VerifiedAt.Add(time.Microsecond)
			case "expired":
				bad.Verification.ValidUntil = proof.VerifiedAt
			case "overlong":
				bad.Verification.ValidUntil = proof.VerifiedAt.Add(time.Minute + time.Microsecond)
			}
			_, err := s.RecordQueryRuntimeVerification(t.Context(), bad)
			d, ok := durable.QueryRejectionDetails(err)
			if !errors.Is(err, durable.ErrQueryRetention) || !ok || d.ObservedAt != proof.VerifiedAt {
				t.Fatalf("missing actual sample: %+v %v", d, err)
			}
			if !reflect.DeepEqual(priorBindings, s.queryRuntimes) || !reflect.DeepEqual(priorReceipts, s.lifecycleReceipts) || !reflect.DeepEqual(priorOutbox, s.outbox) {
				t.Fatal("rejected proof mutated binding, receipt or outbox")
			}
		}
		accepted, err := s.RecordQueryRuntimeVerification(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		replay, err := s.RecordQueryRuntimeVerification(t.Context(), request)
		if err != nil || !reflect.DeepEqual(accepted, replay) || replay.AcceptedAt != proof.VerifiedAt {
			t.Fatalf("expired accepted replay changed: %v", err)
		}
	})
}
