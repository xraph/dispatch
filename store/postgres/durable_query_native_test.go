package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func protocolQueryProof(t *testing.T, p *nativeOldWriter, identity durable.QueryRuntimeIdentity, key durable.Key, id string, validity time.Duration) durable.QueryRuntimeVerification {
	t.Helper()
	request := drt.QueryRequest{Key: key, BuildID: identity.BuildID, Name: "snapshot"}
	raw := p.call(t, "query", map[string]any{"RuntimeID": identity.RuntimeID, "Query": request}, "")
	var result drt.QueryResult
	if json.Unmarshal(raw, &result) != nil || result.Key != key || result.State != durable.StateCompleted || string(result.Output) != "retained-value" || result.Revision < 2 {
		t.Fatalf("actual retained query failed: %s", raw)
	}
	evidence, err := durable.Fingerprint("protocol-one-retained-query.v1", struct {
		Request  drt.QueryRequest
		Result   drt.QueryResult
		Identity durable.QueryRuntimeIdentity
	}{request, result, identity})
	if err != nil {
		t.Fatal(err)
	}
	now := durable.Timestamp(time.Now())
	return durable.QueryRuntimeVerification{Identity: identity, ProofID: id, VerifierID: identity.BuildIdentity.VerifierID, EvidenceDigest: evidence, ProbePolicyID: identity.BuildIdentity.ProbePolicyID, ProbePolicyVersion: identity.BuildIdentity.ProbePolicyVersion, VerifiedAt: now, ValidUntil: now.Add(validity)}
}
func TestQueryNativeProtocolOne(t *testing.T) {
	binary, dsn := os.Getenv("DISPATCH_PROTOCOL_ONE_BINARY"), os.Getenv("DISPATCH_LIFECYCLE_TEST_DSN")
	if binary == "" || dsn == "" {
		if os.Getenv("DISPATCH_QUERY_REQUIRED") == "1" {
			t.Fatal("pinned protocol-one fixture required")
		}
		t.Skip("pinned protocol-one fixture required")
	}
	admin := retirementConn(t, dsn)
	name := fmt.Sprintf("dispatch_query_native_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, "DROP DATABASE "+identifier+" WITH (FORCE)")
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid fixture DSN")
	}
	parsed.Path = "/" + name
	nativeDSN := parsed.String()
	first := startNativeOldWriter(t, binary, nativeDSN)
	first.call(t, "migrate", nil, "")
	ns := durable.NamespaceConfig{InstallationID: "i", Namespace: "native-query", AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}
	first.call(t, "namespace", map[string]any{"Namespace": ns}, "")
	start := durable.StartRequest{Key: durable.Key{Namespace: ns.Namespace, WorkflowID: "first", RunID: "r"}, RequestID: "start", WorkflowType: "retained", BuildID: "protocol-one", Queue: "q", Input: []byte("retained-value")}
	var statusA, statusB drt.WorkerStatus
	if err = json.Unmarshal(first.call(t, "seed_query", map[string]any{"Start": start}, ""), &statusA); err != nil {
		t.Fatal(err)
	}
	second := startNativeOldWriter(t, binary, nativeDSN)
	other := start
	other.WorkflowID = "second"
	if err = json.Unmarshal(second.call(t, "seed_query", map[string]any{"Start": other}, ""), &statusB); err != nil {
		t.Fatal(err)
	}
	if !statusA.AdmissionClosed || !statusB.AdmissionClosed || statusA.InFlight != 0 || statusB.InFlight != 0 || statusA.InstanceID == statusB.InstanceID {
		t.Fatalf("query-only instances not independent: %+v %+v", statusA, statusB)
	}
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns.Namespace}, BuildID: start.BuildID}
	first.call(t, "enroll", map[string]any{"Enrollment": durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}}, "")
	first.call(t, "begin", map[string]any{"Build": durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedEpoch: 1, ExpectedVersion: 1}}, "")
	empty := target
	empty.BuildID = "empty"
	register := durable.RegisterBuildRequest{BuildTarget: empty, RequestID: "empty-register"}
	var before durable.LifecycleReceipt
	if err = json.Unmarshal(first.call(t, "register", map[string]any{"Register": register}, ""), &before); err != nil {
		t.Fatal(err)
	}
	first.call(t, "begin", map[string]any{"Build": durable.BuildRetirementRequest{BuildTarget: empty, RequestID: "empty-begin", ExpectedEpoch: 1, ExpectedVersion: 1}}, "")
	emptyFinal := durable.BuildRetirementRequest{BuildTarget: empty, RequestID: "empty-final", ExpectedEpoch: 2, ExpectedVersion: 2}
	oldFinal := first.call(t, "finalize", map[string]any{"Build": emptyFinal}, "")
	current := openWakeStore(t, nativeDSN)
	if after := first.call(t, "finalize", map[string]any{"Build": emptyFinal}, ""); string(after) != string(oldFinal) {
		t.Fatal("pre-expansion final receipt changed")
	}
	digest, err := durable.Fingerprint(string(durable.OperationRegisterBuild), register)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before.RequestDigest {
		t.Fatal("old RegisterBuild fingerprint changed")
	}
	recovered, err := current.LookupLifecycleReceipt(t.Context(), durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: durable.OperationRegisterBuild, RequestID: register.RequestID, RequestDigest: digest})
	if err != nil || !reflect.DeepEqual(recovered, before) {
		t.Fatalf("pre-expansion receipt: %+v %v", recovered, err)
	}
	deliveries, err := current.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "i", Destination: durable.DestinationChronicle}, Limit: durable.MaxDeliveryBatch})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range deliveries.Records {
		d := record.Delivery
		if d.ID != before.DeliveryID {
			continue
		}
		found = true
		expected, buildErr := durable.NewDelivery(durable.NamespaceRecord{NamespaceConfig: ns}, durable.DestinationChronicle, durable.LifecycleDeliverySource(durable.WithAuditMetadata(t.Context(), d.Metadata), recovered))
		mapped, mapErr := ecosystem.ChronicleRequest(ecosystem.Binding{Producer: "dispatch", InstallationID: "i", Namespace: ns.Namespace, AppID: "a", TenantID: "t"}, d)
		if buildErr != nil || mapErr != nil || d.Verify() != nil || expected.Fingerprint != d.Fingerprint || mapped.SourceFingerprint != d.Fingerprint || mapped.SourceKey != before.DeliveryID {
			t.Fatal("pending old intent no longer verifies")
		}
	}
	if !found {
		t.Fatal("pending pre-expansion intent missing")
	}
	pg := retirementConn(t, nativeDSN)
	final := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}
	refusal := func(reason string) {
		t.Helper()
		var beforeCount, afterCount, receiptCount int64
		var state string
		if err = pg.QueryRow(t.Context(), `SELECT count(*) FROM dispatch_durable_outbox WHERE namespace=$1`, ns.Namespace).Scan(&beforeCount); err != nil {
			t.Fatal(err)
		}
		first.call(t, "finalize", map[string]any{"Build": final}, "DL004")
		if err = pg.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM dispatch_durable_outbox WHERE namespace=$1),(SELECT count(*) FROM dispatch_lifecycle_receipts WHERE namespace=$1 AND request_id='final'),(SELECT state FROM dispatch_build_lifecycle WHERE namespace=$1 AND build_id=$2)`, ns.Namespace, target.BuildID).Scan(&afterCount, &receiptCount, &state); err != nil {
			t.Fatal(err)
		}
		if beforeCount != afterCount || receiptCount != 0 || state != durable.BuildRetiring {
			t.Fatal("refusal published state, receipt or intent")
		}
		t.Log("protocol-one retained query refusal:", reason)
	}
	refusal("historical mapping absent")
	artifact, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(artifact)
	config, err := durable.Fingerprint("protocol-one-query-config.v1", struct{ Namespace, Build, Queue, Workflow, Query string }{ns.Namespace, start.BuildID, start.Queue, start.WorkflowType, "snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := durable.Fingerprint("protocol-one-artifact-enrollment.v1", struct{ Artifact, Configuration string }{hex.EncodeToString(sum[:]), config})
	if err != nil {
		t.Fatal(err)
	}
	buildIdentity := durable.BuildQueryIdentity{ArtifactDigest: hex.EncodeToString(sum[:]), ConfigurationDigest: config, ConfigurationVersion: "protocol-one-query-v1", EnrollmentEvidenceDigest: enrollment, ProbePolicyID: "closed-input-snapshot", ProbePolicyVersion: 1, VerifierID: "local-fixture", MaximumProofValidity: time.Minute}
	if _, err = current.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "actual-identity", ExpectedVersion: 2, Identity: &buildIdentity}); err != nil {
		t.Fatal(err)
	}
	final.ExpectedVersion = 3
	refusal("no current query proof")
	identityA := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: statusA.RuntimeID}, InstanceID: statusA.InstanceID, IdentityVersion: 1, BuildIdentity: buildIdentity}
	identityB := identityA
	identityB.RuntimeID = statusB.RuntimeID
	identityB.InstanceID = statusB.InstanceID
	for i, identity := range []durable.QueryRuntimeIdentity{identityA, identityB} {
		if _, err = current.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: identity, RequestID: fmt.Sprintf("runtime-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	record := func(p *nativeOldWriter, identity durable.QueryRuntimeIdentity, id string, version int64, validity time.Duration) durable.LifecycleReceipt {
		t.Helper()
		accepted, proofErr := current.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, RequestID: id, ExpectedVersion: version, Verification: protocolQueryProof(t, p, identity, start.Key, id, validity)})
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		return accepted
	}
	expired := record(first, identityA, "short-a", 1, 100*time.Millisecond)
	waitProtocolProof(t, expired.QueryRuntime.Verification.ValidUntil)
	refusal("proof expired")
	refreshed := record(first, identityA, "long-a", 2, 30*time.Second)
	shortB := record(second, identityB, "short-b", 1, 100*time.Millisecond)
	reservation, err := current.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: identityA.QueryRuntimeTarget, RequestID: "reserve-a", ExpectedVersion: refreshed.QueryRuntime.Version})
	if err != nil {
		t.Fatal(err)
	}
	waitProtocolProof(t, shortB.QueryRuntime.Verification.ValidUntil)
	refusal("only unexpired proof is removing")
	controller := fixtureRemovalController{fence: reservation.QueryRuntime.Removal, operationID: "owned-native-removal-a", probe: func() durable.QueryRuntimeVerification {
		return protocolQueryProof(t, first, identityA, start.Key, "exists-a", 30*time.Second)
	}}
	settlement, proof, err := controller.VerifyAbort(t.Context(), *reservation.QueryRuntime, reservation.QueryRuntime.Removal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = current.AbortQueryRuntimeRemoval(t.Context(), durable.AbortQueryRemovalRequest{Fence: reservation.QueryRuntime.Removal, Settlement: settlement, VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identityA.QueryRuntimeTarget, RequestID: "abort-a", ExpectedVersion: reservation.QueryRuntime.Version, Verification: proof}}); err != nil {
		t.Fatal(err)
	}
	if err = controller.dispatch(reservation.QueryRuntime.Removal); err == nil {
		t.Fatal("stale owned attempt issued deletion after abort")
	}
	accepted, err := current.FinalizeBuildRetirement(t.Context(), final)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := current.FinalizeBuildRetirement(t.Context(), final)
	if err != nil || !reflect.DeepEqual(accepted, replay) {
		t.Fatal("current exact final response changed")
	}
	first.call(t, "finalize", map[string]any{"Build": final}, "")
	t.Logf("actual retained query artifact=%s configuration=%s cases=closed-input-snapshot instances=2 polling=closed", buildIdentity.ArtifactDigest, config)
}
func waitProtocolProof(t *testing.T, at time.Time) {
	t.Helper()
	timer := time.NewTimer(time.Until(at) + time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}
