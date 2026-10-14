//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

type nativeCommitReadFixture struct {
	NativeStore
	receipt durable.QueryRemovalCommitReceipt
}

func (f *nativeCommitReadFixture) LookupQueryRemovalGuardReceipt(_ context.Context, _ durable.QueryRemovalGuardReceiptLookup) (durable.QueryRemovalGuardReceipt, error) {
	receipt := f.receipt
	return durable.QueryRemovalGuardReceipt{Commit: &receipt}, nil
}
func nativePreparedCommitFixture(t *testing.T) (*NativeGuardHost, nativePreparedCommit, *nativeCommitReadFixture) {
	t.Helper()
	digest := strings.Repeat("a", 64)
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	scope := durable.QueryRemovalExternalScope{AuthorityID: "core", OperationScopeID: "operation", AttemptID: "attempt", ProviderOperationID: "provider", CoverageDigest: digest, ExcludedInstancesDigest: digest, ProducerID: "producer", ProfileID: nativeGuardProfile, ProfileVersion: 1, VerifierID: "verifier"}
	target := durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "installation", Namespace: "namespace"}, BuildID: "build"}, RuntimeID: "runtime"}
	permission := durable.QueryRemovalPermission{ExternalScope: scope, PermissionID: "permission", PermissionDigest: digest, CommandID: "command", StepDigest: digest, ResourceDigest: digest, IncarnationDigest: digest, RevalidationRequestID: "permission-proof", RevalidationDigest: digest, AcceptedAt: now, EvidenceDigest: digest, Attestation: "authenticated-fixture"}
	request := durable.QueryRemovalCommitRequest{Guard: durable.QueryRemovalGuardReference{QueryRuntimeTarget: target, OriginalDigest: digest, GuardID: digest, RootDigest: digest}, RequestID: "bind", CommandDigest: digest, ExpectedCurrentDigest: strings.Repeat("b", 64), CurrentRequestID: "current-proof", Permission: permission}
	if request.Validate() != nil {
		t.Fatal("invalid native fixture")
	}
	prepared := nativePreparedCommit{Input: operator.QueryCommitInput{RequestID: "bind"}, Request: request, Sources: nativeCommitSources{Permission: managed.RecoveryAssociation{Digest: strings.Repeat("c", 64)}, PermissionDigest: digest, PermissionProof: managed.RecoveryAssociation{Digest: strings.Repeat("d", 64)}, Current: managed.RecoveryAssociation{Digest: strings.Repeat("e", 64)}, PermissionNative: managed.RemovalRevalidationFacts{NativeMaterialDigest: digest}, CurrentNative: managed.RemovalRevalidationFacts{NativeMaterialDigest: request.ExpectedCurrentDigest}}}
	requestDigest, err := durable.Fingerprint("dispatch.query-removal.commit.v1", request)
	if err != nil {
		t.Fatal(err)
	}
	store := &nativeCommitReadFixture{receipt: durable.QueryRemovalCommitReceipt{Request: request, RequestDigest: requestDigest, AcceptedAt: now, DeliveryID: "delivery"}}
	if err = store.receipt.Seal(); err != nil {
		t.Fatal(err)
	}
	host := &NativeGuardHost{native: store, key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32)), identity: managed.RecoveryProducerIdentity{ProducerID: "producer", Profile: nativeGuardProfile, Version: "1", VerifierID: "verifier"}}
	return host, prepared, store
}
func TestNativeCommitNormalizationRequiresFullSavedRequest(t *testing.T) {
	host, prepared, store := nativePreparedCommitFixture(t)
	wire := nativeCommitAcceptance(store.receipt)
	facts, err := host.normalizeCommit(t.Context(), prepared, managed.RecoveryAssociation{}, wire)
	if err != nil {
		t.Fatal(err)
	}
	if facts.PermissionDigest != prepared.Request.Permission.PermissionDigest || facts.PermissionDigest == prepared.Sources.Permission.Digest {
		t.Fatal("permission bytes digest replaced by recovery fact digest")
	}
	if facts.PermissionProofDigest != prepared.Sources.PermissionProof.Digest || facts.CurrentProofDigest != prepared.Sources.Current.Digest || facts.PermissionProofDigest == prepared.Request.Permission.RevalidationDigest || facts.CurrentProofDigest == prepared.Request.ExpectedCurrentDigest || facts.NativeRequestDigest != store.receipt.RequestDigest {
		t.Fatal("permission/current/native request domains conflated")
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if facts.Export.RawDigest != managed.Digest(raw) || facts.NativeReceiptDigest != store.receipt.ReceiptDigest {
		t.Fatal("wire hash replaced native receipt digest")
	}
	changed := wire
	changed.InvocationID = "another"
	if _, err = host.normalizeCommit(t.Context(), prepared, managed.RecoveryAssociation{}, changed); err == nil {
		t.Fatal("changed endpoint receipt accepted")
	}
	store.receipt.Request.Permission.ResourceDigest = strings.Repeat("c", 64)
	store.receipt.RequestDigest, err = durable.Fingerprint("dispatch.query-removal.commit.v1", store.receipt.Request)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.receipt.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err = host.normalizeCommit(t.Context(), prepared, managed.RecoveryAssociation{}, nativeCommitAcceptance(store.receipt)); err == nil {
		t.Fatal("canonical rehashed different native request accepted")
	}
}
func TestCommitReplayIgnoresOnlyMutableNativeRuntimeObservation(t *testing.T) {
	_, prepared, _ := nativePreparedCommitFixture(t)
	changed := prepared.Sources
	changed.Binding.Candidate.Version++
	if !sameCommitSources(prepared.Sources, changed) {
		t.Fatal("historical replay depends on current runtime version")
	}
	changed.CurrentNative.NativeMaterialDigest = strings.Repeat("f", 64)
	if sameCommitSources(prepared.Sources, changed) {
		t.Fatal("changed accepted proof ignored")
	}
}
