//go:build linux

package managedhost

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

type nativeCloseReadFixture struct {
	NativeStore
	receipt durable.QueryRemovalGuardCloseReceipt
}

func (f *nativeCloseReadFixture) LookupQueryRemovalGuardReceipt(_ context.Context, _ durable.QueryRemovalGuardReceiptLookup) (durable.QueryRemovalGuardReceipt, error) {
	receipt := f.receipt
	return durable.QueryRemovalGuardReceipt{Close: &receipt}, nil
}
func TestNativeCloseNormalizationBindsCompleteAcceptedRequest(t *testing.T) {
	host, commit, prior := nativePreparedCommitFixture(t)
	digest := strings.Repeat("a", 64)
	closure := durable.QueryRemovalClosure{ExternalScope: commit.Request.Permission.ExternalScope, CutoffReceiptID: "cutoff", CutoffDigest: digest, CoverageDigest: digest, Disposition: "all_positive", TerminalDigest: strings.Repeat("b", 64), CutoffAt: prior.receipt.AcceptedAt, TerminalAt: prior.receipt.AcceptedAt, EvidenceDigest: digest, Attestation: "authenticated-fixture"}
	request := durable.QueryRemovalGuardCloseRequest{Guard: commit.Request.Guard, RequestID: "close", CommandDigest: digest, Closure: closure}
	if request.Validate() != nil {
		t.Fatal("invalid native fixture")
	}
	prepared := nativePreparedClose{Input: operator.QueryGuardCloseInput{RequestID: "close"}, Request: request, Sources: nativeCloseSources{Coverage: managed.RecoveryAssociation{Digest: closure.TerminalDigest}, Cutoff: managed.RecoveryAssociation{Digest: closure.CutoffDigest}}}
	requestDigest, err := durable.Fingerprint("dispatch.query-removal.close.v1", request)
	if err != nil {
		t.Fatal(err)
	}
	store := &nativeCloseReadFixture{receipt: durable.QueryRemovalGuardCloseReceipt{Request: request, RequestDigest: requestDigest, AcceptedAt: prior.receipt.AcceptedAt, DeliveryID: "close-delivery"}}
	if err = store.receipt.Seal(); err != nil {
		t.Fatal(err)
	}
	host.native = store
	facts, err := host.normalizeClose(t.Context(), prepared, managed.RecoveryAssociation{}, nativeCloseAcceptance(store.receipt))
	if err != nil {
		t.Fatal(err)
	}
	if facts.Coverage != prepared.Sources.Coverage || facts.Cutoff != prepared.Sources.Cutoff || facts.NativeRequestDigest != requestDigest {
		t.Fatal("accepted coverage/cutoff association changed")
	}
	store.receipt.Request.Closure.TerminalDigest = strings.Repeat("c", 64)
	store.receipt.RequestDigest, err = durable.Fingerprint("dispatch.query-removal.close.v1", store.receipt.Request)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.receipt.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err = host.normalizeClose(t.Context(), prepared, managed.RecoveryAssociation{}, nativeCloseAcceptance(store.receipt)); err == nil {
		t.Fatal("different rehashed terminal coverage accepted")
	}
}
func TestNativeCloseIntentIncludesAcceptedCoverageBeforeDigest(t *testing.T) {
	_, commit, prior := nativePreparedCommitFixture(t)
	prepared := nativePreparedClose{Input: operator.QueryGuardCloseInput{RequestID: "close"}, Request: durable.QueryRemovalGuardCloseRequest{Guard: commit.Request.Guard, RequestID: "close"}, Sources: nativeCloseSources{Coverage: managed.RecoveryAssociation{Digest: strings.Repeat("a", 64)}, Cutoff: managed.RecoveryAssociation{Digest: strings.Repeat("b", 64)}, Proof: managed.RecoveryAssociation{Digest: strings.Repeat("c", 64)}}}
	prepared.Request.Closure.TerminalAt = prior.receipt.AcceptedAt
	intent, err := closeIntent(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Coverage == nil || *intent.Coverage != prepared.Sources.Coverage || intent.Cutoff == nil || *intent.Cutoff != prepared.Sources.Cutoff || intent.Proof == nil || *intent.Proof != prepared.Sources.Proof {
		t.Fatal("close missing fixed accepted associations")
	}
	changed := prepared
	changed.Request.Closure.TerminalDigest = strings.Repeat("d", 64)
	other, err := closeIntent(changed)
	if err != nil {
		t.Fatal(err)
	}
	if other.RequestDigest == intent.RequestDigest {
		t.Fatal("terminal digest omitted from native request")
	}
}
