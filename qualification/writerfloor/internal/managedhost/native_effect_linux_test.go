//go:build linux

package managedhost

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

func TestNativeEffectRequiresExactCompleteCreationAndPositiveResponse(t *testing.T) {
	resource, incarnation := strings.Repeat("a", 64), strings.Repeat("b", 64)
	observed := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	creation := launcher.ServiceBinding{ResourceID: resource, Incarnation: incarnation, BootNonce: strings.Repeat("c", 64), CreationDigest: strings.Repeat("d", 64)}
	root := managed.ProviderStepRetryRoot{ResourceID: resource, Incarnation: incarnation}
	result := managed.WireProviderStepResult{Outcome: managed.PortTerminal, ResourceID: resource, ObservedAt: observed}
	record := docker.ManagedDockerRecord{State: "complete", Response: &docker.ManagedDockerResponse{ResourceID: resource, ObservedAt: observed}, Custody: &docker.ManagedDockerCustody{ResourceID: resource, Incarnation: incarnation}}
	if !nativePositiveCustodyMatches(creation, root, result, record) {
		t.Fatal("exact positive custody refused")
	}
	for _, state := range []string{"prepared", "delegated", "observed", "closed"} {
		changed := record
		changed.State = state
		if nativePositiveCustodyMatches(creation, root, result, changed) {
			t.Fatalf("state %s discharged process", state)
		}
	}
	changedResult := result
	changedResult.Outcome = managed.PortUnavailable
	if nativePositiveCustodyMatches(creation, root, changedResult, record) {
		t.Fatal("uncertainty discharged process")
	}
	changedResult = result
	changedResult.ObservedAt = observed.Add(time.Nanosecond)
	if nativePositiveCustodyMatches(creation, root, changedResult, record) {
		t.Fatal("different response adopted")
	}
	changedCreation := creation
	changedCreation.Incarnation = strings.Repeat("e", 64)
	if nativePositiveCustodyMatches(changedCreation, root, result, record) {
		t.Fatal("replacement incarnation discharged original")
	}
	changedCreation = creation
	changedCreation.BootNonce = ""
	if nativePositiveCustodyMatches(changedCreation, root, result, record) {
		t.Fatal("unbound process discharged")
	}
}
func TestNativeLiveJournalSignatureBindsCreationAndCompleteEncodedCap(t *testing.T) {
	host := &NativeGuardHost{key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32))}
	creation := launcher.ServiceBinding{ResourceID: strings.Repeat("a", 64), Incarnation: strings.Repeat("b", 64), BootNonce: strings.Repeat("c", 64)}
	prepared, err := host.signPrepared(nativePreparedLive{Creation: &creation})
	if err != nil {
		t.Fatal(err)
	}
	signature := prepared.Signature
	prepared.Signature = nil
	raw, err := bounded(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(ed25519.PublicKey(host.key[ed25519.SeedSize:]), raw, signature) {
		t.Fatal("signed creation tuple refused")
	}
	prepared.Creation.Incarnation = strings.Repeat("d", 64)
	raw, err = bounded(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if ed25519.Verify(ed25519.PublicKey(host.key[ed25519.SeedSize:]), raw, signature) {
		t.Fatal("changed creation tuple retained signature")
	}
	prepared = nativePreparedLive{Profile: ""}
	base, err := host.signPrepared(prepared)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	prepared.Profile = strings.Repeat("a", docker.ManagedDockerJournalLimit-len(encoded))
	accepted, err := host.signPrepared(prepared)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(accepted)
	if err != nil || len(encoded) != docker.ManagedDockerJournalLimit {
		t.Fatal("complete journal cap not measured")
	}
	prepared.Profile += "a"
	if _, err = host.signPrepared(prepared); err == nil {
		t.Fatal("complete journal cap plus one accepted")
	}
}

func TestNativePositiveWireSurvivesPreparedRequestRoundTrip(t *testing.T) {
	observed := time.Date(2026, time.October, 14, 12, 0, 0, 123456789, time.UTC)
	original := durable.QueryRemovalPositiveEffect{PermissionID: "permission", PermissionDigest: "permission-digest", ResultID: "result", ResultDigest: "result-digest", CommandID: "command", ResourceID: "resource", IncarnationID: "incarnation", AssociationDigest: "exact-custody-association", PreEffectDigest: "exact-pre-effect", PreEffectAt: observed.Add(-time.Second), AcceptedAt: observed.Add(-time.Second), CompletedAt: observed}
	positive := nativePositiveWire(original)
	request := (durable.QueryRemovalRevalidationRequest{Evidence: durable.QueryRemovalEvidence{Positive: positive}}).Clone()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var retained durable.QueryRemovalRevalidationRequest
	if err = json.Unmarshal(raw, &retained); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nativePositiveWire(original), retained.Evidence.Positive) {
		t.Fatal("retained positive differs from exact source reconstruction")
	}
	expected := original
	expected.PreEffectAt = durable.Timestamp(expected.PreEffectAt)
	expected.AcceptedAt = durable.Timestamp(expected.AcceptedAt)
	expected.CompletedAt = durable.Timestamp(expected.CompletedAt)
	if !reflect.DeepEqual(positive, expected) {
		t.Fatal("normalization changed positive associations")
	}
	if original.CompletedAt != observed || positive.CompletedAt.Equal(observed) {
		t.Fatal("test must retain exact custody time and exercise sub-microsecond normalization")
	}
}
