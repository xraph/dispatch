//go:build linux

package managedhost

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
	"github.com/xraph/dispatch/operator"
)

func TestPreparedAbortRequiresExactRequestAndUnexpiredOriginalProof(t *testing.T) {
	identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "fixture", Namespace: "fixture"}, BuildID: "build"}, RuntimeID: "original"}, InstanceID: "original-boot", IdentityVersion: 1, BuildIdentity: durabletest.QueryIdentityFixture()}
	proof := durabletest.QueryProofFixture(identity, "actual-shape-fixture")
	now := proof.VerifiedAt
	fence := durable.QueryRemovalFence{Candidate: identity, CandidateStateVersion: 2, RemovalEpoch: 1, RequestID: "begin", AcceptedAt: now.Add(-time.Second)}
	binding := durable.QueryRuntimeBinding{QueryRuntimeIdentity: identity, Version: 2, State: durable.QueryRuntimeRemoving, Removal: fence, RemovalEpoch: 1}
	digest, err := durable.Fingerprint("query_runtime.removal_fence.v1", fence)
	if err != nil {
		t.Fatal(err)
	}
	record := nativePreparedAbort{Input: operator.QueryRemovalInput{RequestID: "abort"}, Request: durable.AbortQueryRemovalRequest{VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, RequestID: "abort", ExpectedVersion: 2, CommandDigest: strings.Repeat("a", 64), Verification: proof}, Fence: fence, Settlement: durable.QueryRemovalSettlement{FenceDigest: digest, VerifierID: identity.BuildIdentity.VerifierID, EvidenceDigest: strings.Repeat("b", 64), Disposition: durable.QueryRemovalSettledWithoutDeletion, ExternalOperationID: "settlement", SettledAt: now}}}
	if err = preparedAbortMatches(record, binding, fence, "abort", record.Request.CommandDigest, now); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"request", "digest", "expired", "future", "replacement-boot", "changed-fence", "already-active", "proof-before-settlement"} {
		t.Run(name, func(t *testing.T) {
			changed, current := record, binding
			when := now
			requestID, commandDigest := "abort", record.Request.CommandDigest
			switch name {
			case "request":
				requestID = "another-abort"
			case "digest":
				commandDigest = strings.Repeat("c", 64)
			case "expired":
				when = proof.ValidUntil
			case "future":
				when = proof.VerifiedAt.Add(-time.Nanosecond)
			case "replacement-boot":
				current.InstanceID = "replacement"
			case "changed-fence":
				current.Removal.RemovalEpoch++
			case "already-active":
				current.State = durable.QueryRuntimeActive
			case "proof-before-settlement":
				changed.Request.Settlement.SettledAt = now.Add(time.Second)
				when = now.Add(2 * time.Second)
			}
			if err = preparedAbortMatches(changed, current, fence, requestID, commandDigest, when); err == nil {
				t.Fatal("invalid prepared request accepted")
			}
		})
	}
}

func TestPreparedAbortRejectsTamperedJournalBeforeAuthorityRead(t *testing.T) {
	journal := &creationTestJournal{rows: map[string][]byte{}}
	host := &NativeGuardHost{key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32)), journal: journal}
	fence := durable.QueryRemovalFence{RequestID: "exact-fence"}
	record := nativePreparedAbort{Request: durable.AbortQueryRemovalRequest{Fence: fence}}
	raw, err := bounded(record)
	if err != nil {
		t.Fatal(err)
	}
	record.Signature = ed25519.Sign(host.key, raw)
	record.Input.RequestID = "changed-after-signature"
	address, err := nativeAbortKey(fence)
	if err != nil {
		t.Fatal(err)
	}
	journal.rows[address], err = bounded(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = host.loadAbort(t.Context(), fence); err == nil {
		t.Fatal("tampered journal accepted")
	}
	if _, _, err = host.VerifyAbort(t.Context(), durable.QueryRuntimeBinding{}, fence); err == nil {
		t.Fatal("legacy unbound verifier accepted")
	}
}
