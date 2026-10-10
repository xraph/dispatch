package durable_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryProofExactTemporalBoundaries(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: "n"}, BuildID: "b"}, RuntimeID: "r"}, InstanceID: "physical", IdentityVersion: 1, BuildIdentity: durabletest.QueryIdentityFixture()}
	cases := []struct {
		name      string
		at, until time.Time
		reason    string
	}{
		{"equality", now, now.Add(time.Minute), ""},
		{"future", now.Add(time.Microsecond), now.Add(time.Minute), "verified_future"},
		{"expiry-equality", now.Add(-time.Second), now, "expired"},
		{"before-expiry", now.Add(-time.Second), now.Add(time.Microsecond), ""},
		{"nonpositive", now, now, "expired"},
		{"overlong", now, now.Add(time.Minute + time.Microsecond), "overlong_validity"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := durabletest.QueryProofFixture(identity, "proof")
			p.VerifiedAt, p.ValidUntil = c.at, c.until
			binding := durable.QueryRuntimeBinding{QueryRuntimeIdentity: identity, State: durable.QueryRuntimeActive, Version: 1}
			next, err := durable.VerifyQueryBinding(binding, durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, ExpectedVersion: 1, Verification: p}, now)
			if c.reason == "" {
				if err != nil || next.Verification.AcceptedAt != now {
					t.Fatalf("valid boundary: %v", err)
				}
				return
			}
			d, ok := durable.QueryRejectionDetails(err)
			if !errors.Is(err, durable.ErrQueryRetention) || !ok || d.Reason != c.reason || d.ObservedAt != now || d.VerifiedAt != c.at || d.ValidUntil != c.until || next != binding {
				t.Fatalf("boundary lost diagnostic or mutated: %+v %v", d, err)
			}
		})
	}
}

func TestQueryProofBeforeSettlementRemainsRefused(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 1, 0, time.UTC)
	identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: "n"}, BuildID: "b"}, RuntimeID: "r"}, InstanceID: "physical", IdentityVersion: 1, BuildIdentity: durabletest.QueryIdentityFixture()}
	fence := durable.QueryRemovalFence{Candidate: identity, CandidateStateVersion: 2, RemovalEpoch: 1, RequestID: "remove", AcceptedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Second)}
	binding := durable.QueryRuntimeBinding{QueryRuntimeIdentity: identity, State: durable.QueryRuntimeRemoving, Version: 2, RemovalEpoch: 1, Removal: fence}
	settled := durabletest.QuerySettlementFixture(t, fence)
	settled.SettledAt = now
	proof := durabletest.QueryProofFixture(identity, "proof")
	proof.VerifiedAt = now.Add(-time.Microsecond)
	proof.ValidUntil = now.Add(time.Second)
	request := durable.AbortQueryRemovalRequest{VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, ExpectedVersion: 2, Verification: proof}, Fence: fence, Settlement: settled}
	after, err := durable.AbortQueryRemoval(binding, request, now)
	d, ok := durable.QueryRejectionDetails(err)
	if !errors.Is(err, durable.ErrQueryRetention) || !ok || d.Reason != "proof_before_settlement" || d.SettledAt != now || after != binding {
		t.Fatalf("settlement boundary changed: %+v %v", d, err)
	}
	request.Verification.VerifiedAt = now
	if _, err = durable.AbortQueryRemoval(binding, request, now); err != nil {
		t.Fatalf("settled proof equality refused: %v", err)
	}
}
