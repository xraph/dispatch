package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

// legacyOffsetStore captures Begin with the pre-canonicalization clock representation.
type legacyOffsetStore struct{ *Store }

func (s legacyOffsetStore) BeginQueryRuntimeRemoval(ctx context.Context, r durable.BeginQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	digest, err := durable.Fingerprint(string(durable.OperationBeginQueryRemoval), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return s.mutateQueryRuntimeAt(ctx, r.QueryRuntimeTarget, durable.OperationBeginQueryRemoval, r.RequestID, digest, "", nil, func(tx driver.Tx, build durable.BuildAdmission, candidate durable.QueryRuntimeBinding, _ bool, _ time.Time) (durable.QueryRuntimeBinding, error) {
		retained, bindings, readErr := queryBindings(ctx, tx, r.BuildTarget)
		if readErr != nil {
			return candidate, readErr
		}
		now, readErr := executionTime(ctx, tx)
		if readErr != nil {
			return candidate, readErr
		}
		return durable.ReserveQueryRemoval(candidate, r, build, retained, bindings, now.In(time.FixedZone("legacy", -5*3600)))
	}, executionTime)
}

func TestQueryGuardPreservesLegacyOffsetReceipt(t *testing.T) {
	s, _ := queryDiagnosticFixture(t)
	f := durabletest.NewQueryGuardFixture(t, legacyOffsetStore{s})
	before, err := json.Marshal(f.Begin)
	if err != nil || !bytes.Contains(before, []byte("-05:00")) {
		t.Fatalf("legacy offset fixture missing: %v", err)
	}
	if _, err = s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request); err != nil {
		t.Fatal(err)
	}
	replayed, err := s.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: f.Begin.RequestID, ExpectedVersion: f.Candidate.Version - 1})
	if err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(replayed)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("legacy Begin receipt bytes changed after guard acceptance: %v", err)
	}
	if err = f.Request.Original.MatchOriginal(replayed); err != nil {
		t.Fatalf("legacy fence digest or original association changed: %v", err)
	}
}
