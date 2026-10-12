package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// fixtureRemovalController owns this test's operation issuance. It is not a
// provider cancellation adapter and cannot settle any issued deletion.
type fixtureRemovalController struct {
	mu          sync.Mutex
	fence       durable.QueryRemovalFence
	operationID string
	issued      bool
	revoked     bool
	probe       func() durable.QueryRuntimeVerification
}

var _ durable.QueryRemovalAbortVerifier = (*fixtureRemovalController)(nil)

func (c *fixtureRemovalController) dispatch(f durable.QueryRemovalFence) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fence != f || c.revoked || c.issued {
		return durable.ErrQueryFence
	}
	c.issued = true
	return nil
}
func (c *fixtureRemovalController) VerifyAbort(ctx context.Context, b durable.QueryRuntimeBinding, f durable.QueryRemovalFence) (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
	c.mu.Lock()
	if ctx.Err() != nil || c.fence != f || b.Removal != f || b.QueryRuntimeIdentity != f.Candidate || c.issued {
		c.mu.Unlock()
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, durable.ErrQueryFence
	}
	c.revoked = true // This same mutex gates every dispatch by this owned controller.
	c.mu.Unlock()
	digest, err := durable.Fingerprint("query_runtime.removal_fence.v1", f)
	if err != nil {
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, err
	}
	evidence, err := durable.Fingerprint("fixture-removal-issuance-revoked.v1", struct{ Fence, Operation string }{digest, c.operationID})
	if err != nil {
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, err
	}
	settlement := durable.QueryRemovalSettlement{FenceDigest: digest, VerifierID: f.Candidate.BuildIdentity.VerifierID, EvidenceDigest: evidence, Disposition: durable.QueryRemovalNotIssued, ExternalOperationID: c.operationID, SettledAt: durable.Timestamp(time.Now())}
	return settlement, c.probe(), nil
}

func TestQueryAbortControllerUnknownDeletion(t *testing.T) {
	f := durable.QueryRemovalFence{RequestID: "reservation", RemovalEpoch: 1}
	b := durable.QueryRuntimeBinding{Removal: f}
	probeCalled := false
	controller := fixtureRemovalController{fence: f, operationID: "owned-operation", probe: func() durable.QueryRuntimeVerification { probeCalled = true; return durable.QueryRuntimeVerification{} }}
	if err := controller.dispatch(f); err != nil {
		t.Fatal(err)
	}
	// The deletion has been issued but its provider outcome is still unknown.
	// Even if a query would still succeed, this controller cannot reactivate it.
	if _, _, err := controller.VerifyAbort(t.Context(), b, f); err == nil {
		t.Fatal("issued unknown deletion settled")
	}
	if probeCalled {
		t.Fatal("healthy query used to settle unknown deletion")
	}
}
