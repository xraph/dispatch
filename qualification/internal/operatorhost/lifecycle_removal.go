package operatorhost

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RemoveLocalQueryRuntime closes this fixture's routing for an exact reservation.
// It does not delete a provider instance. The controller and its state live only
// in this process; another incarnation cannot settle an older process's work.
func (h *Host) RemoveLocalQueryRuntime(ctx context.Context, fence durable.QueryRemovalFence) error {
	if h.lifecycle == nil {
		return durable.ErrQueryFence
	}
	return h.lifecycle.remove(ctx, fence)
}

func (h *lifecycleHost) remove(ctx context.Context, fence durable.QueryRemovalFence) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	digest, err := durable.Fingerprint("query_runtime.removal_fence.v1", fence)
	if err != nil {
		return err
	}
	identity, err := h.ResolveBinding(ctx, fence.Candidate.QueryRuntimeTarget)
	if err != nil || identity != fence.Candidate {
		return durable.ErrQueryFence
	}
	if h.removedRuntimes[identity.RuntimeID] == digest {
		return nil
	}
	if h.revokedOperations[digest] || h.removedRuntimes[identity.RuntimeID] != "" {
		return durable.ErrQueryFence
	}
	store, ok := h.host.Store.(durable.QueryRuntimeStore)
	if !ok {
		return durable.ErrQueryFence
	}
	if _, err = store.CheckQueryRuntimeRemoval(ctx, fence); err != nil {
		return err
	}
	status := h.host.runtime.workers[identity.BuildID].Status()
	if !status.AdmissionClosed || status.InFlight != 0 || status.UnknownClaims != 0 {
		return durable.ErrQueryFence
	}
	if !time.Now().Before(fence.ValidUntil) {
		return durable.ErrQueryFence
	}
	h.removedRuntimes[identity.RuntimeID] = digest
	return nil
}

func (h *lifecycleHost) VerifyRemoved(ctx context.Context, fence durable.QueryRemovalFence) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	identity, err := h.ResolveBinding(ctx, fence.Candidate.QueryRuntimeTarget)
	if err != nil || identity != fence.Candidate {
		return durable.ErrQueryFence
	}
	digest, err := durable.Fingerprint("query_runtime.removal_fence.v1", fence)
	if err != nil || h.removedRuntimes[identity.RuntimeID] != digest {
		return durable.ErrQueryFence
	}
	return nil
}

func (h *lifecycleHost) VerifyAbort(ctx context.Context, binding durable.QueryRuntimeBinding, fence durable.QueryRemovalFence) (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fail := func() (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, durable.ErrQueryFence
	}
	if binding.QueryRuntimeIdentity != fence.Candidate || binding.Removal != fence || binding.State != durable.QueryRuntimeRemoving {
		return fail()
	}
	identity, err := h.ResolveBinding(ctx, binding.QueryRuntimeTarget)
	if err != nil || identity != binding.QueryRuntimeIdentity || h.removedRuntimes[identity.RuntimeID] != "" {
		return fail()
	}
	digest, err := durable.Fingerprint("query_runtime.removal_fence.v1", fence)
	if err != nil {
		return fail()
	}
	// This assignment and local issuance use the same lock. A failed fresh probe
	// keeps issuance revoked but does not release the persisted reservation.
	h.revokedOperations[digest] = true
	now, err := h.queryTime(ctx, binding.QueryRuntimeTarget)
	if err != nil {
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, err
	}
	evidence, err := durable.Fingerprint("operator_host.local_issuance_revoked.v1", struct {
		Fence   string
		Runtime string
		At      time.Time
	}{digest, identity.RuntimeID, now})
	if err != nil {
		return fail()
	}
	settlement := durable.QueryRemovalSettlement{FenceDigest: digest, VerifierID: identity.BuildIdentity.VerifierID, EvidenceDigest: evidence, Disposition: durable.QueryRemovalNotIssued, ExternalOperationID: digest, SettledAt: now}
	proof, err := h.verify(ctx, binding)
	return settlement, proof, err
}
