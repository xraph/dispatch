//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"reflect"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

type nativeTerminalCore interface {
	nativeCloseCore
	ReadManagedRemovalGuardClose(context.Context, managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.RecoveryNativeCloseFacts], error)
}

type nativeTerminalAnchor struct {
	Key       managed.AttemptKey          `json:"key"`
	Close     managed.RecoveryAssociation `json:"close"`
	Fence     durable.QueryRemovalFence   `json:"fence"`
	Signature []byte                      `json:"signature"`
}

func nativeTerminalKey(fence durable.QueryRemovalFence) (string, error) {
	return durable.Fingerprint("writerfloor.native.terminal-anchor.v1", fence)
}

// RetainGuardClose fixes an independently accepted core closure for the exact
// original fence before native Finish or Abort. It never dispatches a mutation.
func (h *NativeGuardHost) RetainGuardClose(ctx context.Context, key managed.AttemptKey, record managed.RecoveryRecord[managed.RecoveryNativeCloseFacts]) error {
	core, ok := h.core.(nativeTerminalCore)
	if !ok {
		return managed.ErrUnsupported
	}
	reference, err := managed.RecoveryRecordAssociation(key, record)
	if err != nil {
		return err
	}
	saved, err := core.ReadManagedRemovalGuardClose(ctx, managed.RecoveryExactRequest{Key: key, Association: reference})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(saved, record) {
		return managed.ErrAuthority
	}
	intent, err := h.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: key, Association: record.Facts.Intent})
	if err != nil {
		return err
	}
	recovered, err := h.RecoverManagedRemovalGuardClose(ctx, managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: key, Intent: intent})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(recovered, record.Facts) {
		return managed.ErrAuthority
	}
	prepared, _, err := h.acceptedClose(ctx, managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: key, Intent: intent})
	if err != nil {
		return err
	}
	anchor := nativeTerminalAnchor{Key: key, Close: reference, Fence: prepared.Sources.Binding.Candidate.Removal}
	if anchor.Fence.Candidate != prepared.Sources.Binding.Candidate.QueryRuntimeIdentity {
		return managed.ErrAuthority
	}
	address, err := nativeTerminalKey(anchor.Fence)
	if err != nil {
		return err
	}
	unsigned, err := bounded(anchor)
	if err != nil {
		return err
	}
	anchor.Signature = ed25519.Sign(h.key, unsigned)
	raw, err := bounded(anchor)
	if err != nil {
		return err
	}
	old, err := h.journal.Load(ctx, address)
	if err != nil {
		return err
	}
	if len(old) > 0 {
		if !bytes.Equal(old, raw) {
			return managed.ErrAuthority
		}
		return nil
	}
	accepted, err := h.journal.CompareAndSwap(ctx, address, nil, raw)
	if err != nil {
		return err
	}
	if !accepted {
		return managed.ErrBlocked
	}
	return nil
}
func (h *NativeGuardHost) terminalClose(ctx context.Context, fence durable.QueryRemovalFence) (nativePreparedClose, managed.RecoveryAssociation, error) {
	var prepared nativePreparedClose
	var anchor nativeTerminalAnchor
	core, ok := h.core.(nativeTerminalCore)
	if !ok {
		return prepared, anchor.Close, managed.ErrUnsupported
	}
	address, err := nativeTerminalKey(fence)
	if err != nil {
		return prepared, anchor.Close, err
	}
	raw, err := h.journal.Load(ctx, address)
	if err != nil {
		return prepared, anchor.Close, err
	}
	if exactDecode(raw, &anchor) != nil {
		return prepared, anchor.Close, managed.ErrAuthority
	}
	signature := anchor.Signature
	anchor.Signature = nil
	unsigned, err := bounded(anchor)
	anchor.Signature = signature
	if err != nil || !ed25519.Verify(ed25519.PublicKey(h.key[ed25519.SeedSize:]), unsigned, signature) || !reflect.DeepEqual(anchor.Fence, fence) {
		return prepared, anchor.Close, managed.ErrAuthority
	}
	saved, err := core.ReadManagedRemovalGuardClose(ctx, managed.RecoveryExactRequest{Key: anchor.Key, Association: anchor.Close})
	if err != nil {
		return prepared, anchor.Close, err
	}
	intent, err := h.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: anchor.Key, Association: saved.Facts.Intent})
	if err != nil {
		return prepared, anchor.Close, err
	}
	producer := managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: anchor.Key, Intent: intent}
	recovered, err := h.RecoverManagedRemovalGuardClose(ctx, producer)
	if err != nil {
		return prepared, anchor.Close, err
	}
	if !reflect.DeepEqual(recovered, saved.Facts) {
		return prepared, anchor.Close, managed.ErrAuthority
	}
	prepared, _, err = h.acceptedClose(ctx, producer)
	if err != nil {
		return prepared, anchor.Close, err
	}
	if !reflect.DeepEqual(prepared.Sources.Binding.Candidate.Removal, fence) {
		return prepared, anchor.Close, managed.ErrAuthority
	}
	return prepared, anchor.Close, nil
}

// ResolveBinding routes only to one fixed enrolled runtime host.
func (h *NativeGuardHost) ResolveBinding(ctx context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	runtime, err := h.runtimeFor(target)
	if err != nil {
		return durable.QueryRuntimeIdentity{}, err
	}
	return runtime.ResolveBinding(ctx, target)
}

// EnrolledIdentity reads the configured creation/boot association without a liveness claim.
func (h *NativeGuardHost) EnrolledIdentity(target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	runtime, err := h.runtimeFor(target)
	if err != nil {
		return durable.QueryRuntimeIdentity{}, err
	}
	return runtime.EnrolledIdentity(target)
}

// WorkerControl returns the authenticated handle fixed to the original process boot.
func (h *NativeGuardHost) WorkerControl(ctx context.Context, target durable.QueryRuntimeTarget) (operator.WorkerControl, error) {
	runtime, err := h.runtimeFor(target)
	if err != nil {
		return nil, err
	}
	return runtime.WorkerControl(ctx, target)
}

// Verify executes the real configured retained-query probe on the exact runtime.
func (h *NativeGuardHost) Verify(ctx context.Context, binding durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	runtime, err := h.runtimeFor(binding.QueryRuntimeTarget)
	if err != nil {
		return durable.QueryRuntimeVerification{}, err
	}
	return runtime.Verify(ctx, binding)
}

// VerifyRemoved requires accepted full positive coverage and permanent native
// closure, including affirmative discharge of the exact original process.
func (h *NativeGuardHost) VerifyRemoved(ctx context.Context, fence durable.QueryRemovalFence) error {
	prepared, _, err := h.terminalClose(ctx, fence)
	if err != nil {
		return err
	}
	if prepared.Request.Closure.Disposition != "all_positive" || prepared.Sources.ProofFacts.ProcessDisposition != "effect_complete" || prepared.Sources.ProofFacts.EffectComplete == nil {
		return managed.ErrAuthority
	}
	return nil
}

// freshAbort requires independently accepted nondeletion coverage and permanent
// closure before it obtains a fresh query proof from the original surviving boot.
func (h *NativeGuardHost) freshAbort(ctx context.Context, binding durable.QueryRuntimeBinding, fence durable.QueryRemovalFence) (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
	var settlement durable.QueryRemovalSettlement
	var proof durable.QueryRuntimeVerification
	if binding.QueryRuntimeIdentity != fence.Candidate || binding.Removal != fence || binding.State != durable.QueryRuntimeRemoving {
		return settlement, proof, managed.ErrAuthority
	}
	prepared, reference, err := h.terminalClose(ctx, fence)
	if err != nil {
		return settlement, proof, err
	}
	if prepared.Request.Closure.Disposition != "settled_without_deletion" {
		return settlement, proof, managed.ErrAuthority
	}
	proof, err = h.Verify(ctx, binding)
	if err != nil {
		return settlement, proof, err
	}
	digest, err := durable.Fingerprint("query_runtime.removal_fence.v1", fence)
	if err != nil {
		return settlement, proof, err
	}
	evidence, err := durable.Fingerprint("writerfloor.native.abort-settlement.v1", struct {
		Close   managed.RecoveryAssociation
		Closure durable.QueryRemovalClosure
	}{reference, prepared.Request.Closure})
	if err != nil {
		return settlement, proof, err
	}
	settlement = durable.QueryRemovalSettlement{FenceDigest: digest, VerifierID: fence.Candidate.BuildIdentity.VerifierID, EvidenceDigest: evidence, Disposition: durable.QueryRemovalSettledWithoutDeletion, ExternalOperationID: prepared.Sources.Binding.Binding.ProviderOperationID.String(), SettledAt: prepared.Request.Closure.TerminalAt}
	if settlement.Validate(fence, proof.VerifiedAt) != nil {
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, managed.ErrAuthority
	}
	return settlement, proof, nil
}

var _ operator.QueryRuntimeHost = (*NativeGuardHost)(nil)
var _ operator.QueryRemovalCompletionVerifier = (*NativeGuardHost)(nil)
var _ durable.QueryRemovalAbortVerifier = (*NativeGuardHost)(nil)
