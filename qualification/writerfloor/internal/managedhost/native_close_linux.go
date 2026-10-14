//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

type nativeCloseCore interface {
	NativeCore
	managed.RemovalCutoffReader
	ReadManagedRemovalRevalidation(context.Context, managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.RemovalRevalidationFacts], error)
	ReadManagedEffectiveRemovalCoverageExact(context.Context, managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.EffectiveRemovalCoverage], error)
}

type nativeCloseSources struct {
	Binding    nativeBinding                    `json:"binding"`
	Proof      managed.RecoveryAssociation      `json:"proof"`
	ProofFacts managed.RemovalRevalidationFacts `json:"proof_facts"`
	Coverage   managed.RecoveryAssociation      `json:"coverage"`
	Cutoff     managed.RecoveryAssociation      `json:"cutoff"`
	External   durable.QueryRemovalClosure      `json:"external"`
}
type nativePreparedClose struct {
	Sources   nativeCloseSources                    `json:"sources"`
	Input     operator.QueryGuardCloseInput         `json:"input"`
	Request   durable.QueryRemovalGuardCloseRequest `json:"request"`
	Signature []byte                                `json:"signature"`
}

func (h *NativeGuardHost) closeSources(ctx context.Context, key managed.AttemptBindingKey, proofReference, coverageReference managed.RecoveryAssociation) (nativeCloseSources, error) {
	var result nativeCloseSources
	core, ok := h.core.(nativeCloseCore)
	if !ok {
		return result, managed.ErrUnsupported
	}
	binding, err := h.readBinding(ctx, key)
	if err != nil {
		return result, err
	}
	proof, err := core.ReadManagedRemovalRevalidation(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: proofReference})
	if err != nil {
		return result, err
	}
	coverage, err := core.ReadManagedEffectiveRemovalCoverageExact(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: coverageReference})
	if err != nil {
		return result, err
	}
	cutoff, err := core.ReadManagedRemovalCutoffExact(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: coverage.Facts.Cutoff})
	if err != nil {
		return result, err
	}
	if proof.Facts.Binding != binding.Binding || coverage.Facts.Key != key.AttemptKey || coverage.Facts.ProviderOperationID != binding.Binding.ProviderOperationID || coverage.Facts.AdmissionDigest != binding.Binding.AdmissionDigest || cutoff.Facts.Key != key.AttemptKey || cutoff.Facts.ProviderOperationID != binding.Binding.ProviderOperationID || coverage.Facts.AttemptRef != cutoff.Facts.AttemptRef {
		return result, managed.ErrAuthority
	}
	disposition := ""
	switch coverage.Facts.Disposition {
	case managed.Removed:
		disposition = "all_positive"
	case managed.SettledWithoutDeletion:
		disposition = "settled_without_deletion"
	default:
		return result, managed.ErrAuthority
	}
	evidence, err := durable.Fingerprint("writerfloor.native.closure-evidence.v1", struct{ Cutoff, Coverage, Proof managed.RecoveryAssociation }{coverage.Facts.Cutoff, coverageReference, proofReference})
	if err != nil {
		return result, err
	}
	closure := durable.QueryRemovalClosure{ExternalScope: binding.Scope, CutoffReceiptID: cutoff.Acceptance.OperationID.String(), CutoffDigest: cutoff.Digest, CoverageDigest: binding.Scope.CoverageDigest, Disposition: disposition, TerminalDigest: coverage.Digest, CutoffAt: cutoff.Facts.CutoffAt, TerminalAt: coverage.Acceptance.Receipt.AcceptedAt, EvidenceDigest: evidence}
	closure.Attestation, err = h.attest("closure", closure)
	if err != nil {
		return result, err
	}
	return nativeCloseSources{Binding: binding, Proof: proofReference, ProofFacts: proof.Facts, Coverage: coverageReference, Cutoff: coverage.Facts.Cutoff, External: closure}, nil
}
func closePreparedKey(key managed.AttemptBindingKey, requestID string) (string, error) {
	if !durable.QueryRemovalToken(requestID) {
		return "", managed.ErrInvalid
	}
	return durable.Fingerprint("writerfloor.native.prepared-close.v1", struct {
		Key       managed.AttemptBindingKey
		RequestID string
	}{key, requestID})
}
func closeIntent(prepared nativePreparedClose) (managed.RecoveryNativeIntent, error) {
	raw, err := json.Marshal(prepared.Input)
	if err != nil {
		return managed.RecoveryNativeIntent{}, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.close.v1", prepared.Request)
	if err != nil {
		return managed.RecoveryNativeIntent{}, err
	}
	cutoff, coverage, proof := prepared.Sources.Cutoff, prepared.Sources.Coverage, prepared.Sources.Proof
	return managed.RecoveryNativeIntent{Binding: prepared.Sources.Binding.Binding, Profile: managed.RemovalRecoveryProfile, Operation: "close", RequestID: prepared.Input.RequestID, RequestDigest: digest, Command: raw, CommandDigest: managed.Digest(raw), Cutoff: &cutoff, Coverage: &coverage, Proof: &proof}, nil
}
func sameCloseSources(left, right nativeCloseSources) bool {
	if !sameNativeBinding(left.Binding, right.Binding) {
		return false
	}
	left.Binding = right.Binding
	return reflect.DeepEqual(left, right)
}
func (h *NativeGuardHost) loadClose(ctx context.Context, address string) (nativePreparedClose, error) {
	var result nativePreparedClose
	if !objectID(address) {
		return result, managed.ErrInvalid
	}
	raw, err := h.journal.Load(ctx, address)
	if err != nil {
		return result, err
	}
	if len(raw) == 0 {
		return result, durable.ErrNotFound
	}
	if exactDecode(raw, &result) != nil {
		return result, managed.ErrAuthority
	}
	signature := bytes.Clone(result.Signature)
	result.Signature = nil
	unsigned, err := bounded(result)
	result.Signature = signature
	if err != nil || !ed25519.Verify(ed25519.PublicKey(h.key[ed25519.SeedSize:]), unsigned, signature) || result.Request.Validate() != nil {
		return result, managed.ErrAuthority
	}
	key, err := closePreparedKey(result.Sources.Binding.Binding.Key, result.Input.RequestID)
	digest, digestErr := durable.Fingerprint("operator.query_runtime.close.v1", result.Input)
	if err != nil || digestErr != nil || key != address || result.Input.ClosureReference != address || result.Request.CommandDigest != digest || result.Request.Closure != result.Sources.External {
		return result, managed.ErrAuthority
	}
	current, err := h.closeSources(ctx, result.Sources.Binding.Binding.Key, result.Sources.Proof, result.Sources.Coverage)
	if err != nil {
		return result, err
	}
	if !sameCloseSources(current, result.Sources) {
		return result, managed.ErrAuthority
	}
	return result, nil
}

// PrepareClose consumes already sealed effective coverage. Its immutable receipt
// precedes this request, so the native digest never depends on a future acceptance.
func (h *NativeGuardHost) PrepareClose(ctx context.Context, key managed.AttemptBindingKey, proof, coverage managed.RecoveryAssociation, requestID string) (managed.RecoveryNativeIntent, error) {
	var zero managed.RecoveryNativeIntent
	address, err := closePreparedKey(key, requestID)
	if err != nil {
		return zero, err
	}
	saved, err := h.loadClose(ctx, address)
	if err == nil {
		if saved.Sources.Proof != proof || saved.Sources.Coverage != coverage {
			return zero, managed.ErrAuthority
		}
		return closeIntent(saved)
	}
	if !errors.Is(err, durable.ErrNotFound) {
		return zero, err
	}
	sources, err := h.closeSources(ctx, key, proof, coverage)
	if err != nil {
		return zero, err
	}
	original, err := durable.Fingerprint("dispatch.query-removal.original.v1", sources.Binding.Original)
	if err != nil {
		return zero, err
	}
	guard := durable.QueryRemovalGuardReference{QueryRuntimeTarget: sources.Binding.Original.QueryRuntimeTarget, GuardID: sources.ProofFacts.GuardID, RootDigest: sources.ProofFacts.GuardRootDigest, OriginalDigest: original}
	input := operator.QueryGuardCloseInput{QueryGuardInput: operator.QueryGuardInput{QueryReservationInput: sources.Binding.Reservation, GuardID: guard.GuardID, RootDigest: guard.RootDigest, OriginalDigest: guard.OriginalDigest}, RequestID: requestID, ClosureReference: address}
	digest, err := durable.Fingerprint("operator.query_runtime.close.v1", input)
	if err != nil {
		return zero, err
	}
	request := durable.QueryRemovalGuardCloseRequest{Guard: guard, RequestID: requestID, CommandDigest: digest, Closure: sources.External}
	if request.Validate() != nil {
		return zero, managed.ErrInvalid
	}
	prepared := nativePreparedClose{Sources: sources, Input: input, Request: request}
	unsigned, err := bounded(prepared)
	if err != nil {
		return zero, err
	}
	prepared.Signature = ed25519.Sign(h.key, unsigned)
	raw, err := bounded(prepared)
	if err != nil {
		return zero, err
	}
	accepted, err := h.journal.CompareAndSwap(ctx, address, nil, raw)
	if err != nil {
		return zero, err
	}
	if !accepted {
		return zero, managed.ErrBlocked
	}
	return closeIntent(prepared)
}

// VerifyClosure requires accepted cutoff, sealed effective coverage and the exact
// accepted close intent. Native protection is never closed from absent observations.
func (h *NativeGuardHost) VerifyClosure(ctx context.Context, guard durable.QueryRemovalGuardReference, address string) (durable.QueryRemovalClosure, error) {
	prepared, err := h.loadClose(ctx, address)
	if err != nil {
		return durable.QueryRemovalClosure{}, err
	}
	if guard != prepared.Request.Guard {
		return durable.QueryRemovalClosure{}, managed.ErrAuthority
	}
	expected, err := closeIntent(prepared)
	if err != nil {
		return durable.QueryRemovalClosure{}, err
	}
	if _, err = h.verifyIntentAnchor(ctx, address, expected); err != nil {
		return durable.QueryRemovalClosure{}, err
	}
	return prepared.Request.Closure, nil
}
func (h *NativeGuardHost) acceptedClose(ctx context.Context, producer managed.RecoveryProducerContext) (nativePreparedClose, managed.RecoveryAssociation, error) {
	var result nativePreparedClose
	var reference managed.RecoveryAssociation
	if producer.Intent.Facts.Operation != "close" {
		return result, reference, managed.ErrAuthority
	}
	var input operator.QueryGuardCloseInput
	if decodeNativeResponse(bytes.NewReader(producer.Intent.Facts.Command), &input) != nil {
		return result, reference, managed.ErrAuthority
	}
	result, err := h.loadClose(ctx, input.ClosureReference)
	if err != nil {
		return result, reference, err
	}
	expected, err := closeIntent(result)
	if err != nil {
		return result, reference, err
	}
	reference, err = h.retainIntentAnchor(ctx, input.ClosureReference, producer, expected)
	return result, reference, err
}
func nativeCloseAcceptance(receipt durable.QueryRemovalGuardCloseReceipt) operator.QueryGuardAcceptance {
	return operator.QueryGuardAcceptance{Kind: "close", GuardID: receipt.Request.Guard.GuardID, RootDigest: receipt.Request.Guard.RootDigest, RequestID: receipt.Request.RequestID, CommandDigest: receipt.Request.CommandDigest, ReceiptDigest: receipt.ReceiptDigest, CutoffDigest: receipt.Request.Closure.CutoffDigest, Disposition: receipt.Request.Closure.Disposition, AcceptedAt: receipt.AcceptedAt}
}
func (h *NativeGuardHost) normalizeClose(ctx context.Context, prepared nativePreparedClose, reference managed.RecoveryAssociation, wire operator.QueryGuardAcceptance) (managed.RecoveryNativeCloseFacts, error) {
	var facts managed.RecoveryNativeCloseFacts
	intent, err := closeIntent(prepared)
	if err != nil {
		return facts, err
	}
	lookup := durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: prepared.Request.Guard.QueryRuntimeTarget, RequestID: prepared.Request.RequestID, CommandDigest: prepared.Request.CommandDigest, RequestDigest: intent.RequestDigest}
	saved, err := h.native.LookupQueryRemovalGuardReceipt(ctx, durable.QueryRemovalGuardReceiptLookup{QueryRemovalRevalidationLookup: lookup, Kind: "close"})
	if err != nil {
		return facts, err
	}
	if saved.Close == nil || saved.Commit != nil || saved.Close.Match(lookup) != nil || !reflect.DeepEqual(saved.Close.Request, prepared.Request) || !reflect.DeepEqual(wire, nativeCloseAcceptance(*saved.Close)) {
		return facts, managed.ErrAuthority
	}
	raw, err := json.Marshal(wire)
	if err != nil || len(raw) > managed.MaxRecoveryExport {
		return facts, managed.ErrCapacity
	}
	attestation, err := h.attest("accepted-close", struct {
		Intent   managed.RecoveryAssociation
		Receipt  string
		Producer managed.RecoveryProducerIdentity
	}{reference, saved.Close.ReceiptDigest, h.identity})
	if err != nil {
		return facts, err
	}
	facts = managed.RecoveryNativeCloseFacts{Binding: prepared.Sources.Binding.Binding, Intent: reference, Cutoff: prepared.Sources.Cutoff, Coverage: prepared.Sources.Coverage, GuardID: wire.GuardID, GuardRootDigest: wire.RootDigest, NativeRequestID: wire.RequestID, NativeRequestDigest: saved.Close.RequestDigest, NativeReceiptDigest: saved.Close.ReceiptDigest, NativeAcceptedAt: wire.AcceptedAt, Export: managed.RecoveryNativeExport{Raw: raw, RawDigest: managed.Digest(raw), Attestation: []byte(attestation), AttestationDigest: managed.Digest([]byte(attestation)), ProducerID: h.identity.ProducerID, ProducerProfile: h.identity.Profile, ProducerVersion: h.identity.Version, VerifierID: h.identity.VerifierID}}
	return facts, managed.ValidateRecoveryNativeExport(facts.Export)
}

// CloseManagedRemovalGuard performs the exact retained native close request.
func (h *NativeGuardHost) CloseManagedRemovalGuard(ctx context.Context, producer managed.RecoveryProducerContext) (managed.RecoveryNativeCloseFacts, error) {
	prepared, reference, err := h.acceptedClose(ctx, producer)
	if err != nil {
		return managed.RecoveryNativeCloseFacts{}, err
	}
	receipt, err := h.client.CloseGuard(ctx, prepared.Input)
	if err != nil {
		return managed.RecoveryNativeCloseFacts{}, err
	}
	return h.normalizeClose(ctx, prepared, reference, receipt)
}

// RecoverManagedRemovalGuardClose reads permanent historical protection closure.
func (h *NativeGuardHost) RecoverManagedRemovalGuardClose(ctx context.Context, producer managed.RecoveryProducerContext) (managed.RecoveryNativeCloseFacts, error) {
	prepared, reference, err := h.acceptedClose(ctx, producer)
	if err != nil {
		return managed.RecoveryNativeCloseFacts{}, err
	}
	receipt, err := h.client.ReadGuardReceipt(ctx, operator.QueryGuardReceiptInput{QueryGuardReadInput: operator.QueryGuardReadInput{QueryReservationInput: prepared.Input.QueryReservationInput, RequestID: prepared.Input.RequestID, CommandDigest: prepared.Request.CommandDigest}, Kind: "close"})
	if err != nil {
		return managed.RecoveryNativeCloseFacts{}, err
	}
	return h.normalizeClose(ctx, prepared, reference, receipt)
}

// VerifyRemovalClose authenticates the accepted native request and full local
// closure dependencies again before normalized facts enter controller authority.
func (h *NativeGuardHost) VerifyRemovalClose(ctx context.Context, intent managed.RecoveryRecord[managed.RecoveryNativeIntent], facts managed.RecoveryNativeCloseFacts) error {
	expected, err := h.RecoverManagedRemovalGuardClose(ctx, managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: intent.Facts.Binding.Key.AttemptKey, Intent: intent})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, facts) {
		return managed.ErrAuthority
	}
	return nil
}

var _ managed.RemovalGuardCloseProducer = (*NativeGuardHost)(nil)
var _ durable.QueryRemovalGuardVerifier = (*NativeGuardHost)(nil)
