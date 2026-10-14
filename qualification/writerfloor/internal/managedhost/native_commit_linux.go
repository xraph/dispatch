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

type nativeCommitCore interface {
	NativeCore
	managed.ProviderInvocationSourcesStore
	ReadManagedProviderInvocation(context.Context, managed.ProviderInvocationKey) (managed.ProviderInvocationRecord, error)
	ReadManagedRemovalRevalidation(context.Context, managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.RemovalRevalidationFacts], error)
}

type nativeCommitSources struct {
	Binding          nativeBinding                    `json:"binding"`
	Invocation       managed.ProviderInvocationKey    `json:"invocation"`
	Permission       managed.RecoveryAssociation      `json:"permission"`
	PermissionDigest string                           `json:"permission_digest"`
	PermissionProof  managed.RecoveryAssociation      `json:"permission_proof"`
	Current          managed.RecoveryAssociation      `json:"current"`
	PermissionNative managed.RemovalRevalidationFacts `json:"permission_native"`
	CurrentNative    managed.RemovalRevalidationFacts `json:"current_native"`
	External         durable.QueryRemovalPermission   `json:"external"`
}

type nativePreparedCommit struct {
	Sources   nativeCommitSources               `json:"sources"`
	Input     operator.QueryCommitInput         `json:"input"`
	Request   durable.QueryRemovalCommitRequest `json:"request"`
	Signature []byte                            `json:"signature"`
}

func (h *NativeGuardHost) commitSources(ctx context.Context, key managed.AttemptBindingKey, invocation managed.ProviderInvocationKey, current managed.RecoveryAssociation) (nativeCommitSources, error) {
	var result nativeCommitSources
	core, ok := h.core.(nativeCommitCore)
	if !ok {
		return result, managed.ErrUnsupported
	}
	if key.AttemptKey != invocation.AttemptKey {
		return result, managed.ErrAuthority
	}
	binding, err := h.readBinding(ctx, key)
	if err != nil {
		return result, err
	}
	selected, err := core.ReadManagedProviderInvocation(ctx, invocation)
	if err != nil {
		return result, err
	}
	sources, err := core.ReadManagedProviderInvocationSources(ctx, invocation)
	if err != nil {
		return result, err
	}
	permissionDigest, err := fullPermissionDigest(selected.Permission.Facts.Permission)
	if err != nil {
		return result, err
	}
	permission, err := managed.RecoveryRecordAssociation(invocation.AttemptKey, selected.Permission)
	if err != nil {
		return result, err
	}
	if sources.Root.Digest != selected.Permission.Facts.RootDigest || sources.Root.Facts.Step != invocation.Step || len(sources.PermissionProofs) < 1 || len(sources.PermissionProofs) > 64 {
		return result, managed.ErrAuthority
	}
	latest, err := core.ReadManagedRemovalRevalidation(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: current})
	if err != nil {
		return result, err
	}
	if latest.Facts.Binding != binding.Binding || latest.Facts.ProcessDisposition != "live" {
		return result, managed.ErrAuthority
	}
	found := false
	var accepted managed.RecoveryRecord[managed.RemovalRevalidationFacts]
	var proofReference managed.RecoveryAssociation
	for _, member := range sources.PermissionProofs {
		if member.Facts.Invocation != invocation || member.Facts.PermissionDigest != permissionDigest {
			return result, managed.ErrAuthority
		}
		proof, readErr := core.ReadManagedRemovalRevalidation(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: member.Facts.Proof})
		if readErr != nil {
			return result, readErr
		}
		if member.Facts.NativeMaterialDigest != proof.Facts.NativeMaterialDigest {
			return result, managed.ErrAuthority
		}
		if proof.Facts.Binding != binding.Binding {
			continue
		}
		if found {
			return result, managed.ErrAuthority
		}
		found = true
		accepted = proof
		proofReference, err = managed.RecoveryRecordAssociation(key.AttemptKey, member)
		if err != nil {
			return result, err
		}
	}
	if !found || accepted.Facts.ProcessDisposition != "live" || accepted.Facts.GuardID != latest.Facts.GuardID || accepted.Facts.GuardRootDigest != latest.Facts.GuardRootDigest || accepted.Facts.NativeScopeDigest != latest.Facts.NativeScopeDigest {
		return result, managed.ErrAuthority
	}
	step, err := durable.Fingerprint("writerfloor.native.provider-step.v1", invocation.Step)
	if err != nil {
		return result, err
	}
	evidence, err := durable.Fingerprint("writerfloor.native.permission-evidence.v1", struct {
		Root              string
		Permission, Proof managed.RecoveryAssociation
	}{sources.Root.Digest, permission, proofReference})
	if err != nil {
		return result, err
	}
	external := durable.QueryRemovalPermission{ExternalScope: binding.Scope, PermissionID: selected.Permission.Acceptance.OperationID.String(), PermissionDigest: permissionDigest, CommandID: invocation.CommandID.String(), StepDigest: step, ResourceDigest: managed.Digest([]byte(sources.Root.Facts.ResourceID)), IncarnationDigest: managed.Digest([]byte(sources.Root.Facts.Incarnation)), RevalidationRequestID: accepted.Facts.NativeRequestID, RevalidationDigest: accepted.Facts.NativeMaterialDigest, AcceptedAt: selected.Permission.Acceptance.Receipt.AcceptedAt, EvidenceDigest: evidence}
	external.Attestation, err = h.attest("permission", external)
	if err != nil {
		return result, err
	}
	result = nativeCommitSources{Binding: binding, Invocation: invocation, Permission: permission, PermissionDigest: permissionDigest, PermissionProof: proofReference, Current: current, PermissionNative: accepted.Facts, CurrentNative: latest.Facts, External: external}
	return result, nil
}
func commitPreparedKey(key managed.AttemptBindingKey, requestID string) (string, error) {
	if !durable.QueryRemovalToken(requestID) {
		return "", managed.ErrInvalid
	}
	return durable.Fingerprint("writerfloor.native.prepared-commit.v1", struct {
		Key       managed.AttemptBindingKey
		RequestID string
	}{key, requestID})
}
func commitIntent(prepared nativePreparedCommit) (managed.RecoveryNativeIntent, error) {
	raw, err := json.Marshal(prepared.Input)
	if err != nil {
		return managed.RecoveryNativeIntent{}, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.commit.v1", prepared.Request)
	if err != nil {
		return managed.RecoveryNativeIntent{}, err
	}
	permission, current := prepared.Sources.Permission, prepared.Sources.Current
	return managed.RecoveryNativeIntent{Binding: prepared.Sources.Binding.Binding, Profile: managed.RemovalRecoveryProfile, Operation: "commit", RequestID: prepared.Input.RequestID, RequestDigest: digest, Command: raw, CommandDigest: managed.Digest(raw), Permission: &permission, Proof: &current}, nil
}
func (h *NativeGuardHost) loadCommit(ctx context.Context, address string) (nativePreparedCommit, error) {
	var result nativePreparedCommit
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
	key, err := commitPreparedKey(result.Sources.Binding.Binding.Key, result.Input.RequestID)
	digest, digestErr := durable.Fingerprint("operator.query_runtime.commit.v1", result.Input)
	if err != nil || digestErr != nil || key != address || result.Input.PermissionReference != address || result.Request.CommandDigest != digest || result.Request.Permission != result.Sources.External {
		return result, managed.ErrAuthority
	}
	current, err := h.commitSources(ctx, result.Sources.Binding.Binding.Key, result.Sources.Invocation, result.Sources.Current)
	if err != nil {
		return result, err
	}
	if !sameCommitSources(current, result.Sources) {
		return result, managed.ErrAuthority
	}
	return result, nil
}

// PrepareCommit fixes the exact original permission proof and separately selected
// current proof before core intent acceptance. It does not grant transport.
func (h *NativeGuardHost) PrepareCommit(ctx context.Context, key managed.AttemptBindingKey, invocation managed.ProviderInvocationKey, current managed.RecoveryAssociation, requestID string) (managed.RecoveryNativeIntent, error) {
	var zero managed.RecoveryNativeIntent
	address, err := commitPreparedKey(key, requestID)
	if err != nil {
		return zero, err
	}
	saved, err := h.loadCommit(ctx, address)
	if err == nil {
		if saved.Sources.Invocation != invocation || saved.Sources.Current != current {
			return zero, managed.ErrAuthority
		}
		return commitIntent(saved)
	}
	if !errors.Is(err, durable.ErrNotFound) {
		return zero, err
	}
	sources, err := h.commitSources(ctx, key, invocation, current)
	if err != nil {
		return zero, err
	}
	proof := sources.CurrentNative
	original, err := durable.Fingerprint("dispatch.query-removal.original.v1", sources.Binding.Original)
	if err != nil {
		return zero, err
	}
	guard := durable.QueryRemovalGuardReference{QueryRuntimeTarget: sources.Binding.Original.QueryRuntimeTarget, GuardID: proof.GuardID, RootDigest: proof.GuardRootDigest, OriginalDigest: original}
	input := operator.QueryCommitInput{QueryGuardInput: operator.QueryGuardInput{QueryReservationInput: sources.Binding.Reservation, GuardID: guard.GuardID, RootDigest: guard.RootDigest, OriginalDigest: guard.OriginalDigest}, RequestID: requestID, PermissionReference: address, ExpectedCurrentDigest: proof.NativeMaterialDigest, CurrentRequestID: proof.NativeRequestID}
	digest, err := durable.Fingerprint("operator.query_runtime.commit.v1", input)
	if err != nil {
		return zero, err
	}
	request := durable.QueryRemovalCommitRequest{Guard: guard, RequestID: requestID, CommandDigest: digest, ExpectedCurrentDigest: proof.NativeMaterialDigest, CurrentRequestID: proof.NativeRequestID, Permission: sources.External}
	if request.Validate() != nil {
		return zero, managed.ErrInvalid
	}
	prepared := nativePreparedCommit{Sources: sources, Input: input, Request: request}
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
	return commitIntent(prepared)
}

func (h *NativeGuardHost) retainIntentAnchor(ctx context.Context, address string, producer managed.RecoveryProducerContext, expected managed.RecoveryNativeIntent) (managed.RecoveryAssociation, error) {
	reference, err := managed.RecoveryRecordAssociation(producer.Key, producer.Intent)
	if err != nil {
		return reference, err
	}
	saved, err := h.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: producer.Key, Association: reference})
	if err != nil {
		return reference, err
	}
	if producer.Profile != managed.RemovalRecoveryProfile || !reflect.DeepEqual(saved, producer.Intent) || !reflect.DeepEqual(expected, saved.Facts) {
		return reference, managed.ErrAuthority
	}
	anchor := nativeIntentAnchor{Key: producer.Key, Intent: reference}
	raw, err := bounded(anchor)
	if err != nil {
		return reference, err
	}
	anchor.Signature = ed25519.Sign(h.key, raw)
	raw, err = bounded(anchor)
	if err != nil {
		return reference, err
	}
	key := h.anchorKey(address)
	prior, err := h.journal.Load(ctx, key)
	if err != nil {
		return reference, err
	}
	if len(prior) > 0 {
		if !bytes.Equal(prior, raw) {
			return reference, managed.ErrAuthority
		}
		return reference, nil
	}
	accepted, err := h.journal.CompareAndSwap(ctx, key, nil, raw)
	if err != nil {
		return reference, err
	}
	if !accepted {
		return reference, managed.ErrBlocked
	}
	return reference, nil
}
func (h *NativeGuardHost) verifyIntentAnchor(ctx context.Context, address string, expected managed.RecoveryNativeIntent) (managed.RecoveryAssociation, error) {
	var anchor nativeIntentAnchor
	raw, err := h.journal.Load(ctx, h.anchorKey(address))
	if err != nil {
		return anchor.Intent, err
	}
	if exactDecode(raw, &anchor) != nil {
		return anchor.Intent, managed.ErrAuthority
	}
	signature := anchor.Signature
	anchor.Signature = nil
	unsigned, err := bounded(anchor)
	anchor.Signature = signature
	if err != nil || !ed25519.Verify(ed25519.PublicKey(h.key[ed25519.SeedSize:]), unsigned, signature) || anchor.Key != expected.Binding.Key.AttemptKey {
		return anchor.Intent, managed.ErrAuthority
	}
	saved, err := h.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: anchor.Key, Association: anchor.Intent})
	if err != nil {
		return anchor.Intent, err
	}
	if !reflect.DeepEqual(saved.Facts, expected) {
		return anchor.Intent, managed.ErrAuthority
	}
	return anchor.Intent, nil
}

// VerifyPermission authenticates exact historical accepted inputs and the retained
// native intent. The native store independently checks current guard freshness.
func (h *NativeGuardHost) VerifyPermission(ctx context.Context, guard durable.QueryRemovalGuardReference, address string) (durable.QueryRemovalPermission, error) {
	prepared, err := h.loadCommit(ctx, address)
	if err != nil {
		return durable.QueryRemovalPermission{}, err
	}
	if guard != prepared.Request.Guard {
		return durable.QueryRemovalPermission{}, managed.ErrAuthority
	}
	expected, err := commitIntent(prepared)
	if err != nil {
		return durable.QueryRemovalPermission{}, err
	}
	if _, err = h.verifyIntentAnchor(ctx, address, expected); err != nil {
		return durable.QueryRemovalPermission{}, err
	}
	return prepared.Request.Permission, nil
}
func (h *NativeGuardHost) acceptedCommit(ctx context.Context, producer managed.RecoveryProducerContext) (nativePreparedCommit, managed.RecoveryAssociation, error) {
	var result nativePreparedCommit
	var reference managed.RecoveryAssociation
	if producer.Intent.Facts.Operation != "commit" {
		return result, reference, managed.ErrAuthority
	}
	var input operator.QueryCommitInput
	if decodeNativeResponse(bytes.NewReader(producer.Intent.Facts.Command), &input) != nil {
		return result, reference, managed.ErrAuthority
	}
	result, err := h.loadCommit(ctx, input.PermissionReference)
	if err != nil {
		return result, reference, err
	}
	expected, err := commitIntent(result)
	if err != nil {
		return result, reference, err
	}
	reference, err = h.retainIntentAnchor(ctx, input.PermissionReference, producer, expected)
	return result, reference, err
}

func nativeCommitAcceptance(receipt durable.QueryRemovalCommitReceipt) operator.QueryGuardAcceptance {
	return operator.QueryGuardAcceptance{Kind: "commit", GuardID: receipt.Request.Guard.GuardID, RootDigest: receipt.Request.Guard.RootDigest, RequestID: receipt.Request.RequestID, CommandDigest: receipt.Request.CommandDigest, ReceiptDigest: receipt.ReceiptDigest, PermissionID: receipt.Request.Permission.PermissionID, InvocationID: receipt.Request.Permission.CommandID, PermissionProofDigest: receipt.Request.Permission.RevalidationDigest, CurrentProofDigest: receipt.Request.ExpectedCurrentDigest, AcceptedAt: receipt.AcceptedAt}
}
func (h *NativeGuardHost) normalizeCommit(ctx context.Context, prepared nativePreparedCommit, reference managed.RecoveryAssociation, wire operator.QueryGuardAcceptance) (managed.ProviderNativeCommitFacts, error) {
	var facts managed.ProviderNativeCommitFacts
	intent, err := commitIntent(prepared)
	if err != nil {
		return facts, err
	}
	lookup := durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: prepared.Request.Guard.QueryRuntimeTarget, RequestID: prepared.Request.RequestID, CommandDigest: prepared.Request.CommandDigest, RequestDigest: intent.RequestDigest}
	saved, err := h.native.LookupQueryRemovalGuardReceipt(ctx, durable.QueryRemovalGuardReceiptLookup{QueryRemovalRevalidationLookup: lookup, Kind: "commit"})
	if err != nil {
		return facts, err
	}
	if saved.Commit == nil || saved.Close != nil || saved.Commit.Match(lookup) != nil || !reflect.DeepEqual(saved.Commit.Request, prepared.Request) || !reflect.DeepEqual(wire, nativeCommitAcceptance(*saved.Commit)) {
		return facts, managed.ErrAuthority
	}
	raw, err := json.Marshal(wire)
	if err != nil || len(raw) > managed.MaxRecoveryExport {
		return facts, managed.ErrCapacity
	}
	attestation, err := h.attest("accepted-commit", struct {
		Intent   managed.RecoveryAssociation
		Receipt  string
		Producer managed.RecoveryProducerIdentity
	}{reference, saved.Commit.ReceiptDigest, h.identity})
	if err != nil {
		return facts, err
	}
	source := prepared.Sources
	facts = managed.ProviderNativeCommitFacts{Binding: source.Binding.Binding, Invocation: source.Invocation, Intent: reference, PermissionDigest: source.PermissionDigest, PermissionProof: source.PermissionProof, CurrentProof: source.Current, PermissionProofDigest: source.PermissionProof.Digest, CurrentProofDigest: source.Current.Digest, GuardID: wire.GuardID, GuardRootDigest: wire.RootDigest, NativeRequestID: wire.RequestID, NativeRequestDigest: saved.Commit.RequestDigest, NativeReceiptDigest: saved.Commit.ReceiptDigest, NativeAcceptedAt: wire.AcceptedAt, Export: managed.RecoveryNativeExport{Raw: raw, RawDigest: managed.Digest(raw), Attestation: []byte(attestation), AttestationDigest: managed.Digest([]byte(attestation)), ProducerID: h.identity.ProducerID, ProducerProfile: h.identity.Profile, ProducerVersion: h.identity.Version, VerifierID: h.identity.VerifierID}}
	return facts, managed.ValidateRecoveryNativeExport(facts.Export)
}

// BindManagedRemovalCommit binds the exact accepted permission and current proof.
// Native replay returns historical acceptance, never another delegation grant.
func (h *NativeGuardHost) BindManagedRemovalCommit(ctx context.Context, producer managed.RecoveryProducerContext) (managed.ProviderNativeCommitFacts, error) {
	prepared, reference, err := h.acceptedCommit(ctx, producer)
	if err != nil {
		return managed.ProviderNativeCommitFacts{}, err
	}
	receipt, err := h.client.BindCommit(ctx, prepared.Input)
	if err != nil {
		return managed.ProviderNativeCommitFacts{}, err
	}
	return h.normalizeCommit(ctx, prepared, reference, receipt)
}

// RecoverManagedRemovalCommit reads the original native acceptance without mutation.
func (h *NativeGuardHost) RecoverManagedRemovalCommit(ctx context.Context, producer managed.RecoveryProducerContext) (managed.ProviderNativeCommitFacts, error) {
	prepared, reference, err := h.acceptedCommit(ctx, producer)
	if err != nil {
		return managed.ProviderNativeCommitFacts{}, err
	}
	receipt, err := h.client.ReadGuardReceipt(ctx, operator.QueryGuardReceiptInput{QueryGuardReadInput: operator.QueryGuardReadInput{QueryReservationInput: prepared.Input.QueryReservationInput, RequestID: prepared.Input.RequestID, CommandDigest: prepared.Request.CommandDigest}, Kind: "commit"})
	if err != nil {
		return managed.ProviderNativeCommitFacts{}, err
	}
	return h.normalizeCommit(ctx, prepared, reference, receipt)
}

// CheckManagedRemovalCommit also requires the native guard to remain active on
// this exact current proof. It still returns no transport permission.
func (h *NativeGuardHost) CheckManagedRemovalCommit(ctx context.Context, producer managed.RecoveryProducerContext) (managed.ProviderNativeCommitFacts, error) {
	prepared, reference, err := h.acceptedCommit(ctx, producer)
	if err != nil {
		return managed.ProviderNativeCommitFacts{}, err
	}
	status, err := h.client.CheckCommit(ctx, operator.QueryGuardReadInput{QueryReservationInput: prepared.Input.QueryReservationInput, RequestID: prepared.Input.RequestID, CommandDigest: prepared.Request.CommandDigest})
	if err != nil {
		return managed.ProviderNativeCommitFacts{}, err
	}
	if status.State != "active" || status.CurrentRequestID != prepared.Request.CurrentRequestID || status.CurrentProofDigest != prepared.Request.ExpectedCurrentDigest || status.ObservedAt.IsZero() || !managed.RecoveryProofFresh(prepared.Sources.CurrentNative, status.ObservedAt) {
		return managed.ProviderNativeCommitFacts{}, managed.ErrAuthority
	}
	return h.normalizeCommit(ctx, prepared, reference, status.Receipt)
}

// VerifyRemovalCommit rereads the exact accepted native request and all original
// permission sources. Rehashed normalized facts do not authenticate acceptance.
func (h *NativeGuardHost) VerifyRemovalCommit(ctx context.Context, intent managed.RecoveryRecord[managed.RecoveryNativeIntent], facts managed.ProviderNativeCommitFacts) error {
	expected, err := h.RecoverManagedRemovalCommit(ctx, managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: intent.Facts.Binding.Key.AttemptKey, Intent: intent})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, facts) {
		return managed.ErrAuthority
	}
	return nil
}

var _ managed.RemovalCommitProducer = (*NativeGuardHost)(nil)

func sameCommitSources(left, right nativeCommitSources) bool {
	if !sameNativeBinding(left.Binding, right.Binding) {
		return false
	}
	// Mutable native runtime observations are not part of the saved permission.
	left.Binding = right.Binding
	return reflect.DeepEqual(left, right)
}

func fullPermissionDigest(wire managed.WireProviderStepPermission) (string, error) {
	permission, err := wire.Value()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(permission)
	if err != nil {
		return "", err
	}
	return managed.Digest(raw), nil
}
