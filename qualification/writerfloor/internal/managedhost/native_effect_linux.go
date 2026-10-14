//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strconv"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

type nativeEffectWitness struct {
	Facts         managed.RecoveryEffectComplete `json:"facts"`
	Before        managed.RecoveryAssociation    `json:"before"`
	BeforeAddress string                         `json:"before_address"`
	Transport     docker.ManagedDockerRequest    `json:"transport"`
}

// WithRemovalCustody returns a configured copy using the concrete durable Docker
// executor for positive responses. It does not execute or adopt any invocation.
func (h *NativeGuardHost) WithRemovalCustody(executor *docker.ManagedDockerExecutor) (*NativeGuardHost, error) {
	if h == nil || executor == nil {
		return nil, managed.ErrUnsupported
	}
	configured := *h
	configured.removal = executor
	return &configured, nil
}
func (h *NativeGuardHost) captureCreation(binding nativeBinding) (launcher.ServiceBinding, error) {
	var result launcher.ServiceBinding
	runtime, err := h.runtimeFor(binding.Original.QueryRuntimeTarget)
	if err != nil {
		return result, err
	}
	id, identity, err := runtime.fixed(binding.Original.QueryRuntimeTarget)
	if err != nil {
		return result, err
	}
	if !nativeIdentityMatchesOriginal(identity, binding.Registration.Material.Registration.Process) {
		return result, managed.ErrAuthority
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	slot := runtime.slots[id]
	if slot == nil || slot.binding == nil {
		return result, managed.ErrAuthority
	}
	return *slot.binding, nil
}
func (h *NativeGuardHost) loadPrepared(ctx context.Context, address string) (nativePreparedLive, error) {
	prepared, err := h.loadPreparedBase(ctx, address)
	if err != nil {
		return prepared, err
	}
	if prepared.Effect == nil {
		if prepared.Request.Evidence.Disposition != "live" {
			return prepared, managed.ErrAuthority
		}
		return prepared, nil
	}
	if prepared.Request.Evidence.Disposition != "effect_complete" {
		return prepared, managed.ErrAuthority
	}
	facts, positive, prior, err := h.effectSources(ctx, prepared.Binding.Binding.Key, prepared.Effect.Facts.Invocation, prepared.Effect.Before, prepared.Effect.Transport)
	if err != nil {
		return prepared, err
	}
	if !reflect.DeepEqual(facts, prepared.Effect.Facts) || !reflect.DeepEqual(positive, prepared.Request.Evidence.Positive) || prepared.Effect.BeforeAddress != prior.Input.EvidenceReference || !reflect.DeepEqual(prepared.Creation, prior.Creation) || !sameNativeBinding(prepared.Binding, prior.Binding) {
		return prepared, managed.ErrAuthority
	}
	return prepared, nil
}

func (h *NativeGuardHost) effectSources(ctx context.Context, key managed.AttemptBindingKey, invocation managed.ProviderInvocationKey, before managed.RecoveryAssociation, transport docker.ManagedDockerRequest) (managed.RecoveryEffectComplete, durable.QueryRemovalPositiveEffect, nativePreparedLive, error) {
	var effect managed.RecoveryEffectComplete
	var positive durable.QueryRemovalPositiveEffect
	var prior nativePreparedLive
	core, ok := h.core.(nativeCommitCore)
	if !ok || h.removal == nil {
		return effect, positive, prior, managed.ErrUnsupported
	}
	sources, err := h.commitSources(ctx, key, invocation, before)
	if err != nil {
		return effect, positive, prior, err
	}
	if !reflect.DeepEqual(sources.CurrentNative, sources.PermissionNative) {
		return effect, positive, prior, managed.ErrAuthority
	}
	selected, err := core.ReadManagedProviderInvocation(ctx, invocation)
	if err != nil {
		return effect, positive, prior, err
	}
	roots, err := core.ReadManagedProviderInvocationSources(ctx, invocation)
	if err != nil {
		return effect, positive, prior, err
	}
	if selected.Result == nil || selected.Marker == nil || selected.Failure != nil || selected.Closure != nil || selected.Result.Facts.Result.Outcome != managed.PortTerminal || selected.Result.Facts.PermissionDigest != sources.PermissionDigest {
		return effect, positive, prior, managed.ErrAuthority
	}
	result := selected.Result.Facts.Result
	if transport.Invocation == nil || !reflect.DeepEqual(*transport.Invocation, selected.Permission.Facts) || !reflect.DeepEqual(transport.Permission, selected.Permission.Facts.Permission) || transport.Command.Kind != managed.StepRemoveContainer || transport.Command.ResourceID != roots.Root.Facts.ResourceID {
		return effect, positive, prior, managed.ErrAuthority
	}
	custody, err := h.removal.Read(ctx, transport)
	if err != nil {
		return effect, positive, prior, err
	}
	if custody.State != "complete" || custody.Response == nil || custody.Custody == nil || custody.Response.ResourceID != roots.Root.Facts.ResourceID || custody.Custody.ResourceID != roots.Root.Facts.ResourceID || custody.Custody.Incarnation != roots.Root.Facts.Incarnation || result.ResourceID != custody.Response.ResourceID || !result.ObservedAt.Equal(custody.Response.ObservedAt) || !reflect.DeepEqual(result.Permission, transport.Permission) {
		return effect, positive, prior, managed.ErrAuthority
	}
	proof, err := core.ReadManagedRemovalRevalidation(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: before})
	if err != nil {
		return effect, positive, prior, err
	}
	intent, err := h.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: proof.Facts.Intent})
	if err != nil {
		return effect, positive, prior, err
	}
	var input operator.QueryRevalidationInput
	if decodeNativeResponse(bytes.NewReader(intent.Facts.Command), &input) != nil {
		return effect, positive, prior, managed.ErrAuthority
	}
	// Read the signed base before following any effect association. This bounds
	// the chain at one original live observation and cannot recurse through effects.
	prior, err = h.loadPreparedBase(ctx, input.EvidenceReference)
	if err != nil {
		return effect, positive, prior, err
	}
	if prior.Effect != nil || prior.Creation == nil || prior.Request.Evidence.Disposition != "live" || !sameNativeBinding(prior.Binding, sources.Binding) {
		return effect, positive, prior, managed.ErrAuthority
	}
	expected, err := preparedIntent(prior)
	if err != nil || !reflect.DeepEqual(expected, intent.Facts) {
		return effect, positive, prior, managed.ErrAuthority
	}
	intentReference, anchorErr := h.verifyIntentAnchor(ctx, input.EvidenceReference, expected)
	if anchorErr != nil {
		return effect, positive, prior, anchorErr
	}
	if intentReference != proof.Facts.Intent {
		return effect, positive, prior, managed.ErrAuthority
	}
	creation := prior.Creation
	if !nativePositiveCustodyMatches(*creation, roots.Root.Facts, result, custody) {
		return effect, positive, prior, managed.ErrAuthority
	}
	material, err := h.native.LookupQueryRemovalRevalidation(ctx, durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: prior.Request.Original.QueryRuntimeTarget, RequestID: proof.Facts.NativeRequestID, RequestDigest: proof.Facts.NativeRequestDigest, CommandDigest: prior.Request.CommandDigest})
	if err != nil {
		return effect, positive, prior, err
	}
	if material.MaterialDigest != proof.Facts.NativeMaterialDigest || !reflect.DeepEqual(material.Request.Clone(), prior.Request.Clone()) || !managed.RecoveryProofFresh(proof.Facts, transport.Permission.IssuedAt) {
		return effect, positive, prior, managed.ErrAuthority
	}
	reference, err := managed.RecoveryRecordAssociation(key.AttemptKey, *selected.Result)
	if err != nil {
		return effect, positive, prior, err
	}
	permissionProof := sources.PermissionProof
	effect = managed.RecoveryEffectComplete{Invocation: invocation, PermissionDigest: sources.PermissionDigest, Result: reference, ResourceID: roots.Root.Facts.ResourceID, Incarnation: roots.Root.Facts.Incarnation, OriginalProcessDigest: sources.Binding.Binding.OriginalProcessDigest, PreEffectProofDigest: proof.Digest, RetryPreEffect: &permissionProof}
	association, err := durable.Fingerprint("writerfloor.native.positive-association.v1", struct {
		Effect   managed.RecoveryEffectComplete
		Creation launcher.ServiceBinding
		Custody  docker.ManagedDockerCustody
	}{effect, *creation, *custody.Custody})
	if err != nil {
		return effect, positive, prior, err
	}
	positive = nativePositiveWire(durable.QueryRemovalPositiveEffect{PermissionID: selected.Permission.Acceptance.OperationID.String(), PermissionDigest: sources.PermissionDigest, ResultID: selected.Result.Acceptance.OperationID.String(), ResultDigest: selected.Result.Digest, CommandID: invocation.CommandID.String(), ResourceID: effect.ResourceID, IncarnationID: effect.Incarnation, AssociationDigest: association, PreEffectDigest: effect.PreEffectProofDigest, PreEffectAt: transport.Permission.IssuedAt, AcceptedAt: transport.Permission.IssuedAt, CompletedAt: result.ObservedAt})
	return effect, positive, prior, nil
}

// nativePositiveWire applies Dispatch's timestamp representation before evidence
// is retained or compared. The signed custody and core result stay exact.
func nativePositiveWire(positive durable.QueryRemovalPositiveEffect) durable.QueryRemovalPositiveEffect {
	return (durable.QueryRemovalRevalidationRequest{Evidence: durable.QueryRemovalEvidence{Positive: positive}}).Clone().Evidence.Positive
}

// PrepareEffectComplete discharges the original process only from an exact
// accepted positive deletion and its authenticated pre-effect creation binding.
// It performs no liveness inference and never sends another provider operation.
func (h *NativeGuardHost) PrepareEffectComplete(ctx context.Context, key managed.AttemptBindingKey, invocation managed.ProviderInvocationKey, before managed.RecoveryAssociation, transport docker.ManagedDockerRequest, requestID, expectedCurrent string) (managed.RecoveryNativeIntent, error) {
	var zero managed.RecoveryNativeIntent
	address, err := nativePreparedKey(key, requestID)
	if err != nil {
		return zero, err
	}
	priorSaved, err := h.loadPrepared(ctx, address)
	if err == nil {
		if priorSaved.Effect == nil || priorSaved.Effect.Facts.Invocation != invocation || priorSaved.Effect.Before != before || !reflect.DeepEqual(priorSaved.Effect.Transport, transport) || priorSaved.Request.ExpectedCurrentDigest != expectedCurrent {
			return zero, managed.ErrAuthority
		}
		return preparedIntent(priorSaved)
	}
	if !errors.Is(err, durable.ErrNotFound) {
		return zero, err
	}
	effect, positive, prior, err := h.effectSources(ctx, key, invocation, before, transport)
	if err != nil {
		return zero, err
	}
	observed, err := h.native.InspectQueryRetention(ctx, prior.Request.Original.BuildTarget)
	if err != nil {
		return zero, err
	}
	if observed.ObservedAt.IsZero() || observed.ObservedAt.Before(positive.CompletedAt) {
		return zero, managed.ErrAuthority
	}
	evidence := prior.Request.Clone().Evidence
	evidence.Disposition = "effect_complete"
	evidence.Positive = positive
	evidence.ObservedAt = observed.ObservedAt
	evidence.ValidUntil = observed.ObservedAt.Add(prior.Binding.Candidate.BuildIdentity.MaximumProofValidity)
	survivor := prior.Request.Survivor
	var survivorVersion int64
	if survivor.RuntimeID != "" {
		selected, readErr := h.nativeRuntime(ctx, survivor)
		if readErr != nil {
			return zero, readErr
		}
		if selected.State != durable.QueryRuntimeActive || selected.QueryRuntimeIdentity != prior.Request.Evidence.SurvivorProof.Identity || selected.Verification.Validate(selected.QueryRuntimeIdentity, observed.ObservedAt) != nil {
			return zero, managed.ErrAuthority
		}
		runtime, readErr := h.runtimeFor(survivor)
		if readErr != nil {
			return zero, readErr
		}
		fresh, verifyErr := runtime.Verify(ctx, selected)
		if verifyErr != nil {
			return zero, verifyErr
		}
		evidence.SurvivorProof = selected.Verification
		survivorVersion = selected.Version
		if evidence.ValidUntil.After(selected.Verification.ValidUntil) {
			evidence.ValidUntil = selected.Verification.ValidUntil
		}
		evidence.SurvivorAttestation, err = h.attest("survivor", struct {
			Stored, Fresh durable.QueryRuntimeVerification
		}{selected.Verification, fresh})
		if err != nil {
			return zero, err
		}
	}
	evidence.PermissionAttestation, err = h.attest("positive-permission", transport.Permission)
	if err != nil {
		return zero, err
	}
	evidence.ResultAttestation, err = h.attest("positive-result", positive)
	if err != nil {
		return zero, err
	}
	evidence.PreEffectAttestation, err = h.attest("positive-pre-effect", before)
	if err != nil {
		return zero, err
	}
	evidence.AssociationAttestation, err = h.attest("positive-association", effect)
	if err != nil {
		return zero, err
	}
	input := prior.Input
	input.RequestID = requestID
	input.ExpectedCurrentDigest = expectedCurrent
	input.EvidenceReference = address
	input.SurvivorVersion = strconv.FormatInt(survivorVersion, 10)
	commandDigest, err := durable.Fingerprint("operator.query_runtime.revalidate.v1", input)
	if err != nil {
		return zero, err
	}
	request := prior.Request
	request.RequestID = requestID
	request.CommandDigest = commandDigest
	request.ExpectedCurrentDigest = expectedCurrent
	request.Evidence = evidence
	request.SurvivorVersion = survivorVersion
	request = request.Clone()
	if request.Validate() != nil {
		return zero, managed.ErrInvalid
	}
	witness := nativeEffectWitness{Facts: effect, Before: before, BeforeAddress: prior.Input.EvidenceReference, Transport: transport}
	prepared, err := h.signPrepared(nativePreparedLive{Profile: nativeGuardProfile, Binding: prior.Binding, Input: input, Request: request, Creation: prior.Creation, Effect: &witness})
	if err != nil {
		return zero, err
	}
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
	return preparedIntent(prepared)
}

func nativePositiveCustodyMatches(creation launcher.ServiceBinding, root managed.ProviderStepRetryRoot, result managed.WireProviderStepResult, record docker.ManagedDockerRecord) bool {
	if record.State != "complete" || record.Response == nil || record.Custody == nil || result.Outcome != managed.PortTerminal {
		return false
	}
	return objectID(creation.BootNonce) && objectID(creation.CreationDigest) && creation.ResourceID == root.ResourceID && creation.Incarnation == root.Incarnation && record.Custody.ResourceID == root.ResourceID && record.Custody.Incarnation == root.Incarnation && record.Response.ResourceID == root.ResourceID && result.ResourceID == root.ResourceID && result.ObservedAt.Equal(record.Response.ObservedAt) && reflect.DeepEqual(result.Permission, record.Request.Permission)
}
