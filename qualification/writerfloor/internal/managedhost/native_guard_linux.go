//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

const nativeGuardProfile = "writerfloor.dispatch.guard.v1"

type nativePreparedLive struct {
	Creation  *launcher.ServiceBinding                `json:"creation,omitempty"`
	Effect    *nativeEffectWitness                    `json:"effect,omitempty"`
	Profile   string                                  `json:"profile"`
	Binding   nativeBinding                           `json:"binding"`
	Input     operator.QueryRevalidationInput         `json:"input"`
	Request   durable.QueryRemovalRevalidationRequest `json:"request"`
	Signature []byte                                  `json:"signature"`
}

// NativeGuardHost prepares authenticated evidence before the core intent. The
// retained core intent, independent acceptance and exact original native receipt
// must all exist before it calls the existing Dispatch mutation contract.
type NativeGuardHost struct {
	removal   *docker.ManagedDockerExecutor
	core      NativeCore
	native    NativeStore
	runtimes  map[durable.QueryRuntimeTarget]*RuntimeHost
	client    *NativeClient
	journal   docker.ManagedDockerJournal
	authority string
	identity  managed.RecoveryProducerIdentity
	key       ed25519.PrivateKey
}

// NewNativeGuardHost fixes the real controller/native stores, protected journal,
// authenticated Dispatch endpoint and exact provider-service runtime composition.
func NewNativeGuardHost(core NativeCore, native NativeStore, runtime *RuntimeHost, client *NativeClient, journal *docker.FileManagedDockerJournal, authority string, identity managed.RecoveryProducerIdentity, key ed25519.PrivateKey, peers ...*RuntimeHost) (*NativeGuardHost, error) {
	if nilInterface(core) || nilInterface(native) || runtime == nil || client == nil || journal == nil || !durable.QueryRemovalToken(authority) || identity.Profile != nativeGuardProfile || identity.Version != "1" || !durable.QueryRemovalToken(identity.ProducerID) || !durable.QueryRemovalToken(identity.VerifierID) || len(key) != ed25519.PrivateKeySize {
		return nil, managed.ErrUnsupported
	}
	registry, err := nativeRuntimeRegistry(append([]*RuntimeHost{runtime}, peers...))
	if err != nil {
		return nil, err
	}
	return &NativeGuardHost{core: core, native: native, runtimes: registry, client: client, journal: journal, authority: authority, identity: identity, key: bytes.Clone(key)}, nil
}
func nativePreparedKey(key managed.AttemptBindingKey, requestID string) (string, error) {
	if !durable.QueryRemovalToken(requestID) {
		return "", managed.ErrInvalid
	}
	return durable.Fingerprint("writerfloor.native.prepared.v1", struct {
		Key       managed.AttemptBindingKey
		RequestID string
	}{key, requestID})
}
func (h *NativeGuardHost) attest(domain string, value any) (string, error) {
	raw, err := bounded(struct {
		Domain string
		Value  any
	}{domain, value})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(h.key, raw)), nil
}
func (h *NativeGuardHost) signPrepared(record nativePreparedLive) (nativePreparedLive, error) {
	record.Signature = nil
	raw, err := bounded(record)
	if err != nil {
		return record, err
	}
	record.Signature = ed25519.Sign(h.key, raw)
	_, err = bounded(record)
	return record, err
}
func (h *NativeGuardHost) loadPreparedBase(ctx context.Context, key string) (nativePreparedLive, error) {
	var result nativePreparedLive
	if !objectID(key) {
		return result, managed.ErrInvalid
	}
	raw, err := h.journal.Load(ctx, key)
	if err != nil {
		return result, err
	}
	if len(raw) == 0 {
		return result, durable.ErrNotFound
	}
	if checkErr := exactDecode(raw, &result); checkErr != nil {
		return result, checkErr
	}
	signature := bytes.Clone(result.Signature)
	result.Signature = nil
	unsigned, err := bounded(result)
	result.Signature = signature
	if err != nil || !ed25519.Verify(ed25519.PublicKey(h.key[ed25519.SeedSize:]), unsigned, signature) || result.Profile != nativeGuardProfile || result.Request.Validate() != nil {
		return result, managed.ErrAuthority
	}
	expected, err := nativePreparedKey(result.Binding.Binding.Key, result.Input.RequestID)
	if err != nil || expected != key || result.Input.EvidenceReference != key {
		return result, managed.ErrAuthority
	}
	inputDigest, err := durable.Fingerprint("operator.query_runtime.revalidate.v1", result.Input)
	if err != nil || result.Request.CommandDigest != inputDigest || result.Request.RequestID != result.Input.RequestID || result.Request.Original != result.Binding.Original || result.Request.ExternalScope != result.Binding.Scope {
		return result, managed.ErrAuthority
	}
	return result, nil
}
func preparedIntent(record nativePreparedLive) (managed.RecoveryNativeIntent, error) {
	raw, err := json.Marshal(record.Input)
	if err != nil {
		return managed.RecoveryNativeIntent{}, err
	}
	digest, err := durable.Fingerprint("dispatch.query-removal.revalidation.v1", record.Request)
	if err != nil {
		return managed.RecoveryNativeIntent{}, err
	}
	return managed.RecoveryNativeIntent{Binding: record.Binding.Binding, Profile: managed.RemovalRecoveryProfile, Operation: "revalidate", RequestID: record.Input.RequestID, RequestDigest: digest, Command: raw, CommandDigest: managed.Digest(raw)}, nil
}

// PrepareLive retains actual quiescence and query evidence before the caller
// records a core native intent. Exact replay returns the original prepared bytes;
// it never substitutes fresh evidence under an existing native request identity.
func (h *NativeGuardHost) PrepareLive(ctx context.Context, key managed.AttemptBindingKey, requestID, expectedCurrent string) (managed.RecoveryNativeIntent, error) {
	var zero managed.RecoveryNativeIntent
	address, err := nativePreparedKey(key, requestID)
	if err != nil {
		return zero, err
	}
	prior, err := h.loadPrepared(ctx, address)
	if err == nil {
		if prior.Request.ExpectedCurrentDigest != expectedCurrent {
			return zero, managed.ErrAuthority
		}
		current, readErr := h.readBinding(ctx, key)
		if readErr != nil {
			return zero, readErr
		}
		if !sameNativeBinding(current, prior.Binding) {
			return zero, managed.ErrAuthority
		}
		return preparedIntent(prior)
	}
	if !errors.Is(err, durable.ErrNotFound) {
		return zero, err
	}
	binding, err := h.readBinding(ctx, key)
	if err != nil {
		return zero, err
	}
	if checkErr := h.matchesLiveProcess(binding); checkErr != nil {
		return zero, checkErr
	}
	originalDrain := binding.Drain.Material.Drain
	handle := drt.DrainHandle{RuntimeID: originalDrain.Process.RuntimeID, DrainRequest: drt.DrainRequest{OperationID: originalDrain.OperationID, Deadline: originalDrain.Deadline}}
	candidateHost, err := h.runtimeFor(binding.Original.QueryRuntimeTarget)
	if err != nil {
		return zero, err
	}
	drained, err := candidateHost.WaitDrain(ctx, binding.Original.QueryRuntimeTarget, handle)
	if err != nil {
		return zero, err
	}
	if !drained.Complete || !drained.Quiescent || drained.DeadlineExpired || drained.UnknownClaims != 0 || drained.InFlight != 0 {
		return zero, managed.ErrUnavailable
	}
	candidate, err := candidateHost.Verify(ctx, binding.Candidate)
	if err != nil {
		return zero, err
	}
	process := binding.Registration.Material.Registration.Process
	evidence := durable.QueryRemovalEvidence{Disposition: "live", CandidateProof: candidate, ExcludedInstances: binding.Exclusions, ObservedAt: candidate.VerifiedAt, ValidUntil: candidate.ValidUntil, Process: durable.QueryRemovalProcess{HostID: process.HostID, ProcessID: process.RuntimeID, DescriptorID: process.Descriptor.BindingID, DescriptorDigest: process.Descriptor.BindingDigest, RegistrationID: binding.Registration.Material.Registration.Native.RequestID, DrainRuntimeID: process.RuntimeID, DrainOperationID: originalDrain.OperationID, DrainDeadline: originalDrain.Deadline, Role: string(binding.Registration.Material.Registration.Role), Admission: "permanently_closed", Claims: "quiescent", DrainOutcome: "completed", ObservedAt: candidate.VerifiedAt, ValidUntil: candidate.ValidUntil}}
	survivor := binding.Candidate.Removal.Survivor
	var survivorVersion int64
	if survivor.RuntimeID != "" {
		selected, readErr := h.nativeRuntime(ctx, survivor.QueryRuntimeTarget)
		if readErr != nil {
			return zero, readErr
		}
		if selected.QueryRuntimeIdentity != survivor || selected.State != durable.QueryRuntimeActive || selected.Verification.Validate(survivor, candidate.VerifiedAt) != nil {
			return zero, managed.ErrAuthority
		}
		// The native stored proof remains the exact accepted verification. A fresh
		// challenge additionally refuses a dead/replaced process before preparation.
		survivorHost, hostErr := h.runtimeFor(survivor.QueryRuntimeTarget)
		if hostErr != nil {
			return zero, hostErr
		}
		fresh, verifyErr := survivorHost.Verify(ctx, selected)
		if verifyErr != nil {
			return zero, verifyErr
		}
		if fresh.Identity != survivor {
			return zero, managed.ErrAuthority
		}
		evidence.SurvivorProof = selected.Verification
		survivorVersion = selected.Version
		if evidence.ValidUntil.After(selected.Verification.ValidUntil) {
			evidence.ValidUntil = selected.Verification.ValidUntil
		}
		evidence.SurvivorAttestation, err = h.attest("survivor", struct {
			Stored durable.QueryRuntimeVerification
			Fresh  durable.QueryRuntimeVerification
		}{selected.Verification, fresh})
		if err != nil {
			return zero, err
		}
	}
	evidence.CandidateAttestation, err = h.attest("candidate", candidate)
	if err != nil {
		return zero, err
	}
	evidence.ProcessAttestation, err = h.attest("process", struct {
		Process durable.QueryRemovalProcess
		Drain   drt.DrainResult
	}{evidence.Process, drained})
	if err != nil {
		return zero, err
	}
	evidence.ScopeAttestation, err = h.attest("scope", binding.Scope)
	if err != nil {
		return zero, err
	}
	scope := binding.Scope
	input := operator.QueryRevalidationInput{QueryReservationInput: binding.Reservation, RequestID: requestID, Scope: operator.QueryGuardScope{AuthorityID: scope.AuthorityID, OperationScopeID: scope.OperationScopeID, AttemptID: scope.AttemptID, ProviderOperationID: scope.ProviderOperationID, CoverageDigest: scope.CoverageDigest, ExcludedInstancesDigest: scope.ExcludedInstancesDigest, ProducerID: scope.ProducerID, ProfileID: scope.ProfileID, ProfileVersion: "1", VerifierID: scope.VerifierID}, ExpectedCurrentDigest: expectedCurrent, EvidenceReference: address, SurvivorRuntimeID: survivor.RuntimeID, SurvivorVersion: strconv.FormatInt(survivorVersion, 10)}
	digest, err := durable.Fingerprint("operator.query_runtime.revalidate.v1", input)
	if err != nil {
		return zero, err
	}
	request := durable.QueryRemovalRevalidationRequest{Original: binding.Original, ExternalScope: scope, RequestID: requestID, CommandDigest: digest, ExpectedCurrentDigest: expectedCurrent, Survivor: survivor.QueryRuntimeTarget, SurvivorVersion: survivorVersion, Evidence: evidence}.Clone()
	if request.Validate() != nil {
		return zero, managed.ErrInvalid
	}
	creation, err := h.captureCreation(binding)
	if err != nil {
		return zero, err
	}
	record, err := h.signPrepared(nativePreparedLive{Profile: nativeGuardProfile, Binding: binding, Input: input, Request: request, Creation: &creation})
	if err != nil {
		return zero, err
	}
	raw, err := bounded(record)
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
	return preparedIntent(record)
}

type nativeIntentAnchor struct {
	Key       managed.AttemptKey          `json:"key"`
	Intent    managed.RecoveryAssociation `json:"intent"`
	Signature []byte                      `json:"signature"`
}

func (h *NativeGuardHost) anchorKey(prepared string) string {
	return managed.Digest([]byte(nativeGuardProfile + "/intent/" + prepared))
}
func (h *NativeGuardHost) acceptedIntent(ctx context.Context, producer managed.RecoveryProducerContext) (nativePreparedLive, managed.RecoveryAssociation, error) {
	var zero nativePreparedLive
	reference, err := managed.RecoveryRecordAssociation(producer.Key, producer.Intent)
	if err != nil {
		return zero, reference, err
	}
	saved, err := h.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: producer.Key, Association: reference})
	if err != nil {
		return zero, reference, err
	}
	if producer.Profile != managed.RemovalRecoveryProfile || !reflect.DeepEqual(saved, producer.Intent) || saved.Facts.Operation != "revalidate" {
		return zero, reference, managed.ErrAuthority
	}
	var input operator.QueryRevalidationInput
	if checkErr := decodeNativeResponse(bytes.NewReader(saved.Facts.Command), &input); checkErr != nil {
		return zero, reference, checkErr
	}
	prepared, err := h.loadPrepared(ctx, input.EvidenceReference)
	if err != nil {
		return zero, reference, err
	}
	expected, err := preparedIntent(prepared)
	if err != nil || !reflect.DeepEqual(expected, saved.Facts) {
		return zero, reference, managed.ErrAuthority
	}
	current, err := h.readBinding(ctx, saved.Facts.Binding.Key)
	if err != nil {
		return zero, reference, err
	}
	if !sameNativeBinding(prepared.Binding, current) {
		return zero, reference, managed.ErrAuthority
	}
	anchor := nativeIntentAnchor{Key: producer.Key, Intent: reference}
	unsigned, err := bounded(anchor)
	if err != nil {
		return zero, reference, err
	}
	anchor.Signature = ed25519.Sign(h.key, unsigned)
	raw, err := bounded(anchor)
	if err != nil {
		return zero, reference, err
	}
	address := h.anchorKey(input.EvidenceReference)
	prior, err := h.journal.Load(ctx, address)
	if err != nil {
		return zero, reference, err
	}
	if len(prior) > 0 {
		if !bytes.Equal(prior, raw) {
			return zero, reference, managed.ErrAuthority
		}
	} else {
		accepted, writeErr := h.journal.CompareAndSwap(ctx, address, nil, raw)
		if writeErr != nil {
			return zero, reference, writeErr
		}
		if !accepted {
			return zero, reference, managed.ErrBlocked
		}
	}
	return prepared, reference, nil
}

// VerifyRevalidation is the configured Dispatch verifier's live preparation path.
// The protected anchor must resolve an independently accepted core intent before
// even an authenticated direct native caller can use this prepared evidence.
func (h *NativeGuardHost) VerifyRevalidation(ctx context.Context, probe durable.QueryRemovalGuardProbe) (durable.QueryRemovalEvidence, error) {
	var zero durable.QueryRemovalEvidence
	prepared, err := h.loadPrepared(ctx, probe.EvidenceReference)
	if err != nil {
		return zero, err
	}
	if probe.Original != prepared.Request.Original || probe.ExternalScope != prepared.Request.ExternalScope || probe.Survivor != prepared.Request.Survivor || probe.SurvivorVersion != prepared.Request.SurvivorVersion {
		return zero, managed.ErrAuthority
	}
	raw, err := h.journal.Load(ctx, h.anchorKey(probe.EvidenceReference))
	if err != nil {
		return zero, err
	}
	var anchor nativeIntentAnchor
	if checkErr := exactDecode(raw, &anchor); checkErr != nil {
		return zero, checkErr
	}
	signature := anchor.Signature
	anchor.Signature = nil
	unsigned, err := bounded(anchor)
	anchor.Signature = signature
	if err != nil || !ed25519.Verify(ed25519.PublicKey(h.key[ed25519.SeedSize:]), unsigned, signature) || anchor.Key != prepared.Binding.Binding.Key.AttemptKey {
		return zero, managed.ErrAuthority
	}
	saved, err := h.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: anchor.Key, Association: anchor.Intent})
	if err != nil {
		return zero, err
	}
	expected, err := preparedIntent(prepared)
	if err != nil || !reflect.DeepEqual(saved.Facts, expected) {
		return zero, managed.ErrAuthority
	}
	return prepared.Request.Clone().Evidence, nil
}

func (h *NativeGuardHost) normalizeLive(ctx context.Context, prepared nativePreparedLive, reference managed.RecoveryAssociation, receipt durable.QueryRemovalRevalidationReceipt) (managed.RemovalRevalidationFacts, error) {
	var facts managed.RemovalRevalidationFacts
	expected, err := preparedIntent(prepared)
	if err != nil {
		return facts, err
	}
	saved, err := h.native.LookupQueryRemovalRevalidation(ctx, durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: prepared.Request.Original.QueryRuntimeTarget, RequestID: prepared.Request.RequestID, CommandDigest: prepared.Request.CommandDigest, RequestDigest: expected.RequestDigest})
	if err != nil {
		return facts, err
	}
	compact, err := saved.Compact()
	if err != nil || !reflect.DeepEqual(saved.Request.Clone(), prepared.Request.Clone()) || !reflect.DeepEqual(compact, receipt) {
		return facts, managed.ErrAuthority
	}
	root, err := durable.QueryRemovalRoot(prepared.Request, prepared.Binding.Candidate.QueryRuntimeIdentity)
	if err != nil {
		return facts, err
	}
	if receipt.Schema != "dispatch.query-removal.revalidation.v1" || receipt.RequestID != expected.RequestID || receipt.RequestDigest != expected.RequestDigest || receipt.GuardID != root.GuardID || receipt.RootDigest != root.RootDigest || receipt.OriginalDigest != root.OriginalDigest || receipt.ExternalScopeDigest != root.ExternalScopeDigest || receipt.CandidateDigest != root.CandidateDigest || receipt.ProcessDigest != root.ProcessDigest || receipt.ExclusionDigest != prepared.Binding.Scope.ExcludedInstancesDigest || receipt.CoverageDigest != prepared.Binding.Scope.CoverageDigest || receipt.Disposition != prepared.Request.Evidence.Disposition || !receipt.ValidUntil.Equal(prepared.Request.Evidence.ValidUntil) || receipt.AcceptedAt.Before(prepared.Request.Evidence.ObservedAt) {
		return facts, managed.ErrAuthority
	}
	raw, err := json.Marshal(receipt)
	if err != nil || len(raw) > managed.MaxRecoveryExport {
		return facts, managed.ErrCapacity
	}
	receiptDigest, err := durable.Fingerprint("writerfloor.native.revalidation-receipt.v1", receipt)
	if err != nil {
		return facts, err
	}
	attestation, err := h.attest("accepted-revalidation", struct {
		Intent   managed.RecoveryAssociation
		Receipt  string
		Producer managed.RecoveryProducerIdentity
	}{reference, receiptDigest, h.identity})
	if err != nil {
		return facts, err
	}
	proof := prepared.Request.Evidence
	facts = managed.RemovalRevalidationFacts{Binding: prepared.Binding.Binding, Intent: reference, GuardID: receipt.GuardID, GuardRootDigest: receipt.RootDigest, NativeRequestID: receipt.RequestID, NativeRequestDigest: receipt.RequestDigest, NativeMaterialDigest: receipt.MaterialDigest, NativeReceiptDigest: receiptDigest, NativeCurrentDigest: receipt.MaterialDigest, NativeScopeDigest: receipt.ExternalScopeDigest, CandidateVersion: managed.Decimal(strconv.FormatInt(prepared.Binding.Original.CandidateStateVersion, 10)), RemovalEpoch: managed.Decimal(strconv.FormatInt(prepared.Binding.Original.RemovalEpoch, 10)), CandidateDigest: receipt.CandidateDigest, BuildDigest: receipt.BuildDigest, SurvivorDigest: receipt.SurvivorDigest, ExclusionDigest: receipt.ExclusionDigest, CoverageDigest: receipt.CoverageDigest, ProcessDisposition: prepared.Request.Evidence.Disposition, SourceObservedAt: proof.ObservedAt, NativeAcceptedAt: receipt.AcceptedAt, ValidUntil: receipt.ValidUntil, Export: managed.RecoveryNativeExport{Raw: raw, RawDigest: managed.Digest(raw), Attestation: []byte(attestation), AttestationDigest: managed.Digest([]byte(attestation)), ProducerID: h.identity.ProducerID, ProducerProfile: h.identity.Profile, ProducerVersion: h.identity.Version, VerifierID: h.identity.VerifierID}}
	if prepared.Effect != nil {
		effect := prepared.Effect.Facts
		facts.EffectComplete = &effect
	}
	return facts, managed.ValidateRecoveryNativeExport(facts.Export)
}

// RevalidateManagedRemoval invokes only the exact independently retained intent.
func (h *NativeGuardHost) RevalidateManagedRemoval(ctx context.Context, producer managed.RecoveryProducerContext) (managed.RemovalRevalidationFacts, error) {
	prepared, reference, err := h.acceptedIntent(ctx, producer)
	if err != nil {
		return managed.RemovalRevalidationFacts{}, err
	}
	receipt, err := h.client.Revalidate(ctx, prepared.Input)
	if err != nil {
		return managed.RemovalRevalidationFacts{}, err
	}
	return h.normalizeLive(ctx, prepared, reference, receipt)
}

// RecoverManagedRemovalRevalidation reads exact historical native acceptance.
func (h *NativeGuardHost) RecoverManagedRemovalRevalidation(ctx context.Context, producer managed.RecoveryProducerContext) (managed.RemovalRevalidationFacts, error) {
	prepared, reference, err := h.acceptedIntent(ctx, producer)
	if err != nil {
		return managed.RemovalRevalidationFacts{}, err
	}
	receipt, err := h.client.ReadRevalidation(ctx, operator.QueryGuardReadInput{QueryReservationInput: prepared.Input.QueryReservationInput, RequestID: prepared.Input.RequestID, CommandDigest: prepared.Request.CommandDigest})
	if err != nil {
		return managed.RemovalRevalidationFacts{}, err
	}
	return h.normalizeLive(ctx, prepared, reference, receipt)
}

// VerifyRemovalRevalidation authenticates the exact native receipt again. Neither
// self-hashed facts nor an expired prepared proof can create new native acceptance.
func (h *NativeGuardHost) VerifyRemovalRevalidation(ctx context.Context, intent managed.RecoveryRecord[managed.RecoveryNativeIntent], facts managed.RemovalRevalidationFacts) error {
	expected, err := h.RecoverManagedRemovalRevalidation(ctx, managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: intent.Facts.Binding.Key.AttemptKey, Intent: intent})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, facts) {
		return managed.ErrAuthority
	}
	return nil
}

var _ managed.RemovalRevalidationProducer = (*NativeGuardHost)(nil)

func (h *NativeGuardHost) matchesLiveProcess(binding nativeBinding) error {
	runtime, err := h.runtimeFor(binding.Original.QueryRuntimeTarget)
	if err != nil {
		return err
	}
	id, identity, err := runtime.fixed(binding.Original.QueryRuntimeTarget)
	if err != nil {
		return err
	}
	process := binding.Registration.Material.Registration.Process
	if !nativeIdentityMatchesOriginal(identity, process) {
		return managed.ErrAuthority
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	slot := runtime.slots[id]
	if slot == nil || slot.binding == nil || slot.startup.HostID != process.HostID || slot.startup.Queue != process.Queue || slot.startup.Service.Startup != process.Descriptor || slot.startup.ProducerVerifier != process.ProducerVerifier {
		return managed.ErrAuthority
	}
	return nil
}

func nativeRuntimeRegistry(hosts []*RuntimeHost) (map[durable.QueryRuntimeTarget]*RuntimeHost, error) {
	if len(hosts) < 1 || len(hosts) > 16 {
		return nil, managed.ErrCapacity
	}
	registry := make(map[durable.QueryRuntimeTarget]*RuntimeHost)
	for _, host := range hosts {
		if host == nil {
			return nil, managed.ErrUnsupported
		}
		host.mu.Lock()
		for _, slot := range host.slots {
			plan := slot.startup.ServicePlan
			target := durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: plan.Installation, Namespace: plan.Namespace}, BuildID: plan.Build}, RuntimeID: plan.RuntimeID}
			if target.Validate() != nil || registry[target] != nil || len(registry) >= 16 {
				host.mu.Unlock()
				return nil, managed.ErrAuthority
			}
			registry[target] = host
		}
		host.mu.Unlock()
	}
	if len(registry) == 0 {
		return nil, managed.ErrUnsupported
	}
	return registry, nil
}
func (h *NativeGuardHost) runtimeFor(target durable.QueryRuntimeTarget) (*RuntimeHost, error) {
	runtime := h.runtimes[target]
	if runtime == nil {
		return nil, managed.ErrAuthority
	}
	if _, identity, err := runtime.fixed(target); err != nil || identity.QueryRuntimeTarget != target {
		return nil, managed.ErrAuthority
	}
	return runtime, nil
}
