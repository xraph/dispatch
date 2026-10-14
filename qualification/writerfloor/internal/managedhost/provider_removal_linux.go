//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

const removalCustodyProfile = "writerfloor.docker.removal.v1"

// RemovalCore exposes exact retained authority and the fresh commit operation.
// The configured recovery repository supplies the mutation, not a historical reader.
type RemovalCore interface {
	ReadManagedTarget(context.Context, managed.Key) (*managed.Target, error)
	ReadManagedRemovalAttempt(context.Context, managed.AttemptKey) (managed.AttemptRecord, error)
	ReadProviderEffectBinding(context.Context, managed.ProviderEffectBindingKey) (managed.ProviderEffectBindingRecord, error)
	ReadManagedProviderInvocation(context.Context, managed.ProviderInvocationKey) (managed.ProviderInvocationRecord, error)
	ReadManagedProviderInvocationSources(context.Context, managed.ProviderInvocationKey) (managed.ProviderInvocationSources, error)
	ReadManagedProviderInvocationCommits(context.Context, managed.ProviderInvocationKey) ([]managed.RecoveryRecord[managed.ProviderNativeCommitFacts], error)
	ReadManagedRecoveryNativeIntent(context.Context, managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.RecoveryNativeIntent], error)
	CommitManagedProviderInvocation(context.Context, managed.ProviderInvocationCommitRequest) (managed.ProviderInvocationGrant, error)
}

type removalGrant struct {
	Request    docker.ManagedDockerRequest
	Command    managed.RecoveryCommand
	Marker     managed.RecoveryRecord[managed.ProviderInvocationMarker]
	Invocation admission.Invocation
}

// ProviderRemoval connects one fresh controller marker to the existing outer
// admission runtime and one exact SDK invocation. Reopening loses volatile grants.
// The durable executor journal survives and never permits a delegated resend.
type ProviderRemoval struct {
	core     RemovalCore
	creation *CreationTransport
	native   *NativeGuardHost
	journal  docker.ManagedDockerJournal
	key      ed25519.PrivateKey
	executor *docker.ManagedDockerExecutor
	mu       sync.Mutex
	grants   map[string]removalGrant
	sending  map[string]removalGrant
}

// NewProviderRemoval requires concrete configured custody, SDK and native guard
// implementations. Retries require exact accepted predecessor closure evidence.
func NewProviderRemoval(core RemovalCore, creation *CreationTransport, native *NativeGuardHost, sdk *docker.SDKManagedDockerTransport, journal *docker.FileManagedDockerJournal, key ed25519.PrivateKey) (*ProviderRemoval, error) {
	if nilInterface(core) || creation == nil || native == nil || sdk == nil || journal == nil || len(key) != ed25519.PrivateKeySize {
		return nil, managed.ErrUnsupported
	}
	p := &ProviderRemoval{core: core, creation: creation, native: native, journal: journal, key: bytes.Clone(key), grants: map[string]removalGrant{}, sending: map[string]removalGrant{}}
	executor, err := docker.NewManagedDockerExecutor(journal, sdk, p, creation, creation.backend)
	if err != nil {
		return nil, err
	}
	p.executor = executor
	return p, nil
}

// Executor returns the same custody executor used for delegation and native
// effect-complete verification. Direct Execute has no armed admission grant.
func (p *ProviderRemoval) Executor() *docker.ManagedDockerExecutor { return p.executor }

func removalJournalKey(key managed.ProviderInvocationKey) (string, error) {
	raw, err := json.Marshal(struct {
		Step    managed.ProviderStepKey `json:"Step"`
		Command string                  `json:"Command"`
	}{key.Step, key.CommandID.String()})
	if err != nil {
		return "", err
	}
	return managed.Digest(raw), nil
}
func removalRequestDigest(request docker.ManagedDockerRequest) (string, error) {
	raw, err := bounded(request)
	if err != nil {
		return "", err
	}
	return managed.Digest(raw), nil
}
func removalInvocation(request docker.ManagedDockerRequest) (admission.Invocation, error) {
	if request.Invocation == nil || request.Binding == nil {
		return admission.Invocation{}, managed.ErrInvalid
	}
	permission, err := request.Permission.Value()
	if err != nil {
		return admission.Invocation{}, err
	}
	raw, err := bounded(permission)
	if err != nil {
		return admission.Invocation{}, err
	}
	step := permission.Step
	slot := string(step.Kind)
	if step.Service != "" {
		slot += ":" + step.Service
	}
	return admission.Invocation{ID: permission.CommandID.String(), Tenant: step.TenantID, Target: step.InstanceID.String(), Operation: step.ProviderOperationID.String(), Attempt: request.Invocation.Key.AttemptKey.AttemptID.String(), Generation: string(request.Invocation.Key.AttemptKey.Generation), Permission: managed.Digest(raw), Command: request.Binding.ProviderCommandDigest, Kind: "provider", Slot: slot}, nil
}
func cloneRemovalRequest(request docker.ManagedDockerRequest) (docker.ManagedDockerRequest, error) {
	var out docker.ManagedDockerRequest
	raw, err := bounded(request)
	if err != nil {
		return out, err
	}
	if operationErr := exactDecode(raw, &out); operationErr != nil {
		return out, operationErr
	}
	return out, nil
}

// VerifyRequest authenticates immutable permission, receipt, exact invocation and
// signed original resource custody. It grants no current delegation authority.
func (p *ProviderRemoval) VerifyRequest(ctx context.Context, request docker.ManagedDockerRequest) error {
	return p.verifyRequest(ctx, request, true)
}

func (p *ProviderRemoval) verifyRequest(ctx context.Context, request docker.ManagedDockerRequest, requireMarker bool) error {
	if operationErr := ctx.Err(); operationErr != nil {
		return operationErr
	}
	if request.Invocation == nil || request.Binding == nil {
		return managed.ErrUnsupported
	}
	if request.BackendAuthority != p.creation.backend || (request.Command.Kind != managed.StepRemoveContainer && request.Command.Kind != managed.StepRemoveNetwork) || request.Command.ResourceID != request.Permission.ResourceID {
		return managed.ErrAuthority
	}
	if !reflect.DeepEqual(request.Invocation.Permission, request.Permission) || request.Invocation.Key.Step != request.Permission.Step || request.Invocation.Key.CommandID != request.Permission.CommandID || request.Invocation.Key.AttemptKey != request.Binding.Original.Key.AttemptKey {
		return managed.ErrAuthority
	}
	binding, err := p.core.ReadProviderEffectBinding(ctx, request.Binding.Original.Key)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(binding.Facts, *request.Binding) {
		return managed.ErrAuthority
	}
	if operationErr := p.creation.VerifyProviderEffectBinding(ctx, request.Binding.Original, *request.Binding); operationErr != nil {
		return operationErr
	}
	record, err := p.core.ReadManagedProviderInvocation(ctx, request.Invocation.Key)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(record.Permission.Facts, *request.Invocation) {
		return managed.ErrAuthority
	}
	if request.Invocation.Ordinal == "0" {
		if request.Retry != nil || !reflect.DeepEqual(request.Permission, request.Binding.Original.Permission) || request.Receipt != request.Binding.Original.Receipt {
			return managed.ErrAuthority
		}
		return nil
	}
	return p.verifyRetry(ctx, request, record, requireMarker)
}

// VerifyRetry reloads the accepted successor and immediate predecessor. It grants
// no send and never follows a recursive invocation history chain.
func (p *ProviderRemoval) VerifyRetry(ctx context.Context, request docker.ManagedDockerRequest) error {
	if request.Retry == nil || request.Invocation == nil || request.Invocation.Ordinal == "0" {
		return managed.ErrAuthority
	}
	return p.VerifyRequest(ctx, request)
}

// AdmitRetry consumes the same fresh one-shot grant used for original invocations.
func (p *ProviderRemoval) AdmitRetry(ctx context.Context, request docker.ManagedDockerRequest) error {
	if operationErr := p.VerifyRetry(ctx, request); operationErr != nil {
		return operationErr
	}
	return p.Admit(ctx, request)
}

func (p *ProviderRemoval) verifyRetry(ctx context.Context, request docker.ManagedDockerRequest, current managed.ProviderInvocationRecord, requireMarker bool) error {
	e := request.Retry
	if e == nil || current.Authorization == nil || !reflect.DeepEqual(current.Permission, e.Permission) || !reflect.DeepEqual(*current.Authorization, e.Authorization) || request.Receipt != current.Permission.Acceptance.Receipt {
		return managed.ErrAuthority
	}
	if _, err := managed.EncodeRecoveryRecord(e.Root); err != nil {
		return err
	}
	if requireMarker && (current.Marker == nil || !reflect.DeepEqual(*current.Marker, e.Marker)) {
		return managed.ErrAuthority
	}
	sources, err := p.core.ReadManagedProviderInvocationSources(ctx, request.Invocation.Key)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(sources.Root, e.Root) || current.Permission.Facts.RootDigest != e.Root.Digest {
		return managed.ErrAuthority
	}
	previous := e.Authorization.Facts.Previous
	key := current.Permission.Facts.Key
	key.CommandID = previous.CommandID
	selected, err := p.core.ReadManagedProviderInvocation(ctx, key)
	if err != nil {
		return err
	}
	if selected.Failure == nil || selected.Closure == nil || selected.Result != nil || selected.Permission.Facts.Ordinal != previous.Ordinal || selected.Permission.Facts.RootDigest != e.Root.Digest {
		return managed.ErrAuthority
	}
	failureRef, err := managed.RecoveryAssociationFor(previous.Failure.Key, *selected.Failure)
	if err != nil || failureRef != previous.Failure {
		return managed.ErrAuthority
	}
	closureRef, err := managed.RecoveryAssociationFor(previous.Closure.Key, *selected.Closure)
	if err != nil || closureRef != previous.Closure {
		return managed.ErrAuthority
	}
	if previous.Ordinal == "0" {
		authority, referenceErr := managed.RecoveryAssociationFor(previous.Authority.Key, e.Root)
		if referenceErr != nil || authority != previous.Authority {
			return managed.ErrAuthority
		}
	} else {
		if selected.Authorization == nil {
			return managed.ErrAuthority
		}
		authority, referenceErr := managed.RecoveryAssociationFor(previous.Authority.Key, *selected.Authorization)
		if referenceErr != nil || authority != previous.Authority {
			return managed.ErrAuthority
		}
	}
	// The independent core certificate authenticates the immediate predecessor.
	// Its local journal must still retain the same permanent signed closure.
	address, err := removalJournalKey(key)
	if err != nil {
		return err
	}
	raw, err := p.journal.Load(ctx, address)
	if err != nil {
		return err
	}
	var saved docker.ManagedDockerRecord
	if operationErr := exactDecode(raw, &saved); operationErr != nil {
		return operationErr
	}
	if saved.SchemaVersion != "1" || saved.State != "closed" || saved.Response != nil || saved.Custody != nil || saved.Request.Invocation == nil || saved.Request.Invocation.Key != key || !reflect.DeepEqual(saved.Request.Permission, selected.Permission.Facts.Permission) || !reflect.DeepEqual(saved.Request.Binding, request.Binding) || saved.Closure == nil || !reflect.DeepEqual(*saved.Closure, selected.Failure.Facts) {
		return managed.ErrAuthority
	}
	signed, err := p.verifyAttestation(saved.Closure.Failure.Witness)
	if err != nil {
		return err
	}
	digest, err := removalRequestDigest(saved.Request)
	if err != nil {
		return err
	}
	saved.Closure = nil
	closed, err := bounded(saved)
	if err != nil || signed.Request != digest || signed.Closed != managed.Digest(closed) || signed.Response != nil {
		return managed.ErrAuthority
	}
	return ctx.Err()
}

func (p *ProviderRemoval) retainRetryInputs(ctx context.Context, request docker.ManagedDockerRequest) error {
	address, err := removalJournalKey(request.Invocation.Key)
	if err != nil {
		return err
	}
	address = managed.Digest([]byte("writerfloor.removal.retry.inputs.v1\n" + address))
	raw, err := bounded(request)
	if err != nil {
		return err
	}
	old, err := p.journal.Load(ctx, address)
	if err != nil {
		return err
	}
	if len(old) != 0 {
		if !bytes.Equal(old, raw) {
			return managed.ErrConflict
		}
		return nil
	}
	accepted, err := p.journal.CompareAndSwap(ctx, address, nil, raw)
	if err != nil {
		return err
	}
	if !accepted {
		return managed.ErrConflict
	}
	return nil
}

// Prepare persists the exact request before the controller's send marker. It never
// issues transport, even when the same historical request already exists.
func (p *ProviderRemoval) Prepare(ctx context.Context, request docker.ManagedDockerRequest) error {
	_, err := p.executor.Prepare(ctx, request)
	return err
}

// Request reads immutable prepared inputs for exact recovery and effect inspection.
func (p *ProviderRemoval) Request(ctx context.Context, key managed.ProviderInvocationKey) (docker.ManagedDockerRequest, error) {
	var empty docker.ManagedDockerRequest
	address, err := removalJournalKey(key)
	if err != nil {
		return empty, err
	}
	raw, err := p.journal.Load(ctx, address)
	if err != nil {
		return empty, err
	}
	var record docker.ManagedDockerRecord
	if operationErr := exactDecode(raw, &record); operationErr != nil {
		return empty, operationErr
	}
	if record.Request.Invocation == nil || record.Request.Invocation.Key != key {
		return empty, managed.ErrAuthority
	}
	exact, err := p.executor.Read(ctx, record.Request)
	if err != nil {
		return empty, err
	}
	return cloneRemovalRequest(exact.Request)
}

// Authorize commits the exact controller marker and captures only a newly issued
// grant. A lost reply or recovered marker cannot repopulate this volatile map.
func (p *ProviderRemoval) Authorize(ctx context.Context, command managed.ProviderInvocationCommitRequest, request docker.ManagedDockerRequest) (admission.Invocation, error) {
	var empty admission.Invocation
	request, err := cloneRemovalRequest(request)
	if err != nil {
		return empty, err
	}
	if request.Invocation == nil || command.Invocation != request.Invocation.Key {
		return empty, managed.ErrAuthority
	}
	if request.Retry == nil {
		if operationErr := p.Prepare(ctx, request); operationErr != nil {
			return empty, operationErr
		}
	} else {
		if !reflect.DeepEqual(request.Retry.Marker, managed.RecoveryRecord[managed.ProviderInvocationMarker]{}) {
			return empty, managed.ErrAuthority
		}
		if operationErr := p.verifyRequest(ctx, request, false); operationErr != nil {
			return empty, operationErr
		}
		if operationErr := p.retainRetryInputs(ctx, request); operationErr != nil {
			return empty, operationErr
		}
	}
	p.mu.Lock()
	full := len(p.grants)+len(p.sending) >= managed.MaxRecoverySteps
	p.mu.Unlock()
	if full {
		return empty, managed.ErrCapacity
	}
	grant, err := p.core.CommitManagedProviderInvocation(ctx, command)
	if err != nil {
		return empty, err
	}
	if !grant.MayDelegate {
		return empty, managed.ErrUnavailable
	}
	if request.Retry != nil {
		request.Retry.Marker = grant.Marker
		if operationErr := p.Prepare(ctx, request); operationErr != nil {
			return empty, operationErr
		}
	}
	invocation, err := removalInvocation(request)
	if err != nil {
		return empty, err
	}
	candidate := removalGrant{Request: request, Command: command.Command, Marker: grant.Marker, Invocation: invocation}
	if operationErr := p.fresh(ctx, candidate); operationErr != nil {
		return empty, operationErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.grants)+len(p.sending) >= managed.MaxRecoverySteps {
		return empty, managed.ErrCapacity
	}
	if _, exists := p.grants[invocation.ID]; exists {
		return empty, managed.ErrConflict
	}
	if _, exists := p.sending[invocation.ID]; exists {
		return empty, managed.ErrConflict
	}
	p.grants[invocation.ID] = candidate
	return invocation, nil
}

func (p *ProviderRemoval) fresh(ctx context.Context, grant removalGrant) error {
	if operationErr := p.VerifyRequest(ctx, grant.Request); operationErr != nil {
		return operationErr
	}
	key := grant.Request.Invocation.Key
	record, err := p.core.ReadManagedProviderInvocation(ctx, key)
	if err != nil {
		return err
	}
	if record.Marker == nil || !reflect.DeepEqual(*record.Marker, grant.Marker) || record.Closure != nil || record.Failure != nil || record.Result != nil {
		return managed.ErrAuthority
	}
	target, err := p.core.ReadManagedTarget(ctx, key.AttemptKey.Key)
	if err != nil {
		return err
	}
	revision, err := grant.Marker.Acceptance.Receipt.Revision.Uint64()
	if err != nil {
		return err
	}
	epoch, err := grant.Command.LeaseEpoch.Uint64()
	if err != nil {
		return err
	}
	if target == nil || target.Revision != revision || target.LeaseEpoch != epoch || target.LeaseOwner != grant.Command.Owner || !target.LeaseUntil.After(time.Now().UTC()) {
		return managed.ErrAuthority
	}
	attempt, err := p.core.ReadManagedRemovalAttempt(ctx, key.AttemptKey)
	if err != nil {
		return err
	}
	if attempt.State.AttemptRef != grant.Command.AttemptRef || attempt.State.RevocationDigest != "" || attempt.State.SettlementDigest != "" || attempt.State.AdmissionDigest == "" {
		return managed.ErrAuthority
	}
	commits, err := p.core.ReadManagedProviderInvocationCommits(ctx, key)
	if err != nil {
		return err
	}
	count, err := grant.Marker.Facts.BindingCount.Uint64()
	if err != nil || count == 0 || count > managed.MaxRecoveryBindings || uint64(len(commits)) != count {
		return managed.ErrAuthority
	}
	seen := map[managed.AttemptBindingKey]bool{}
	for _, commit := range commits {
		if commit.Facts.Invocation != key || seen[commit.Facts.Binding.Key] {
			return managed.ErrAuthority
		}
		seen[commit.Facts.Binding.Key] = true
		intent, readErr := p.core.ReadManagedRecoveryNativeIntent(ctx, managed.RecoveryExactRequest{Key: key.AttemptKey, Association: commit.Facts.Intent})
		if readErr != nil {
			return readErr
		}
		checked, checkErr := p.native.CheckManagedRemovalCommit(ctx, managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: key.AttemptKey, Intent: intent})
		if checkErr != nil {
			return checkErr
		}
		if !reflect.DeepEqual(checked, commit.Facts) {
			return managed.ErrAuthority
		}
	}
	return ctx.Err()
}

// Verify checks the fresh grant while the outer admission ledger holds its floor
// and restore-incarnation transaction. It never recovers a historical grant.
func (p *ProviderRemoval) Verify(ctx context.Context, invocation admission.Invocation) error {
	p.mu.Lock()
	grant, exists := p.grants[invocation.ID]
	p.mu.Unlock()
	if !exists || grant.Invocation != invocation {
		return managed.ErrAuthority
	}
	return p.fresh(ctx, grant)
}

// Dispatch consumes the outer grant before arming one executor admission. Only the
// configured admission Runtime calls it after durable external delegation commit.
func (p *ProviderRemoval) Dispatch(ctx context.Context, delegation admission.Delegation) error {
	p.mu.Lock()
	grant, exists := p.grants[delegation.Invocation.ID]
	delete(p.grants, delegation.Invocation.ID)
	p.mu.Unlock()
	if !exists || grant.Invocation != delegation.Invocation {
		return managed.ErrAuthority
	}
	if operationErr := p.fresh(ctx, grant); operationErr != nil {
		return operationErr
	}
	p.mu.Lock()
	p.sending[delegation.Invocation.ID] = grant
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.sending, delegation.Invocation.ID); p.mu.Unlock() }()
	_, err := p.executor.Execute(ctx, grant.Request)
	return err
}

// Admit consumes the armed grant even on refusal. It is called by the executor
// before its durable delegated transition, and cannot reissue after restart.
func (p *ProviderRemoval) Admit(ctx context.Context, request docker.ManagedDockerRequest) error {
	invocation, err := removalInvocation(request)
	if err != nil {
		return err
	}
	p.mu.Lock()
	grant, exists := p.sending[invocation.ID]
	delete(p.sending, invocation.ID)
	p.mu.Unlock()
	if !exists || grant.Invocation != invocation || !reflect.DeepEqual(grant.Request, request) {
		return managed.ErrAuthority
	}
	return p.fresh(ctx, grant)
}

type removalAttestation struct {
	Profile    string                        `json:"profile"`
	Request    string                        `json:"request"`
	Response   *docker.ManagedDockerResponse `json:"response,omitempty"`
	Closed     string                        `json:"closed,omitempty"`
	ObservedAt time.Time                     `json:"observed_at"`
	Signature  []byte                        `json:"signature"`
}

func (p *ProviderRemoval) attestation(value removalAttestation) ([]byte, error) {
	value.Signature = nil
	raw, err := bounded(value)
	if err != nil {
		return nil, err
	}
	value.Signature = ed25519.Sign(p.key, raw)
	return bounded(value)
}
func (p *ProviderRemoval) verifyAttestation(raw []byte) (removalAttestation, error) {
	var value removalAttestation
	if len(raw) > managed.MaxProviderEffectAttestation {
		return value, managed.ErrCapacity
	}
	if operationErr := exactDecode(raw, &value); operationErr != nil {
		return value, operationErr
	}
	signature := bytes.Clone(value.Signature)
	value.Signature = nil
	unsigned, err := bounded(value)
	value.Signature = signature
	if err != nil || value.Profile != removalCustodyProfile || !ed25519.Verify(ed25519.PublicKey(p.key[ed25519.SeedSize:]), unsigned, signature) {
		return value, managed.ErrAuthority
	}
	return value, nil
}
func (p *ProviderRemoval) journalRecord(ctx context.Context, request docker.ManagedDockerRequest) (docker.ManagedDockerRecord, error) {
	var result docker.ManagedDockerRecord
	if request.Invocation == nil {
		return result, managed.ErrInvalid
	}
	if operationErr := p.VerifyRequest(ctx, request); operationErr != nil {
		return result, operationErr
	}
	address, err := removalJournalKey(request.Invocation.Key)
	if err != nil {
		return result, err
	}
	raw, err := p.journal.Load(ctx, address)
	if err != nil {
		return result, err
	}
	if operationErr := exactDecode(raw, &result); operationErr != nil {
		return result, operationErr
	}
	if result.SchemaVersion != "1" || !reflect.DeepEqual(result.Request, request) {
		return result, managed.ErrAuthority
	}
	return result, nil
}

// SealResponse signs only the executor's exact durable observed response and the
// original signed creation incarnation. A caller-supplied success cannot qualify.
func (p *ProviderRemoval) SealResponse(ctx context.Context, request docker.ManagedDockerRequest, response docker.ManagedDockerResponse) (docker.ManagedDockerCustody, error) {
	var empty docker.ManagedDockerCustody
	record, err := p.journalRecord(ctx, request)
	if err != nil {
		return empty, err
	}
	if (record.State != "observed" && record.State != "complete") || record.Response == nil || *record.Response != response || response.ResourceID != request.Permission.ResourceID || response.ObservedAt.IsZero() {
		return empty, managed.ErrAuthority
	}
	command, err := managed.DecodeProviderEffectCommand(request.Binding.Command)
	if err != nil {
		return empty, err
	}
	digest, err := removalRequestDigest(request)
	if err != nil {
		return empty, err
	}
	attestation, err := p.attestation(removalAttestation{Profile: removalCustodyProfile, Request: digest, Response: &response, ObservedAt: response.ObservedAt})
	if err != nil {
		return empty, err
	}
	return docker.ManagedDockerCustody{ResourceID: response.ResourceID, Incarnation: command.Incarnation, Attestation: attestation}, nil
}

// VerifyCustody independently authenticates the immutable response and incarnation.
func (p *ProviderRemoval) VerifyCustody(ctx context.Context, request docker.ManagedDockerRequest, response docker.ManagedDockerResponse, custody docker.ManagedDockerCustody) error {
	expected, err := p.SealResponse(ctx, request, response)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, custody) {
		return managed.ErrAuthority
	}
	return nil
}

// NoDispatch signs only an already permanently closed exact journal row.
func (p *ProviderRemoval) NoDispatch(ctx context.Context, request docker.ManagedDockerRequest, closedDigest string) (managed.ProviderInvocationFailure, error) {
	var empty managed.ProviderInvocationFailure
	record, err := p.journalRecord(ctx, request)
	if err != nil {
		return empty, err
	}
	record.Closure = nil
	raw, err := bounded(record)
	if err != nil || record.State != "closed" || record.Response != nil || record.Custody != nil || managed.Digest(raw) != closedDigest {
		return empty, managed.ErrAuthority
	}
	digest, err := removalRequestDigest(request)
	if err != nil {
		return empty, err
	}
	stamp := time.Now().UTC()
	witness, err := p.attestation(removalAttestation{Profile: removalCustodyProfile, Request: digest, Closed: closedDigest, ObservedAt: stamp})
	if err != nil {
		return empty, err
	}
	permission, err := request.Permission.Value()
	if err != nil {
		return empty, err
	}
	permissionRaw, err := bounded(permission)
	if err != nil {
		return empty, err
	}
	return managed.ProviderInvocationFailure{Key: request.Invocation.Key, RootDigest: request.Invocation.RootDigest, PermissionDigest: managed.Digest(permissionRaw), Failure: managed.ProviderStepFailure{SchemaVersion: "1", Permission: permission, AttemptRef: request.Binding.Original.AttemptRef, InvocationID: request.Invocation.Key.CommandID, ProducerProfile: managed.LocalNoDispatchProfile, VerifierID: p.creation.identity.VerifierID, Class: "local_no_dispatch", Phase: "before_transport_delegate", Reason: "invocation_closed", ObservedAt: stamp, Witness: witness, WitnessDigest: managed.Digest(witness)}}, nil
}

// VerifyNoDispatch verifies a saved witness without mutating the executor journal.
func (p *ProviderRemoval) VerifyNoDispatch(ctx context.Context, request docker.ManagedDockerRequest, closedDigest string, witness managed.ProviderInvocationFailure) error {
	record, err := p.journalRecord(ctx, request)
	if err != nil {
		return err
	}
	record.Closure = nil
	raw, err := bounded(record)
	if err != nil || record.State != "closed" || record.Response != nil || record.Custody != nil || managed.Digest(raw) != closedDigest {
		return managed.ErrAuthority
	}
	signed, err := p.verifyAttestation(witness.Failure.Witness)
	if err != nil {
		return err
	}
	digest, err := removalRequestDigest(request)
	if err != nil {
		return err
	}
	permission, err := request.Permission.Value()
	if err != nil {
		return err
	}
	permissionRaw, err := bounded(permission)
	if err != nil {
		return err
	}
	expected := managed.ProviderInvocationFailure{Key: request.Invocation.Key, RootDigest: request.Invocation.RootDigest, PermissionDigest: managed.Digest(permissionRaw), Failure: managed.ProviderStepFailure{SchemaVersion: "1", Permission: permission, AttemptRef: request.Binding.Original.AttemptRef, InvocationID: request.Invocation.Key.CommandID, ProducerProfile: managed.LocalNoDispatchProfile, VerifierID: p.creation.identity.VerifierID, Class: "local_no_dispatch", Phase: "before_transport_delegate", Reason: "invocation_closed", ObservedAt: signed.ObservedAt, Witness: witness.Failure.Witness, WitnessDigest: managed.Digest(witness.Failure.Witness)}}
	if signed.Request != digest || signed.Closed != closedDigest || signed.Response != nil || signed.ObservedAt.Before(permission.IssuedAt) || !reflect.DeepEqual(expected, witness) {
		return managed.ErrAuthority
	}
	return nil
}

// VerifyProviderNoDispatch implements the configured core verifier's read-only
// closure check. It never calls the executor's mutating Witness operation.
func (p *ProviderRemoval) VerifyProviderNoDispatch(ctx context.Context, permission managed.RecoveryRecord[managed.ProviderInvocationPermission], marker *managed.RecoveryRecord[managed.ProviderInvocationMarker], failure managed.ProviderInvocationFailure) error {
	request, err := p.Request(ctx, permission.Facts.Key)
	if err != nil {
		return err
	}
	selected, err := p.core.ReadManagedProviderInvocation(ctx, permission.Facts.Key)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(selected.Permission, permission) || !reflect.DeepEqual(selected.Marker, marker) {
		return managed.ErrAuthority
	}
	record, err := p.executor.Read(ctx, request)
	if err != nil {
		return err
	}
	if record.State != "closed" || record.Closure == nil || !reflect.DeepEqual(*record.Closure, failure) {
		return managed.ErrAuthority
	}
	return nil
}

var (
	_ docker.ManagedDockerAuthority      = (*ProviderRemoval)(nil)
	_ docker.ManagedDockerRetryAuthority = (*ProviderRemoval)(nil)
	_ admission.Verifier                 = (*ProviderRemoval)(nil)
	_ admission.Delegate                 = (*ProviderRemoval)(nil)
)
