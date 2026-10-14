//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

type removalTestCore struct {
	sources     managed.ProviderInvocationSources
	binding     managed.ProviderEffectBindingRecord
	invocation  managed.ProviderInvocationRecord
	target      *managed.Target
	commitCalls int
	history     map[managed.ProviderInvocationKey]managed.ProviderInvocationRecord
}

func (c *removalTestCore) ReadManagedTarget(context.Context, managed.Key) (*managed.Target, error) {
	return c.target, nil
}
func (c *removalTestCore) ReadManagedRemovalAttempt(context.Context, managed.AttemptKey) (managed.AttemptRecord, error) {
	return managed.AttemptRecord{}, managed.ErrUnavailable
}
func (c *removalTestCore) ReadProviderEffectBinding(_ context.Context, key managed.ProviderEffectBindingKey) (managed.ProviderEffectBindingRecord, error) {
	if key != c.binding.Facts.Original.Key {
		return managed.ProviderEffectBindingRecord{}, managed.ErrAuthority
	}
	return c.binding, nil
}
func (c *removalTestCore) ReadManagedProviderInvocation(_ context.Context, key managed.ProviderInvocationKey) (managed.ProviderInvocationRecord, error) {
	if key != c.invocation.Permission.Facts.Key {
		if record, exists := c.history[key]; exists {
			return record, nil
		}
		return managed.ProviderInvocationRecord{}, managed.ErrAuthority
	}
	return c.invocation, nil
}
func (c *removalTestCore) ReadManagedProviderInvocationSources(_ context.Context, key managed.ProviderInvocationKey) (managed.ProviderInvocationSources, error) {
	if _, err := c.ReadManagedProviderInvocation(context.Background(), key); err != nil {
		return managed.ProviderInvocationSources{}, err
	}
	return c.sources, nil
}
func (c *removalTestCore) ReadManagedProviderInvocationCommits(context.Context, managed.ProviderInvocationKey) ([]managed.RecoveryRecord[managed.ProviderNativeCommitFacts], error) {
	return nil, managed.ErrUnavailable
}
func (c *removalTestCore) ReadManagedRecoveryNativeIntent(context.Context, managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.RecoveryNativeIntent], error) {
	return managed.RecoveryRecord[managed.RecoveryNativeIntent]{}, managed.ErrUnavailable
}
func (c *removalTestCore) CommitManagedProviderInvocation(context.Context, managed.ProviderInvocationCommitRequest) (managed.ProviderInvocationGrant, error) {
	c.commitCalls++
	return managed.ProviderInvocationGrant{}, nil
}

func removalTestFixture(t *testing.T) (*ProviderRemoval, *removalTestCore, *creationTestJournal, *creationTestTransport, docker.ManagedDockerRequest) {
	t.Helper()
	creation, _, accepted, _, command := newCreationTest(t)
	if err := creation.PrepareCreation(t.Context(), accepted.authorization, command); err != nil {
		t.Fatal(err)
	}
	resource, err := creation.Execute(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	permission := accepted.acceptance.Permission
	permission.Step.Kind = managed.StepRemoveNetwork
	permission.Step.ActionID = id.New(managed.PrefixOperation)
	permission.Step.ProviderOperationID = id.New(managed.PrefixOperation)
	permission.CommandID = id.New(managed.PrefixOperation)
	permission.ResourceID = resource
	permission.Seal = 1
	permission.FenceDigest = strings.Repeat("f", 64)
	receipt := accepted.acceptance.Receipt
	receipt.ID = permission.CommandID
	receipt.AcceptedAt = permission.IssuedAt
	attempt := managed.AttemptKey{Key: permission.Step.Key, AttemptID: id.New(managed.PrefixOperation), Generation: "1"}
	original := managed.ProviderEffectOriginal{Key: managed.ProviderEffectBindingKey{AttemptKey: attempt, Step: permission.Step, OriginalCommandID: permission.CommandID}, AttemptRef: managed.AttemptRef{AttemptID: attempt.AttemptID, Generation: "1", DescriptorDigest: strings.Repeat("e", 64)}, Permission: managed.ProviderPermissionWire(permission), Receipt: managed.ReceiptWire(receipt)}
	facts, err := creation.RecoverProviderEffectBinding(t.Context(), original)
	if err != nil {
		t.Fatal(err)
	}
	invocation := managed.ProviderInvocationPermission{Key: managed.ProviderInvocationKey{AttemptKey: attempt, Step: permission.Step, CommandID: permission.CommandID}, RootDigest: strings.Repeat("b", 64), Ordinal: "0", Permission: original.Permission, ProviderCommandDigest: facts.ProviderCommandDigest}
	core := &removalTestCore{binding: managed.ProviderEffectBindingRecord{Facts: facts}, invocation: managed.ProviderInvocationRecord{Permission: managed.RecoveryRecord[managed.ProviderInvocationPermission]{Facts: invocation}}, target: accepted.target}
	journal := &creationTestJournal{rows: map[string][]byte{}}
	transport := &creationTestTransport{}
	p := &ProviderRemoval{core: core, creation: creation, journal: journal, key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{9}, 32)), grants: map[string]removalGrant{}, sending: map[string]removalGrant{}}
	p.executor, err = docker.NewManagedDockerExecutor(journal, transport, p, creation, creation.backend)
	if err != nil {
		t.Fatal(err)
	}
	request := docker.ManagedDockerRequest{BackendAuthority: creation.backend, Permission: original.Permission, Receipt: original.Receipt, Command: docker.ManagedDockerCommand{Kind: managed.StepRemoveNetwork, ResourceID: resource}, Binding: &facts, Invocation: &invocation}
	return p, core, journal, transport, request
}

func TestRemovalHistoricalMarkerNeverArmsExecutor(t *testing.T) {
	p, core, _, transport, request := removalTestFixture(t)
	if err := p.Prepare(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Executor().Execute(t.Context(), request); err == nil {
		t.Fatal("direct executor bypassed fresh outer grant")
	}
	if _, err := p.Authorize(t.Context(), managed.ProviderInvocationCommitRequest{Invocation: request.Invocation.Key}, request); err == nil || core.commitCalls != 1 {
		t.Fatal("historical marker became fresh", err)
	}
	invocation, err := removalInvocation(request)
	if err != nil {
		t.Fatal(err)
	}
	if p.Verify(t.Context(), invocation) == nil || p.Dispatch(t.Context(), admission.Delegation{Invocation: invocation}) == nil || transport.calls != 0 {
		t.Fatal("recovered permission delegated")
	}
	reopened := &ProviderRemoval{core: p.core, creation: p.creation, native: p.native, journal: p.journal, key: bytes.Clone(p.key), grants: map[string]removalGrant{}, sending: map[string]removalGrant{}}
	if reopened.Admit(t.Context(), request) == nil {
		t.Fatal("restart recovered fresh grant")
	}
}

func TestRemovalCustodyRequiresObservedExactJournal(t *testing.T) {
	p, _, journal, transport, request := removalTestFixture(t)
	if err := p.Prepare(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	response := docker.ManagedDockerResponse{ResourceID: request.Command.ResourceID, ObservedAt: time.Now().UTC()}
	if _, err := p.SealResponse(t.Context(), request, response); err == nil {
		t.Fatal("prepared row fabricated successful removal")
	}
	address, err := removalJournalKey(request.Invocation.Key)
	if err != nil {
		t.Fatal(err)
	}
	record := docker.ManagedDockerRecord{SchemaVersion: "1", Request: request, State: "observed", Response: &response}
	raw, err := bounded(record)
	if err != nil {
		t.Fatal(err)
	}
	journal.rows[address] = raw
	custody, err := p.SealResponse(t.Context(), request, response)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.VerifyCustody(t.Context(), request, response, custody); err != nil {
		t.Fatal(err)
	}
	command, err := managed.DecodeProviderEffectCommand(request.Binding.Command)
	if err != nil || custody.Incarnation != command.Incarnation || custody.OriginalProcess != nil {
		t.Fatal("custody inferred process", err)
	}
	forged := custody
	forged.Attestation = append([]byte(nil), custody.Attestation...)
	forged.Attestation[len(forged.Attestation)-2] ^= 1
	if p.VerifyCustody(t.Context(), request, response, forged) == nil {
		t.Fatal("forged custody accepted")
	}
	changed := response
	changed.ResourceID = strings.Repeat("f", 64)
	if _, err = p.SealResponse(t.Context(), request, changed); err == nil {
		t.Fatal("substituted API response accepted")
	}
	record.State = "complete"
	record.Custody = &custody
	raw, err = bounded(record)
	if err != nil {
		t.Fatal(err)
	}
	journal.rows[address] = raw
	saved, err := p.Request(t.Context(), request.Invocation.Key)
	if err != nil || !reflect.DeepEqual(saved, request) {
		t.Fatal("exact request recovery", err)
	}
	if _, err = p.Executor().Execute(t.Context(), request); err != nil || transport.calls != 0 {
		t.Fatal("complete replay resent", err)
	}
}

func TestRemovalNoDispatchRequiresPermanentExactClose(t *testing.T) {
	p, core, _, transport, request := removalTestFixture(t)
	if err := p.Prepare(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := p.NoDispatch(t.Context(), request, strings.Repeat("a", 64)); err == nil {
		t.Fatal("caller invented closure")
	}
	closed, err := p.Executor().WitnessManagedProviderNoDispatch(t.Context(), request.Invocation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.VerifyProviderNoDispatch(t.Context(), core.invocation.Permission, nil, closed); err != nil {
		t.Fatal(err)
	}
	replay, err := p.Executor().WitnessManagedProviderNoDispatch(t.Context(), request.Invocation.Key)
	if err != nil || !reflect.DeepEqual(replay, closed) {
		t.Fatal("closure replay changed", err)
	}
	if _, err = p.Executor().Execute(t.Context(), request); err == nil || transport.calls != 0 {
		t.Fatal("closed executor delegated", err)
	}
	forged := closed
	forged.Failure.Reason = "fabricated"
	if p.VerifyProviderNoDispatch(t.Context(), core.invocation.Permission, nil, forged) == nil {
		t.Fatal("changed closure accepted")
	}
}

func TestRemovalRequestRejectsOriginalSubstitution(t *testing.T) {
	for _, change := range []string{"receipt", "resource", "incarnation", "ordinal"} {
		t.Run(change, func(t *testing.T) {
			p, _, _, transport, request := removalTestFixture(t)
			cloned, err := cloneRemovalRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "receipt":
				cloned.Receipt.Digest = strings.Repeat("c", 64)
			case "resource":
				cloned.Command.ResourceID = strings.Repeat("d", 64)
			case "incarnation":
				cloned.Binding.Attestation = []byte("forged")
			case "ordinal":
				cloned.Invocation.Ordinal = "1"
			}
			if p.Prepare(t.Context(), cloned) == nil || transport.calls != 0 {
				t.Fatal("changed original prepared or delegated")
			}
		})
	}
}

func removalRetryRecord[T any](t *testing.T, kind managed.RecoveryRecordKind, facts T, revision uint64, at time.Time) managed.RecoveryRecord[T] {
	t.Helper()
	digest, err := managed.RecoveryFactDigest(facts)
	if err != nil {
		t.Fatal(err)
	}
	receipt := managed.Receipt{ID: id.New(managed.PrefixOperation), Digest: strings.Repeat("d", 64), Revision: revision, LeaseEpoch: 1, AcceptedAt: at, LeaseUntil: at.Add(time.Minute)}
	record := managed.RecoveryRecord[T]{SchemaVersion: "1", Kind: kind, Facts: facts, Digest: digest, Acceptance: managed.RecoveryAcceptance{OperationID: receipt.ID, RequestDigest: receipt.Digest, Receipt: managed.ReceiptWire(receipt)}}
	if _, err := managed.EncodeRecoveryRecord(record); err != nil {
		t.Fatal(err)
	}
	return record
}

func removalRetryReference[T any](t *testing.T, label string, record managed.RecoveryRecord[T]) managed.RecoveryAssociation {
	t.Helper()
	reference, err := managed.RecoveryAssociationFor(managed.Digest([]byte(label)), record)
	if err != nil {
		t.Fatal(err)
	}
	return reference
}

// This helper supplies accepted core records, while the predecessor closure is
// produced by the actual executor. Real core issuance is covered by the live drill.
func removalRetryFixture(t *testing.T, ordinal uint64) (*ProviderRemoval, *removalTestCore, *creationTestJournal, *creationTestTransport, docker.ManagedDockerRequest) {
	t.Helper()
	p, core, journal, transport, original := removalTestFixture(t)
	at := original.Permission.IssuedAt
	command, err := managed.DecodeProviderEffectCommand(original.Binding.Command)
	if err != nil {
		t.Fatal(err)
	}
	root := removalRetryRecord(t, managed.RecoveryRootRecord, managed.ProviderStepRetryRoot{AttemptRef: original.Binding.Original.AttemptRef, Step: original.Permission.Step, Profile: managed.ProviderStepRetryProfile, SpecDigest: original.Permission.SpecDigest, ResourceID: original.Permission.ResourceID, Incarnation: command.Incarnation, ProviderCommandDigest: original.Binding.ProviderCommandDigest, OriginalPermission: original.Permission, OriginalPermissionReceipt: original.Receipt}, 6, at)
	core.sources = managed.ProviderInvocationSources{Root: root}
	original.Invocation.RootDigest = root.Digest
	core.invocation.Permission = removalRetryRecord(t, managed.RecoveryPermissionRecord, *original.Invocation, 6, at)
	core.invocation.Permission.Acceptance = root.Acceptance
	core.history = map[managed.ProviderInvocationKey]managed.ProviderInvocationRecord{}
	request := original
	for index := uint64(1); index <= ordinal; index++ {
		if err := p.Prepare(t.Context(), request); err != nil {
			t.Fatal("prepare predecessor", err)
		}
		failure, err := p.Executor().WitnessManagedProviderNoDispatch(t.Context(), request.Invocation.Key)
		if err != nil {
			t.Fatal("close predecessor", err)
		}
		failed := removalRetryRecord(t, managed.RecoveryFailureRecord, failure, 6+index*3, at)
		closed := removalRetryRecord(t, managed.RecoveryClosureRecord, managed.ProviderInvocationClosure{Key: request.Invocation.Key, RootDigest: root.Digest, PermissionDigest: failure.PermissionDigest, FailureDigest: failed.Digest, ProducerProfile: managed.LocalNoDispatchProfile, VerifierID: p.creation.identity.VerifierID, ClosedAt: failure.Failure.ObservedAt}, 6+index*3, at)
		closed.Acceptance = failed.Acceptance
		predecessor := core.invocation
		predecessor.Failure, predecessor.Closure = &failed, &closed
		core.history[request.Invocation.Key] = predecessor
		authority := removalRetryReference(t, "root", root)
		if predecessor.Authorization != nil {
			authority = removalRetryReference(t, "authorization", *predecessor.Authorization)
		}
		previous := managed.ProviderStepRetryPrevious{SchemaVersion: "1", RootDigest: root.Digest, CommandID: request.Permission.CommandID, Ordinal: request.Invocation.Ordinal, PermissionDigest: failure.PermissionDigest, Failure: removalRetryReference(t, "failure", failed), Closure: removalRetryReference(t, "closure", closed), Authority: authority}
		previous.Digest, err = managed.RetryPreviousDigest(previous)
		if err != nil {
			t.Fatal(err)
		}
		permission := original.Permission
		permission.CommandID = id.New(managed.PrefixOperation)
		permission.Digest = strings.Repeat("d", 64)
		permission.Revision = managed.Decimal(strconv.FormatUint(7+index*3, 10))
		key := original.Invocation.Key
		key.CommandID = permission.CommandID
		accepted := removalRetryRecord(t, managed.RecoveryPermissionRecord, managed.ProviderInvocationPermission{Key: key, RootDigest: root.Digest, Ordinal: managed.Decimal(strconv.FormatUint(index, 10)), Permission: permission, ProviderCommandDigest: root.Facts.ProviderCommandDigest, PermissionProofCount: "1", PermissionProofSetDigest: strings.Repeat("a", 64)}, 7+index*3, at)
		authorized := removalRetryRecord(t, managed.RecoveryAuthorizationRecord, managed.ProviderStepRetryAuthorization{Key: key, RootDigest: root.Digest, Ordinal: accepted.Facts.Ordinal, Permission: removalRetryReference(t, "permission", accepted), Previous: previous, ExpectedCurrentDigest: strings.Repeat("b", 64), ParentRevision: permission.Revision, AttemptRevision: "3", AuthorizedAt: at}, 7+index*3, at)
		authorized.Acceptance = accepted.Acceptance
		local, err := permission.Value()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := bounded(local)
		if err != nil {
			t.Fatal(err)
		}
		marker := removalRetryRecord(t, managed.RecoveryMarkerRecord, managed.ProviderInvocationMarker{Key: key, RootDigest: root.Digest, PermissionDigest: managed.Digest(raw), SelectionDigest: strings.Repeat("c", 64), NativeCommitSetDigest: strings.Repeat("e", 64), CurrentProofSetDigest: strings.Repeat("f", 64), BindingCount: "1", AcceptedAt: at}, 8+index*3, at)
		request = docker.ManagedDockerRequest{BackendAuthority: original.BackendAuthority, Permission: permission, Receipt: accepted.Acceptance.Receipt, Command: original.Command, Binding: original.Binding, Invocation: &accepted.Facts, Retry: &docker.ManagedDockerRetryEvidence{Root: root, Permission: accepted, Authorization: authorized, Marker: marker}}
		core.invocation = managed.ProviderInvocationRecord{Permission: accepted, Authorization: &authorized, Marker: &marker}
	}
	return p, core, journal, transport, request
}

func TestRemovalRetryExactPredecessorAndReopen(t *testing.T) {
	for _, ordinal := range []uint64{1, 2} {
		t.Run(strconv.FormatUint(ordinal, 10), func(t *testing.T) {
			p, _, _, transport, request := removalRetryFixture(t, ordinal)
			if err := p.Prepare(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := p.VerifyRetry(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			saved, err := p.Request(t.Context(), request.Invocation.Key)
			if err != nil || !reflect.DeepEqual(saved, request) {
				t.Fatal("retry inspection", err)
			}
			if _, err := p.Executor().Execute(t.Context(), request); err == nil || transport.calls != 0 {
				t.Fatal("historical retry sent", err)
			}
			if err := p.AdmitRetry(t.Context(), request); err == nil {
				t.Fatal("retry manufactured grant")
			}
		})
	}
}

func TestRemovalRetryAuthoritySubstitution(t *testing.T) {
	for _, change := range []string{"root_receipt", "authorization", "marker", "predecessor_missing", "predecessor_delegated", "predecessor_result", "journal_witness"} {
		t.Run(change, func(t *testing.T) {
			p, core, journal, transport, request := removalRetryFixture(t, 1)
			previousKey := request.Invocation.Key
			previousKey.CommandID = request.Retry.Authorization.Facts.Previous.CommandID
			switch change {
			case "root_receipt":
				request.Retry.Root.Acceptance.Receipt.Revision = "7"
			case "authorization":
				request.Retry.Authorization.Facts.Previous.Digest = strings.Repeat("f", 64)
			case "marker":
				request.Retry.Marker.Facts.SelectionDigest = strings.Repeat("0", 64)
			case "predecessor_missing":
				delete(core.history, previousKey)
			case "predecessor_result":
				previous := core.history[previousKey]
				previous.Result = &managed.RecoveryRecord[managed.ProviderInvocationResult]{}
				core.history[previousKey] = previous
			case "predecessor_delegated", "journal_witness":
				address, err := removalJournalKey(previousKey)
				if err != nil {
					t.Fatal(err)
				}
				var previous docker.ManagedDockerRecord
				if decodeErr := exactDecode(journal.rows[address], &previous); decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if change == "predecessor_delegated" {
					previous.State = "delegated"
				} else {
					previous.Closure.Failure.Witness = []byte("forged")
				}
				journal.rows[address], err = bounded(previous)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := p.Prepare(t.Context(), request); err == nil || transport.calls != 0 {
				t.Fatal("altered retry prepared", err)
			}
		})
	}
}

func TestRemovalRetryRetainsInputsBeforeFreshMarker(t *testing.T) {
	p, core, journal, transport, request := removalRetryFixture(t, 1)
	request.Retry.Marker = managed.RecoveryRecord[managed.ProviderInvocationMarker]{}
	core.invocation.Marker = nil
	command := managed.ProviderInvocationCommitRequest{Invocation: request.Invocation.Key}
	if _, err := p.Authorize(t.Context(), command, request); err == nil || core.commitCalls != 1 {
		t.Fatal("historical retry marker granted", err)
	}
	address, err := removalJournalKey(request.Invocation.Key)
	if err != nil {
		t.Fatal(err)
	}
	preimageKey := managed.Digest([]byte("writerfloor.removal.retry.inputs.v1\n" + address))
	raw, err := bounded(request)
	if err != nil || !bytes.Equal(journal.rows[preimageKey], raw) || len(journal.rows[address]) != 0 || transport.calls != 0 {
		t.Fatal("pre-marker request not retained without transport", err)
	}
	changed, err := cloneRemovalRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	changed.Command.ResourceID = strings.Repeat("b", 64)
	if err := p.retainRetryInputs(t.Context(), changed); err == nil {
		t.Fatal("pre-marker inputs overwritten")
	}
}
