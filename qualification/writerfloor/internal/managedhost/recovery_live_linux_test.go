//go:build linux

package managedhost

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

// This is a real PostgreSQL, Docker, Authsome, Warden and mutual-TLS drill.
// Its helpers retain existing public receipts; they never manufacture native proof.
type liveRecoveryCore interface {
	NativeCore
	managed.RemovalRevalidationStore
	managed.ProviderInvocationStore
	managed.ProviderStepRetryStore
	managed.RemovalRecoveryLifecycleStore
	managed.RemovalCoverageSealer
	managed.ProviderInvocationSourcesStore
	RemovalCore
	ReadManagedProviderInvocationCurrent(context.Context, managed.ProviderInvocationKey) (managed.ProviderStepRetryCurrent, error)
}

type liveRecoveryHost struct {
	*NativeGuardHost
	*ProviderRemoval
}

func (h *liveRecoveryHost) WitnessManagedProviderNoDispatch(ctx context.Context, key managed.ProviderInvocationKey) (managed.ProviderInvocationFailure, error) {
	if h.ProviderRemoval == nil {
		return managed.ProviderInvocationFailure{}, managed.ErrUnavailable
	}
	return h.ProviderRemoval.Executor().WitnessManagedProviderNoDispatch(ctx, key)
}

type liveRecoveryFixture struct {
	original *liveOriginalLifecycle
	core     liveRecoveryCore
	effects  interface {
		RetainProviderEffectBinding(context.Context, managed.ProviderEffectBindingRequest) (managed.ProviderEffectBindingRecord, error)
	}
	guard     *NativeGuardHost
	provider  *ProviderRemoval
	admission *liveAdmissionClient
	ref       managed.AttemptRef
	proof     managed.RecoveryRecord[managed.RemovalRevalidationFacts]
}

func liveAssociation[T any](t *testing.T, key managed.AttemptKey, record managed.RecoveryRecord[T]) managed.RecoveryAssociation {
	t.Helper()
	ref, err := managed.RecoveryRecordAssociation(key, record)
	if err != nil {
		t.Fatal("accepted association", err)
	}
	return ref
}
func (f *liveRecoveryFixture) command(t *testing.T) managed.RecoveryCommand {
	t.Helper()
	command := liveCoreCommand(t, f.original.Candidate)
	return managed.RecoveryCommand{Key: command.Key, ID: command.ID, Revision: managed.Decimal(strconv.FormatUint(command.Revision, 10)), LeaseEpoch: managed.Decimal(strconv.FormatUint(command.LeaseEpoch, 10)), Owner: command.Owner, AttemptRef: f.ref, Profile: managed.RemovalRecoveryProfile}
}
func (f *liveRecoveryFixture) attempt(t *testing.T) managed.AttemptRequest {
	t.Helper()
	return managed.AttemptRequest{Command: liveCoreCommand(t, f.original.Candidate), AttemptID: f.original.Attempt.AttemptID, Generation: f.original.Attempt.Generation}
}
func (f *liveRecoveryFixture) retainIntent(t *testing.T, prepared managed.RecoveryNativeIntent) managed.RecoveryProducerContext {
	t.Helper()
	accepted, err := f.core.RecordManagedRecoveryNativeIntent(t.Context(), managed.RecoveryNativeIntentRequest{Command: f.command(t), Intent: prepared})
	if err != nil {
		t.Fatal("retain exact native intent", prepared.Operation, err)
	}
	return managed.RecoveryProducerContext{Profile: managed.RemovalRecoveryProfile, Key: f.original.Attempt, Intent: accepted}
}
func (f *liveRecoveryFixture) acceptProof(t *testing.T, prepared managed.RecoveryNativeIntent, expected string) {
	t.Helper()
	producer := f.retainIntent(t, prepared)
	facts, err := f.guard.RevalidateManagedRemoval(t.Context(), producer)
	if err != nil {
		t.Fatal("actual native revalidation", err)
	}
	f.proof, err = f.core.AcceptManagedRemovalRevalidation(t.Context(), managed.RemovalRevalidationRequest{Command: f.command(t), ExpectedCurrentDigest: expected, Facts: facts})
	if err != nil {
		t.Fatal("accept exact native revalidation", err)
	}
}

func newLiveRecovery(t *testing.T) *liveRecoveryFixture {
	t.Helper()
	original := newLiveOriginalLifecycle(t)
	candidate := original.Candidate
	preparation := prepareLiveAdmissionCore(t, candidate)
	admitted := admitLiveOriginal(t, original)
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	journalDirectory := filepath.Join(candidate.Root, "recovery-custody")
	if directoryErr := os.Mkdir(journalDirectory, 0700); directoryErr != nil {
		t.Fatal("private recovery journal", directoryErr)
	}
	journal, err := docker.OpenFileManagedDockerJournal(journalDirectory)
	if err != nil {
		t.Fatal(err)
	}
	identity := managed.RecoveryProducerIdentity{ProducerID: "actual-dispatch-guard", Profile: nativeGuardProfile, Version: "1", VerifierID: "actual-dispatch-verifier"}
	guard, err := NewNativeGuardHost(candidate.Core, candidate.Native, candidate.Runtime, original.Client, journal, "actual-dispatch-authority", identity, signingKey, original.Survivor.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	host := &liveRecoveryHost{NativeGuardHost: guard}
	repository, err := candidate.Core.WithRemovalRecovery(managed.RemovalRecoveryHost{Revalidation: host, Commit: host, Close: host, Closure: host, Verifier: host, RevalidationIdentity: identity, CommitIdentity: identity, CloseIdentity: identity, ClosureVerifierID: candidate.Custody.identity.VerifierID})
	if err != nil {
		t.Fatal("configure real recovery producers", err)
	}
	target, err := candidate.Core.ReadManagedTarget(t.Context(), candidate.Plan.Target)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := repository.WithProviderEffectBindings(managed.ProviderEffectBindingHost{Producer: candidate.Custody, Verifier: candidate.Custody, Identity: candidate.Custody.identity, Provider: target.Physical.Provider, BackendAuthority: candidate.Custody.backend})
	if err != nil {
		t.Fatal(err)
	}
	guard.core = repository
	provider, err := NewProviderRemoval(repository, candidate.Custody, guard, candidate.SDK, journal, signingKey)
	if err != nil {
		t.Fatal(err)
	}
	guard, err = guard.WithRemovalCustody(provider.Executor())
	if err != nil {
		t.Fatal(err)
	}
	host.NativeGuardHost, host.ProviderRemoval = guard, provider
	controller, err := candidate.Launcher.OpenQualificationOperator(t.Context(), candidate.Native, launcher.QualificationOperatorOptions{ControllerID: "recovery", Installation: candidate.Plan.Installation, Namespace: candidate.Plan.Namespace, Targets: []durable.QueryRuntimeTarget{candidate.NativeIdentity.QueryRuntimeTarget, original.Survivor.NativeIdentity.QueryRuntimeTarget}, Runtime: guard, GuardVerifier: guard})
	if err != nil {
		t.Fatal("real guarded operator", err)
	}
	server := httptest.NewUnstartedServer(controller.Handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{candidate.certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: candidate.roots}
	server.StartTLS()
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := controller.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	credential := controller.Credential
	client, err := NewNativeClient(NativeEndpoint{URL: server.URL, ServerName: candidate.Plan.ServerName, CAPEM: credential.CAPEM, CertificatePEM: credential.CertificatePEM, KeyPEM: credential.KeyPEM, Bearer: credential.Bearer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	// Both fixed host copies use the same authenticated endpoint. The provider copy
	// needs current native commits, while effect observations use its same executor.
	guard.client, provider.native.client = client, client
	fixture := &liveRecoveryFixture{original: original, core: repository, effects: effects, guard: guard, provider: provider, ref: admitted.State.AttemptRef}
	fixture.admission = newLiveAdmission(t, candidate, preparation, provider, provider)
	if _, err = repository.ActivateManagedRemovalRecovery(t.Context(), managed.RemovalRecoveryActivationRequest{Command: fixture.command(t)}); err != nil {
		t.Fatal("activate original graph", err)
	}
	live, err := guard.PrepareLive(t.Context(), original.Binding, "actual-live-proof", "")
	if err != nil {
		t.Fatal("prepare actual live guard", err)
	}
	fixture.acceptProof(t, live, "")
	return fixture
}

func (f *liveRecoveryFixture) remove(t *testing.T, service string) (managed.ProviderInvocationKey, docker.ManagedDockerRequest) {
	t.Helper()
	key := f.original.Attempt
	kind := managed.StepRemoveContainer
	if service == "" {
		kind = managed.StepRemoveNetwork
	}
	attempt, err := f.core.ReadManagedRemovalAttempt(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	step := managed.ProviderStepKey{OperationKey: managed.OperationKey{Key: key.Key, ActionID: attempt.Descriptor.ActionID}, ProviderOperationID: attempt.Descriptor.ProviderOperationID, Kind: kind, Service: service}
	pendingRequest := managed.GuardedOriginalRequest{Command: f.command(t), Step: step}
	pending, err := f.core.PrepareManagedGuardedOriginal(t.Context(), pendingRequest)
	if err != nil {
		t.Fatal("nonissuing original", service, err)
	}
	replay, err := f.core.PrepareManagedGuardedOriginal(t.Context(), pendingRequest)
	if err != nil || !reflect.DeepEqual(replay, pending) {
		t.Fatal("original acceptance replay", err)
	}
	binding, err := f.effects.RetainProviderEffectBinding(t.Context(), managed.ProviderEffectBindingRequest{Command: liveCoreCommand(t, f.original.Candidate), Original: pending.Original.Key})
	if err != nil {
		t.Fatal("signed creation custody", err)
	}
	root, err := f.core.MaterializeManagedGuardedOriginal(t.Context(), managed.GuardedOriginalMaterializationRequest{Command: f.command(t), Step: step, PendingDigest: pending.PendingDigest})
	if err != nil {
		t.Fatal("materialize original", err)
	}
	invocation := managed.ProviderInvocationKey{AttemptKey: key, Step: step, CommandID: pending.Original.Permission.CommandID}
	selected, err := f.core.ReadManagedProviderInvocation(t.Context(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := f.guard.PrepareCommit(t.Context(), f.original.Binding, invocation, liveAssociation(t, key, f.proof), "actual-commit-"+invocation.CommandID.String())
	if err != nil {
		t.Fatal("prepare native bind", err)
	}
	commit, err := f.guard.BindManagedRemovalCommit(t.Context(), f.retainIntent(t, prepared))
	if err != nil {
		t.Fatal("actual native bind", err)
	}
	if _, err = f.core.RecordManagedProviderNativeCommit(t.Context(), managed.ProviderNativeCommitRequest{Command: f.command(t), Facts: commit}); err != nil {
		t.Fatal("retain native bind", err)
	}
	request := docker.ManagedDockerRequest{BackendAuthority: f.original.Candidate.Custody.backend, Permission: binding.Facts.Original.Permission, Receipt: binding.Facts.Original.Receipt, Command: docker.ManagedDockerCommand{Kind: kind, ResourceID: pending.Original.Permission.ResourceID}, Binding: &binding.Facts, Invocation: &selected.Permission.Facts}
	current, err := f.core.ReadManagedProviderInvocationCurrent(t.Context(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	marker := managed.ProviderInvocationCommitRequest{Command: f.command(t), Invocation: invocation, ExpectedCurrentDigest: current.Digest}
	grant, err := f.provider.Authorize(t.Context(), marker, request)
	if err != nil {
		t.Fatal("fresh one-shot marker", err)
	}
	if err = f.admission.Invoke(t.Context(), grant); err != nil {
		t.Fatal("authenticated outer admission and actual SDK remove", err)
	}
	if _, err = f.provider.Authorize(t.Context(), marker, request); err == nil {
		t.Fatal("accepted marker recreated fresh send")
	}
	if err = f.admission.Invoke(t.Context(), grant); err != nil {
		t.Fatal("outer exact replay", err)
	}
	observed, err := f.provider.Executor().Read(t.Context(), request)
	if err != nil || observed.State != "complete" || observed.Response == nil || observed.Custody == nil {
		t.Fatal("durable actual Docker completion", err)
	}
	rows, err := f.admission.runtime.Page(t.Context(), "", 64)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Invocation == grant {
			count++
		}
	}
	if count != 1 {
		t.Fatal("replay created another outer delegation", count)
	}
	current, err = f.core.ReadManagedProviderInvocationCurrent(t.Context(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := fullPermissionDigest(pending.Original.Permission)
	if err != nil {
		t.Fatal(err)
	}
	result := managed.ProviderInvocationObservationRequest{Command: f.command(t), ExpectedCurrentDigest: current.Digest, Result: managed.ProviderInvocationResult{Key: invocation, RootDigest: root.Digest, PermissionDigest: digest, Result: managed.WireProviderStepResult{Permission: pending.Original.Permission, Outcome: managed.PortTerminal, ResourceID: observed.Response.ResourceID, Evidence: observed.Custody.Attestation, ObservedAt: observed.Response.ObservedAt}}}
	if err = f.core.RecordManagedProviderInvocationResult(t.Context(), result); err != nil {
		t.Fatal("retain exact actual response", err)
	}
	if err = f.core.RecordManagedProviderInvocationResult(t.Context(), result); err != nil {
		t.Fatal("exact result replay", err)
	}
	return invocation, request
}

func TestLiveManagedRecoveryPositiveFinish(t *testing.T) {
	f := newLiveRecovery(t)
	key := f.original.Attempt
	invocation, request := f.remove(t, "worker")
	f.remove(t, "")
	set, err := f.core.ReadManagedRemovalRevalidationSet(t.Context(), key)
	if err != nil || len(set.Current) != 1 {
		t.Fatal("exact pre-effect selection", err)
	}
	complete, err := f.guard.PrepareEffectComplete(t.Context(), f.original.Binding, invocation, liveAssociation(t, key, f.proof), request, "actual-effect-complete", f.proof.Facts.NativeMaterialDigest)
	if err != nil {
		t.Fatal("authenticated dead-process completion", err)
	}
	f.acceptProof(t, complete, set.Current[0].Digest)
	if f.proof.Facts.ProcessDisposition != "effect_complete" || f.proof.Facts.EffectComplete == nil {
		t.Fatal("missing affirmative effect completion")
	}
	if _, err = f.core.CutoffManagedRemovalRecovery(t.Context(), f.command(t)); err != nil {
		t.Fatal("permanent cutoff", err)
	}
	coverage, err := f.core.SealManagedEffectiveRemovalCoverage(t.Context(), f.command(t))
	if err != nil || coverage.Facts.Disposition != managed.Removed {
		t.Fatal("accepted complete positive coverage", err)
	}
	prepared, err := f.guard.PrepareClose(t.Context(), f.original.Binding, liveAssociation(t, key, f.proof), liveAssociation(t, key, coverage), "actual-close")
	if err != nil {
		t.Fatal("prepare close over sealed coverage", err)
	}
	closed, err := f.guard.CloseManagedRemovalGuard(t.Context(), f.retainIntent(t, prepared))
	if err != nil {
		t.Fatal("actual native guard close", err)
	}
	accepted, err := f.core.RecordManagedRemovalGuardClose(t.Context(), managed.RecoveryNativeCloseRequest{Command: f.command(t), Facts: closed})
	if err != nil {
		t.Fatal("retain native close", err)
	}
	if err = f.guard.RetainGuardClose(t.Context(), key, accepted); err != nil {
		t.Fatal("exact terminal closure anchor", err)
	}
	evidence := liveJSON(t, struct{ Coverage, Close managed.RecoveryAssociation }{liveAssociation(t, key, coverage), liveAssociation(t, key, accepted)})
	settlement := managed.AttemptSettlementRequest{AttemptRequest: f.attempt(t), Disposition: managed.Removed, ProducerProfile: "ctrlplane.managed.remove.v1", VerifierID: f.original.Candidate.Custody.identity.VerifierID, Evidence: evidence, SettledAt: time.Now().UTC()}
	receipt, err := f.core.SettleManagedRemovalRecovery(t.Context(), settlement)
	if err != nil {
		t.Fatal("complete retained settlement", err)
	}
	if replay, e := f.core.SettleManagedRemovalRecovery(t.Context(), settlement); e != nil || replay != receipt {
		t.Fatal("settlement replay", e)
	}
	f.finish(t)
}

func (f *liveRecoveryFixture) finish(t *testing.T) {
	t.Helper()
	key := f.original.Attempt
	binding, err := f.guard.readBinding(t.Context(), f.original.Binding)
	if err != nil {
		t.Fatal(err)
	}
	originalBinding, err := f.core.ReadManagedAttemptBinding(t.Context(), f.original.Binding)
	if err != nil || originalBinding.Fence == nil {
		t.Fatal("original terminal fence", err)
	}
	input := operator.QueryRemovalInput{QueryReservationInput: binding.Reservation, RequestID: "actual-native-finish"}
	native := durable.FinishQueryRemovalRequest{Fence: f.original.NativeBegin.QueryRuntime.Removal, RequestID: input.RequestID}
	original := liveJSON(t, input)
	nativeBytes := liveJSON(t, struct {
		Operation string `json:"operation"`
		Request   any    `json:"request"`
	}{string(durable.OperationFinishQueryRemoval), native})
	command := managed.PortCommand{SchemaVersion: "1", OperationID: cid.New(managed.PrefixOperation), RequestID: input.RequestID, Original: original, OriginalDigest: managed.Digest(original), Native: nativeBytes, NativeDigest: managed.Digest(nativeBytes)}
	authorized, err := f.core.RecordManagedRemovalRecoveryCommand(t.Context(), managed.AttemptCommandRequest{AttemptRequest: f.attempt(t), ReservationID: f.original.Binding.ReservationID, Kind: managed.AttemptFinish, ExpectedNativeVersion: originalBinding.Fence.NativeAcceptance.BindingVersion, NativeCommand: command})
	if err != nil || !authorized.NewlyIssued {
		t.Fatal("retained original Finish command", err)
	}
	finished, err := f.guard.client.FinishRemoval(t.Context(), input)
	if err != nil {
		t.Fatal("actual native Finish", err)
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: f.original.Candidate.NativeIdentity.NamespaceTarget, Operation: durable.OperationFinishQueryRemoval, RequestID: input.RequestID, RequestDigest: command.NativeDigest}
	exact, err := f.original.Candidate.Native.LookupLifecycleReceipt(t.Context(), lookup)
	if err != nil || exact.Match(lookup) != nil || exact.QueryRuntime == nil {
		t.Fatal("exact native Finish receipt", err)
	}
	raw := liveJSON(t, finished)
	proof := liveJSON(t, *f.original.Registration.Material.Registration)
	acceptance := managed.AttemptNativeAcceptance{NativeAcceptance: managed.AttemptNativeFacts{RequestID: exact.RequestID, RequestDigest: exact.RequestDigest, CommandDigest: command.OriginalDigest, ResponseVersion: "1", BindingVersion: managed.Decimal(strconv.FormatInt(exact.QueryRuntime.Version, 10)), RemovalEpoch: managed.Decimal(strconv.FormatInt(native.Fence.RemovalEpoch, 10)), AcceptedAt: exact.AcceptedAt}, NativeFenceDigest: binding.Original.FenceDigest, ReceiptID: exact.DeliveryID, Receipt: raw, ReceiptDigest: managed.Digest(raw), OriginalBindingProof: proof, ProofDigest: managed.Digest(proof)}
	request := managed.AttemptFinishRequest{AttemptRequest: f.attempt(t), ReservationID: f.original.Binding.ReservationID, Acceptance: acceptance}
	receipt, err := f.core.RecordManagedRemovalRecoveryFinish(t.Context(), request)
	if err != nil {
		t.Fatal("core Finish over exact native receipt", err)
	}
	if replay, e := f.core.RecordManagedRemovalRecoveryFinish(t.Context(), request); e != nil || replay != receipt {
		t.Fatal("Finish exact replay", e)
	}
	tombstone := f.attempt(t)
	receipt, err = f.core.TombstoneManagedRemovalRecovery(t.Context(), tombstone)
	if err != nil {
		t.Fatal("complete tombstone", err)
	}
	if replay, e := f.core.TombstoneManagedRemovalRecovery(t.Context(), tombstone); e != nil || replay != receipt {
		t.Fatal("tombstone exact replay", e)
	}
	target, err := f.core.ReadManagedTarget(t.Context(), key.Key)
	if err != nil || target.Phase != managed.Deleted {
		t.Fatal("terminal target", err)
	}
	if _, err = f.core.ReadManagedEffectiveRemovalCoverage(t.Context(), key); err != nil {
		t.Fatal("historical retained coverage", err)
	}
}
