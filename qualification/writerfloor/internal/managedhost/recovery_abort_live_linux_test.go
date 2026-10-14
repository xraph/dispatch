//go:build linux

package managedhost

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

// This live rollback leaves every provider slot unissued. No absence, timeout,
// healthy process or synthetic provider response is used as nondeletion proof.
func TestLiveManagedRecoverySafeAbort(t *testing.T) {
	f := newLiveRecovery(t)
	key := f.original.Attempt
	candidate := f.original.Candidate
	originalContainer, originalNetwork := candidate.Container, candidate.Network
	if f.proof.Facts.ProcessDisposition != "live" || f.proof.Facts.EffectComplete != nil {
		t.Fatal("original live proof required")
	}
	cutoffRequest := f.command(t)
	cutoff, err := f.core.CutoffManagedRemovalRecovery(t.Context(), cutoffRequest)
	if err != nil {
		t.Fatal("permanent nonissuance cutoff", err)
	}
	if replay, e := f.core.CutoffManagedRemovalRecovery(t.Context(), cutoffRequest); e != nil || !reflect.DeepEqual(replay, cutoff) {
		t.Fatal("cutoff exact replay", e)
	}
	coverageRequest := f.command(t)
	coverage, err := f.core.SealManagedEffectiveRemovalCoverage(t.Context(), coverageRequest)
	if err != nil || coverage.Facts.Disposition != managed.SettledWithoutDeletion || len(coverage.Facts.Steps) == 0 {
		t.Fatal("complete retained nondeletion coverage", err)
	}
	for _, step := range coverage.Facts.Steps {
		if step.Disposition != managed.EffectiveStepUnissued {
			t.Fatal("abort fixture issued provider work")
		}
	}
	if replay, e := f.core.SealManagedEffectiveRemovalCoverage(t.Context(), coverageRequest); e != nil || !reflect.DeepEqual(replay, coverage) {
		t.Fatal("coverage exact replay", e)
	}
	preparedClose, err := f.guard.PrepareClose(t.Context(), f.original.Binding, liveAssociation(t, key, f.proof), liveAssociation(t, key, coverage), "actual-abort-close")
	if err != nil {
		t.Fatal("prepare nondeletion guard close", err)
	}
	producer := f.retainIntent(t, preparedClose)
	closed, err := f.guard.CloseManagedRemovalGuard(t.Context(), producer)
	if err != nil {
		t.Fatal("actual permanent native close", err)
	}
	if recovered, e := f.guard.RecoverManagedRemovalGuardClose(t.Context(), producer); e != nil || !reflect.DeepEqual(recovered, closed) {
		t.Fatal("exact native close recovery", e)
	}
	closeRequest := managed.RecoveryNativeCloseRequest{Command: f.command(t), Facts: closed}
	acceptedClose, err := f.core.RecordManagedRemovalGuardClose(t.Context(), closeRequest)
	if err != nil {
		t.Fatal("retain exact nondeletion close", err)
	}
	if err = f.guard.RetainGuardClose(t.Context(), key, acceptedClose); err != nil {
		t.Fatal("retain exact terminal anchor", err)
	}
	evidence := liveJSON(t, struct{ Coverage, Close managed.RecoveryAssociation }{liveAssociation(t, key, coverage), liveAssociation(t, key, acceptedClose)})
	settlement := managed.AttemptSettlementRequest{AttemptRequest: f.attempt(t), Disposition: managed.SettledWithoutDeletion, ProducerProfile: managed.LocalNoDispatchProfile, VerifierID: candidate.Custody.identity.VerifierID, Evidence: evidence, SettledAt: time.Now().UTC()}
	settled, err := f.core.SettleManagedRemovalRecovery(t.Context(), settlement)
	if err != nil {
		t.Fatal("accepted safe settlement", err)
	}
	if replay, e := f.core.SettleManagedRemovalRecovery(t.Context(), settlement); e != nil || replay != settled {
		t.Fatal("settlement exact replay", e)
	}
	original, err := f.core.ReadManagedAttemptBinding(t.Context(), f.original.Binding)
	if err != nil || original.Fence == nil {
		t.Fatal("exact original fence", err)
	}
	factDigest, err := managed.AttemptFenceFactDigest(*original.Fence)
	if err != nil {
		t.Fatal(err)
	}
	witnessRequest := managed.AttemptBindingSettlementRequest{AttemptRequest: f.attempt(t), ReservationID: f.original.Binding.ReservationID, FenceFactDigest: factDigest}
	witness, err := f.core.RecordManagedRemovalRecoveryBindingSettlement(t.Context(), witnessRequest)
	if err != nil {
		t.Fatal("original binding settlement witness", err)
	}
	if replay, e := f.core.RecordManagedRemovalRecoveryBindingSettlement(t.Context(), witnessRequest); e != nil || replay != witness {
		t.Fatal("witness exact replay", e)
	}
	binding, err := f.guard.readBinding(t.Context(), f.original.Binding)
	if err != nil {
		t.Fatal(err)
	}
	input := operator.QueryRemovalInput{QueryReservationInput: binding.Reservation, RequestID: "actual-prepared-native-abort"}
	fence := f.original.NativeBegin.QueryRuntime.Removal
	nativeRequest, err := f.guard.PrepareAbort(t.Context(), binding.Candidate, fence, input)
	if err != nil {
		t.Fatal("prepare real original-process Abort proof", err)
	}
	if replay, e := f.guard.PrepareAbort(t.Context(), binding.Candidate, fence, input); e != nil || !reflect.DeepEqual(replay, nativeRequest) {
		t.Fatal("preparation replaced fresh proof", e)
	}
	changed := input
	changed.RequestID = "different-prepared-abort"
	if _, e := f.guard.PrepareAbort(t.Context(), binding.Candidate, fence, changed); e == nil {
		t.Fatal("closed fence accepted different preparation")
	}
	originalBytes := liveJSON(t, input)
	nativeBytes := liveJSON(t, struct {
		Operation string `json:"operation"`
		Request   any    `json:"request"`
	}{string(durable.OperationAbortQueryRemoval), nativeRequest})
	command := managed.PortCommand{SchemaVersion: "2", OperationID: cid.New(managed.PrefixOperation), RequestID: input.RequestID, Original: originalBytes, OriginalDigest: managed.Digest(originalBytes), Native: nativeBytes, NativeDigest: managed.Digest(nativeBytes)}
	if validationErr := command.Validate(); validationErr != nil {
		t.Fatalf("exact Abort command envelope original=%d native=%d: %v", len(originalBytes), len(nativeBytes), validationErr)
	}
	commandRequest := managed.AttemptCommandRequest{AttemptRequest: f.attempt(t), ReservationID: f.original.Binding.ReservationID, Kind: managed.AttemptAbort, ExpectedNativeVersion: original.Fence.NativeAcceptance.BindingVersion, NativeCommand: command}
	grant, err := f.core.RecordManagedRemovalRecoveryCommand(t.Context(), commandRequest)
	if err != nil || !grant.NewlyIssued {
		t.Fatal("exact Abort command before native dispatch", err)
	}
	if replay, e := f.core.RecordManagedRemovalRecoveryCommand(t.Context(), commandRequest); e != nil || replay.NewlyIssued || !reflect.DeepEqual(replay.Command, grant.Command) {
		t.Fatal("command replay issued fresh authority", e)
	}
	if _, e := f.guard.client.AbortRemoval(t.Context(), changed); e == nil {
		t.Fatal("authenticated changed request bypassed prepared binding")
	}
	aborted, err := f.guard.client.AbortRemoval(t.Context(), input)
	if err != nil || aborted.Binding.State != durable.QueryRuntimeActive {
		t.Fatal("actual authenticated native Abort", err)
	}
	if replay, e := f.guard.client.AbortRemoval(t.Context(), input); e != nil || !reflect.DeepEqual(replay, aborted) {
		t.Fatal("native Abort replay changed receipt", e)
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: candidate.NativeIdentity.NamespaceTarget, Operation: durable.OperationAbortQueryRemoval, RequestID: input.RequestID, RequestDigest: command.NativeDigest, CommandDigest: nativeRequest.CommandDigest}
	exact, err := candidate.Native.LookupLifecycleReceipt(t.Context(), lookup)
	if err != nil || exact.Match(lookup) != nil || exact.QueryRuntime == nil || exact.QueryAbort == nil {
		t.Fatal("exact actual native Abort receipt", err)
	}
	if exact.QueryRuntime.QueryRuntimeIdentity != candidate.NativeIdentity || exact.QueryAbort.Fence != fence || exact.QueryAbort.Settlement != nativeRequest.Settlement || !exact.QueryRuntime.Verification.VerifiedAt.Equal(nativeRequest.Verification.VerifiedAt) {
		t.Fatal("native Abort replaced original process/proof")
	}
	raw := liveJSON(t, aborted)
	proof := liveJSON(t, *f.original.Registration.Material.Registration)
	acceptance := managed.AttemptNativeAcceptance{NativeAcceptance: managed.AttemptNativeFacts{RequestID: exact.RequestID, RequestDigest: exact.RequestDigest, CommandDigest: command.OriginalDigest, ResponseVersion: "1", BindingVersion: managed.Decimal(strconv.FormatInt(exact.QueryRuntime.Version, 10)), RemovalEpoch: managed.Decimal(strconv.FormatInt(fence.RemovalEpoch, 10)), AcceptedAt: exact.AcceptedAt}, NativeFenceDigest: binding.Original.FenceDigest, ReceiptID: exact.DeliveryID, Receipt: raw, ReceiptDigest: managed.Digest(raw), OriginalBindingProof: proof, ProofDigest: managed.Digest(proof)}
	abortRequest := managed.AttemptAbortRequest{AttemptRequest: f.attempt(t), ReservationID: f.original.Binding.ReservationID, Acceptance: acceptance, ProofVerifiedAt: nativeRequest.Verification.VerifiedAt, ProofValidUntil: nativeRequest.Verification.ValidUntil}
	receipt, err := f.core.RecordManagedRemovalRecoveryAbort(t.Context(), abortRequest)
	if err != nil {
		t.Fatal("core terminal Abort from actual native receipt", err)
	}
	if replay, e := f.core.RecordManagedRemovalRecoveryAbort(t.Context(), abortRequest); e != nil || replay != receipt {
		t.Fatal("core Abort exact replay", e)
	}
	retained, err := f.core.ReadManagedAttemptBinding(t.Context(), f.original.Binding)
	if err != nil || retained.Abort == nil || retained.Finish != nil || retained.Witness == nil || retained.Abort.NativeAcceptance.RequestDigest != command.NativeDigest {
		t.Fatal("missing exact terminal Abort", err)
	}
	target, err := f.core.ReadManagedTarget(t.Context(), key.Key)
	if err != nil || target.Phase == managed.Deleted {
		t.Fatal("safe Abort deleted target", err)
	}
	if _, e := f.core.TombstoneManagedRemovalRecovery(t.Context(), f.attempt(t)); e == nil {
		t.Fatal("nondeletion settlement authorized tombstone")
	}
	afterRefusal, err := f.core.ReadManagedTarget(t.Context(), key.Key)
	if err != nil || !reflect.DeepEqual(afterRefusal, target) {
		t.Fatal("refused tombstone changed original target", err)
	}
	if historical, e := f.core.ReadManagedEffectiveRemovalCoverage(t.Context(), key); e != nil || !reflect.DeepEqual(historical, coverage) {
		t.Fatal("historical nondeletion coverage changed", e)
	}
	delegations, err := f.admission.runtime.Page(t.Context(), "", 64)
	if err != nil || len(delegations) != 0 {
		t.Fatal("safe Abort delegated provider deletion", err)
	}
	current, err := f.guard.nativeRuntime(t.Context(), candidate.NativeIdentity.QueryRuntimeTarget)
	if err != nil || current.State != durable.QueryRuntimeActive || current.QueryRuntimeIdentity != candidate.NativeIdentity {
		t.Fatal("original native process not restored active", err)
	}
	fresh, err := candidate.Runtime.Verify(t.Context(), current)
	if err != nil || fresh.Identity != candidate.NativeIdentity || fresh.Validate(candidate.NativeIdentity, time.Now().UTC()) != nil {
		t.Fatal("original process no longer serves retained queries", err)
	}
	got := strings.TrimSpace(string(liveRestoreDocker(t, nil, 1024, "inspect", "--format", "{{.Id}} {{.State.Running}}", originalContainer)))
	if got != originalContainer+" true" {
		t.Fatal("original provider process replaced or removed")
	}
	got = strings.TrimSpace(string(liveRestoreDocker(t, nil, 1024, "network", "inspect", "--format", "{{.Id}}", originalNetwork)))
	if got != originalNetwork {
		t.Fatal("original provider network replaced or removed")
	}
	t.Log("safe Abort retained original process and network; exact native/core replay; no provider delegation")
}
