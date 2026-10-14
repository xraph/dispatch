//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

// liveOriginalLifecycle retains the real native and core witnesses for later
// guarded removal qualification. It does not grant provider deletion authority.
type liveOriginalLifecycle struct {
	Candidate, Survivor *liveServiceFixture
	Controller          *launcher.QualificationOperator
	Client              *NativeClient
	Action              managed.ActionSubmission
	Registration        managed.OriginalLifecycleRecord
	Drain               managed.OriginalLifecycleRecord
	Attempt             managed.AttemptKey
	Binding             managed.AttemptBindingKey
	Begin               managed.AttemptCommandGrant
	NativeBegin         durable.LifecycleReceipt
}

func liveOriginalCommand(t *testing.T, f *liveServiceFixture, reservation cid.ID) managed.OriginalCommand {
	t.Helper()
	c := liveCoreCommand(t, f)
	return managed.OriginalCommand{Key: c.Key, ID: c.ID, Revision: managed.Decimal(strconv.FormatUint(c.Revision, 10)), LeaseEpoch: managed.Decimal(strconv.FormatUint(c.LeaseEpoch, 10)), Owner: c.Owner, ReservationID: reservation}
}
func liveNativeAcceptance(receipt durable.LifecycleReceipt) managed.OriginalNativeAcceptance {
	return managed.OriginalNativeAcceptance{Operation: string(receipt.Operation), RequestID: receipt.RequestID, RequestDigest: receipt.RequestDigest, CommandDigest: receipt.CommandDigest, ResponseVersion: managed.Decimal(strconv.Itoa(receipt.ResponseVersion)), DeliveryID: receipt.DeliveryID, AcceptedAt: receipt.AcceptedAt}
}
func liveNativeReceipt(t *testing.T, f *liveServiceFixture, operation durable.LifecycleOperation, requestID, commandDigest string) durable.LifecycleReceipt {
	t.Helper()
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: f.NativeIdentity.NamespaceTarget, Operation: operation, RequestID: requestID, CommandDigest: commandDigest}
	receipt, err := f.Native.LookupLifecycleReceipt(t.Context(), lookup)
	if err != nil || receipt.Match(lookup) != nil {
		t.Fatal("exact native original receipt", err)
	}
	return receipt
}
func liveFingerprint(t *testing.T, operation string, request any) string {
	t.Helper()
	digest, err := durable.Fingerprint(operation, request)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
func liveProcess(t *testing.T, f *liveServiceFixture) managed.OriginalProcessFacts {
	t.Helper()
	identity, err := f.Runtime.ResolveBinding(t.Context(), f.NativeIdentity.QueryRuntimeTarget)
	if err != nil || identity != f.NativeIdentity {
		t.Fatal("authenticated original process", err)
	}
	b := identity.BuildIdentity
	process := managed.OriginalProcessFacts{Installation: identity.InstallationID, Namespace: identity.Namespace, Build: identity.BuildID, Queue: f.Startup.Queue, RuntimeID: identity.RuntimeID, InstanceID: identity.InstanceID, HostID: f.Startup.HostID, IdentityVersion: managed.Decimal(strconv.FormatInt(identity.IdentityVersion, 10)), ArtifactDigest: b.ArtifactDigest, ConfigurationDigest: b.ConfigurationDigest, ConfigurationVersion: b.ConfigurationVersion, EnrollmentEvidenceDigest: b.EnrollmentEvidenceDigest, ProbePolicyID: b.ProbePolicyID, ProbePolicyVersion: managed.Decimal(strconv.FormatInt(b.ProbePolicyVersion, 10)), MaximumProofValidityNS: managed.Decimal(strconv.FormatInt(int64(b.MaximumProofValidity), 10)), VerifierID: b.VerifierID, Descriptor: f.Startup.Service.Startup, ProducerVerifier: f.Startup.ProducerVerifier}
	if !nativeIdentityMatchesOriginal(identity, process) {
		t.Fatal("original process conversion")
	}
	return process
}

func newLiveOriginalLifecycle(t *testing.T) *liveOriginalLifecycle {
	t.Helper()
	f := newLiveService(t)
	survivor := newLiveServiceSibling(t, f)
	controller, client := newLiveOperator(t, f, survivor)
	out := &liveOriginalLifecycle{Candidate: f, Survivor: survivor, Controller: controller, Client: client}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	process := liveProcess(t, f)
	// ResolveBinding challenges the actual process after its exact fresh start.
	if err := f.Core.RecordManagedProviderStepResult(ctx, managed.ProviderStepResult{Permission: f.Start.Permission, Outcome: managed.PortTerminal, ResourceID: f.Container, Evidence: liveJSON(t, process), ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal("exact start result", err)
	}
	network, err := f.Custody.ReadCreation(ctx, docker.ManagedDockerCommand{Kind: managed.StepCreateNetwork, Name: "managed-" + f.Plan.Target.InstanceID.String() + "-network"})
	if err != nil {
		t.Fatal(err)
	}
	container, err := f.Custody.ReadCreation(ctx, docker.ManagedDockerCommand{Kind: managed.StepCreateContainer, Name: "managed-" + f.Plan.Target.InstanceID.String() + "-worker", NetworkID: f.Network, Service: &f.Plan.Service})
	if err != nil {
		t.Fatal(err)
	}
	if network.ResourceID != f.Network || container.ResourceID != f.Container {
		t.Fatal("signed creation resource mismatch")
	}
	if _, err = f.Core.BindManagedResources(ctx, managed.ResourcesRequest{Command: liveCoreCommand(t, f), OperationID: f.Plan.ProviderOperationID, Receipt: liveJSON(t, []docker.ManagedDockerCustody{network, container}), Resources: map[string]string{"network": f.Network, "worker": f.Container}}); err != nil {
		t.Fatal("bind signed resources", err)
	}
	input := operator.QueryRuntimeInput{BuildInput: operator.BuildInput{Namespace: f.Plan.Namespace, BuildID: f.Plan.Build}, RuntimeID: f.Plan.RuntimeID}
	registration := operator.QueryRuntimeCommand{QueryRuntimeInput: input, RequestID: "core-original-register", ExpectedVersion: "0"}
	registrationBytes := liveJSON(t, registration)
	rid := cid.New(managed.PrefixReservation)
	if _, err = f.Core.ReserveManagedRegistration(ctx, managed.ReserveRequest{Command: liveCoreCommand(t, f), Registration: managed.Registration{ID: rid, Member: "worker", RuntimeID: process.RuntimeID, InstanceID: process.InstanceID, HostID: process.HostID, RequestID: registration.RequestID, Command: registrationBytes, CommandDigest: managed.Digest(registrationBytes)}}); err != nil {
		t.Fatal("reserve original registration", err)
	}
	if _, err = f.Core.IssueManagedRegistration(ctx, managed.ReservationRequest{Command: liveCoreCommand(t, f), ReservationID: rid}); err != nil {
		t.Fatal("issue original registration", err)
	}
	registered, err := client.RegisterRuntime(ctx, registration)
	if err != nil || registered.Binding.Version != "1" || registered.Binding.InstanceID != process.InstanceID {
		t.Fatal("native registration", err)
	}
	receipt := liveNativeReceipt(t, f, durable.OperationRegisterQueryRuntime, registration.RequestID, liveFingerprint(t, "operator.query_runtime.register.v1", registration))
	if receipt.QueryRuntime == nil || receipt.QueryRuntime.QueryRuntimeIdentity != f.NativeIdentity {
		t.Fatal("original native registration identity")
	}
	nativeBytes := liveJSON(t, receipt)
	acceptance := managed.OriginalRegistrationAcceptanceRequest{Command: liveOriginalCommand(t, f, rid), Facts: managed.OriginalRegistrationFacts{Process: process, Native: liveNativeAcceptance(receipt), Member: "worker", Role: managed.Polling, StoredCommandDigest: managed.Digest(registrationBytes), BindingVersion: "1", BindingState: receipt.QueryRuntime.State, RemovalEpoch: "0", CreatedAt: receipt.QueryRuntime.CreatedAt, ChangedAt: receipt.QueryRuntime.ChangedAt}, NativeReceipt: nativeBytes, NativeReceiptDigest: managed.Digest(nativeBytes)}
	if _, err = f.Core.AcceptManagedOriginalRegistration(ctx, acceptance); err != nil {
		t.Fatal("retain original registration", err)
	}
	out.Registration, err = f.Core.ReadManagedOriginalLifecycle(ctx, managed.OriginalLifecycleKey{Key: f.Plan.Target, Kind: managed.OriginalRegistration, OperationID: acceptance.Command.ID})
	if err != nil {
		t.Fatal(err)
	}
	target, err := f.Core.ReadManagedTarget(ctx, f.Plan.Target)
	if err != nil {
		t.Fatal(err)
	}
	action := managed.ActionSubmission{Command: liveCoreCommand(t, f), Kind: managed.ActionPause, Installation: f.Action.Installation, Authority: f.Action.Authority, ApplicationID: f.Action.ApplicationID, OrganizationID: f.Action.OrganizationID, Actor: f.Action.Actor, OriginalCommand: []byte(`{"operation":"pause-original-service"}`), ExpectedDesiredRevision: target.DesiredRevision, OccurredAt: time.Now().UTC()}
	action.OriginalDigest = managed.Digest(action.OriginalCommand)
	action.CanonicalDigest, err = managed.ActionDigest(action)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Core.SubmitManagedAction(ctx, action); err != nil {
		t.Fatal("accepted pause action", err)
	}
	out.Action = action
	drain := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput(input), RequestID: "core-original-drain", OperationID: "core-process-drain", Deadline: durable.Timestamp(time.Now().Add(45 * time.Second))}
	drained, err := client.DrainWorker(ctx, drain)
	if err != nil || drained.Process != "complete" || !drained.Quiescent || !drained.Complete || drained.RuntimeID != process.RuntimeID || drained.InstanceID != process.InstanceID || drained.OperationID != drain.OperationID || !drained.Deadline.Equal(drain.Deadline) {
		t.Fatal("actual original drain", err)
	}
	receipt = liveNativeReceipt(t, f, durable.OperationRequestWorkerDrain, drain.RequestID, liveFingerprint(t, "operator.worker.drain.v1", drain))
	nativeBytes = liveJSON(t, receipt)
	drainAcceptance := managed.OriginalDrainAcceptanceRequest{Command: liveOriginalCommand(t, f, rid), Facts: managed.OriginalDrainFacts{Process: process, Native: liveNativeAcceptance(receipt), OperationID: drained.OperationID, Deadline: drained.Deadline, ProcessAccepted: true, AdmissionClosed: true, ObservedAt: time.Now().UTC()}, NativeReceipt: nativeBytes, NativeReceiptDigest: managed.Digest(nativeBytes)}
	if _, err = f.Core.RecordManagedOriginalDrain(ctx, drainAcceptance); err != nil {
		t.Fatal("retain original drain", err)
	}
	out.Drain, err = f.Core.ReadManagedOriginalLifecycle(ctx, managed.OriginalLifecycleKey{Key: f.Plan.Target, Kind: managed.OriginalDrain, OperationID: drainAcceptance.Command.ID})
	if err != nil {
		t.Fatal(err)
	}
	// Only the survivor persists a verification. The first candidate Begin uses
	// the original version 1, as required by the core generation-one contract.
	survivorInput := operator.QueryRuntimeInput{BuildInput: input.BuildInput, RuntimeID: survivor.Plan.RuntimeID}
	if _, err = client.RegisterRuntime(ctx, operator.QueryRuntimeCommand{QueryRuntimeInput: survivorInput, RequestID: "core-survivor-register", ExpectedVersion: "0"}); err != nil {
		t.Fatal(err)
	}
	survivorDrain := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: input.BuildInput, RuntimeID: survivorInput.RuntimeID}, RequestID: "core-survivor-drain", OperationID: "core-survivor-process-drain", Deadline: drain.Deadline}
	if result, e := client.DrainWorker(ctx, survivorDrain); e != nil || !result.Complete || !result.Quiescent {
		t.Fatal("survivor actual drain", e)
	}
	if verified, e := client.VerifyRuntime(ctx, operator.QueryRuntimeCommand{QueryRuntimeInput: survivorInput, RequestID: "core-survivor-verify", ExpectedVersion: "1"}); e != nil || verified.Binding.ProofID == "" {
		t.Fatal("survivor native proof", e)
	}
	if _, err = f.Core.SealManagedTarget(ctx, liveCoreCommand(t, f)); err != nil {
		t.Fatal(err)
	}
	start := managed.AttemptStartRequest{Command: liveCoreCommand(t, f), ActionID: action.ID, AttemptID: cid.New(managed.PrefixOperation), ProviderOperationID: cid.New(managed.PrefixOperation), VerifierID: "service-custodian"}
	started, err := f.Core.BeginManagedRemovalAttempt(ctx, start)
	if err != nil {
		t.Fatal("begin retained attempt", err)
	}
	out.Attempt = managed.AttemptKey{Key: f.Plan.Target, AttemptID: started.AttemptID, Generation: started.Generation}
	out.Binding = managed.AttemptBindingKey{AttemptKey: out.Attempt, ReservationID: rid}
	begin := operator.QueryRuntimeCommand{QueryRuntimeInput: input, RequestID: "core-original-begin", ExpectedVersion: "1"}
	nativeRequest := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: f.NativeIdentity.QueryRuntimeTarget, RequestID: begin.RequestID, ExpectedVersion: 1}
	nativeEnvelope := liveJSON(t, struct {
		Operation string `json:"operation"`
		Request   any    `json:"request"`
	}{string(durable.OperationBeginQueryRemoval), nativeRequest})
	original := liveJSON(t, begin)
	command := managed.PortCommand{SchemaVersion: "1", OperationID: cid.New(managed.PrefixOperation), RequestID: begin.RequestID, Original: original, OriginalDigest: managed.Digest(original), Native: nativeEnvelope, NativeDigest: managed.Digest(nativeEnvelope)}
	out.Begin, err = f.Core.AuthorizeManagedAttemptCommand(ctx, managed.AttemptCommandRequest{AttemptRequest: managed.AttemptRequest{Command: liveCoreCommand(t, f), AttemptID: out.Attempt.AttemptID, Generation: out.Attempt.Generation}, ReservationID: rid, Kind: managed.AttemptBegin, ExpectedNativeVersion: "1", NativeCommand: command})
	if err != nil || !out.Begin.NewlyIssued {
		t.Fatal("fresh original Begin authorization", err)
	}
	begun, err := client.BeginRemoval(ctx, begin)
	if err != nil || begun.Fence.SurvivorInstanceID != survivor.Plan.InstanceID {
		t.Fatal("actual original Begin", err)
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: f.NativeIdentity.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: begin.RequestID, RequestDigest: command.NativeDigest}
	out.NativeBegin, err = f.Native.LookupLifecycleReceipt(ctx, lookup)
	if err != nil || out.NativeBegin.Match(lookup) != nil || out.NativeBegin.QueryRuntime == nil {
		t.Fatal("exact Begin receipt", err)
	}
	nativeFence := out.NativeBegin.QueryRuntime.Removal
	fenceBytes := liveJSON(t, nativeFence)
	receiptBytes := liveJSON(t, begun)
	proofBytes := liveJSON(t, *out.Registration.Material.Registration)
	compatibility, err := f.Native.InspectCompatibility(ctx, f.NativeIdentity.NamespaceTarget)
	if err != nil || compatibility.NamespaceTarget != f.NativeIdentity.NamespaceTarget || !compatibility.Enrolled || compatibility.SchemaVersion != durable.RetirementSchemaVersion || compatibility.WriterProtocol != 1 || compatibility.QueryRetentionSchemaVersion != durable.QueryRetentionSchemaVersion || compatibility.WorkerDrainSchemaVersion != durable.WorkerDrainSchemaVersion {
		t.Fatal("original native compatibility changed", err)
	}
	fenceRequest := managed.AttemptFenceRequest{AttemptRequest: managed.AttemptRequest{Command: liveCoreCommand(t, f), AttemptID: out.Attempt.AttemptID, Generation: out.Attempt.Generation}, ReservationID: rid, Acceptance: managed.AttemptNativeAcceptance{NativeAcceptance: managed.AttemptNativeFacts{RequestID: out.NativeBegin.RequestID, RequestDigest: out.NativeBegin.RequestDigest, CommandDigest: command.OriginalDigest, ResponseVersion: "1", BindingVersion: managed.Decimal(begun.Binding.Version), RemovalEpoch: managed.Decimal(begun.Fence.RemovalEpoch), AcceptedAt: out.NativeBegin.AcceptedAt}, NativeFenceDigest: begun.Fence.Digest, ReceiptID: out.NativeBegin.DeliveryID, Receipt: receiptBytes, ReceiptDigest: managed.Digest(receiptBytes), OriginalBindingProof: proofBytes, ProofDigest: managed.Digest(proofBytes)}, Fence: managed.WireFence{Epoch: managed.Decimal(begun.Fence.RemovalEpoch), Evidence: fenceBytes, Digest: managed.Digest(fenceBytes), CompatibilityEvidence: liveJSON(t, compatibility), ValidUntil: nativeFence.ValidUntil, Survivors: []string{nativeFence.Survivor.InstanceID}}}
	if len(fenceBytes) > managed.MaxEvidence || len(receiptBytes) > managed.MaxEvidence || len(proofBytes) > managed.MaxEvidence {
		t.Fatalf("bounded original export fence=%d receipt=%d proof=%d", len(fenceBytes), len(receiptBytes), len(proofBytes))
	}
	if _, err = f.Core.RecordManagedAttemptFence(ctx, fenceRequest); err != nil {
		t.Fatal("retain actual original fence", err)
	}
	if _, err = f.Core.ReadManagedAttemptBinding(ctx, out.Binding); err != nil {
		t.Fatal("exact retained attempt binding", err)
	}
	return out
}

func TestLiveCoreOriginalLifecycleAndNativeBegin(t *testing.T) {
	f := newLiveOriginalLifecycle(t)
	if f.Registration.Material.Registration.Process != f.Drain.Material.Drain.Process || f.Begin.Command.BindingReference.OriginalRegistrationOperationID != f.Registration.Reference.OperationID || f.Begin.Command.BindingReference.OriginalRegistrationDigest != f.Registration.Reference.MaterialDigest {
		t.Fatal("original core association lost")
	}
	target, err := f.Candidate.Core.ReadManagedTarget(t.Context(), f.Candidate.Plan.Target)
	if err != nil || target.Deletion != nil {
		t.Fatal("Begin unexpectedly granted provider deletion", err)
	}
}

// liveQuiescenceWitness keeps the exact authenticated channel observations outside
// the core acceptance row. The host signs only after their full association check.
type liveQuiescenceWitness struct {
	Reference   managed.QuiescenceReference   `json:"reference"`
	Status      ServiceResponse               `json:"status"`
	Drain       ServiceResponse               `json:"drain"`
	Proof       ServiceResponse               `json:"proof"`
	Observation managed.QuiescenceObservation `json:"observation"`
}

func checkLiveQuiescence(f *liveOriginalLifecycle, w liveQuiescenceWitness, pub ed25519.PublicKey) error {
	reference, err := managed.NewQuiescenceReference(f.Registration, f.Drain)
	if err != nil || reference != w.Reference || w.Observation.Reference != reference {
		return managed.ErrAuthority
	}
	for _, response := range []ServiceResponse{w.Status, w.Drain, w.Proof} {
		if !validServiceResponse(response) || response.Failed || response.BootstrapID != f.Candidate.Plan.BootstrapID || !objectID(response.Nonce) {
			return managed.ErrAuthority
		}
	}
	if w.Status.Kind != "status" || w.Drain.Kind != "wait_drain" || w.Proof.Kind != "verify" || w.Status.Nonce == w.Drain.Nonce || w.Status.Nonce == w.Proof.Nonce || w.Drain.Nonce == w.Proof.Nonce {
		return managed.ErrAuthority
	}
	status, result, proof := w.Status.Status, w.Drain.Drain, w.Proof.Proof
	identity := f.Candidate.NativeIdentity
	handle := drt.DrainHandle{RuntimeID: reference.Process.RuntimeID, DrainRequest: drt.DrainRequest{OperationID: reference.OperationID, Deadline: reference.Deadline}}
	if status == nil || result == nil || proof == nil || status.RuntimeID != identity.RuntimeID || status.InstanceID != identity.InstanceID || status.Namespace != identity.Namespace || status.BuildID != identity.BuildID || status.Queue != reference.Process.Queue || status.Drain == nil || !sameDrainHandle(*status.Drain, handle) || !status.AdmissionClosed || status.InFlight != 0 || status.UnknownClaims != 0 || status.Failure != "" || !sameDrainHandle(result.Handle, handle) || !result.Complete || !result.Quiescent || result.DeadlineExpired || result.InFlight != 0 || result.UnknownClaims != 0 || result.ObservedAt.Before(status.ObservedAt) || proof.Validate(identity, proof.VerifiedAt) != nil {
		return managed.ErrAuthority
	}
	expected := managed.QuiescenceLeaf{Reference: reference, ObservedAt: result.ObservedAt, ValidUntil: proof.ValidUntil, AdmissionClosed: status.AdmissionClosed, Quiescent: result.Quiescent, Complete: result.Complete, DeadlineExpired: result.DeadlineExpired, ActiveClaims: managed.Decimal(strconv.FormatInt(result.InFlight, 10)), UnknownClaims: managed.Decimal(strconv.FormatInt(result.UnknownClaims, 10)), ObservationID: w.Drain.Nonce, ChallengeID: w.Status.Nonce, VerifierID: reference.Process.ProducerVerifier}
	if maximum := result.ObservedAt.Add(time.Minute); expected.ValidUntil.After(maximum) {
		expected.ValidUntil = maximum
	}
	if expected != w.Observation.QuiescenceLeaf {
		return managed.ErrAuthority
	}
	normalized, err := w.Observation.Normalize()
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, []byte(normalized.ChallengeDigest), normalized.Attestation) {
		return managed.ErrAuthority
	}
	return nil
}

// admitLiveOriginal observes the already closed original process. It performs no
// BeginDrain, native binding mutation, provider send or replacement enrollment.
func admitLiveOriginal(t *testing.T, f *liveOriginalLifecycle) managed.AttemptRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	reference, err := managed.NewQuiescenceReference(f.Registration, f.Drain)
	if err != nil {
		t.Fatal(err)
	}
	id, identity, err := f.Candidate.Runtime.fixed(f.Candidate.NativeIdentity.QueryRuntimeTarget)
	if err != nil || !nativeIdentityMatchesOriginal(identity, reference.Process) {
		t.Fatal("original process binding", err)
	}
	status, err := f.Candidate.Runtime.call(ctx, id, ServiceCommand{Kind: "status"})
	if err != nil {
		t.Fatal("authenticated original status", err)
	}
	handle := drt.DrainHandle{RuntimeID: reference.Process.RuntimeID, DrainRequest: drt.DrainRequest{OperationID: reference.OperationID, Deadline: reference.Deadline}}
	result, err := f.Candidate.Runtime.call(ctx, id, ServiceCommand{Kind: "wait_drain", Handle: &handle})
	if err != nil {
		t.Fatal("authenticated original drain", err)
	}
	proof, err := f.Candidate.Runtime.call(ctx, id, ServiceCommand{Kind: "verify", Binding: f.NativeBegin.QueryRuntime})
	if err != nil {
		t.Fatal("authenticated original query", err)
	}
	if status.Status == nil || result.Drain == nil || proof.Proof == nil {
		t.Fatal("missing authenticated observations")
	}
	witness := liveQuiescenceWitness{Reference: reference, Status: status, Drain: result, Proof: proof, Observation: managed.QuiescenceObservation{QuiescenceLeaf: managed.QuiescenceLeaf{Reference: reference, ObservedAt: result.Drain.ObservedAt, ValidUntil: proof.Proof.ValidUntil, AdmissionClosed: status.Status.AdmissionClosed, Quiescent: result.Drain.Quiescent, Complete: result.Drain.Complete, DeadlineExpired: result.Drain.DeadlineExpired, ActiveClaims: managed.Decimal(strconv.FormatInt(result.Drain.InFlight, 10)), UnknownClaims: managed.Decimal(strconv.FormatInt(result.Drain.UnknownClaims, 10)), ObservationID: result.Nonce, ChallengeID: status.Nonce, VerifierID: reference.Process.ProducerVerifier}}}
	// Source observation and proof have slightly different clocks. Cap validity at
	// one minute from the exact drain source timestamp without extending the proof.
	maximum := result.Drain.ObservedAt.Add(time.Minute)
	if witness.Observation.ValidUntil.After(maximum) {
		witness.Observation.ValidUntil = maximum
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	witness.Observation.ChallengeDigest, err = managed.QuiescenceChallengeDigest(witness.Observation.QuiescenceLeaf)
	if err != nil {
		t.Fatal(err)
	}
	witness.Observation.Attestation = ed25519.Sign(key, []byte(witness.Observation.ChallengeDigest))
	path := filepath.Join(t.TempDir(), "quiescence-witness.json")
	livePrivate(t, filepath.Join(filepath.Dir(path), "producer-key"), key)
	raw := liveJSON(t, witness)
	if len(raw) > serviceWireLimit {
		t.Fatal("bounded quiescence witness")
	}
	livePrivate(t, path, raw)
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var retained liveQuiescenceWitness
	if err = json.Unmarshal(saved, &retained); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, liveJSON(t, retained)) {
		t.Fatal("noncanonical quiescence witness")
	}
	if err = checkLiveQuiescence(f, retained, pub); err != nil {
		t.Fatal("configured host verification", err)
	}
	changed := retained
	changed.Observation.Reference.Process.InstanceID += "-replaced"
	if checkLiveQuiescence(f, changed, pub) == nil {
		t.Fatal("replacement process accepted")
	}
	changed = retained
	changed.Observation.Attestation = append([]byte(nil), retained.Observation.Attestation...)
	changed.Observation.Attestation[0] ^= 1
	if checkLiveQuiescence(f, changed, pub) == nil {
		t.Fatal("forged quiescence accepted")
	}
	accepted := managed.QuiescenceAcceptanceRequest{Command: liveOriginalCommand(t, f.Candidate, f.Binding.ReservationID), Observation: retained.Observation}
	if _, err = f.Candidate.Core.AcceptManagedQuiescence(ctx, accepted); err != nil {
		t.Fatal("retained actual quiescence", err)
	}
	if _, err = f.Candidate.Core.AdmitManagedRemovalAttempt(ctx, managed.AttemptAdmissionRequest{AttemptRequest: managed.AttemptRequest{Command: liveCoreCommand(t, f.Candidate), AttemptID: f.Attempt.AttemptID, Generation: f.Attempt.Generation}, VerifierID: "service-custodian"}); err != nil {
		t.Fatal("actual removal admission", err)
	}
	record, err := f.Candidate.Core.ReadManagedRemovalAttempt(ctx, f.Attempt)
	if err != nil || record.State.AdmissionDigest == "" {
		t.Fatal("exact admission record", err)
	}
	return record
}

func TestLiveCoreOriginalQuiescenceAdmission(t *testing.T) {
	f := newLiveOriginalLifecycle(t)
	admitLiveOriginal(t, f)
}
