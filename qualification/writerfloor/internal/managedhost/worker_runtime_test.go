package managedhost

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func nativeWorkerFixture(t *testing.T) (*WorkerRuntime, durable.QueryRuntimeBinding) {
	t.Helper()
	store := memory.New()
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "managed-service", Namespace: "native"}, BuildID: "service-v1"}
	key := durable.Key{Namespace: target.Namespace, WorkflowID: "history", RunID: "original"}
	if _, err := store.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "history-start", WorkflowType: "history", BuildID: target.BuildID, Queue: "fixed", Input: []byte("retained")}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: target.InstallationID, Namespace: target.Namespace, AppID: "application", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "retirement", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("artifact: %v %v", readErr, closeErr)
	}
	identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: target, RuntimeID: "configured-runtime"}, InstanceID: "configured-instance", IdentityVersion: 1, BuildIdentity: durable.BuildQueryIdentity{ArtifactDigest: hex.EncodeToString(hash.Sum(nil)), ConfigurationDigest: strings.Repeat("a", 64), ConfigurationVersion: "native-test-v1", EnrollmentEvidenceDigest: strings.Repeat("b", 64), ProbePolicyID: "history", ProbePolicyVersion: 1, VerifierID: "native-worker", MaximumProofValidity: time.Minute}}
	// Enrollment is fixture configuration; probe execution and native store reads are real.
	if _, err = store.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "build", ExpectedVersion: 1, Identity: &identity.BuildIdentity}); err != nil {
		t.Fatal(err)
	}
	worker, err := drt.NewWorker(store, drt.Options{Namespace: target.Namespace, Queue: "fixed", BuildID: target.BuildID, Owner: "owner", RuntimeID: identity.RuntimeID, InstanceID: identity.InstanceID, Workflows: map[string]drt.WorkflowFunc{"history": func(w *drt.Workflow, input []byte) ([]byte, error) {
		w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
		return input, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	output := sha256.Sum256([]byte("retained"))
	probes := []RetainedProbe{{Name: "retained-status", Request: drt.QueryRequest{Key: key, Selection: durable.RunExplicit, BuildID: target.BuildID, Name: "status"}, ExpectedDigest: hex.EncodeToString(output[:])}}
	host, err := NewWorkerRuntime(worker, store, identity, probes)
	if err != nil {
		t.Fatal(err)
	}
	return host, durable.QueryRuntimeBinding{QueryRuntimeIdentity: identity}
}

func TestNativeWorkerUsesRealDrainAndRetainedQueryReplay(t *testing.T) {
	host, binding := nativeWorkerFixture(t)
	if _, err := host.Verify(t.Context(), binding); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("open admission proved: %v", err)
	}
	handle, err := host.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "original-drain", Deadline: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	drained, err := host.WaitDrain(t.Context(), handle)
	if err != nil || !drained.Complete || !drained.Quiescent {
		t.Fatalf("drain %#v %v", drained, err)
	}
	proof, err := host.Verify(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if err = proof.Validate(binding.QueryRuntimeIdentity, proof.VerifiedAt); err != nil {
		t.Fatal(err)
	}
	if !objectID(proof.EvidenceDigest) || proof.ProofID == "" {
		t.Fatal("missing actual probe evidence")
	}
	host.probes[0].ExpectedDigest = strings.Repeat("f", 64)
	if _, err = host.Verify(t.Context(), binding); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("wrong historical output proved: %v", err)
	}
}

func TestNativeWorkerRefusesReplacedIdentityAndArtifact(t *testing.T) {
	host, binding := nativeWorkerFixture(t)
	changed := binding.QueryRuntimeTarget
	changed.RuntimeID = "replacement"
	if _, err := host.ResolveBinding(t.Context(), changed); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("replacement: %v", err)
	}
	identity := binding.QueryRuntimeIdentity
	identity.BuildIdentity.ArtifactDigest = strings.Repeat("f", 64)
	if _, err := NewWorkerRuntime(host.worker, host.store, identity, host.probes); !errors.Is(err, durable.ErrQueryRetention) {
		t.Fatalf("unmeasured artifact: %v", err)
	}
}
