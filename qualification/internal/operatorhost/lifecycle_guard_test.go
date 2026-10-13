package operatorhost

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/store/memory"
)

// guardPortFixture supplies trusted local test records, not provider authentication.
type guardPortFixture struct {
	scope      durable.QueryRemovalExternalScope
	guard      durable.QueryRemovalGuardReference
	evidence   durable.QueryRemovalEvidence
	permission durable.QueryRemovalPermission
	closure    durable.QueryRemovalClosure
	calls      int
}

func (v *guardPortFixture) VerifyRevalidation(_ context.Context, p durable.QueryRemovalGuardProbe) (durable.QueryRemovalEvidence, error) {
	v.calls++
	if p.EvidenceReference != "local-evidence" || p.Original.BeginRequestID != "port-reserve" || p.ExternalScope != v.scope {
		return durable.QueryRemovalEvidence{}, durable.ErrInvalid
	}
	return v.evidence, nil
}
func (v *guardPortFixture) VerifyPermission(_ context.Context, guard durable.QueryRemovalGuardReference, ref string) (durable.QueryRemovalPermission, error) {
	v.calls++
	if guard != v.guard || ref != "local-permission" {
		return durable.QueryRemovalPermission{}, durable.ErrInvalid
	}
	return v.permission, nil
}
func (v *guardPortFixture) VerifyClosure(_ context.Context, guard durable.QueryRemovalGuardReference, ref string) (durable.QueryRemovalClosure, error) {
	v.calls++
	if guard != v.guard || ref != "local-cutoff" {
		return durable.QueryRemovalClosure{}, durable.ErrInvalid
	}
	return v.closure, nil
}

func TestMemoryNativeGuardPort(t *testing.T) { testNativeGuardPort(t, memory.New()) }
func TestPostgresNativeGuardPort(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	testNativeGuardPort(t, postgresCommands(t))
}
func testNativeGuardPort(t *testing.T, store Store) {
	t.Helper()
	verifier := &guardPortFixture{}
	a := newConfiguredCommandClient(t, store, &LifecycleOptions{InstanceID: "guard-host-a", QueryRemovalGuardVerifier: verifier})
	b := newConfiguredCommandClient(t, store, &LifecycleOptions{InstanceID: "guard-host-b"})
	a.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "port-enroll"}, 200)
	build := operator.BuildInput{Namespace: "production", BuildID: "operator-v1"}
	a.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "port-build", ExpectedVersion: "0"}, 200)
	probe := defaultProbes(build.BuildID)[0]
	a.command("durable.start", operator.StartInput{Key: probe.Key, RequestID: "port-history", WorkflowType: "operator", BuildID: build.BuildID, Queue: "operator", Input: []byte("history")}, 200)
	for _, client := range []*commandClient{a, b} {
		worker := client.host.runtime.workers[build.BuildID]
		runtimeID := worker.Status().RuntimeID
		in := operator.QueryRuntimeInput{BuildInput: build, RuntimeID: runtimeID}
		client.command("durable.queryRuntimeRegister", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "register-" + runtimeID, ExpectedVersion: "0"}, 200)
		handle, err := worker.BeginDrainBeforeDeadline(t.Context(), drt.DrainRequest{OperationID: "port-drain", Deadline: time.Now().Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		if result, waitErr := worker.WaitDrain(t.Context(), handle); waitErr != nil || !result.Complete {
			t.Fatalf("drain: %v", waitErr)
		}
		client.command("durable.queryRuntimeVerify", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "verify-" + runtimeID, ExpectedVersion: "1"}, 200)
	}
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "operator-host", Namespace: "production"}, BuildID: build.BuildID}
	runtimes, err := store.(durable.QueryRuntimeStore).ListQueryRuntimes(t.Context(), durable.QueryRuntimeList{NamespaceTarget: target.NamespaceTarget, BuildID: target.BuildID, Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	var candidate, survivor durable.QueryRuntimeBinding
	for _, r := range runtimes.Items {
		if r.InstanceID == "guard-host-a" {
			candidate = r
		}
		if r.InstanceID == "guard-host-b" {
			survivor = r
		}
	}
	if candidate.RuntimeID == "" || survivor.RuntimeID == "" {
		t.Fatal("runtime fixtures missing")
	}
	in := operator.QueryRuntimeInput{BuildInput: build, RuntimeID: candidate.RuntimeID}
	a.command("durable.queryRuntimeRemove", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "port-reserve", ExpectedVersion: "2"}, 200)
	original := operator.QueryReservationInput{QueryRuntimeInput: in, ReservationRequestID: "port-reserve", ReservationVersion: "2"}
	excluded := []string{candidate.InstanceID}
	exclusionDigest, err := durable.Fingerprint("dispatch.query-removal.exclusions.v1", excluded)
	if err != nil {
		t.Fatal(err)
	}
	scope := durable.QueryRemovalExternalScope{AuthorityID: "fixture-authority", OperationScopeID: "fixture-operation", AttemptID: "attempt", ProviderOperationID: "provider-operation", CoverageDigest: strings.Repeat("a", 64), ExcludedInstancesDigest: exclusionDigest, ProducerID: "fixture-producer", ProfileID: "fixture-profile", ProfileVersion: 1, VerifierID: "local-test-verifier"}
	verifier.scope = scope
	now := durable.Timestamp(time.Now())
	until := now.Add(20 * time.Second)
	verifier.evidence = durable.QueryRemovalEvidence{Disposition: "live", CandidateProof: candidate.Verification, SurvivorProof: survivor.Verification, ExcludedInstances: excluded, ObservedAt: now, ValidUntil: until, CandidateAttestation: "local-candidate", SurvivorAttestation: "local-survivor", ProcessAttestation: "local-process", ScopeAttestation: "local-scope", Process: durable.QueryRemovalProcess{HostID: "guard-host-a", ProcessID: "test-process", DescriptorID: "test-descriptor", DescriptorDigest: strings.Repeat("b", 64), RegistrationID: "registration", DrainRuntimeID: candidate.RuntimeID, DrainOperationID: "port-drain", DrainDeadline: now.Add(time.Second), Role: "polling", Admission: "permanently_closed", Claims: "quiescent", DrainOutcome: "completed", ObservedAt: now, ValidUntil: until}}
	revalidate := operator.QueryRevalidationInput{QueryReservationInput: original, RequestID: "port-proof", Scope: operator.QueryGuardScope{AuthorityID: scope.AuthorityID, OperationScopeID: scope.OperationScopeID, AttemptID: scope.AttemptID, ProviderOperationID: scope.ProviderOperationID, CoverageDigest: scope.CoverageDigest, ExcludedInstancesDigest: scope.ExcludedInstancesDigest, ProducerID: scope.ProducerID, ProfileID: scope.ProfileID, ProfileVersion: "1", VerifierID: scope.VerifierID}, EvidenceReference: "local-evidence", SurvivorRuntimeID: survivor.RuntimeID, SurvivorVersion: strconv.FormatInt(survivor.Version, 10)}
	assertGuardUnknownInput(t, a, "durable.queryRemovalRevalidate", revalidate, verifier)
	b.command("durable.queryRemovalRevalidate", revalidate, http.StatusServiceUnavailable)
	accepted := data[durable.QueryRemovalRevalidationReceipt](t, a.command("durable.queryRemovalRevalidate", revalidate, 200))
	guard := operator.QueryGuardInput{QueryReservationInput: original, GuardID: accepted.GuardID, RootDigest: accepted.RootDigest, OriginalDigest: accepted.OriginalDigest}
	verifier.guard = durable.QueryRemovalGuardReference{QueryRuntimeTarget: candidate.QueryRuntimeTarget, GuardID: accepted.GuardID, RootDigest: accepted.RootDigest, OriginalDigest: accepted.OriginalDigest}
	verifier.permission = durable.QueryRemovalPermission{ExternalScope: scope, PermissionID: "permission", PermissionDigest: strings.Repeat("c", 64), CommandID: "invocation", StepDigest: strings.Repeat("d", 64), ResourceDigest: strings.Repeat("e", 64), IncarnationDigest: strings.Repeat("f", 64), RevalidationRequestID: accepted.RequestID, RevalidationDigest: accepted.MaterialDigest, AcceptedAt: accepted.AcceptedAt, EvidenceDigest: strings.Repeat("1", 64), Attestation: "local-permission-evidence"}
	bind := operator.QueryCommitInput{QueryGuardInput: guard, RequestID: "port-bind", PermissionReference: "local-permission", ExpectedCurrentDigest: accepted.MaterialDigest, CurrentRequestID: accepted.RequestID}
	assertGuardUnknownInput(t, a, "durable.queryRemovalCommit", bind, verifier)
	bound := data[operator.QueryGuardAcceptance](t, a.command("durable.queryRemovalCommit", bind, 200))
	now = durable.Timestamp(time.Now())
	verifier.closure = durable.QueryRemovalClosure{ExternalScope: scope, CutoffReceiptID: "cutoff", CutoffDigest: strings.Repeat("2", 64), CoverageDigest: scope.CoverageDigest, Disposition: "settled_without_deletion", TerminalDigest: strings.Repeat("3", 64), CutoffAt: now, TerminalAt: now, EvidenceDigest: strings.Repeat("4", 64), Attestation: "local-cutoff-evidence"}
	closeInput := operator.QueryGuardCloseInput{QueryGuardInput: guard, RequestID: "port-close", ClosureReference: "local-cutoff"}
	assertGuardUnknownInput(t, a, "durable.queryRemovalGuardClose", closeInput, verifier)
	closed := data[operator.QueryGuardAcceptance](t, a.command("durable.queryRemovalGuardClose", closeInput, 200))
	statusInput := operator.QueryGuardReadInput{QueryReservationInput: original, RequestID: bound.RequestID, CommandDigest: bound.CommandDigest}
	status := data[operator.QueryCommitStatus](t, a.command("durable.queryRemovalCommitStatus", statusInput, 200))
	if status.State != "closed" {
		t.Fatal("saved commit did not report closed guard")
	}
	proofCommand, err := durable.Fingerprint("operator.query_runtime.revalidate.v1", revalidate)
	if err != nil {
		t.Fatal(err)
	}
	proofRead := operator.QueryGuardReadInput{QueryReservationInput: original, RequestID: accepted.RequestID, CommandDigest: proofCommand}
	closeRead := operator.QueryGuardReceiptInput{QueryGuardReadInput: operator.QueryGuardReadInput{QueryReservationInput: original, RequestID: closed.RequestID, CommandDigest: closed.CommandDigest}, Kind: "close"}
	for _, read := range []struct {
		name  string
		input any
	}{
		{"durable.queryRemovalRevalidation", proofRead},
		{"durable.queryRemovalCommitStatus", statusInput},
		{"durable.queryRemovalGuardReceipt", closeRead},
	} {
		a.command(read.name, read.input, 200)
		assertGuardUnknownInput(t, a, read.name, read.input, verifier)
	}
	assertGuardUnknownInput(t, a, "durable.queryRemovalRevalidate", revalidate, verifier)
	assertGuardUnknownInput(t, a, "durable.queryRemovalCommit", bind, verifier)
	assertGuardUnknownInput(t, a, "durable.queryRemovalGuardClose", closeInput, verifier)
	calls := verifier.calls
	if replay := data[durable.QueryRemovalRevalidationReceipt](t, b.command("durable.queryRemovalRevalidate", revalidate, 200)); !reflect.DeepEqual(replay, accepted) {
		t.Fatal("revalidation recovery changed")
	}
	if replay := data[operator.QueryGuardAcceptance](t, b.command("durable.queryRemovalCommit", bind, 200)); !reflect.DeepEqual(replay, bound) {
		t.Fatal("bind recovery changed")
	}
	if replay := data[operator.QueryGuardAcceptance](t, b.command("durable.queryRemovalGuardClose", closeInput, 200)); !reflect.DeepEqual(replay, closed) {
		t.Fatal("close recovery changed")
	}
	if verifier.calls != calls {
		t.Fatal("saved replay called verifier")
	}
	for _, command := range []struct {
		name    string
		payload any
	}{{"durable.queryRemovalRevalidate", revalidate}, {"durable.queryRemovalCommit", bind}, {"durable.queryRemovalGuardClose", closeInput}} {
		status, _ := a.request(http.MethodPost, "/api/dashboard/v1", a.host.Credentials["denied"].Token, a.envelope(command.name, command.payload))
		if status != 403 {
			t.Fatalf("denied replay %s: %d", command.name, status)
		}
	}
	changed := bind
	changed.PermissionReference = "changed"
	a.command("durable.queryRemovalCommit", changed, 409)
}

// assertGuardUnknownInput adds only unknown fields to otherwise valid input.
func assertGuardUnknownInput(t *testing.T, client *commandClient, intent string, input any, verifier *guardPortFixture) {
	t.Helper()
	for _, surface := range []string{"payload", "params"} {
		for _, nested := range []bool{false, true} {
			if nested && intent != "durable.queryRemovalRevalidate" {
				continue
			}
			var envelope map[string]any
			if err := json.Unmarshal(client.envelope(intent, input), &envelope); err != nil {
				t.Fatal(err)
			}
			payload := envelope["payload"].(map[string]any)
			if surface == "params" {
				envelope["params"] = payload
				envelope["payload"] = map[string]any{}
			}
			if nested {
				payload["scope"].(map[string]any)["evidence"] = true
			} else {
				payload["evidence"] = map[string]any{"trusted": true}
			}
			raw, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			calls := verifier.calls
			status, _ := client.request(http.MethodPost, "/api/dashboard/v1", client.host.Credentials["commander"].Token, raw)
			if status != http.StatusBadRequest || verifier.calls != calls {
				t.Fatalf("%s %s nested=%v unknown field: status=%d verifier calls=%d want=%d", intent, surface, nested, status, verifier.calls, calls)
			}
		}
	}
}
