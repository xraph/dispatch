package operatorhost

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/operator"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func TestLifecycleHostCapturesExecutableAndControlsPrestart(t *testing.T) {
	h, err := NewWithLifecycle(t.Context(), memory.New(), LifecycleOptions{InstanceID: "host-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range h.runtime.workers {
		status := w.Status()
		target := durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "operator-host", Namespace: status.Namespace}, BuildID: status.BuildID}, RuntimeID: status.RuntimeID}
		identity, resolveErr := h.lifecycle.ResolveBinding(t.Context(), target)
		if resolveErr != nil {
			t.Fatal(resolveErr)
		}
		if identity.InstanceID != "host-a" || identity.BuildIdentity.ArtifactDigest != fmt.Sprintf("%x", sha256.Sum256(raw)) {
			t.Fatal("identity did not capture running executable")
		}
		if startErr := h.lifecycle.authorizeStartup(t.Context(), w); startErr == nil {
			t.Fatal("unregistered runtime permitted to poll")
		}
		if _, probeErr := h.lifecycle.Verify(t.Context(), durable.QueryRuntimeBinding{QueryRuntimeIdentity: identity}); probeErr == nil {
			t.Fatal("probe accepted open admission")
		}
		control, controlErr := h.lifecycle.workerControl(t.Context(), target)
		if controlErr != nil {
			t.Fatal(controlErr)
		}
		handle, drainErr := control.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "prestart", Deadline: time.Now().Add(time.Second)})
		if drainErr != nil {
			t.Fatal(drainErr)
		}
		result, waitErr := control.WaitDrain(t.Context(), handle)
		if waitErr != nil || !result.Complete {
			t.Fatalf("prestart drain: %v %+v", waitErr, result)
		}
		if runErr := w.Run(t.Context()); runErr == nil {
			t.Fatal("completed prestart drain allowed later Run")
		}
		target.RuntimeID = "replacement"
		if _, controlErr = h.lifecycle.workerControl(t.Context(), target); controlErr == nil {
			t.Fatal("replacement target resolved to original process")
		}
	}
}

func TestMemoryLifecycleHistoricalProbes(t *testing.T) {
	testLifecycleHistoricalProbes(t, memory.New())
}
func TestPostgresLifecycleHistoricalProbes(t *testing.T) {
	testLifecycleHistoricalProbes(t, postgresCommands(t))
}

func testLifecycleHistoricalProbes(t *testing.T, store Store) {
	var registrations atomic.Int32
	options := &LifecycleOptions{InstanceID: "historical-host", RegistrationPolicy: func(context.Context, durable.RegisterQueryRuntimeRequest) error { registrations.Add(1); return nil }}
	c := newConfiguredCommandClient(t, store, options)
	c.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "enroll-lifecycle"}, 200)
	for _, identity := range c.host.RuntimeIdentities() {
		build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
		c.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "register-" + identity.BuildID, ExpectedVersion: "0"}, 200)
		in := operator.QueryRuntimeInput{BuildInput: build, RuntimeID: identity.RuntimeID}
		register := operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "register-" + identity.RuntimeID, ExpectedVersion: "0"}
		accepted := data[operator.QueryRuntimeAcceptance](t, c.command("durable.queryRuntimeRegister", register, 200))
		if accepted.Binding.InstanceID != identity.InstanceID {
			t.Fatal("host identity lost")
		}
		if err := c.host.lifecycle.authorizeStartup(t.Context(), c.host.runtime.workers[identity.BuildID]); err != nil {
			t.Fatal(err)
		}
		probe := defaultProbes(identity.BuildID)[0]
		c.command("durable.start", operator.StartInput{Key: probe.Key, RequestID: "history-" + identity.BuildID, WorkflowType: "operator", BuildID: identity.BuildID, Queue: "operator", Input: []byte("history")}, 200)
		verify := operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "verify-" + identity.RuntimeID, ExpectedVersion: "1"}
		c.command("durable.queryRuntimeVerify", verify, 409)
		w := c.host.runtime.workers[identity.BuildID]
		handle, err := w.BeginDrainBeforeDeadline(t.Context(), drt.DrainRequest{OperationID: "probe-drain", Deadline: time.Now().Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		if result, waitErr := w.WaitDrain(t.Context(), handle); waitErr != nil || !result.Complete {
			t.Fatalf("drain failed: %+v %v", result, waitErr)
		}
		verified := data[operator.QueryRuntimeAcceptance](t, c.command("durable.queryRuntimeVerify", verify, 200))
		if verified.Binding.ProofID == "" || verified.Binding.Version != "2" {
			t.Fatal("actual historical probe missing")
		}
		if replay := data[operator.QueryRuntimeAcceptance](t, c.command("durable.queryRuntimeVerify", verify, 200)); !reflect.DeepEqual(replay, verified) {
			t.Fatal("probe retry changed accepted proof")
		}
		before := registrations.Load()
		if replay := data[operator.QueryRuntimeAcceptance](t, c.command("durable.queryRuntimeRegister", register, 200)); !reflect.DeepEqual(replay, accepted) || registrations.Load() != before {
			t.Fatal("accepted registration reissued")
		}
		status, _ := c.request(http.MethodPost, "/api/dashboard/v1", c.host.Credentials["denied"].Token, c.envelope("durable.queryRuntime", in))
		if status != 403 {
			t.Fatalf("denied query read HTTP %d", status)
		}
		policy, err := c.host.Policies.GetPolicy(t.Context(), "tenant-production", c.host.CommandPolicies[operator.VerifyQueryRuntime])
		if err != nil {
			t.Fatal(err)
		}
		policy.IsActive = false
		if err = c.host.Policies.UpdatePolicy(t.Context(), policy); err != nil {
			t.Fatal(err)
		}
		c.command("durable.queryRuntimeVerify", verify, 403)
		policy.IsActive = true
		if err = c.host.Policies.UpdatePolicy(t.Context(), policy); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLifecycleLocalRemovalSettlement(t *testing.T) {
	store := memory.New()
	a := newConfiguredCommandClient(t, store, &LifecycleOptions{InstanceID: "local-a"})
	b := newConfiguredCommandClient(t, store, &LifecycleOptions{InstanceID: "local-b"})
	a.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "local-enroll"}, 200)
	build := operator.BuildInput{Namespace: "production", BuildID: "operator-v1"}
	a.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "local-build", ExpectedVersion: "0"}, 200)
	probe := defaultProbes(build.BuildID)[0]
	a.command("durable.start", operator.StartInput{Key: probe.Key, RequestID: "local-history", WorkflowType: "operator", BuildID: build.BuildID, Queue: "operator", Input: []byte("history")}, 200)
	for _, client := range []*commandClient{a, b} {
		worker := client.host.runtime.workers[build.BuildID]
		runtimeID := worker.Status().RuntimeID
		in := operator.QueryRuntimeInput{BuildInput: build, RuntimeID: runtimeID}
		client.command("durable.queryRuntimeRegister", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "register-" + runtimeID, ExpectedVersion: "0"}, 200)
		handle, err := worker.BeginDrainBeforeDeadline(t.Context(), drt.DrainRequest{OperationID: "settlement-drain", Deadline: time.Now().Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		if result, waitErr := worker.WaitDrain(t.Context(), handle); waitErr != nil || !result.Complete {
			t.Fatal("drain failed", waitErr)
		}
		client.command("durable.queryRuntimeVerify", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "verify-" + runtimeID, ExpectedVersion: "1"}, 200)
	}
	runtimeID := a.host.runtime.workers[build.BuildID].Status().RuntimeID
	in := operator.QueryRuntimeInput{BuildInput: build, RuntimeID: runtimeID}
	reserve := operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "local-reserve", ExpectedVersion: "2"}
	a.command("durable.queryRuntimeRemove", reserve, 200)
	digest, _ := durable.Fingerprint(string(durable.OperationBeginQueryRemoval), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "operator-host", Namespace: "production"}, BuildID: build.BuildID}, RuntimeID: runtimeID}, RequestID: reserve.RequestID, ExpectedVersion: 2})
	receipt, err := store.LookupLifecycleReceipt(t.Context(), durable.LifecycleReceiptLookup{NamespaceTarget: durable.NamespaceTarget{InstallationID: "operator-host", Namespace: "production"}, Operation: durable.OperationBeginQueryRemoval, RequestID: reserve.RequestID, RequestDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	fence := receipt.QueryRuntime.Removal
	finish := operator.QueryRemovalInput{QueryReservationInput: operator.QueryReservationInput{QueryRuntimeInput: in, ReservationRequestID: reserve.RequestID, ReservationVersion: reserve.ExpectedVersion}, RequestID: "local-finish"}
	a.command("durable.queryRuntimeFinish", finish, 409)
	abort := finish
	abort.RequestID = "local-abort"
	a.command("durable.queryRuntimeAbort", abort, 200)
	if err = a.host.RemoveLocalQueryRuntime(t.Context(), fence); err == nil {
		t.Fatal("aborted operation could still issue")
	}
	reserve.RequestID = "local-reserve-again"
	reserve.ExpectedVersion = "4"
	a.command("durable.queryRuntimeRemove", reserve, 200)
	digest, _ = durable.Fingerprint(string(durable.OperationBeginQueryRemoval), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: fence.Candidate.QueryRuntimeTarget, RequestID: reserve.RequestID, ExpectedVersion: 4})
	receipt, err = store.LookupLifecycleReceipt(t.Context(), durable.LifecycleReceiptLookup{NamespaceTarget: fence.Candidate.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: reserve.RequestID, RequestDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	fence = receipt.QueryRuntime.Removal
	if err = a.host.RemoveLocalQueryRuntime(t.Context(), fence); err != nil {
		t.Fatal(err)
	}
	finish.ReservationRequestID = reserve.RequestID
	finish.ReservationVersion = reserve.ExpectedVersion
	result := data[operator.QueryRemovalAcceptance](t, a.command("durable.queryRuntimeFinish", finish, 200))
	if result.Binding.State != durable.QueryRuntimeRemoved {
		t.Fatal("local removal did not finish")
	}
	a.command("durable.queryRuntimeRegister", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "register-" + runtimeID, ExpectedVersion: "0"}, 200)
	if _, err = a.host.worker("production", build.BuildID); err == nil {
		t.Fatal("registration replay reopened removed local query route")
	}
	if _, err = b.host.worker("production", build.BuildID); err != nil {
		t.Fatal("local removal closed survivor route")
	}
}

// staleRemovalCheckStore models a successful remote check whose reply arrives
// after its fence deadline. The local issuance boundary must recheck time.
type staleRemovalCheckStore struct{ *memory.Store }

func (s *staleRemovalCheckStore) CheckQueryRuntimeRemoval(_ context.Context, f durable.QueryRemovalFence) (durable.QueryRemovalFacts, error) {
	return durable.QueryRemovalFacts{Fence: f, CheckedAt: f.AcceptedAt}, nil
}
func TestLocalRemovalRejectsExpiredSuccessfulCheck(t *testing.T) {
	h, err := NewWithLifecycle(t.Context(), &staleRemovalCheckStore{memory.New()}, LifecycleOptions{InstanceID: "late-local"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	identity := h.RuntimeIdentities()[0]
	worker := h.runtime.workers[identity.BuildID]
	handle, err := worker.BeginDrainBeforeDeadline(t.Context(), drt.DrainRequest{OperationID: "late-drain", Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = worker.WaitDrain(t.Context(), handle); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	fence := durable.QueryRemovalFence{Candidate: identity, RequestID: "late-check", AcceptedAt: now.Add(-time.Second), ValidUntil: now.Add(-time.Millisecond)}
	if err = h.RemoveLocalQueryRuntime(t.Context(), fence); err == nil {
		t.Fatal("expired check permitted new local removal")
	}
	if _, err = h.worker(identity.Namespace, identity.BuildID); err != nil {
		t.Fatal("expired check closed query route")
	}
}
