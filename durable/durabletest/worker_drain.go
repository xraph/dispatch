package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func RunWorkerDrainReceipt(t *testing.T, s durable.Store) {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: fmt.Sprintf("drain-%d", time.Now().UnixNano())}, BuildID: "b"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: target.Namespace, AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "build"}); err != nil {
		t.Fatal(err)
	}
	request := durable.WorkerDrainRequest{WorkerProcessIdentity: durable.WorkerProcessIdentity{BuildTarget: target, Queue: "q", RuntimeID: "incarnation", InstanceID: "host"}, RequestID: "request", CommandDigest: strings.Repeat("a", 64), OperationID: "drain", Deadline: durable.Timestamp(time.Now().Add(100 * time.Millisecond))}
	accepted, err := life.RequestWorkerDrain(t.Context(), request)
	if err != nil || accepted.WorkerDrain == nil || *accepted.WorkerDrain != request {
		t.Fatalf("drain acceptance: %+v %v", accepted, err)
	}
	verifyLifecycleDelivery(t, s, accepted)
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: durable.OperationRequestWorkerDrain, RequestID: request.RequestID, CommandDigest: request.CommandDigest}
	recovered, lookupErr := life.LookupLifecycleReceipt(t.Context(), lookup)
	if lookupErr != nil || !reflect.DeepEqual(recovered, accepted) {
		t.Fatalf("stable command lookup: %+v %v", recovered, lookupErr)
	}
	for name, damage := range map[string]func(*durable.LifecycleReceipt){
		"missing":     func(r *durable.LifecycleReceipt) { r.WorkerDrain = nil },
		"incarnation": func(r *durable.LifecycleReceipt) { r.WorkerDrain.RuntimeID = "replacement" },
		"instance":    func(r *durable.LifecycleReceipt) { r.WorkerDrain.InstanceID = "replacement" },
		"deadline":    func(r *durable.LifecycleReceipt) { r.WorkerDrain.Deadline = r.WorkerDrain.Deadline.Add(time.Hour) },
		"queue":       func(r *durable.LifecycleReceipt) { r.WorkerDrain.Queue = "other" },
		"operation":   func(r *durable.LifecycleReceipt) { r.WorkerDrain.OperationID = "other" },
	} {
		broken := accepted.Clone()
		damage(&broken)
		if matchErr := broken.Match(lookup); !errors.Is(matchErr, durable.ErrInvalid) {
			t.Fatalf("corrupt %s receipt accepted: %v", name, matchErr)
		}
	}
	facts, factsErr := life.InspectCompatibility(t.Context(), target.NamespaceTarget)
	if factsErr != nil || facts.WorkerDrainSchemaVersion != durable.WorkerDrainSchemaVersion {
		t.Fatalf("drain schema capability: %+v %v", facts, factsErr)
	}

	changed := request
	changed.RuntimeID = "replacement"
	if _, err = life.RequestWorkerDrain(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("replacement retargeted receipt: %v", err)
	}
	changed = request
	changed.Deadline = changed.Deadline.Add(time.Hour)
	if _, err = life.RequestWorkerDrain(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("replay extended deadline: %v", err)
	}
	waitDeferral(t, request.Deadline)
	replay, err := life.RequestWorkerDrain(t.Context(), request)
	if err != nil || !reflect.DeepEqual(replay, accepted) {
		t.Fatalf("accepted expired request lost: %+v %v", replay, err)
	}
	changed = request
	changed.RequestID = "expired-new"
	if _, err = life.RequestWorkerDrain(t.Context(), changed); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("new expired request accepted: %v", err)
	}
	accepted.WorkerDrain.RuntimeID = "mutated-return"
	replay, err = life.RequestWorkerDrain(t.Context(), request)
	if err != nil || replay.WorkerDrain.RuntimeID != request.RuntimeID {
		t.Fatalf("receipt aliases caller: %+v %v", replay, err)
	}
}

// RunWorkerDrainIntentRollback checks acceptance and its required intent together.
func RunWorkerDrainIntentRollback(t *testing.T, s durable.Store, snapshot func(*testing.T, durable.NamespaceTarget) any, reject func(*testing.T) func()) {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: fmt.Sprintf("drain-rollback-%d", time.Now().UnixNano())}, BuildID: "b"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: target.Namespace, AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "build"}); err != nil {
		t.Fatal(err)
	}
	request := durable.WorkerDrainRequest{WorkerProcessIdentity: durable.WorkerProcessIdentity{BuildTarget: target, Queue: "q", RuntimeID: "runtime", InstanceID: "instance"}, RequestID: "drain", CommandDigest: strings.Repeat("b", 64), OperationID: "drain", Deadline: durable.Timestamp(time.Now().Add(time.Minute))}
	before := snapshot(t, target.NamespaceTarget)
	restore := reject(t)
	if _, err := life.RequestWorkerDrain(t.Context(), request); err == nil {
		t.Fatal("drain accepted without required intent")
	}
	restore()
	if after := snapshot(t, target.NamespaceTarget); !reflect.DeepEqual(before, after) {
		t.Fatalf("failed drain intent mutated persistence: before=%+v after=%+v", before, after)
	}
	if _, err := life.LookupLifecycleReceipt(t.Context(), durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: durable.OperationRequestWorkerDrain, RequestID: request.RequestID, CommandDigest: request.CommandDigest}); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("failed acceptance left receipt: %v", err)
	}
	receipt, err := life.RequestWorkerDrain(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	verifyLifecycleDelivery(t, s, receipt)
}
