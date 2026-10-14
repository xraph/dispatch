//go:build linux

package managedhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func pendingFixture() *serviceSlot {
	return &serviceSlot{pending: &servicePending{command: ServiceCommand{Nonce: strings.Repeat("a", 64), Kind: "status"}, result: make(chan ServiceResponse, 1)}}
}

func TestServiceCommandDeliveryAndResponseAreSingleUse(t *testing.T) {
	slot := pendingFixture()
	response := ServiceResponse{Nonce: slot.pending.command.Nonce, Kind: "status", Status: &drt.WorkerStatus{RuntimeID: "actual"}}
	if acceptServiceResult(slot, response) {
		t.Fatal("result before delivery accepted")
	}
	command := takeServiceCommand(slot)
	if command == nil || command.Nonce != response.Nonce {
		t.Fatal("missing command")
	}
	if takeServiceCommand(slot) != nil {
		t.Fatal("ambiguous first delivery was redelivered")
	}
	changed := response
	changed.Nonce = strings.Repeat("b", 64)
	if acceptServiceResult(slot, changed) {
		t.Fatal("another challenge completed this call")
	}
	changed = response
	changed.Kind = "verify"
	if acceptServiceResult(slot, changed) {
		t.Fatal("another operation completed this call")
	}
	changed = response
	changed.Proof = &durable.QueryRuntimeVerification{}
	if acceptServiceResult(slot, changed) {
		t.Fatal("ambiguous response shape accepted")
	}
	if !acceptServiceResult(slot, response) {
		t.Fatal("exact result refused")
	}
	<-slot.pending.result
	if acceptServiceResult(slot, response) {
		t.Fatal("second result accepted after receiver consumed first")
	}
}

func TestServiceLostCommandAndLateResultNeverAuthorizeAnotherCall(t *testing.T) {
	slot := pendingFixture()
	old := takeServiceCommand(slot)
	// A caller cancellation clears its pending slot, never its delivery marker.
	// The old response cannot satisfy a subsequent, independently challenged call.
	slot.pending = &servicePending{command: ServiceCommand{Nonce: strings.Repeat("b", 64), Kind: "status"}, result: make(chan ServiceResponse, 1)}
	if acceptServiceResult(slot, ServiceResponse{Nonce: old.Nonce, Kind: old.Kind, Status: &drt.WorkerStatus{}}) {
		t.Fatal("old result adopted")
	}
	next := takeServiceCommand(slot)
	if next == nil || next.Nonce == old.Nonce {
		t.Fatal("old command reconstructed")
	}
	if takeServiceCommand(slot) != nil {
		t.Fatal("second transport delivery")
	}
}

func TestServiceWireCompleteCapAndUnknownFields(t *testing.T) {
	type value struct {
		Padding string `json:"padding"`
	}
	base, err := json.Marshal(value{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value{Padding: strings.Repeat("a", serviceWireLimit-len(base))})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != serviceWireLimit {
		t.Fatal("fixture cap")
	}
	var out value
	if err = decodeService(bytes.NewReader(raw), &out); err != nil {
		t.Fatal(err)
	}
	if err = decodeService(bytes.NewReader(append(raw, ' ')), &out); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("cap plus one: %v", err)
	}
	for _, raw := range []string{`{"unexpected":true}`, `{"padding":"ok"} {"padding":"second"}`} {
		if decodeService(strings.NewReader(raw), &out) == nil {
			t.Fatal("unknown or trailing object accepted")
		}
	}
}

func TestServiceDispatchUsesRealWorkerAndNativeDrain(t *testing.T) {
	worker, binding := nativeWorkerFixture(t)
	nonce := strings.Repeat("a", 64)
	status := executeServiceCommand(t.Context(), worker, ServiceCommand{Nonce: nonce, Kind: "status"})
	if status.Failed || status.Status == nil || status.Status.RuntimeID != binding.RuntimeID {
		t.Fatal("missing real worker status")
	}
	proof := executeServiceCommand(t.Context(), worker, ServiceCommand{Nonce: nonce, Kind: "verify", Binding: &binding})
	if !proof.Failed || proof.Proof != nil {
		t.Fatal("open admission verified")
	}
	request := drt.DrainRequest{OperationID: "fixed-service-drain", Deadline: time.Now().Add(time.Minute)}
	begun := executeServiceCommand(t.Context(), worker, ServiceCommand{Nonce: nonce, Kind: "begin_drain", Drain: &request})
	if begun.Failed || begun.Handle == nil {
		t.Fatal("real drain refused")
	}
	drained := executeServiceCommand(t.Context(), worker, ServiceCommand{Nonce: nonce, Kind: "wait_drain", Handle: begun.Handle})
	if drained.Failed || drained.Drain == nil || !drained.Drain.Complete || !drained.Drain.Quiescent {
		t.Fatal("real drain not complete")
	}
	proof = executeServiceCommand(t.Context(), worker, ServiceCommand{Nonce: nonce, Kind: "verify", Binding: &binding})
	if proof.Failed || proof.Proof == nil || proof.Proof.Validate(binding.QueryRuntimeIdentity, proof.Proof.VerifiedAt) != nil {
		t.Fatal("real retained query did not verify")
	}
	forged := binding
	forged.RuntimeID = "other-runtime"
	refused := executeServiceCommand(t.Context(), worker, ServiceCommand{Nonce: nonce, Kind: "verify", Binding: &forged})
	if !refused.Failed || refused.Proof != nil {
		t.Fatal("replacement runtime adopted")
	}
	for _, command := range []ServiceCommand{{Kind: "provider_remove"}, {Kind: "verify"}, {Kind: "status", Binding: &binding}, {Kind: "begin_drain", Drain: &request, Handle: begun.Handle}} {
		if !executeServiceCommand(t.Context(), worker, command).Failed {
			t.Fatal("unsupported or ambiguous command accepted")
		}
	}
}

func TestDrainAssociationSurvivesJSON(t *testing.T) {
	for _, deadline := range []time.Time{time.Now().Add(time.Minute), time.Now().In(time.FixedZone("test", 3600)).Add(time.Minute)} {
		original := drt.DrainHandle{RuntimeID: "runtime", DrainRequest: drt.DrainRequest{OperationID: "drain", Deadline: deadline}}
		raw, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var decoded drt.DrainHandle
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if !sameDrainRequest(original.DrainRequest, decoded.DrainRequest) || !sameDrainHandle(original, decoded) {
			t.Fatal("wire round trip changed drain association")
		}
		decoded.Deadline = decoded.Deadline.Add(time.Nanosecond)
		if sameDrainRequest(original.DrainRequest, decoded.DrainRequest) || sameDrainHandle(original, decoded) {
			t.Fatal("different deadline accepted")
		}
	}
}

func TestServiceStartWaitsForRealWorkerAndCannotReopenDrain(t *testing.T) {
	runtime, binding := nativeWorkerFixture(t)
	process := newServiceProcess(t.Context(), runtime)
	defer process.stop()
	if process.started || runtime.worker.Status().Ready {
		t.Fatal("worker claimed before explicit start")
	}
	command := ServiceCommand{Kind: "start", Nonce: strings.Repeat("a", 64)}
	first := process.execute(t.Context(), command)
	if first.Failed || first.Status == nil || !first.Status.Ready || first.Status.RuntimeID != binding.RuntimeID {
		t.Fatal("start returned before actual readiness")
	}
	done := process.finished
	repeated := process.execute(t.Context(), command)
	if repeated.Failed || process.finished != done {
		t.Fatal("start replay launched a second worker")
	}
	drain := drt.DrainRequest{OperationID: "start-drain", Deadline: time.Now().Add(time.Minute)}
	begun := process.execute(t.Context(), ServiceCommand{Kind: "begin_drain", Drain: &drain})
	if begun.Failed || begun.Handle == nil {
		t.Fatal("drain failed")
	}
	if !process.execute(t.Context(), command).Failed {
		t.Fatal("start reopened closed admission")
	}
}

func TestServiceDrainWirePreservesExactDeadline(t *testing.T) {
	original := drt.DrainHandle{RuntimeID: "runtime", DrainRequest: drt.DrainRequest{OperationID: "operation", Deadline: time.Now().Add(time.Minute)}}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded drt.DrainHandle
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !sameDrainHandle(original, decoded) || !sameDrainRequest(original.DrainRequest, decoded.DrainRequest) {
		t.Fatal("JSON representation changed exact native deadline")
	}
	changed := decoded
	changed.Deadline = changed.Deadline.Add(time.Nanosecond)
	if sameDrainHandle(original, changed) {
		t.Fatal("changed deadline accepted")
	}
	changed = decoded
	changed.OperationID = "another"
	if sameDrainHandle(original, changed) {
		t.Fatal("changed operation accepted")
	}
}
