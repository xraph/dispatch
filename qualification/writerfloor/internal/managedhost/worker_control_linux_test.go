//go:build linux

package managedhost

import (
	"reflect"
	"testing"
	"time"

	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

func TestServiceReadinessUsesActualWorker(t *testing.T) {
	runtime, _ := nativeWorkerFixture(t)
	expected, err := runtime.worker.Readiness(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := ServiceCommand{Nonce: "nonce", Kind: "readiness"}
	response := executeServiceCommand(t.Context(), runtime, command)
	if response.Failed || response.Readiness == nil || !validServiceResponse(response) {
		t.Fatalf("actual readiness not retained: %+v", response)
	}
	if response.Readiness.Worker.ObservedAt.Before(expected.Worker.ObservedAt) || response.Readiness.Worker.ObservedAt.After(time.Now().UTC()) {
		t.Fatal("readiness observation is outside the call interval")
	}
	expected.Worker.ObservedAt = response.Readiness.Worker.ObservedAt
	if !reflect.DeepEqual(*response.Readiness, expected) {
		t.Fatal("readiness changed actual worker facts")
	}
	response.Status = &drt.WorkerStatus{}
	if validServiceResponse(response) {
		t.Fatal("mixed readiness/status accepted")
	}
	command.Drain = &drt.DrainRequest{}
	if !executeServiceCommand(t.Context(), runtime, command).Failed {
		t.Fatal("readiness accepted mutation payload")
	}
}
func TestWorkerControlRefusesChangedOrMissingBootBeforeSend(t *testing.T) {
	binding := launcher.ServiceBinding{BootNonce: "original"}
	slot := &serviceSlot{binding: &binding}
	host := &RuntimeHost{slots: map[string]*serviceSlot{"fixed": slot}}
	control := &remoteWorkerControl{host: host, id: "fixed", binding: binding}
	if !control.unchanged() {
		t.Fatal("exact boot refused")
	}
	slot.binding = &launcher.ServiceBinding{BootNonce: "replacement"}
	if _, err := control.Status(t.Context()); err == nil {
		t.Fatal("replacement boot accepted")
	}
	if slot.pending != nil {
		t.Fatal("replacement received a command")
	}
	slot.binding = nil
	if _, err := control.Readiness(t.Context()); err == nil {
		t.Fatal("missing boot accepted")
	}
	if slot.pending != nil {
		t.Fatal("missing boot received a command")
	}
}
