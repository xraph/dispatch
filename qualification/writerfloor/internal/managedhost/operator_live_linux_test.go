//go:build linux

package managedhost

import (
	"context"
	"testing"
	"time"

	aid "github.com/xraph/authsome/id"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

// newLiveOperator mounts the actual Forge contract on the existing mutual-TLS
// listener. The private controller credential is distinct from service startup.
func newLiveOperator(t *testing.T, f *liveServiceFixture, siblings ...*liveServiceFixture) (*launcher.QualificationOperator, *NativeClient) {
	t.Helper()
	fixtures := append([]*liveServiceFixture{f}, siblings...)
	targets := make([]durable.QueryRuntimeTarget, 0, len(fixtures))
	for _, fixture := range fixtures {
		targets = append(targets, fixture.NativeIdentity.QueryRuntimeTarget)
	}
	runtime := newLivePairRuntime(t, fixtures...)
	controller, err := f.Launcher.OpenQualificationOperator(t.Context(), f.Native, launcher.QualificationOperatorOptions{Installation: f.Plan.Installation, Namespace: f.Plan.Namespace, Targets: targets, Runtime: runtime})
	if err != nil {
		t.Fatal("real operator composition", err)
	}
	f.Mux.Handle("/api/dashboard/v1", controller.Handler)
	f.Mux.Handle("/api/dashboard/v1/csrf", controller.Handler)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := controller.Close(ctx); closeErr != nil {
			t.Error("operator credential cleanup", closeErr)
		}
	})
	credential := controller.Credential
	client, err := NewNativeClient(NativeEndpoint{URL: f.Plan.Endpoint, ServerName: f.Plan.ServerName, CAPEM: credential.CAPEM, CertificatePEM: credential.CertificatePEM, KeyPEM: credential.KeyPEM, Bearer: credential.Bearer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return controller, client
}

func TestLiveServiceForgeOperatorRegisterDrainAndBegin(t *testing.T) {
	f := newLiveService(t)
	survivor := newLiveServiceSibling(t, f)
	controller, client := newLiveOperator(t, f, survivor)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	input := operator.QueryRuntimeInput{BuildInput: operator.BuildInput{Namespace: f.Plan.Namespace, BuildID: f.Plan.Build}, RuntimeID: f.Plan.RuntimeID}
	registration := operator.QueryRuntimeCommand{QueryRuntimeInput: input, RequestID: "original-operator-register", ExpectedVersion: "0"}
	registered, err := client.RegisterRuntime(ctx, registration)
	if err != nil || registered.Binding.RuntimeID != f.Plan.RuntimeID || registered.Binding.InstanceID != f.Plan.InstanceID || registered.Binding.Version != "1" {
		t.Fatal("Forge runtime registration", err)
	}
	replay, err := client.RegisterRuntime(ctx, registration)
	if err != nil || replay.Binding != registered.Binding {
		t.Fatal("exact native registration replay", err)
	}
	drain := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput(input), RequestID: "original-operator-drain", OperationID: "original-worker-drain", Deadline: durable.Timestamp(time.Now().Add(45 * time.Second))}
	drained, err := client.DrainWorker(ctx, drain)
	if err != nil || !drained.Complete || !drained.Quiescent || drained.RuntimeID != input.RuntimeID || drained.InstanceID != f.Plan.InstanceID || drained.OperationID != drain.OperationID || !drained.Deadline.Equal(drain.Deadline) {
		t.Fatal("Forge actual worker drain", err)
	}
	retained, err := client.ReadDrain(ctx, drain)
	if err != nil || retained.OperationID != drain.OperationID || retained.RuntimeID != input.RuntimeID || !retained.Deadline.Equal(drain.Deadline) {
		t.Fatal("retained native drain receipt", err)
	}
	verified, err := client.VerifyRuntime(ctx, operator.QueryRuntimeCommand{QueryRuntimeInput: input, RequestID: "original-operator-verify", ExpectedVersion: "1"})
	if err != nil || verified.Binding.Version != "2" || verified.Binding.ProofID == "" {
		t.Fatal("Forge actual retained query proof", err)
	}
	// A sole runtime with retained history has no eligible survivor. The real
	// store must refuse Begin, rather than manufacturing an empty coverage claim.
	if _, err = client.BeginRemoval(ctx, operator.QueryRuntimeCommand{QueryRuntimeInput: input, RequestID: "refused-sole-runtime-removal", ExpectedVersion: "2"}); err == nil {
		t.Fatal("sole history runtime removal accepted")
	}
	page, err := f.Native.ListQueryRuntimes(ctx, durable.QueryRuntimeList{NamespaceTarget: f.NativeIdentity.NamespaceTarget, BuildID: f.Plan.Build, Limit: 16})
	if err != nil || len(page.Items) != 1 || page.Items[0].State == durable.QueryRuntimeRemoving {
		t.Fatal("refused Begin changed retained runtime", err)
	}
	// A separately created, authenticated same-build process now supplies real
	// retained-history coverage. Its namespace/build configuration must be identical.
	if survivor.NativeIdentity.BuildIdentity != f.NativeIdentity.BuildIdentity || survivor.Plan.RuntimeID == f.Plan.RuntimeID || survivor.Plan.InstanceID == f.Plan.InstanceID {
		t.Fatal("invalid independent survivor fixture")
	}
	survivorInput := operator.QueryRuntimeInput{BuildInput: input.BuildInput, RuntimeID: survivor.Plan.RuntimeID}
	if _, err = client.RegisterRuntime(ctx, operator.QueryRuntimeCommand{QueryRuntimeInput: survivorInput, RequestID: "survivor-operator-register", ExpectedVersion: "0"}); err != nil {
		t.Fatal("survivor registration", err)
	}
	survivorDrain := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: input.BuildInput, RuntimeID: survivorInput.RuntimeID}, RequestID: "survivor-operator-drain", OperationID: "survivor-worker-drain", Deadline: durable.Timestamp(time.Now().Add(45 * time.Second))}
	if observed, e := client.DrainWorker(ctx, survivorDrain); e != nil || !observed.Complete || !observed.Quiescent {
		t.Fatal("survivor actual drain", e)
	}
	if verified, e := client.VerifyRuntime(ctx, operator.QueryRuntimeCommand{QueryRuntimeInput: survivorInput, RequestID: "survivor-operator-verify", ExpectedVersion: "1"}); e != nil || verified.Binding.ProofID == "" {
		t.Fatal("survivor actual query proof", e)
	}
	removal := operator.QueryRuntimeCommand{QueryRuntimeInput: input, RequestID: "original-operator-removal", ExpectedVersion: "2"}
	begun, err := client.BeginRemoval(ctx, removal)
	if err != nil || begun.Binding.State != durable.QueryRuntimeRemoving || begun.Fence.SurvivorRuntimeID != survivor.Plan.RuntimeID || begun.Fence.SurvivorInstanceID != survivor.Plan.InstanceID {
		t.Fatal("positive original Begin with exact survivor", err)
	}
	begunAgain, err := client.BeginRemoval(ctx, removal)
	if err != nil || begunAgain.Fence != begun.Fence {
		t.Fatal("exact original Begin replay", err)
	}
	// A valid bearer from the controller is insufficient with the worker's TLS key.
	wrongPeer, err := NewNativeClient(NativeEndpoint{URL: f.Plan.Endpoint, ServerName: f.Plan.ServerName, CAPEM: f.Startup.CAPEM, CertificatePEM: f.Startup.ClientCertificatePEM, KeyPEM: f.Startup.ClientKeyPEM, Bearer: controller.Credential.Bearer})
	if err != nil {
		t.Fatal(err)
	}
	defer wrongPeer.Close()
	if _, err = wrongPeer.RegisterRuntime(ctx, registration); err == nil {
		t.Fatal("wrong TLS principal replayed controller command")
	}
	if err = controller.Close(ctx); err != nil {
		t.Fatal(err)
	}
	keyID, err := aid.ParseAPIKeyID(controller.Credential.Key)
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Auth.GetAPIKey(ctx, keyID)
	if err != nil || key == nil || !key.Revoked {
		t.Fatal("controller key revocation not persisted", err)
	}
	if _, err = client.RegisterRuntime(ctx, registration); err == nil {
		t.Fatal("revoked controller replay accepted")
	}
}
