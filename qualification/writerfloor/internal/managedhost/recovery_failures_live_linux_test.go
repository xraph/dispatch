//go:build linux

package managedhost

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

// livePendingRemoval stops before the fresh marker. Every authority fact comes
// from the same public API used by the positive lifecycle drill.
func livePendingRemoval(t *testing.T, f *liveRecoveryFixture) (managed.ProviderInvocationKey, docker.ManagedDockerRequest) {
	t.Helper()
	key := f.original.Attempt
	attempt, err := f.core.ReadManagedRemovalAttempt(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	step := managed.ProviderStepKey{OperationKey: managed.OperationKey{Key: key.Key, ActionID: attempt.Descriptor.ActionID}, ProviderOperationID: attempt.Descriptor.ProviderOperationID, Kind: managed.StepRemoveContainer, Service: "worker"}
	pending, err := f.core.PrepareManagedGuardedOriginal(t.Context(), managed.GuardedOriginalRequest{Command: f.command(t), Step: step})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := f.effects.RetainProviderEffectBinding(t.Context(), managed.ProviderEffectBindingRequest{Command: liveCoreCommand(t, f.original.Candidate), Original: pending.Original.Key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.MaterializeManagedGuardedOriginal(t.Context(), managed.GuardedOriginalMaterializationRequest{Command: f.command(t), Step: step, PendingDigest: pending.PendingDigest}); err != nil {
		t.Fatal(err)
	}
	keyInvocation := managed.ProviderInvocationKey{AttemptKey: key, Step: step, CommandID: pending.Original.Permission.CommandID}
	selected, err := f.core.ReadManagedProviderInvocation(t.Context(), keyInvocation)
	if err != nil {
		t.Fatal(err)
	}
	request := docker.ManagedDockerRequest{BackendAuthority: f.original.Candidate.Custody.backend, Permission: binding.Facts.Original.Permission, Receipt: binding.Facts.Original.Receipt, Command: docker.ManagedDockerCommand{Kind: step.Kind, ResourceID: pending.Original.Permission.ResourceID}, Binding: &binding.Facts, Invocation: &selected.Permission.Facts}
	liveBindInvocation(t, f, keyInvocation)
	return keyInvocation, request
}
func liveBindInvocation(t *testing.T, f *liveRecoveryFixture, key managed.ProviderInvocationKey) {
	t.Helper()
	prepared, err := f.guard.PrepareCommit(t.Context(), f.original.Binding, key, liveAssociation(t, key.AttemptKey, f.proof), "failure-bind-"+key.CommandID.String())
	if err != nil {
		t.Fatal(err)
	}
	facts, err := f.guard.BindManagedRemovalCommit(t.Context(), f.retainIntent(t, prepared))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.RecordManagedProviderNativeCommit(t.Context(), managed.ProviderNativeCommitRequest{Command: f.command(t), Facts: facts}); err != nil {
		t.Fatal(err)
	}
}
func liveMarker(t *testing.T, f *liveRecoveryFixture, key managed.ProviderInvocationKey) managed.ProviderInvocationCommitRequest {
	t.Helper()
	current, err := f.core.ReadManagedProviderInvocationCurrent(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	return managed.ProviderInvocationCommitRequest{Command: f.command(t), Invocation: key, ExpectedCurrentDigest: current.Digest}
}

func TestLiveManagedRecoveryLostGrantClosesThenRetries(t *testing.T) {
	f := newLiveRecovery(t)
	key, request := livePendingRemoval(t, f)
	marker := liveMarker(t, f, key)
	abandoned, err := f.provider.Authorize(t.Context(), marker, request)
	if err != nil {
		t.Fatal(err)
	}
	journal, ok := f.provider.journal.(*docker.FileManagedDockerJournal)
	if !ok {
		t.Fatal("concrete durable journal missing")
	}
	reopened, err := NewProviderRemoval(f.core, f.original.Candidate.Custody, f.guard, f.original.Candidate.SDK, journal, f.provider.key)
	if err != nil {
		t.Fatal(err)
	}
	// A new configured controller instance has no volatile grant. Reading the old
	// receipt or journal must never arm the accepted invocation again.
	if err = reopened.Verify(t.Context(), abandoned); err == nil {
		t.Fatal("restart restored fresh authority")
	}
	if _, err = reopened.Authorize(t.Context(), marker, request); err == nil {
		t.Fatal("historical marker issued on restart")
	}
	witness, err := reopened.Executor().WitnessManagedProviderNoDispatch(t.Context(), key)
	if err != nil {
		t.Fatal("affirmative permanent pre-delegation closure", err)
	}
	if _, err = f.core.CloseManagedProviderInvocation(t.Context(), managed.ProviderInvocationCloseRequest{Command: f.command(t), Failure: witness}); err != nil {
		t.Fatal("retain permanent no-dispatch witness", err)
	}
	// Even the old in-memory holder is now fenced by the permanent journal/core
	// closure. Its previously fresh handle must not enter the SDK.
	if err = f.provider.Dispatch(t.Context(), admission.Delegation{Invocation: abandoned}); err == nil {
		t.Fatal("old process sent after permanent closure")
	}
	if _, err = f.original.Candidate.Runtime.ResolveBinding(t.Context(), f.original.Candidate.NativeIdentity.QueryRuntimeTarget); err != nil {
		t.Fatal("closed invocation changed live process", err)
	}
	set, err := f.core.ReadManagedRemovalRevalidationSet(t.Context(), key.AttemptKey)
	if err != nil || len(set.Current) != 1 {
		t.Fatal(err)
	}
	prepared, err := f.guard.PrepareLive(t.Context(), f.original.Binding, "retry-fresh-native", f.proof.Facts.NativeMaterialDigest)
	if err != nil {
		t.Fatal(err)
	}
	f.acceptProof(t, prepared, set.Current[0].Digest)
	current, err := f.core.ReadManagedProviderInvocationCurrent(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	authorization, err := f.core.AuthorizeManagedProviderStepRetry(t.Context(), managed.ProviderStepRetryRequest{Command: f.command(t), Step: key.Step, ExpectedCurrentDigest: current.Digest, CommandID: cid.New(managed.PrefixOperation)})
	if err != nil || authorization.Facts.Ordinal != "1" {
		t.Fatal("same-step exact retry", err)
	}
	retryKey := authorization.Facts.Key
	liveBindInvocation(t, f, retryKey)
	selected, err := f.core.ReadManagedProviderInvocation(t.Context(), retryKey)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := f.core.ReadManagedProviderInvocationSources(t.Context(), retryKey)
	if err != nil {
		t.Fatal(err)
	}
	retry := request
	retry.Permission = selected.Permission.Facts.Permission
	retry.Receipt = selected.Permission.Acceptance.Receipt
	retry.Invocation = &selected.Permission.Facts
	retry.Retry = &docker.ManagedDockerRetryEvidence{Root: sources.Root, Permission: selected.Permission, Authorization: authorization}
	grant, err := f.provider.Authorize(t.Context(), liveMarker(t, f, retryKey), retry)
	if err != nil {
		t.Fatal("fresh retry marker", err)
	}
	if err = f.admission.Invoke(t.Context(), grant); err != nil {
		t.Fatal("real retry SDK dispatch", err)
	}
	retained, err := f.provider.Request(t.Context(), retryKey)
	if err != nil || retained.Retry == nil || retained.Retry.Marker.Digest == "" {
		t.Fatal("exact marked retry request", err)
	}
	completed, err := f.provider.Executor().Read(t.Context(), retained)
	if err != nil || completed.State != "complete" || completed.Response == nil {
		t.Fatal("actual retry completion", err)
	}
	old, err := reopened.Executor().Read(t.Context(), request)
	if err != nil || old.State != "closed" || old.Response != nil {
		t.Fatal("original possible-effect history changed", err)
	}
	rows, err := f.admission.runtime.Page(t.Context(), "", 64)
	if err != nil || len(rows) != 1 || rows[0].Invocation != grant {
		t.Fatal("retry outer delegation cardinality", err)
	}
}

// liveDropDockerResponse forwards the exact SDK DELETE to the actual configured
// Docker daemon, waits for its real response, then destroys only the return path.
// Its observation never enters core effect evidence. The executor must stay unknown.
func liveDropDockerResponse(t *testing.T, f *liveRecoveryFixture, resource string) (*docker.SDKManagedDockerTransport, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	upstream, err := url.Parse(liveRequired(t, "WRITERFLOOR_SERVICE_TEST_DOCKER_HOST"))
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ResponseHeaderTimeout: 15 * time.Second}
	base := ""
	switch upstream.Scheme {
	case "unix":
		socket := upstream.Path
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}
		base = "http://docker"
	case "tcp", "http":
		base = "http://" + upstream.Host
	default:
		t.Fatal("response-loss drill requires configured local Unix or HTTP Docker daemon")
	}
	target, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.CloseIdleConnections)
	sends, success := new(atomic.Int32), new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/v1.44/containers/"+resource {
			http.Error(w, "unexpected transport command", http.StatusForbidden)
			return
		}
		sends.Add(1)
		forwarded := r.Clone(r.Context())
		forwarded.URL.Scheme, forwarded.URL.Host = target.Scheme, target.Host
		forwarded.Host = target.Host
		forwarded.RequestURI = ""
		response, callErr := transport.RoundTrip(forwarded)
		if callErr != nil {
			http.Error(w, "upstream response unavailable", http.StatusBadGateway)
			return
		}
		if response.StatusCode == http.StatusNoContent {
			success.Add(1)
		}
		_, drainErr := io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		closeErr := response.Body.Close()
		if drainErr != nil || closeErr != nil {
			http.Error(w, "upstream response incomplete", http.StatusBadGateway)
			return
		}
		// The SDK receives EOF after the actual daemon response. No synthetic 2xx is
		// returned and no caller can turn this process-local test counter into custody.
		connection, _, hijackErr := http.NewResponseController(w).Hijack()
		if hijackErr != nil {
			return
		}
		if closeErr = connection.Close(); closeErr != nil {
			return
		}
	}))
	t.Cleanup(server.Close)
	sdk, err := docker.NewSDKManagedDockerTransport("tcp://"+strings.TrimPrefix(server.URL, "http://"), "1.44", &http.Transport{ResponseHeaderTimeout: 20 * time.Second}, f.original.Candidate.Enrollment)
	if err != nil {
		t.Fatal(err)
	}
	return sdk, sends, success
}

func TestLiveManagedRecoveryLostDockerResponseNeverResends(t *testing.T) {
	f := newLiveRecovery(t)
	key, request := livePendingRemoval(t, f)
	sdk, sends, success := liveDropDockerResponse(t, f, request.Command.ResourceID)
	executor, err := docker.NewManagedDockerExecutor(f.provider.journal, sdk, f.provider, f.original.Candidate.Custody, f.original.Candidate.Custody.backend)
	if err != nil {
		t.Fatal(err)
	}
	f.provider.executor = executor
	f.guard.removal = executor
	marker := liveMarker(t, f, key)
	grant, err := f.provider.Authorize(t.Context(), marker, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.admission.Invoke(t.Context(), grant); err == nil {
		t.Fatal("lost actual Docker response returned success")
	}
	if sends.Load() != 1 || success.Load() != 1 {
		t.Fatal("actual daemon DELETE did not execute exactly once", sends.Load(), success.Load())
	}
	unknown, err := executor.Read(t.Context(), request)
	if err != nil || unknown.State != "delegated" || unknown.Response != nil || unknown.Custody != nil {
		t.Fatal("lost response fabricated positive custody", err)
	}
	if _, err = executor.Execute(t.Context(), request); err == nil {
		t.Fatal("delegated SDK command resent")
	}
	if _, err = executor.WitnessManagedProviderNoDispatch(t.Context(), key); err == nil {
		t.Fatal("possible effect became pre-delegation closure")
	}
	if _, err = f.provider.Authorize(t.Context(), marker, request); err == nil {
		t.Fatal("marker replay recreated permission")
	}
	// Exact outer replay can return its saved delegation, but never calls the SDK.
	if err = f.admission.Invoke(t.Context(), grant); err != nil {
		t.Fatal("outer historical delegation replay", err)
	}
	if sends.Load() != 1 {
		t.Fatal("uncertain replay resent Docker command", sends.Load())
	}
	current, err := f.core.ReadManagedProviderInvocationCurrent(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := fullPermissionDigest(request.Permission)
	if err != nil {
		t.Fatal(err)
	}
	observation := managed.ProviderInvocationObservationRequest{Command: f.command(t), ExpectedCurrentDigest: current.Digest, Result: managed.ProviderInvocationResult{Key: key, RootDigest: request.Invocation.RootDigest, PermissionDigest: digest, Result: managed.WireProviderStepResult{Permission: request.Permission, Outcome: managed.PortUnavailable, ResourceID: request.Permission.ResourceID, Evidence: []byte("actual Docker response channel lost after delegation"), ObservedAt: time.Now().UTC()}}}
	if err = f.core.RecordManagedProviderInvocationResult(t.Context(), observation); err != nil {
		t.Fatal("retain actual uncertain observation", err)
	}
	current, err = f.core.ReadManagedProviderInvocationCurrent(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.AuthorizeManagedProviderStepRetry(t.Context(), managed.ProviderStepRetryRequest{Command: f.command(t), Step: key.Step, ExpectedCurrentDigest: current.Digest, CommandID: cid.New(managed.PrefixOperation)}); err == nil {
		t.Fatal("uncertainty authorized retry")
	}
	if _, err = f.core.CutoffManagedRemovalRecovery(t.Context(), f.command(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.SealManagedEffectiveRemovalCoverage(t.Context(), f.command(t)); err == nil {
		t.Fatal("uncertainty became complete coverage")
	}
	if sends.Load() != 1 {
		t.Fatal("unresolved recovery resent mutation", sends.Load())
	}
}
