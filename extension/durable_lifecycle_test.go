package extension_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

func TestExtensionDrainKeepsAuthorizedAuditAndPublisherAvailable(t *testing.T) {
	s := memory.New()
	auth := security.AuthenticatorFunc(func(context.Context, *http.Request) (security.Principal, error) {
		return security.Principal{Subject: "operator", Kind: "user"}, nil
	})
	boundary := security.Boundary{Resource: security.Resource{InstallationID: "installation", PolicyTenant: "policy-tenant"}, Authorizer: security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return nil })}
	cfg := auditConfig()
	ns := cfg.AuditNamespace
	ns.Namespace = "operations"
	cfg.Namespaces = []durable.NamespaceConfig{ns}
	entered, release := make(chan struct{}), make(chan struct{})
	o := durableOptions(func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("work", "work", "", nil).Get() })
	o.Activities = map[string]drt.ActivityFunc{"work": func(ctx context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
		close(entered)
		select {
		case <-release:
			return []byte("done"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	e := extension.New(extension.WithStore(s), extension.WithRemoteSecurity(auth, boundary), extension.WithMemoryAuditForTesting(cfg), extension.WithDurableWorkflows(o))
	app := forgetesting.NewTestApp("drain", "1")
	if err := e.Register(app); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Engine().StartDurableWorkflow(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: o.BuildID, Queue: o.Queue}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("activity did not start")
	}
	h, err := e.BeginDurableDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	observer, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	err = e.Stop(observer)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop observer: %v", err)
	}
	read := func() int {
		rec := httptest.NewRecorder()
		app.Router().Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/dispatch/v1/stats", nil))
		return rec.Code
	}
	if code := read(); code != http.StatusOK {
		t.Fatalf("audit boundary unavailable during drain: %d", code)
	}
	until := time.Now().Add(time.Second)
	for {
		status, e2 := e.DeliveryStatus(t.Context(), durable.DestinationChronicle)
		if e2 != nil {
			t.Fatal(e2)
		}
		if status.Pending == 0 && status.InFlight == 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatalf("publisher stopped during drain: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
	if status, e2 := e.DurableStatus(); e2 != nil || status.Ready || status.State != drt.WorkerDraining {
		t.Fatalf("readiness during drain: %+v %v", status, e2)
	}
	if readiness, readErr := e.DurableReadiness(t.Context()); readErr != nil || readiness.Ready || readiness.Worker.State != drt.WorkerDraining || readiness.RetirementStatus != "unenrolled" {
		t.Fatalf("extension readiness during drain: %+v %v", readiness, readErr)
	}
	close(release)
	if result, e2 := e.WaitDurableDrain(t.Context(), h); e2 != nil || !result.Complete {
		t.Fatalf("graceful drain: %+v %v", result, e2)
	}
	if err = e.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if code := read(); code != http.StatusServiceUnavailable {
		t.Fatalf("final audit still active: %d", code)
	}
}
