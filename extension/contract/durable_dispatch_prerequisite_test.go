package contract

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xraph/forge/extensions/dashboard"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/idempotency"

	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/internal/audittest"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

// These regressions exercise the default Forge dashboard idempotency composition.
// Current admission must run before any generic cached response is returned.
func TestDurableCachedDispatchReauthorizes(t *testing.T) {
	store := memory.New()
	deps := durableDeps(t, store)
	worker, err := drt.NewWorker(store, drt.Options{Namespace: "allowed", BuildID: "historic", Queue: "queue", Owner: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	allowed := true
	deps.Durable, err = operator.New(operator.Options{Store: store, Reads: store, Catalog: store, InstallationID: "test", Audit: deps.Security, CursorKeys: operator.CursorKeys{Active: "v1", Keys: map[string][]byte{"v1": make([]byte, 32)}}, Runtime: func(string, string) (*drt.Worker, error) { return worker, nil }, Authorizer: operator.AuthorizerFunc(func(context.Context, security.Principal, string, operator.Resource) error {
		if !allowed {
			return security.ErrForbidden
		}
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	d := dispatcher.NewWithOptions(nil, dispatcher.WithIdempotencyStore(dashboard.AdaptIdempotencyStore(idempotency.NewInMemoryStore())))
	if err = Register(d, fc.NewRegistry(), fc.NewWardenRegistry(), deps); err != nil {
		t.Fatal(err)
	}
	req := fc.Request{Contributor: ContributorName, Intent: "durable.signal", IntentVersion: 1, Kind: fc.KindCommand, IdempotencyKey: "request", Payload: []byte(`{"namespace":"allowed","workflow_id":"workflow","run_id":"run","build_id":"historic","request_id":"request","name":"signal"}`)}
	if _, _, err = d.Dispatch(t.Context(), req, testPrincipal()); err != nil {
		t.Fatal(err)
	}
	allowed = false
	if _, _, err = d.Dispatch(t.Context(), req, testPrincipal()); !errors.Is(err, fc.ErrPermissionDenied) {
		t.Fatalf("cached durable receipt bypassed revoked permission: %v", err)
	}
}
func TestLegacyCachedDispatchReauthorizes(t *testing.T) {
	deps := contractDeps(t, memory.New())
	allowed := true
	deps.Security.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		if !allowed {
			return security.ErrForbidden
		}
		return nil
	})
	j := seedJob(t, deps, "cached-legacy", job.StatePending, "", "", "mail")
	d := dispatcher.NewWithOptions(nil, dispatcher.WithIdempotencyStore(dashboard.AdaptIdempotencyStore(idempotency.NewInMemoryStore())))
	if err := Register(d, fc.NewRegistry(), fc.NewWardenRegistry(), deps); err != nil {
		t.Fatal(err)
	}
	req := fc.Request{Contributor: ContributorName, Intent: "jobs.cancel", IntentVersion: 1, Kind: fc.KindCommand, IdempotencyKey: "legacy", Payload: []byte(`{"id":"` + j.ID.String() + `"}`)}
	if _, _, err := d.Dispatch(t.Context(), req, testPrincipal()); err != nil {
		t.Fatal(err)
	}
	allowed = false
	if _, _, err := d.Dispatch(t.Context(), req, testPrincipal()); !errors.Is(err, fc.ErrPermissionDenied) {
		t.Fatalf("cached legacy response bypassed revoked permission: %v", err)
	}
}

func TestLegacyCacheBindsTrustedScopeAndKeepsDeduplication(t *testing.T) {
	deps := contractDeps(t, memory.New())
	j := seedJob(t, deps, "scope-legacy", job.StatePending, "", "", "mail")
	cache := dashboard.AdaptIdempotencyStore(idempotency.NewInMemoryStore())
	makeDispatcher := func(scoped Deps) *dispatcher.Dispatcher {
		t.Helper()
		d := dispatcher.NewWithOptions(nil, dispatcher.WithIdempotencyStore(cache))
		if err := Register(d, fc.NewRegistry(), fc.NewWardenRegistry(), scoped); err != nil {
			t.Fatal(err)
		}
		return d
	}
	req := fc.Request{Contributor: ContributorName, Intent: "jobs.cancel", IntentVersion: 1, Kind: fc.KindCommand, IdempotencyKey: "scoped-legacy", Params: map[string]any{"id": "invalid-param-overridden"}, Payload: []byte(`{"id":"` + j.ID.String() + `"}`)}
	original := makeDispatcher(deps)
	first, _, err := original.Dispatch(t.Context(), req, testPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := original.Dispatch(t.Context(), req, testPrincipal())
	if err != nil || !bytes.Equal(first, again) {
		t.Fatalf("same-scope replay: %v", err)
	}
	// A second cancellation would fail on the terminal job. Success above proves
	// this remains a generic replay instead of another mutation.
	for _, scope := range []string{"installation", "policy-tenant"} {
		t.Run(scope, func(t *testing.T) {
			changed := deps
			if scope == "installation" {
				changed.Security.Resource.InstallationID += "/other"
			} else {
				changed.Security.Resource.PolicyTenant += "/other"
			}
			changed.Security = audittest.WithMemory(changed.Security)
			result, _, dispatchErr := makeDispatcher(changed).Dispatch(t.Context(), req, testPrincipal())
			var conflict *fc.Error
			if result != nil || !errors.As(dispatchErr, &conflict) || conflict.Code != fc.CodeConflict {
				t.Fatalf("cross-scope cached disclosure: %v %v", result, dispatchErr)
			}
		})
	}
	malformed := req
	malformed.Params = map[string]any{"id": j.ID.String()}
	malformed.Payload = []byte(`{"id":"invalid-payload-overrides-param"}`)
	if result, _, err := original.Dispatch(t.Context(), malformed, testPrincipal()); result != nil || !errors.Is(err, fc.ErrBadRequest) {
		t.Fatalf("decoded target validation did not precede cache: %v %v", result, err)
	}
	deps.Security.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		return security.ErrForbidden
	})
	if result, _, err := makeDispatcher(deps).Dispatch(t.Context(), req, testPrincipal()); result != nil || !errors.Is(err, fc.ErrPermissionDenied) {
		t.Fatalf("decoded target bypass: %v %v", result, err)
	}
}
