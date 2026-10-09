package security

import (
	"context"
	"errors"
	"testing"
)

func TestBoundaryFailClosed(t *testing.T) {
	resource := Resource{InstallationID: "installation", PolicyTenant: "policy-tenant"}
	p := Principal{Subject: "operator", Kind: "user"}
	calls := 0
	b := Boundary{Resource: resource, Authorizer: AuthorizerFunc(func(ctx context.Context, principal Principal, action string, r Resource) error {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded policy")
		}
		if r != resource || principal != p {
			t.Fatal("identity/resource changed")
		}
		if action == PayloadRead {
			return ErrForbidden
		}
		return nil
	})}
	if err := b.Check(context.Background(), p, Operation{Action: OperatorRead, Payload: true}); !errors.Is(err, ErrForbidden) || calls != 2 {
		t.Fatalf("payload bypass: %v calls %d", err, calls)
	}
	for _, principal := range []Principal{{}, {Subject: "", Kind: "user"}, {Subject: "x", Kind: "agent"}, {Subject: "x", Kind: ""}} {
		if err := b.Check(context.Background(), principal, Operation{Action: OperatorRead}); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal(err)
		}
	}
	if err := b.Check(context.Background(), p, Operation{Action: "arbitrary"}); !errors.Is(err, ErrForbidden) {
		t.Fatal(err)
	}
	for _, boundary := range []Boundary{{}, {Resource: resource}, {Authorizer: b.Authorizer}} {
		if err := boundary.Check(context.Background(), p, Operation{Action: OperatorRead}); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
	}
	b.Authorizer = AuthorizerFunc(func(context.Context, Principal, string, Resource) error { return errors.New("secret backend detail") })
	if err := b.Check(context.Background(), p, Operation{Action: OperatorRead}); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
func TestVerifiedPrincipalKinds(t *testing.T) {
	for _, kind := range []string{"user", "service", "api_key", "service_account", "service_acct"} {
		p, err := verifiedPrincipal("subject", map[string]any{"principal_kind": kind, "principal_id": "subject"})
		if err != nil {
			t.Fatal(kind, err)
		}
		if kind == "service_account" && p.Kind != "service_acct" {
			t.Fatal(p)
		}
	}
	for _, claims := range []map[string]any{{"principal_kind": 1}, {"principal_kind": ""}, {"principal_kind": "workload"}, {"principal_kind": "agent"}, {"principal_id": 2}, {"principal_id": "subject"}, {"principal_id": "foreign"}} {
		if _, err := verifiedPrincipal("subject", claims); err == nil {
			t.Fatal(claims)
		}
	}
}
