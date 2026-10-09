package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/security"
)

func TestRemoteBoundaryAllMountsAndNoMutation(t *testing.T) {
	f := newFixture(t)
	j := jobInState(t, f, job.StatePending)
	cases := []struct {
		name                 string
		principal            security.Principal
		authErr, errorPolicy error
		want                 int
	}{
		{name: "empty", want: 401},
		{name: "anonymous", authErr: security.ErrUnauthenticated, want: 401},
		{name: "policy denial", principal: security.Principal{Subject: "operator", Kind: "user"}, errorPolicy: security.ErrForbidden, want: 403},
		{name: "policy failure", principal: security.Principal{Subject: "operator", Kind: "user"}, errorPolicy: errors.New("database secret"), want: 503},
	}
	for _, mount := range []string{"handler", "register"} {
		for _, tc := range cases {
			t.Run(mount+"/"+tc.name, func(t *testing.T) {
				boundary := security.Boundary{Resource: security.Resource{InstallationID: "installation", PolicyTenant: "tenant"}, Authorizer: security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return tc.errorPolicy })}
				router := forge.NewRouter()
				a := api.New(f.eng, router, api.WithSecurity(security.AuthenticatorFunc(func(context.Context, *http.Request) (security.Principal, error) { return tc.principal, tc.authErr }), boundary))
				var handler http.Handler
				if mount == "handler" {
					handler = a.Handler()
				} else {
					if err := a.RegisterRoutes(router.Group("/operator")); err != nil {
						t.Fatal(err)
					}
					handler = router.Handler()
				}
				prefix := ""
				if mount == "register" {
					prefix = "/operator"
				}
				for _, path := range []string{"/v1/jobs", "/v1/stats", "/v1/jobs/" + j.ID.String() + "/cancel"} {
					method := http.MethodGet
					if path[len(path)-6:] == "cancel" {
						method = http.MethodPost
					}
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), method, prefix+path, nil))
					if rec.Code != tc.want {
						t.Fatalf("%s: %d %s", path, rec.Code, rec.Body)
					}
				}
				if got := storedJob(t, f, j.ID); got.State != job.StatePending {
					t.Fatal("denied mutation ran")
				}
			})
		}
	}
}
func TestDefaultAndPayloadAuthorization(t *testing.T) {
	f := newFixture(t)
	handler := api.New(f.eng, nil).Handler()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil))
	wantStatus(t, rec, 401)
	calls := []string{}
	handler = api.New(f.eng, nil, api.WithSecurity(security.AuthenticatorFunc(func(context.Context, *http.Request) (security.Principal, error) {
		return security.Principal{Subject: "operator", Kind: "user"}, nil
	}), security.Boundary{Resource: security.Resource{InstallationID: "test", PolicyTenant: "test"}, Authorizer: security.AuthorizerFunc(func(_ context.Context, _ security.Principal, a string, _ security.Resource) error {
		calls = append(calls, a)
		if a == security.PayloadRead {
			return security.ErrForbidden
		}
		return nil
	})})).Handler()
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil))
	wantStatus(t, rec, 403)
	if len(calls) != 2 || calls[0] != security.OperatorRead || calls[1] != security.PayloadRead {
		t.Fatal(calls)
	}
}

func TestEveryRegisteredRouteDeniesAnonymous(t *testing.T) {
	f := newFixture(t)
	router := forge.NewRouter()
	if err := api.New(f.eng, router).RegisterRoutes(router); err != nil {
		t.Fatal(err)
	}
	for _, route := range router.Routes() {
		t.Run(route.Method+route.Path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			router.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), route.Method, route.Path, nil))
			if recorder.Code != 401 {
				t.Fatalf("anonymous route status %d %s", recorder.Code, recorder.Body)
			}
		})
	}
}
