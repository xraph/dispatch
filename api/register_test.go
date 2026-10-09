package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/security"
)

// allRoutes is every route the api serves, as forge lists them.
var allRoutes = []string{
	"GET /v1/jobs",
	"GET /v1/jobs/:jobId",
	"POST /v1/jobs/:jobId/cancel",
	"POST /v1/jobs/:jobId/retry",
	"GET /v1/jobs/counts",
	"GET /v1/workflows",
	"GET /v1/workflows/runs",
	"GET /v1/workflows/runs/:runId",
	"GET /v1/workflows/runs/:runId/replay",
	"POST /v1/workflows/runs/:runId/replay",
	"GET /v1/dlq",
	"GET /v1/dlq/:entryId",
	"POST /v1/dlq/:entryId/replay",
	"DELETE /v1/dlq/:entryId",
	"POST /v1/dlq/replay-all",
	"POST /v1/dlq/purge",
	"GET /v1/dlq/count",
	"GET /v1/crons",
	"GET /v1/crons/:cronId",
	"POST /v1/crons/:cronId/enable",
	"POST /v1/crons/:cronId/disable",
	"DELETE /v1/crons/:cronId",
	"POST /v1/crons/:cronId/trigger",
	"GET /v1/stats",
}

// TestRegisterRoutes mounts the api on a fresh router. Forge must accept
// every route, and the router must hold exactly the routes the api serves.
func TestRegisterRoutes(t *testing.T) {
	f := newFixture(t)
	r := forge.NewRouter()

	if err := api.New(f.eng, r, testSecurity()).RegisterRoutes(r); err != nil {
		t.Fatalf("RegisterRoutes: %v", err)
	}

	got := make([]string, 0, len(allRoutes))
	for _, info := range r.Routes() {
		got = append(got, info.Method+" "+info.Path)
		if security.RESTOperation(info.Method, strings.TrimPrefix(info.Path, "/v1")).Action == "" {
			t.Fatalf("route has no closed authorization mapping: %s %s", info.Method, info.Path)
		}
	}
	slices.Sort(got)
	want := slices.Sorted(slices.Values(allRoutes))
	if !slices.Equal(got, want) {
		t.Errorf("routes =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestRegisterRoutes_ReportsRefusedRoutes mounts the api twice on one
// router, so every route collides with itself. RegisterRoutes must return
// forge's refusals, each named by method and path.
func TestRegisterRoutes_ReportsRefusedRoutes(t *testing.T) {
	f := newFixture(t)
	r := forge.NewRouter()
	a := api.New(f.eng, r, testSecurity())

	if err := a.RegisterRoutes(r); err != nil {
		t.Fatalf("first RegisterRoutes: %v", err)
	}

	err := a.RegisterRoutes(r)
	if err == nil {
		t.Fatal("second RegisterRoutes on the same router returned nil")
	}
	for _, route := range []string{"GET /v1/jobs:", "POST /v1/dlq/purge:", "DELETE /v1/crons/:cronId:", "GET /v1/stats:"} {
		if !strings.Contains(err.Error(), route) {
			t.Errorf("error does not name %q:\n%v", strings.TrimSuffix(route, ":"), err)
		}
	}
}

// TestHandler_PanicsOnARefusedRoute builds a handler on a router that
// already holds every route. Handler has no error to return, so it must
// panic with forge's reason instead of serving an api with holes in it.
func TestHandler_PanicsOnARefusedRoute(t *testing.T) {
	f := newFixture(t)
	r := forge.NewRouter()
	if err := api.New(f.eng, r, testSecurity()).RegisterRoutes(r); err != nil {
		t.Fatalf("RegisterRoutes: %v", err)
	}

	defer func() {
		msg, _ := recover().(string)
		if !strings.Contains(msg, "dispatch api: ") || !strings.Contains(msg, "GET /v1/jobs:") {
			t.Errorf("panic = %q, want one naming GET /v1/jobs", msg)
		}
	}()

	api.New(f.eng, r, testSecurity()).Handler()
	t.Error("Handler returned on a router where every route collides")
}

// TestHandler_RegistersOnce calls Handler twice. The second call must not
// register every route again, which would collide and panic.
func TestHandler_RegistersOnce(t *testing.T) {
	f := newFixture(t)
	a := api.New(f.eng, nil, testSecurity())

	a.Handler()
	h := a.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/stats", nil))
	wantStatus(t, rec, http.StatusOK)
}
