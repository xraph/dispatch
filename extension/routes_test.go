package extension_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/store/memory"
)

// TestExtension_RoutesRegister mounts the api through Register and reads
// a list route under the default base path. Mounting it again on the same
// path collides route by route, and RegisterRoutes must report that.
func TestExtension_RoutesRegister(t *testing.T) {
	ext := extension.New(extension.WithStore(memory.New()))
	fapp := forgetesting.NewTestApp("routes-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/dispatch/v1/jobs", nil)
	fapp.Router().Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "authentication required") {
		t.Errorf("GET /dispatch/v1/jobs = %d %q, want 401 authentication required", rec.Code, rec.Body.String())
	}

	err := ext.RegisterRoutes(fapp.Router().Group("/dispatch"))
	if err == nil || !strings.Contains(err.Error(), "GET /v1/jobs:") {
		t.Errorf("RegisterRoutes on a path that already holds the api = %v, want an error naming GET /v1/jobs", err)
	}
}
