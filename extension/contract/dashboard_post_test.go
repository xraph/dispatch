package contract

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
)

// dashboardPost builds a dashboard envelope request the way a browser client
// sends one. Forge refuses commands that aren't sent as JSON.
func dashboardPost(ctx context.Context, body io.Reader) *http.Request {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/v1", body)
	req.Header.Set("Content-Type", "application/json")

	return req
}
