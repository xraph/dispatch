package client_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/client"
	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// ── Test Helpers ──────────────────────────────────────

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// setupClientTest creates a full Forge app with DWP routes on an httptest
// server, then dials a Go client. Returns the client, engine, store, and
// a cleanup function.
func setupClientTest(t *testing.T) (*client.Client, *engine.Engine, *memory.Store, func()) {
	t.Helper()

	// 1. Build engine with memory store and stream broker.
	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(2),
		dispatch.WithQueues([]string{"default", "high"}),
		dispatch.WithLogger(testLogger()),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d, engine.WithStreamBroker())
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	// 2. Create DWP handler and server.
	broker := eng.StreamBroker()
	logger := testLogger()
	handler := dwp.NewHandler(eng, broker, logger)
	dwpServer := dwp.NewServer(broker, handler,
		dwp.WithAuth(dwp.NewAPIKeyAuthenticator(dwp.APIKeyEntry{
			Token: "test-token",
			Identity: dwp.Identity{
				Subject: "test-user",
				AppID:   "app-1",
				OrgID:   "org-1",
				Scopes:  []string{dwp.ScopeAll},
			},
		})),
		dwp.WithLogger(logger),
	)

	// 3. Create Forge test app and register DWP routes.
	fapp := forgetesting.NewTestApp("client-test-app", "0.1.0")
	dwpServer.RegisterRoutes(fapp.Router())

	// 4. Start an httptest server backed by the forge router.
	ts := httptest.NewServer(fapp.Router())

	// 5. Dial the Go client to the WS endpoint.
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/dwp"
	c, dialErr := client.DialContext(context.Background(), wsURL,
		client.WithToken("test-token"),
		client.WithLogger(logger),
	)
	if dialErr != nil {
		ts.Close()
		t.Fatalf("DialContext: %v", dialErr)
	}

	cleanup := func() {
		_ = c.Close()
		ts.Close()
	}

	return c, eng, s, cleanup
}

// ── Connection Tests ──────────────────────────────────

func TestClient_DialAndClose(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	// Session ID should be assigned after auth.
	if c.SessionID() == "" {
		t.Error("expected non-empty session ID after dial")
	}

	// Close should not error.
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestClient_DialAuthFailure(t *testing.T) {
	// Set up server but dial with wrong token.
	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithLogger(testLogger()),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d, engine.WithStreamBroker())
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	broker := eng.StreamBroker()
	logger := testLogger()
	handler := dwp.NewHandler(eng, broker, logger)
	dwpServer := dwp.NewServer(broker, handler,
		dwp.WithAuth(dwp.NewAPIKeyAuthenticator(dwp.APIKeyEntry{
			Token: "valid-token",
			Identity: dwp.Identity{
				Subject: "user",
				Scopes:  []string{dwp.ScopeAll},
			},
		})),
		dwp.WithLogger(logger),
	)

	fapp := forgetesting.NewTestApp("auth-fail-test", "0.1.0")
	dwpServer.RegisterRoutes(fapp.Router())
	ts := httptest.NewServer(fapp.Router())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/dwp"
	_, dialErr := client.DialContext(context.Background(), wsURL,
		client.WithToken("wrong-token"),
		client.WithLogger(logger),
	)
	if dialErr == nil {
		t.Fatal("expected error for invalid token")
	}
	if !strings.Contains(dialErr.Error(), "auth") {
		t.Errorf("error = %q, want to contain 'auth'", dialErr.Error())
	}
}

// ── Job Tests ─────────────────────────────────────────

func TestClient_Enqueue(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	// Register a job so the server can enqueue it.
	engine.Register(eng, job.NewDefinition("send-email", func(_ context.Context, _ struct {
		To string `json:"to"`
	}) error {
		return nil
	}))

	result, err := c.Enqueue(context.Background(), "send-email", map[string]string{
		"to": "user@example.com",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if result.JobID == "" {
		t.Error("expected non-empty job_id")
	}
}

func TestClient_GetJob(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	engine.Register(eng, job.NewDefinition("fetch-data", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	// Enqueue first.
	result, err := c.Enqueue(context.Background(), "fetch-data", struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Get by ID.
	raw, getErr := c.GetJob(context.Background(), result.JobID)
	if getErr != nil {
		t.Fatalf("GetJob: %v", getErr)
	}

	if raw == nil {
		t.Fatal("expected non-nil response data")
	}

	// Verify the response contains the job ID.
	var resp map[string]interface{}
	if jsonErr := json.Unmarshal(raw, &resp); jsonErr != nil {
		t.Fatalf("unmarshal response: %v", jsonErr)
	}
	if resp["id"] != result.JobID {
		t.Errorf("response id = %v, want %q", resp["id"], result.JobID)
	}
}

func TestClient_CancelJob(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	engine.Register(eng, job.NewDefinition("cancel-target", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	result, err := c.Enqueue(context.Background(), "cancel-target", struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Cancel the job.
	if cancelErr := c.CancelJob(context.Background(), result.JobID); cancelErr != nil {
		t.Fatalf("CancelJob: %v", cancelErr)
	}

	// Verify the job was cancelled by fetching it.
	raw, getErr := c.GetJob(context.Background(), result.JobID)
	if getErr != nil {
		t.Fatalf("GetJob after cancel: %v", getErr)
	}

	var resp map[string]interface{}
	if jsonErr := json.Unmarshal(raw, &resp); jsonErr != nil {
		t.Fatalf("unmarshal: %v", jsonErr)
	}
	if state, ok := resp["state"].(string); ok {
		if state != string(job.StateCancelled) {
			t.Errorf("state = %q, want %q", state, job.StateCancelled)
		}
	}
}

func TestClient_EnqueueWithOptions(t *testing.T) {
	c, eng, s, cleanup := setupClientTest(t)
	defer cleanup()

	engine.Register(eng, job.NewDefinition("priority-job", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	result, err := c.Enqueue(context.Background(), "priority-job", struct{}{},
		client.WithQueue("high"),
		client.WithPriority(10),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Verify the queue and priority in the store.
	jobID, parseErr := id.Parse(result.JobID)
	if parseErr != nil {
		t.Fatalf("id.Parse: %v", parseErr)
	}
	j, getErr := s.GetJob(context.Background(), jobID)
	if getErr != nil {
		t.Fatalf("store.GetJob: %v", getErr)
	}
	if j.Queue != "high" {
		t.Errorf("queue = %q, want %q", j.Queue, "high")
	}
	if j.Priority != 10 {
		t.Errorf("priority = %d, want 10", j.Priority)
	}
}

// ── Workflow Tests ─────────────────────────────────────

func TestClient_StartWorkflow(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	// Register a simple workflow.
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("order-pipeline",
		func(wf *workflow.Workflow, _ struct {
			OrderID string `json:"order_id"`
		}) error {
			return wf.Step("validate", func(_ context.Context) error {
				return nil
			})
		},
	))

	result, err := c.StartWorkflow(context.Background(), "order-pipeline", map[string]string{
		"order_id": "ORD-001",
	})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	if result.RunID == "" {
		t.Error("expected non-empty run_id")
	}
}

func TestClient_GetWorkflow(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("get-wf",
		func(_ *workflow.Workflow, _ struct{}) error {
			return nil
		},
	))

	result, err := c.StartWorkflow(context.Background(), "get-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	// Get workflow run.
	raw, getErr := c.GetWorkflow(context.Background(), result.RunID)
	if getErr != nil {
		t.Fatalf("GetWorkflow: %v", getErr)
	}

	if raw == nil {
		t.Fatal("expected non-nil response data")
	}

	// Verify the response contains the run ID.
	var resp map[string]interface{}
	if jsonErr := json.Unmarshal(raw, &resp); jsonErr != nil {
		t.Fatalf("unmarshal: %v", jsonErr)
	}
	if resp["id"] != result.RunID {
		t.Errorf("response id = %v, want %q", resp["id"], result.RunID)
	}
}

func TestClient_PublishEvent(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	// Register a workflow (events are stored independently of workflow).
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("event-wf",
		func(_ *workflow.Workflow, _ struct{}) error {
			return nil
		},
	))

	// Publish an event.
	err := c.PublishEvent(context.Background(), "order.confirmed", map[string]string{
		"order_id": "ORD-99",
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
}

// ── Subscription Tests ────────────────────────────────

func TestClient_SubscribeAndUnsubscribe(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	// Subscribe to a channel.
	ch, err := c.Subscribe(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}

	// Unsubscribe.
	if unsubErr := c.Unsubscribe(context.Background(), "jobs"); unsubErr != nil {
		t.Fatalf("Unsubscribe: %v", unsubErr)
	}
}

func TestClient_Watch(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("watch-wf",
		func(_ *workflow.Workflow, _ struct{}) error {
			return nil
		},
	))

	result, err := c.StartWorkflow(context.Background(), "watch-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	// Watch uses Subscribe("workflow:<runID>").
	ch, watchErr := c.Watch(context.Background(), result.RunID)
	if watchErr != nil {
		t.Fatalf("Watch: %v", watchErr)
	}
	if ch == nil {
		t.Fatal("expected non-nil watch channel")
	}
}

// ── Stats Test ────────────────────────────────────────

func TestClient_Stats(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	raw, err := c.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if raw == nil {
		t.Fatal("expected non-nil stats data")
	}

	// Verify it's valid JSON.
	var stats map[string]interface{}
	if jsonErr := json.Unmarshal(raw, &stats); jsonErr != nil {
		t.Fatalf("stats unmarshal: %v", jsonErr)
	}
}

// ── Error Handling Tests ──────────────────────────────

func TestClient_GetJob_NotFound(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	_, err := c.GetJob(context.Background(), "nonexistent-job-id")
	if err == nil {
		t.Fatal("expected error for nonexistent job")
	}
}

func TestClient_GetWorkflow_NotFound(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	_, err := c.GetWorkflow(context.Background(), "nonexistent-run-id")
	if err == nil {
		t.Fatal("expected error for nonexistent workflow run")
	}
}

func TestClient_StartWorkflow_Unknown(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	_, err := c.StartWorkflow(context.Background(), "unknown-workflow", struct{}{})
	if err == nil {
		t.Fatal("expected error for unknown workflow")
	}
}

func TestClient_CancelJob_NotFound(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	err := c.CancelJob(context.Background(), "nonexistent-job-id")
	if err == nil {
		t.Fatal("expected error for cancelling nonexistent job")
	}
}

// ── Context Cancellation Tests ────────────────────────

func TestClient_ContextTimeout(t *testing.T) {
	c, _, _, cleanup := setupClientTest(t)
	defer cleanup()

	// Create a context that's already cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond) // Ensure timeout fires.

	_, err := c.Enqueue(ctx, "any-job", struct{}{})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

// ── Multiple Operations Test ──────────────────────────

func TestClient_MultipleSequentialOperations(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	engine.Register(eng, job.NewDefinition("multi-job", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	// Enqueue multiple jobs sequentially.
	ctx := context.Background()
	ids := make([]string, 5)
	for i := range 5 {
		result, err := c.Enqueue(ctx, "multi-job", map[string]int{"n": i})
		if err != nil {
			t.Fatalf("Enqueue[%d]: %v", i, err)
		}
		ids[i] = result.JobID
	}

	// Verify all jobs exist by fetching them.
	for i, id := range ids {
		raw, err := c.GetJob(ctx, id)
		if err != nil {
			t.Errorf("GetJob[%d] (%s): %v", i, id, err)
			continue
		}
		if raw == nil {
			t.Errorf("GetJob[%d]: expected non-nil data", i)
		}
	}
}

// ── Full Workflow E2E Test ────────────────────────────

func TestClient_WorkflowE2E(t *testing.T) {
	c, eng, _, cleanup := setupClientTest(t)
	defer cleanup()

	// Register a multi-step workflow.
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("e2e-wf",
		func(wf *workflow.Workflow, _ struct {
			Name string `json:"name"`
		}) error {
			if err := wf.Step("greet", func(_ context.Context) error {
				return nil
			}); err != nil {
				return err
			}
			return wf.Step("farewell", func(_ context.Context) error {
				return nil
			})
		},
	))

	ctx := context.Background()

	// Start the workflow.
	result, err := c.StartWorkflow(ctx, "e2e-wf", map[string]string{
		"name": "World",
	})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if result.RunID == "" {
		t.Fatal("expected non-empty run_id")
	}

	// Get the workflow run and verify completion.
	raw, getErr := c.GetWorkflow(ctx, result.RunID)
	if getErr != nil {
		t.Fatalf("GetWorkflow: %v", getErr)
	}

	var run map[string]interface{}
	if jsonErr := json.Unmarshal(raw, &run); jsonErr != nil {
		t.Fatalf("unmarshal run: %v", jsonErr)
	}

	// The workflow should have completed since it has no async steps.
	if state, ok := run["state"].(string); ok {
		if state != "completed" {
			t.Errorf("workflow state = %q, want %q", state, "completed")
		}
	}
}
