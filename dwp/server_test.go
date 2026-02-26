package dwp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/stream"
	"github.com/xraph/dispatch/workflow"
)

// ── Test Helpers ──────────────────────────────────────

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// setupTestEngine creates a full Engine + stream broker for integration tests.
func setupTestEngine(t *testing.T) (*engine.Engine, *memory.Store) {
	t.Helper()
	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(2),
		dispatch.WithQueues([]string{"default", "high"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d, engine.WithStreamBroker())
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}
	return eng, s
}

// setupTestServer creates a full DWP server with engine, handler, and auth.
func setupTestServer(t *testing.T) (*Server, *engine.Engine, *memory.Store) {
	t.Helper()
	eng, s := setupTestEngine(t)
	broker := eng.StreamBroker()
	logger := testLogger()
	handler := NewHandler(eng, broker, logger)

	srv := NewServer(broker, handler,
		WithAuth(NewAPIKeyAuthenticator(APIKeyEntry{
			Token: "test-token",
			Identity: Identity{
				Subject: "test-user",
				AppID:   "app-1",
				OrgID:   "org-1",
				Scopes:  []string{ScopeAll},
			},
		}, APIKeyEntry{
			Token: "limited-token",
			Identity: Identity{
				Subject: "limited-user",
				AppID:   "app-2",
				OrgID:   "org-2",
				Scopes:  []string{ScopeJobRead},
			},
		})),
		WithLogger(logger),
	)

	return srv, eng, s
}

// ── Server Unit Tests ─────────────────────────────────

func TestServer_NewServer(t *testing.T) {
	broker := stream.NewBroker(testLogger())
	handler := &Handler{logger: testLogger()}

	srv := NewServer(broker, handler)

	if srv.broker != broker {
		t.Error("broker not set")
	}
	if srv.handler != handler {
		t.Error("handler not set")
	}
	if srv.conns == nil {
		t.Error("connection manager not created")
	}
	if srv.basePath != "/dwp" {
		t.Errorf("basePath = %q, want /dwp", srv.basePath)
	}
	// Default auth should be NoopAuthenticator.
	if srv.auth == nil {
		t.Error("auth not set")
	}
}

func TestServer_NewServerWithOptions(t *testing.T) {
	broker := stream.NewBroker(testLogger())
	handler := &Handler{logger: testLogger()}
	auth := NewAPIKeyAuthenticator(APIKeyEntry{Token: "k", Identity: Identity{Subject: "s"}})
	logger := testLogger()

	srv := NewServer(broker, handler,
		WithAuth(auth),
		WithLogger(logger),
		WithPath("/custom"),
		WithCodec(&MsgpackCodec{}),
	)

	if srv.basePath != "/custom" {
		t.Errorf("basePath = %q, want /custom", srv.basePath)
	}
	if srv.defaultCodec.Name() != CodecNameMsgpack {
		t.Errorf("codec = %q, want %q", srv.defaultCodec.Name(), CodecNameMsgpack)
	}
}

func TestServer_ConnectionManager(t *testing.T) {
	srv, _, _ := setupTestServer(t)

	if srv.Connections().Count() != 0 {
		t.Errorf("initial connections = %d, want 0", srv.Connections().Count())
	}

	// Add connections.
	conn1 := NewConnection("conn-1", &Identity{Subject: "user1"}, &JSONCodec{})
	conn2 := NewConnection("conn-2", &Identity{Subject: "user2"}, &JSONCodec{})
	srv.Connections().Add(conn1)
	srv.Connections().Add(conn2)

	if srv.Connections().Count() != 2 {
		t.Errorf("connections = %d, want 2", srv.Connections().Count())
	}

	// Get by ID.
	got, ok := srv.Connections().Get("conn-1")
	if !ok {
		t.Error("expected to find conn-1")
	}
	if got.Identity.Subject != "user1" {
		t.Errorf("subject = %q, want user1", got.Identity.Subject)
	}

	// Remove.
	srv.Connections().Remove("conn-1")
	if srv.Connections().Count() != 1 {
		t.Errorf("connections after remove = %d, want 1", srv.Connections().Count())
	}

	_, ok = srv.Connections().Get("conn-1")
	if ok {
		t.Error("expected conn-1 to be removed")
	}
}

// ── Handler Integration Tests (with real Engine) ──────

func TestHandler_JobEnqueueViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())

	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	frame := &Frame{
		ID:     "req-enqueue",
		Type:   FrameRequest,
		Method: MethodJobEnqueue,
		Data: mustJSON(JobEnqueueRequest{
			Name:    "test-job",
			Payload: json.RawMessage(`{"key":"value"}`),
			Queue:   "default",
		}),
	}

	resp := handler.Handle(context.Background(), frame, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", resp.Type, FrameResponse, resp.Error)
	}
	if resp.CorrelID != "req-enqueue" {
		t.Errorf("CorrelID = %q, want req-enqueue", resp.CorrelID)
	}

	var result JobEnqueueResponse
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result.JobID == "" {
		t.Error("expected non-empty job_id")
	}
	if result.Queue != "default" {
		t.Errorf("queue = %q, want default", result.Queue)
	}
	if result.State != string(job.StatePending) {
		t.Errorf("state = %q, want pending", result.State)
	}
}

func TestHandler_JobGetViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	// Enqueue first.
	enqFrame := &Frame{
		ID:     "req-1",
		Type:   FrameRequest,
		Method: MethodJobEnqueue,
		Data:   mustJSON(JobEnqueueRequest{Name: "get-test", Payload: json.RawMessage(`{}`)}),
	}
	enqResp := handler.Handle(context.Background(), enqFrame, conn)
	var enqResult JobEnqueueResponse
	_ = json.Unmarshal(enqResp.Data, &enqResult)

	// Get the job.
	getFrame := &Frame{
		ID:     "req-2",
		Type:   FrameRequest,
		Method: MethodJobGet,
		Data:   mustJSON(JobGetRequest{JobID: enqResult.JobID}),
	}
	getResp := handler.Handle(context.Background(), getFrame, conn)
	if getResp == nil {
		t.Fatal("expected response")
	}
	if getResp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", getResp.Type, FrameResponse, getResp.Error)
	}

	// Verify job data is returned.
	var jobData map[string]any
	if err := json.Unmarshal(getResp.Data, &jobData); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if jobData["name"] != "get-test" {
		t.Errorf("name = %v, want get-test", jobData["name"])
	}
}

func TestHandler_JobCancelViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	// Enqueue.
	enqResp := handler.Handle(context.Background(), &Frame{
		ID: "req-1", Type: FrameRequest, Method: MethodJobEnqueue,
		Data: mustJSON(JobEnqueueRequest{Name: "cancel-test", Payload: json.RawMessage(`{}`)}),
	}, conn)
	var enqResult JobEnqueueResponse
	_ = json.Unmarshal(enqResp.Data, &enqResult)

	// Cancel.
	cancelResp := handler.Handle(context.Background(), &Frame{
		ID: "req-2", Type: FrameRequest, Method: MethodJobCancel,
		Data: mustJSON(JobCancelRequest{JobID: enqResult.JobID}),
	}, conn)
	if cancelResp == nil {
		t.Fatal("expected response")
	}
	if cancelResp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", cancelResp.Type, FrameResponse, cancelResp.Error)
	}

	// Verify cancelled.
	var cancelResult map[string]string
	_ = json.Unmarshal(cancelResp.Data, &cancelResult)
	if cancelResult["status"] != "cancelled" {
		t.Errorf("status = %q, want cancelled", cancelResult["status"])
	}
}

func TestHandler_WorkflowStartViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	// Register a simple workflow.
	reg := eng.WorkflowRunner()
	_ = reg // Runner is used by StartRaw internally.

	wfReg := eng.Dispatcher().Store()
	_ = wfReg

	// Use the engine's workflow registry.
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("start-test-wf", func(_ *workflow.Workflow, _ struct{}) error {
		return nil
	}))

	// Start via handler.
	startResp := handler.Handle(context.Background(), &Frame{
		ID: "req-wf", Type: FrameRequest, Method: MethodWorkflowStart,
		Data: mustJSON(WorkflowStartRequest{Name: "start-test-wf", Input: json.RawMessage(`{}`)}),
	}, conn)
	if startResp == nil {
		t.Fatal("expected response")
	}
	if startResp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", startResp.Type, FrameResponse, startResp.Error)
	}

	var wfResult WorkflowStartResponse
	if err := json.Unmarshal(startResp.Data, &wfResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if wfResult.RunID == "" {
		t.Error("expected non-empty run_id")
	}
	if wfResult.Name != "start-test-wf" {
		t.Errorf("name = %q, want start-test-wf", wfResult.Name)
	}
}

func TestHandler_WorkflowGetViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("get-test-wf", func(_ *workflow.Workflow, _ struct{}) error {
		return nil
	}))

	// Start first.
	startResp := handler.Handle(context.Background(), &Frame{
		ID: "req-1", Type: FrameRequest, Method: MethodWorkflowStart,
		Data: mustJSON(WorkflowStartRequest{Name: "get-test-wf", Input: json.RawMessage(`{}`)}),
	}, conn)
	var startResult WorkflowStartResponse
	_ = json.Unmarshal(startResp.Data, &startResult)

	// Get the workflow.
	getResp := handler.Handle(context.Background(), &Frame{
		ID: "req-2", Type: FrameRequest, Method: MethodWorkflowGet,
		Data: mustJSON(WorkflowGetRequest{RunID: startResult.RunID}),
	}, conn)
	if getResp == nil {
		t.Fatal("expected response")
	}
	if getResp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", getResp.Type, FrameResponse, getResp.Error)
	}

	var runData map[string]any
	_ = json.Unmarshal(getResp.Data, &runData)
	if runData["name"] != "get-test-wf" {
		t.Errorf("name = %v, want get-test-wf", runData["name"])
	}
}

func TestHandler_WorkflowEventViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())
	conn := NewConnection("c-1", &Identity{
		Subject: "test",
		AppID:   "app-1",
		OrgID:   "org-1",
		Scopes:  []string{ScopeAll},
	}, &JSONCodec{})

	// Publish event.
	evtResp := handler.Handle(context.Background(), &Frame{
		ID: "req-evt", Type: FrameRequest, Method: MethodWorkflowEvent,
		Data: mustJSON(WorkflowEventRequest{Name: "order.created", Payload: json.RawMessage(`{"id":"o-1"}`)}),
	}, conn)
	if evtResp == nil {
		t.Fatal("expected response")
	}
	if evtResp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", evtResp.Type, FrameResponse, evtResp.Error)
	}

	var evtResult map[string]string
	_ = json.Unmarshal(evtResp.Data, &evtResult)
	if evtResult["event_id"] == "" {
		t.Error("expected non-empty event_id")
	}
	if evtResult["name"] != "order.created" {
		t.Errorf("name = %q, want order.created", evtResult["name"])
	}
}

func TestHandler_WorkflowTimelineViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	// Register a multi-step workflow.
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("timeline-wf", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("s1", func(_ context.Context) error { return nil }); err != nil {
			return err
		}
		return wf.Step("s2", func(_ context.Context) error { return nil })
	}))

	// Start it.
	startResp := handler.Handle(context.Background(), &Frame{
		ID: "req-1", Type: FrameRequest, Method: MethodWorkflowStart,
		Data: mustJSON(WorkflowStartRequest{Name: "timeline-wf", Input: json.RawMessage(`{}`)}),
	}, conn)
	var startResult WorkflowStartResponse
	_ = json.Unmarshal(startResp.Data, &startResult)

	// Get timeline.
	tlResp := handler.Handle(context.Background(), &Frame{
		ID: "req-2", Type: FrameRequest, Method: MethodWorkflowTimeline,
		Data: mustJSON(WorkflowTimelineRequest{RunID: startResult.RunID}),
	}, conn)
	if tlResp == nil {
		t.Fatal("expected response")
	}
	if tlResp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", tlResp.Type, FrameResponse, tlResp.Error)
	}

	var timeline []map[string]any
	if err := json.Unmarshal(tlResp.Data, &timeline); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(timeline) != 2 {
		t.Errorf("timeline entries = %d, want 2", len(timeline))
	}
}

func TestHandler_StatsViaHandler(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	broker := eng.StreamBroker()
	handler := NewHandler(eng, broker, testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	resp := handler.Handle(context.Background(), &Frame{
		ID: "req-stats", Type: FrameRequest, Method: MethodStats,
	}, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q", resp.Type, FrameResponse)
	}

	var stats map[string]any
	if err := json.Unmarshal(resp.Data, &stats); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := stats["broker"]; !ok {
		t.Error("expected broker stats")
	}
}

// ── Auth Tests ──────────────────────────────────────

func TestServer_AuthSuccess(t *testing.T) {
	srv, _, _ := setupTestServer(t)

	identity, err := srv.auth.Authenticate(context.Background(), "test-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.Subject != "test-user" {
		t.Errorf("Subject = %q, want test-user", identity.Subject)
	}
	if identity.AppID != "app-1" {
		t.Errorf("AppID = %q, want app-1", identity.AppID)
	}
	if !identity.HasScope(ScopeAll) {
		t.Error("expected wildcard scope")
	}
}

func TestServer_AuthFailure(t *testing.T) {
	srv, _, _ := setupTestServer(t)

	_, err := srv.auth.Authenticate(context.Background(), "invalid-token")
	if err == nil {
		t.Fatal("expected auth error")
	}
}

func TestServer_ScopeAuthorization(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		scopes  []string
		allowed bool
	}{
		{"wildcard allows everything", MethodJobEnqueue, []string{ScopeAll}, true},
		{"job:write allows enqueue", MethodJobEnqueue, []string{ScopeJobWrite}, true},
		{"job:read allows get", MethodJobGet, []string{ScopeJobRead}, true},
		{"job:read denies enqueue", MethodJobEnqueue, []string{ScopeJobRead}, false},
		{"workflow:write allows start", MethodWorkflowStart, []string{ScopeWorkflowWrite}, true},
		{"workflow:read allows get", MethodWorkflowGet, []string{ScopeWorkflowRead}, true},
		{"subscribe scope allows subscribe", MethodSubscribe, []string{ScopeSubscribe}, true},
		{"job:read denies subscribe", MethodSubscribe, []string{ScopeJobRead}, false},
		{"workflow:write allows workflow.event", MethodWorkflowEvent, []string{ScopeWorkflowWrite}, true},
		{"stats:read allows stats", MethodStats, []string{ScopeStatsRead}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := &Identity{Subject: "test", Scopes: tt.scopes}
			reqScope := RequiredScope(tt.method)

			if reqScope == "" {
				// No scope required — always allowed.
				return
			}

			allowed := identity.HasScope(reqScope)
			if allowed != tt.allowed {
				t.Errorf("HasScope(%q) for %v = %v, want %v",
					reqScope, tt.scopes, allowed, tt.allowed)
			}
		})
	}
}

// ── Codec Tests ──────────────────────────────────────

func TestServer_CodecNegotiation(t *testing.T) {
	tests := []struct {
		format string
		expect string
	}{
		{"", CodecNameJSON},
		{"json", CodecNameJSON},
		{"msgpack", CodecNameMsgpack},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			codec := GetCodec(tt.format)
			if codec.Name() != tt.expect {
				t.Errorf("GetCodec(%q) = %q, want %q", tt.format, codec.Name(), tt.expect)
			}
		})
	}
}

func TestServer_CodecRoundTrip(t *testing.T) {
	codecs := []Codec{&JSONCodec{}, &MsgpackCodec{}}

	for _, codec := range codecs {
		t.Run(codec.Name(), func(t *testing.T) {
			original := &Frame{
				ID:       "frame-1",
				Type:     FrameRequest,
				Method:   MethodJobEnqueue,
				CorrelID: "",
				Data:     json.RawMessage(`{"name":"test"}`),
			}

			encoded, err := codec.Encode(original)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			decoded, err := codec.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if decoded.ID != original.ID {
				t.Errorf("ID = %q, want %q", decoded.ID, original.ID)
			}
			if decoded.Type != original.Type {
				t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
			}
			if decoded.Method != original.Method {
				t.Errorf("Method = %q, want %q", decoded.Method, original.Method)
			}
		})
	}
}

// ── Connection Tests ──────────────────────────────────

func TestConnection_Subscriptions(t *testing.T) {
	conn := NewConnection("test-conn", &Identity{Subject: "user"}, &JSONCodec{})

	if len(conn.Subscriptions()) != 0 {
		t.Errorf("initial subscriptions = %d, want 0", len(conn.Subscriptions()))
	}

	conn.AddSubscription("jobs")
	conn.AddSubscription("workflows")

	subs := conn.Subscriptions()
	if len(subs) != 2 {
		t.Errorf("subscriptions = %d, want 2", len(subs))
	}

	conn.RemoveSubscription("jobs")
	subs = conn.Subscriptions()
	if len(subs) != 1 {
		t.Errorf("subscriptions after remove = %d, want 1", len(subs))
	}
}

func TestConnection_Touch(t *testing.T) {
	conn := NewConnection("test-conn", &Identity{Subject: "user"}, &JSONCodec{})
	initial := conn.LastActivity.Load().(time.Time)

	time.Sleep(10 * time.Millisecond)
	conn.Touch()

	updated := conn.LastActivity.Load().(time.Time)
	if !updated.After(initial) {
		t.Error("Touch did not update LastActivity")
	}
}

// ── Error Handling Tests ─────────────────────────────

func TestHandler_JobGetInvalidID(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	handler := NewHandler(eng, eng.StreamBroker(), testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	resp := handler.Handle(context.Background(), &Frame{
		ID: "req-1", Type: FrameRequest, Method: MethodJobGet,
		Data: mustJSON(JobGetRequest{JobID: "not-a-valid-id"}),
	}, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameErr {
		t.Errorf("Type = %q, want %q", resp.Type, FrameErr)
	}
	if resp.Error == nil {
		t.Fatal("expected error detail")
	}
}

func TestHandler_JobGetNotFound(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	handler := NewHandler(eng, eng.StreamBroker(), testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	resp := handler.Handle(context.Background(), &Frame{
		ID: "req-1", Type: FrameRequest, Method: MethodJobGet,
		Data: mustJSON(JobGetRequest{JobID: "job_000000000000000000000000000"}),
	}, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameErr {
		t.Errorf("Type = %q, want %q", resp.Type, FrameErr)
	}
}

func TestHandler_WorkflowStartUnknown(t *testing.T) {
	_, eng, _ := setupTestServer(t)
	handler := NewHandler(eng, eng.StreamBroker(), testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	resp := handler.Handle(context.Background(), &Frame{
		ID: "req-1", Type: FrameRequest, Method: MethodWorkflowStart,
		Data: mustJSON(WorkflowStartRequest{Name: "nonexistent", Input: json.RawMessage(`{}`)}),
	}, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameErr {
		t.Errorf("Type = %q, want %q", resp.Type, FrameErr)
	}
	if resp.Error == nil || resp.Error.Code != ErrCodeInternal {
		t.Errorf("Error.Code = %v, want %d", resp.Error, ErrCodeInternal)
	}
}

// ── Job Enqueue With Options ──────────────────────────

func TestHandler_JobEnqueueWithOptions(t *testing.T) {
	_, eng, s := setupTestServer(t)
	handler := NewHandler(eng, eng.StreamBroker(), testLogger())
	conn := NewConnection("c-1", &Identity{Subject: "test", Scopes: []string{ScopeAll}}, &JSONCodec{})

	resp := handler.Handle(context.Background(), &Frame{
		ID: "req-1", Type: FrameRequest, Method: MethodJobEnqueue,
		Data: mustJSON(JobEnqueueRequest{
			Name:     "priority-job",
			Payload:  json.RawMessage(`{}`),
			Queue:    "high",
			Priority: 5,
		}),
	}, conn)
	if resp.Type != FrameResponse {
		t.Fatalf("Type = %q, want %q, error = %v", resp.Type, FrameResponse, resp.Error)
	}

	var result JobEnqueueResponse
	_ = json.Unmarshal(resp.Data, &result)

	if result.Queue != "high" {
		t.Errorf("queue = %q, want high", result.Queue)
	}

	// Verify in store.
	jobs, err := s.ListJobsByState(context.Background(), job.StatePending, job.ListOpts{Limit: 100})
	if err != nil {
		t.Fatalf("ListJobsByState: %v", err)
	}
	var found bool
	for _, j := range jobs {
		if j.Queue == "high" && j.Priority == 5 {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected job with queue=high and priority=5 in store")
	}
}
