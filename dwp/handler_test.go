package dwp

import (
	"context"
	"encoding/json"
	"testing"
)

// mockBrokerForHandler satisfies the handler's broker dependency for stats.
// The handler only calls broker.Stats(), so we test that path via handleStats.

func TestHandler_HandleSubscribe(t *testing.T) {
	t.Parallel()

	h := &Handler{logger: testLogger()}

	frame := &Frame{
		ID:     "req-1",
		Type:   FrameRequest,
		Method: MethodSubscribe,
		Data:   mustJSON(SubscribeRequest{Channel: "jobs"}),
	}
	conn := NewConnection("conn-1", &Identity{Subject: "test", Scopes: []string{"*"}}, &JSONCodec{})

	resp := h.Handle(context.Background(), frame, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameResponse {
		t.Errorf("Type = %q, want %q", resp.Type, FrameResponse)
	}
	if resp.CorrelID != "req-1" {
		t.Errorf("CorrelID = %q, want %q", resp.CorrelID, "req-1")
	}

	var result map[string]string
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["channel"] != "jobs" {
		t.Errorf("channel = %q, want %q", result["channel"], "jobs")
	}
	if result["status"] != "subscribed" {
		t.Errorf("status = %q, want %q", result["status"], "subscribed")
	}
}

func TestHandler_HandleUnsubscribe(t *testing.T) {
	t.Parallel()

	h := &Handler{logger: testLogger()}

	frame := &Frame{
		ID:     "req-2",
		Type:   FrameRequest,
		Method: MethodUnsubscribe,
		Data:   mustJSON(UnsubscribeRequest{Channel: "jobs"}),
	}
	conn := NewConnection("conn-1", &Identity{Subject: "test", Scopes: []string{"*"}}, &JSONCodec{})

	resp := h.Handle(context.Background(), frame, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameResponse {
		t.Errorf("Type = %q, want %q", resp.Type, FrameResponse)
	}

	var result map[string]string
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["status"] != "unsubscribed" {
		t.Errorf("status = %q, want %q", result["status"], "unsubscribed")
	}
}

func TestHandler_HandleSubscribeInvalidTopic(t *testing.T) {
	t.Parallel()

	h := &Handler{logger: testLogger()}

	frame := &Frame{
		ID:     "req-3",
		Type:   FrameRequest,
		Method: MethodSubscribe,
		Data:   mustJSON(SubscribeRequest{Channel: "invalid"}),
	}
	conn := NewConnection("conn-1", &Identity{Subject: "test", Scopes: []string{"*"}}, &JSONCodec{})

	resp := h.Handle(context.Background(), frame, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameErr {
		t.Errorf("Type = %q, want %q", resp.Type, FrameErr)
	}
	if resp.Error == nil {
		t.Fatal("expected error detail")
	}
	if resp.Error.Code != ErrCodeBadRequest {
		t.Errorf("Error.Code = %d, want %d", resp.Error.Code, ErrCodeBadRequest)
	}
}

func TestHandler_HandleUnknownMethod(t *testing.T) {
	t.Parallel()

	h := &Handler{logger: testLogger()}

	frame := &Frame{
		ID:     "req-4",
		Type:   FrameRequest,
		Method: "nonexistent.method",
	}
	conn := NewConnection("conn-1", &Identity{Subject: "test", Scopes: []string{"*"}}, &JSONCodec{})

	resp := h.Handle(context.Background(), frame, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameErr {
		t.Errorf("Type = %q, want %q", resp.Type, FrameErr)
	}
	if resp.Error == nil {
		t.Fatal("expected error detail")
	}
	if resp.Error.Code != ErrCodeMethodNotFound {
		t.Errorf("Error.Code = %d, want %d", resp.Error.Code, ErrCodeMethodNotFound)
	}
}

func TestHandler_HandleBadJSON(t *testing.T) {
	t.Parallel()

	h := &Handler{logger: testLogger()}

	frame := &Frame{
		ID:     "req-5",
		Type:   FrameRequest,
		Method: MethodSubscribe,
		Data:   json.RawMessage(`{invalid json}`),
	}
	conn := NewConnection("conn-1", &Identity{Subject: "test", Scopes: []string{"*"}}, &JSONCodec{})

	resp := h.Handle(context.Background(), frame, conn)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Type != FrameErr {
		t.Errorf("Type = %q, want %q", resp.Type, FrameErr)
	}
}

func TestHandler_FederationNotEnabled(t *testing.T) {
	t.Parallel()

	h := &Handler{logger: testLogger()}

	methods := []string{MethodFederationEnqueue, MethodFederationEvent, MethodFederationHeartbeat}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			frame := &Frame{
				ID:     "req-fed",
				Type:   FrameRequest,
				Method: method,
				Data:   json.RawMessage(`{}`),
			}
			conn := NewConnection("conn-1", &Identity{Subject: "test", Scopes: []string{"*"}}, &JSONCodec{})

			resp := h.Handle(context.Background(), frame, conn)
			if resp == nil {
				t.Fatal("expected response")
			}
			if resp.Type != FrameErr {
				t.Errorf("Type = %q, want %q", resp.Type, FrameErr)
			}
			if resp.Error == nil {
				t.Fatal("expected error detail")
			}
			if resp.Error.Code != ErrCodeMethodNotFound {
				t.Errorf("Error.Code = %d, want %d", resp.Error.Code, ErrCodeMethodNotFound)
			}
		})
	}
}

func TestHandler_SetFederation(t *testing.T) {
	t.Parallel()

	h := &Handler{logger: testLogger()}

	if h.federation != nil {
		t.Fatal("expected nil federation initially")
	}

	// Can't fully construct a Federation without engine, but we test the setter.
	h.SetFederation(nil)
	if h.federation != nil {
		t.Fatal("expected nil after setting nil")
	}
}

// mustJSON marshals to JSON or panics.
func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
