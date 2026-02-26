package dwp

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewRequestFrame(t *testing.T) {
	t.Parallel()

	data := map[string]string{"name": "test-job"}
	frame, err := NewRequestFrame("frame-1", MethodJobEnqueue, data)
	if err != nil {
		t.Fatalf("NewRequestFrame: %v", err)
	}

	if frame.ID != "frame-1" {
		t.Errorf("ID = %q, want %q", frame.ID, "frame-1")
	}
	if frame.Type != FrameRequest {
		t.Errorf("Type = %q, want %q", frame.Type, FrameRequest)
	}
	if frame.Method != MethodJobEnqueue {
		t.Errorf("Method = %q, want %q", frame.Method, MethodJobEnqueue)
	}
	if frame.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}

	var payload map[string]string
	if err := json.Unmarshal(frame.Data, &payload); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if payload["name"] != "test-job" {
		t.Errorf("payload name = %q, want %q", payload["name"], "test-job")
	}
}

func TestNewResponseFrame(t *testing.T) {
	t.Parallel()

	frame, err := NewResponseFrame("correl-1", map[string]string{"status": "ok"})
	if err != nil {
		t.Fatalf("NewResponseFrame: %v", err)
	}

	if frame.Type != FrameResponse {
		t.Errorf("Type = %q, want %q", frame.Type, FrameResponse)
	}
	if frame.CorrelID != "correl-1" {
		t.Errorf("CorrelID = %q, want %q", frame.CorrelID, "correl-1")
	}
	if frame.ID == "" {
		t.Error("ID should be auto-generated")
	}
}

func TestNewErrorFrame(t *testing.T) {
	t.Parallel()

	frame := NewErrorFrame("correl-2", ErrCodeNotFound, "not found")
	if frame.Type != FrameErr {
		t.Errorf("Type = %q, want %q", frame.Type, FrameErr)
	}
	if frame.CorrelID != "correl-2" {
		t.Errorf("CorrelID = %q, want %q", frame.CorrelID, "correl-2")
	}
	if frame.Error == nil {
		t.Fatal("Error should not be nil")
	}
	if frame.Error.Code != ErrCodeNotFound {
		t.Errorf("Error.Code = %d, want %d", frame.Error.Code, ErrCodeNotFound)
	}
	if frame.Error.Message != "not found" {
		t.Errorf("Error.Message = %q, want %q", frame.Error.Message, "not found")
	}
}

func TestNewEventFrame(t *testing.T) {
	t.Parallel()

	frame, err := NewEventFrame("workflow:run-1", map[string]string{"step": "validate"})
	if err != nil {
		t.Fatalf("NewEventFrame: %v", err)
	}

	if frame.Type != FrameEvent {
		t.Errorf("Type = %q, want %q", frame.Type, FrameEvent)
	}
	if frame.Channel != "workflow:run-1" {
		t.Errorf("Channel = %q, want %q", frame.Channel, "workflow:run-1")
	}
}

func TestGenerateFrameID(t *testing.T) {
	t.Parallel()

	id1 := GenerateFrameID()
	if id1 == "" {
		t.Error("GenerateFrameID returned empty string")
	}

	// Should produce unique IDs.
	time.Sleep(time.Millisecond)
	id2 := GenerateFrameID()
	if id1 == id2 {
		t.Error("two calls to GenerateFrameID should produce different IDs")
	}
}

func TestCodecJSONRoundtrip(t *testing.T) {
	t.Parallel()

	codec := &JSONCodec{}
	if codec.Name() != CodecNameJSON {
		t.Errorf("Name = %q, want %q", codec.Name(), CodecNameJSON)
	}

	original := &Frame{
		ID:        "test-1",
		Type:      FrameRequest,
		Method:    MethodJobEnqueue,
		Token:     "secret",
		AppID:     "app-1",
		OrgID:     "org-1",
		Data:      json.RawMessage(`{"name":"test"}`),
		Channel:   "",
		Credits:   10,
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
	}

	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := codec.Decode(data)
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
	if decoded.Token != original.Token {
		t.Errorf("Token = %q, want %q", decoded.Token, original.Token)
	}
	if decoded.Credits != original.Credits {
		t.Errorf("Credits = %d, want %d", decoded.Credits, original.Credits)
	}
}

func TestCodecMsgpackRoundtrip(t *testing.T) {
	t.Parallel()

	codec := &MsgpackCodec{}
	if codec.Name() != CodecNameMsgpack {
		t.Errorf("Name = %q, want %q", codec.Name(), CodecNameMsgpack)
	}

	original := &Frame{
		ID:        "test-2",
		Type:      FrameResponse,
		CorrelID:  "correl-1",
		Data:      json.RawMessage(`{"result":"ok"}`),
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
	}

	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.CorrelID != original.CorrelID {
		t.Errorf("CorrelID = %q, want %q", decoded.CorrelID, original.CorrelID)
	}
}

func TestCodecErrorFrame(t *testing.T) {
	t.Parallel()

	codecs := []Codec{&JSONCodec{}, &MsgpackCodec{}}

	for _, codec := range codecs {
		t.Run(codec.Name(), func(t *testing.T) {
			original := &Frame{
				ID:       "err-1",
				Type:     FrameErr,
				CorrelID: "req-1",
				Error: &ErrorDetail{
					Code:    500,
					Message: "internal error",
					Details: "stack trace here",
				},
				Timestamp: time.Now().UTC().Truncate(time.Millisecond),
			}

			data, err := codec.Encode(original)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			decoded, err := codec.Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if decoded.Error == nil {
				t.Fatal("Error should not be nil")
			}
			if decoded.Error.Code != 500 {
				t.Errorf("Error.Code = %d, want %d", decoded.Error.Code, 500)
			}
			if decoded.Error.Message != "internal error" {
				t.Errorf("Error.Message = %q, want %q", decoded.Error.Message, "internal error")
			}
			if decoded.Error.Details != "stack trace here" {
				t.Errorf("Error.Details = %q, want %q", decoded.Error.Details, "stack trace here")
			}
		})
	}
}

func TestGetCodec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expected string
	}{
		{"json", CodecNameJSON},
		{"msgpack", CodecNameMsgpack},
		{"", CodecNameJSON},
		{"unknown", CodecNameJSON},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := GetCodec(tt.name)
			if codec.Name() != tt.expected {
				t.Errorf("GetCodec(%q).Name() = %q, want %q", tt.name, codec.Name(), tt.expected)
			}
		})
	}
}

func TestFramePayloadTypes(t *testing.T) {
	t.Parallel()

	t.Run("AuthRequest", func(t *testing.T) {
		req := AuthRequest{Token: "test-token", Format: "json"}
		data, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		var decoded AuthRequest
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Token != req.Token {
			t.Errorf("Token = %q, want %q", decoded.Token, req.Token)
		}
	})

	t.Run("JobEnqueueRequest", func(t *testing.T) {
		req := JobEnqueueRequest{
			Name:     "test-job",
			Payload:  json.RawMessage(`{"key":"value"}`),
			Queue:    "critical",
			Priority: 5,
		}
		data, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		var decoded JobEnqueueRequest
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Name != req.Name {
			t.Errorf("Name = %q, want %q", decoded.Name, req.Name)
		}
		if decoded.Queue != req.Queue {
			t.Errorf("Queue = %q, want %q", decoded.Queue, req.Queue)
		}
		if decoded.Priority != req.Priority {
			t.Errorf("Priority = %d, want %d", decoded.Priority, req.Priority)
		}
	})

	t.Run("WorkflowStartRequest", func(t *testing.T) {
		req := WorkflowStartRequest{
			Name:  "order-pipeline",
			Input: json.RawMessage(`{"order_id":"123"}`),
		}
		data, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		var decoded WorkflowStartRequest
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Name != req.Name {
			t.Errorf("Name = %q, want %q", decoded.Name, req.Name)
		}
	})
}
