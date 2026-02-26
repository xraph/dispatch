// Package dwp implements the Dispatch Wire Protocol (DWP) — a message-based
// protocol for client↔server and server↔server communication. DWP is
// transported over WebSocket (primary), SSE (read-only fallback), and
// HTTP (one-shot RPC).
package dwp

import (
	"encoding/json"
	"time"
)

// FrameType identifies the frame category.
type FrameType string

const (
	FrameRequest  FrameType = "request"
	FrameResponse FrameType = "response"
	FrameEvent    FrameType = "event"
	FrameErr      FrameType = "error"
	FramePing     FrameType = "ping"
	FramePong     FrameType = "pong"
)

// Frame is the DWP message envelope. Every message exchanged over
// the protocol is a Frame.
type Frame struct {
	// ID uniquely identifies this frame (ULID or UUID).
	ID string `json:"id" msgpack:"id"`

	// Type categorizes the frame.
	Type FrameType `json:"type" msgpack:"type"`

	// Method names the operation for request frames (e.g., "job.enqueue").
	Method string `json:"method,omitempty" msgpack:"method,omitempty"`

	// CorrelID links a response to its originating request.
	CorrelID string `json:"correl_id,omitempty" msgpack:"correl_id,omitempty"`

	// Token carries auth credentials (typically only on the auth frame).
	Token string `json:"token,omitempty" msgpack:"token,omitempty"`

	// AppID scopes the request to a tenant application.
	AppID string `json:"app_id,omitempty" msgpack:"app_id,omitempty"`

	// OrgID scopes the request to a tenant organization.
	OrgID string `json:"org_id,omitempty" msgpack:"org_id,omitempty"`

	// Data carries the method-specific payload.
	Data json.RawMessage `json:"data,omitempty" msgpack:"data,omitempty"`

	// Error carries error details for error frames.
	Error *ErrorDetail `json:"error,omitempty" msgpack:"error,omitempty"`

	// Channel identifies the subscription channel for event/subscribe frames.
	Channel string `json:"channel,omitempty" msgpack:"channel,omitempty"`

	// Credits replenishes flow-control credits (backpressure).
	Credits int `json:"credits,omitempty" msgpack:"credits,omitempty"`

	// Timestamp records when this frame was created.
	Timestamp time.Time `json:"ts" msgpack:"ts"`
}

// ErrorDetail describes an error in a response or error frame.
type ErrorDetail struct {
	Code    int    `json:"code" msgpack:"code"`
	Message string `json:"message" msgpack:"message"`
	Details string `json:"details,omitempty" msgpack:"details,omitempty"`
}

// ── Well-known methods ──────────────────────────────

const (
	// Auth methods.
	MethodAuth = "auth"

	// Job methods.
	MethodJobEnqueue = "job.enqueue"
	MethodJobGet     = "job.get"
	MethodJobCancel  = "job.cancel"
	MethodJobList    = "job.list"

	// Workflow methods.
	MethodWorkflowStart    = "workflow.start"
	MethodWorkflowGet      = "workflow.get"
	MethodWorkflowEvent    = "workflow.event"
	MethodWorkflowCancel   = "workflow.cancel"
	MethodWorkflowTimeline = "workflow.timeline"
	MethodWorkflowReplay   = "workflow.replay"

	// Subscription methods.
	MethodSubscribe   = "subscribe"
	MethodUnsubscribe = "unsubscribe"

	// Admin methods.
	MethodCronList  = "cron.list"
	MethodDLQList   = "dlq.list"
	MethodDLQReplay = "dlq.replay"
	MethodStats     = "stats"

	// Federation methods (server-to-server).
	MethodFederationEnqueue   = "federation.enqueue"
	MethodFederationEvent     = "federation.event"
	MethodFederationHeartbeat = "federation.heartbeat"
)

// ── Well-known error codes ──────────────────────────

const (
	ErrCodeBadRequest     = 400
	ErrCodeUnauthorized   = 401
	ErrCodeForbidden      = 403
	ErrCodeNotFound       = 404
	ErrCodeMethodNotFound = 405
	ErrCodeConflict       = 409
	ErrCodeInternal       = 500
)

// ── Request/Response payloads ───────────────────────

// AuthRequest is sent by clients to authenticate.
type AuthRequest struct {
	Token  string `json:"token"`
	Format string `json:"format,omitempty"` // "json" (default), "msgpack", "protobuf"
}

// AuthResponse is returned after successful authentication.
type AuthResponse struct {
	Format    string `json:"format"`
	SessionID string `json:"session_id"`
}

// JobEnqueueRequest submits a new job.
type JobEnqueueRequest struct {
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload"`
	Queue    string          `json:"queue,omitempty"`
	Priority int             `json:"priority,omitempty"`
}

// JobEnqueueResponse confirms job creation.
type JobEnqueueResponse struct {
	JobID string `json:"job_id"`
	Queue string `json:"queue"`
	State string `json:"state"`
}

// JobGetRequest retrieves a job by ID.
type JobGetRequest struct {
	JobID string `json:"job_id"`
}

// JobCancelRequest cancels a job.
type JobCancelRequest struct {
	JobID string `json:"job_id"`
}

// WorkflowStartRequest starts a new workflow.
type WorkflowStartRequest struct {
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// WorkflowStartResponse confirms workflow start.
type WorkflowStartResponse struct {
	RunID string `json:"run_id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// WorkflowGetRequest retrieves a workflow run.
type WorkflowGetRequest struct {
	RunID string `json:"run_id"`
}

// WorkflowEventRequest publishes an event to trigger workflow continuation.
type WorkflowEventRequest struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// WorkflowCancelRequest cancels a workflow run.
type WorkflowCancelRequest struct {
	RunID string `json:"run_id"`
}

// WorkflowTimelineRequest gets the step timeline for a run.
type WorkflowTimelineRequest struct {
	RunID string `json:"run_id"`
}

// SubscribeRequest subscribes to a topic channel.
type SubscribeRequest struct {
	Channel string `json:"channel"`
	Credits int    `json:"credits,omitempty"` // Initial credits (0 = use default)
}

// UnsubscribeRequest removes a subscription.
type UnsubscribeRequest struct {
	Channel string `json:"channel"`
}

// NewRequestFrame creates a new request frame.
func NewRequestFrame(id, method string, data any) (*Frame, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &Frame{
		ID:        id,
		Type:      FrameRequest,
		Method:    method,
		Data:      raw,
		Timestamp: time.Now().UTC(),
	}, nil
}

// NewResponseFrame creates a response to a request.
func NewResponseFrame(correlID string, data any) (*Frame, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &Frame{
		ID:        generateFrameID(),
		Type:      FrameResponse,
		CorrelID:  correlID,
		Data:      raw,
		Timestamp: time.Now().UTC(),
	}, nil
}

// NewErrorFrame creates an error response to a request.
func NewErrorFrame(correlID string, code int, message string) *Frame {
	return &Frame{
		ID:       generateFrameID(),
		Type:     FrameErr,
		CorrelID: correlID,
		Error: &ErrorDetail{
			Code:    code,
			Message: message,
		},
		Timestamp: time.Now().UTC(),
	}
}

// NewEventFrame creates an event frame for a subscription channel.
func NewEventFrame(channel string, data any) (*Frame, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &Frame{
		ID:        generateFrameID(),
		Type:      FrameEvent,
		Channel:   channel,
		Data:      raw,
		Timestamp: time.Now().UTC(),
	}, nil
}

// GenerateFrameID returns a new unique frame ID.
// Uses a simple timestamp + counter approach for performance.
func GenerateFrameID() string {
	return time.Now().UTC().Format("20060102150405.000000000")
}

// generateFrameID is an internal alias for backward compatibility.
func generateFrameID() string { return GenerateFrameID() }
