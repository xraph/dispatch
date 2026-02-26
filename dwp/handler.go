package dwp

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/scope"
	"github.com/xraph/dispatch/stream"
	"github.com/xraph/dispatch/workflow"
)

// Handler dispatches DWP frames to engine operations.
type Handler struct {
	eng        *engine.Engine
	broker     *stream.Broker
	federation *Federation
	logger     *slog.Logger
}

// NewHandler creates a new DWP method handler.
func NewHandler(eng *engine.Engine, broker *stream.Broker, logger *slog.Logger) *Handler {
	return &Handler{eng: eng, broker: broker, logger: logger}
}

// SetFederation attaches a federation manager for handling server-to-server methods.
func (h *Handler) SetFederation(f *Federation) {
	h.federation = f
}

// Handle processes a single DWP request frame and returns a response.
func (h *Handler) Handle(ctx context.Context, frame *Frame, conn *Connection) *Frame {
	// Inject scope from connection identity.
	if conn.Identity != nil {
		ctx = scope.Restore(ctx, conn.Identity.AppID, conn.Identity.OrgID)
	}

	switch frame.Method {
	case MethodJobEnqueue:
		return h.handleJobEnqueue(ctx, frame)
	case MethodJobGet:
		return h.handleJobGet(ctx, frame)
	case MethodJobCancel:
		return h.handleJobCancel(ctx, frame)
	case MethodWorkflowStart:
		return h.handleWorkflowStart(ctx, frame)
	case MethodWorkflowGet:
		return h.handleWorkflowGet(ctx, frame)
	case MethodWorkflowEvent:
		return h.handleWorkflowEvent(ctx, frame, conn)
	case MethodSubscribe:
		return h.handleSubscribe(frame)
	case MethodUnsubscribe:
		return h.handleUnsubscribe(frame)
	case MethodWorkflowTimeline:
		return h.handleWorkflowTimeline(ctx, frame)
	case MethodStats:
		return h.handleStats(frame)
	case MethodFederationEnqueue:
		return h.handleFederationEnqueue(ctx, frame)
	case MethodFederationEvent:
		return h.handleFederationEvent(ctx, frame, conn)
	case MethodFederationHeartbeat:
		return h.handleFederationHeartbeat(frame)
	default:
		return NewErrorFrame(frame.ID, ErrCodeMethodNotFound, "unknown method: "+frame.Method)
	}
}

// mustResponseFrame creates a response frame, returning an error frame on marshal failure.
func mustResponseFrame(frameID string, data any) *Frame {
	resp, err := NewResponseFrame(frameID, data)
	if err != nil {
		return NewErrorFrame(frameID, ErrCodeInternal, "marshal response: "+err.Error())
	}
	return resp
}

func (h *Handler) handleJobEnqueue(ctx context.Context, frame *Frame) *Frame {
	var req JobEnqueueRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	opts := make([]job.Option, 0, 2)
	if req.Queue != "" {
		opts = append(opts, job.WithQueue(req.Queue))
	}
	if req.Priority > 0 {
		opts = append(opts, job.WithPriority(req.Priority))
	}

	j, err := h.eng.EnqueueRaw(ctx, req.Name, req.Payload, opts...)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "enqueue failed: "+err.Error())
	}

	return mustResponseFrame(frame.ID, JobEnqueueResponse{
		JobID: j.ID.String(),
		Queue: j.Queue,
		State: string(j.State),
	})
}

func (h *Handler) handleJobGet(ctx context.Context, frame *Frame) *Frame {
	var req JobGetRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	jobID, err := id.ParseJobID(req.JobID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid job ID: "+err.Error())
	}

	type jobGetter interface {
		GetJob(context.Context, id.JobID) (*job.Job, error)
	}
	store, ok := h.eng.Dispatcher().Store().(jobGetter)
	if !ok {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "store does not support GetJob")
	}

	j, err := store.GetJob(ctx, jobID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeNotFound, "job not found: "+err.Error())
	}

	return mustResponseFrame(frame.ID, j)
}

func (h *Handler) handleJobCancel(ctx context.Context, frame *Frame) *Frame {
	var req JobCancelRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	jobID, err := id.ParseJobID(req.JobID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid job ID: "+err.Error())
	}

	type jobStore interface {
		GetJob(context.Context, id.JobID) (*job.Job, error)
		UpdateJob(context.Context, *job.Job) error
	}
	store, ok := h.eng.Dispatcher().Store().(jobStore)
	if !ok {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "store does not support job operations")
	}

	j, getErr := store.GetJob(ctx, jobID)
	if getErr != nil {
		return NewErrorFrame(frame.ID, ErrCodeNotFound, "job not found")
	}

	j.State = job.StateCancelled
	if updateErr := store.UpdateJob(ctx, j); updateErr != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "cancel failed: "+updateErr.Error())
	}

	return mustResponseFrame(frame.ID, map[string]string{"status": "cancelled"})
}

func (h *Handler) handleWorkflowStart(ctx context.Context, frame *Frame) *Frame {
	var req WorkflowStartRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	run, err := h.eng.WorkflowRunner().StartRaw(ctx, req.Name, req.Input)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "workflow start failed: "+err.Error())
	}

	return mustResponseFrame(frame.ID, WorkflowStartResponse{
		RunID: run.ID.String(),
		Name:  run.Name,
		State: string(run.State),
	})
}

func (h *Handler) handleWorkflowGet(ctx context.Context, frame *Frame) *Frame {
	var req WorkflowGetRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	runID, err := id.ParseRunID(req.RunID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid run ID: "+err.Error())
	}

	wfStore, ok := h.eng.Dispatcher().Store().(workflow.Store)
	if !ok {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "store does not support workflow operations")
	}

	run, getErr := wfStore.GetRun(ctx, runID)
	if getErr != nil {
		return NewErrorFrame(frame.ID, ErrCodeNotFound, "run not found: "+getErr.Error())
	}

	return mustResponseFrame(frame.ID, run)
}

func (h *Handler) handleWorkflowEvent(ctx context.Context, frame *Frame, conn *Connection) *Frame {
	var req WorkflowEventRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	appID := ""
	orgID := ""
	if conn.Identity != nil {
		appID = conn.Identity.AppID
		orgID = conn.Identity.OrgID
	}

	evt, err := h.eng.EventBus().Publish(ctx, req.Name, req.Payload, appID, orgID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "publish event failed: "+err.Error())
	}

	return mustResponseFrame(frame.ID, map[string]string{
		"event_id": evt.ID.String(),
		"name":     evt.Name,
	})
}

func (h *Handler) handleSubscribe(frame *Frame) *Frame {
	var req SubscribeRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	if err := stream.ValidateTopic(req.Channel); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, err.Error())
	}

	// Actual subscription is done in the server loop after response is sent.
	return mustResponseFrame(frame.ID, map[string]string{
		"channel": req.Channel,
		"status":  "subscribed",
	})
}

func (h *Handler) handleUnsubscribe(frame *Frame) *Frame {
	var req UnsubscribeRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	// Actual unsubscription is done in the server loop after response is sent.
	return mustResponseFrame(frame.ID, map[string]string{
		"channel": req.Channel,
		"status":  "unsubscribed",
	})
}

func (h *Handler) handleWorkflowTimeline(ctx context.Context, frame *Frame) *Frame {
	var req WorkflowTimelineRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid request: "+err.Error())
	}

	runID, err := id.ParseRunID(req.RunID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid run ID: "+err.Error())
	}

	timeline, err := h.eng.WorkflowRunner().GetTimeline(ctx, runID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "timeline failed: "+err.Error())
	}

	return mustResponseFrame(frame.ID, timeline)
}

func (h *Handler) handleStats(frame *Frame) *Frame {
	brokerStats := h.broker.Stats()
	connCount := 0 // Will be filled by the server.

	stats := map[string]any{
		"broker":      brokerStats,
		"connections": connCount,
	}

	if h.federation != nil {
		stats["federation"] = h.federation.Stats()
	}

	return mustResponseFrame(frame.ID, stats)
}

func (h *Handler) handleFederationEnqueue(ctx context.Context, frame *Frame) *Frame {
	if h.federation == nil {
		return NewErrorFrame(frame.ID, ErrCodeMethodNotFound, "federation not enabled")
	}
	return h.federation.HandleFederationEnqueue(ctx, frame)
}

func (h *Handler) handleFederationEvent(ctx context.Context, frame *Frame, conn *Connection) *Frame {
	if h.federation == nil {
		return NewErrorFrame(frame.ID, ErrCodeMethodNotFound, "federation not enabled")
	}
	return h.federation.HandleFederationEvent(ctx, frame, conn)
}

func (h *Handler) handleFederationHeartbeat(frame *Frame) *Frame {
	if h.federation == nil {
		return NewErrorFrame(frame.ID, ErrCodeMethodNotFound, "federation not enabled")
	}
	return h.federation.HandleFederationHeartbeat(frame)
}
