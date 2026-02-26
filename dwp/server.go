package dwp

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/stream"
)

// Server is the DWP server that handles WebSocket, SSE, and HTTP RPC
// connections. It integrates with the Dispatch engine via the stream
// broker and handles frame-based communication with clients.
type Server struct {
	broker       *stream.Broker
	handler      *Handler
	auth         Authenticator
	defaultCodec Codec
	conns        *ConnectionManager
	logger       *slog.Logger
	basePath     string
}

// NewServer creates a new DWP server.
func NewServer(broker *stream.Broker, handler *Handler, opts ...Option) *Server {
	s := &Server{
		broker:       broker,
		handler:      handler,
		defaultCodec: &JSONCodec{},
		conns:        NewConnectionManager(),
		logger:       slog.Default(),
		basePath:     "/dwp",
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.auth == nil {
		s.auth = &NoopAuthenticator{}
	}
	return s
}

// Broker returns the underlying stream broker.
func (s *Server) Broker() *stream.Broker { return s.broker }

// Connections returns the connection manager.
func (s *Server) Connections() *ConnectionManager { return s.conns }

// RegisterRoutes mounts DWP endpoints on a Forge router.
func (s *Server) RegisterRoutes(router forge.Router) {
	// Primary: WebSocket
	if err := router.WebSocket(s.basePath, s.handleWebSocket); err != nil {
		s.logger.Error("failed to register DWP WebSocket", slog.String("error", err.Error()))
	}

	// Fallback: SSE for read-only subscriptions (uses EventStream handler)
	if err := router.EventStream(s.basePath+"/sse", s.handleSSE); err != nil {
		s.logger.Error("failed to register DWP SSE", slog.String("error", err.Error()))
	}

	// One-shot: HTTP RPC
	if err := router.POST(s.basePath+"/rpc", s.handleHTTPRPC); err != nil {
		s.logger.Error("failed to register DWP RPC", slog.String("error", err.Error()))
	}
}

// handleWebSocket is the main WebSocket connection handler.
func (s *Server) handleWebSocket(ctx forge.Context, conn forge.Connection) error {
	connID := conn.ID()
	s.logger.Info("DWP WebSocket connected", slog.String("conn_id", connID))

	// Wait for auth frame.
	authData, readErr := conn.Read()
	if readErr != nil {
		return fmt.Errorf("dwp: read auth frame: %w", readErr)
	}

	// Auth frames are always JSON (before codec negotiation).
	var authFrame Frame
	if err := json.Unmarshal(authData, &authFrame); err != nil {
		//nolint:errcheck // best-effort error response before disconnect
		conn.WriteJSON(NewErrorFrame("", ErrCodeBadRequest, "invalid auth frame"))
		return fmt.Errorf("dwp: unmarshal auth frame: %w", err)
	}

	if authFrame.Method != MethodAuth {
		//nolint:errcheck // best-effort error response before disconnect
		conn.WriteJSON(NewErrorFrame(authFrame.ID, ErrCodeBadRequest, "first frame must be auth"))
		return fmt.Errorf("dwp: expected auth frame, got %q", authFrame.Method)
	}

	// Parse auth request.
	var authReq AuthRequest
	if len(authFrame.Data) > 0 {
		if err := json.Unmarshal(authFrame.Data, &authReq); err != nil {
			//nolint:errcheck // best-effort error response before disconnect
			conn.WriteJSON(NewErrorFrame(authFrame.ID, ErrCodeBadRequest, "invalid auth data"))
			return err
		}
	}

	// Authenticate.
	token := authReq.Token
	if token == "" {
		token = authFrame.Token
	}
	identity, authErr := s.auth.Authenticate(ctx.Context(), token)
	if authErr != nil {
		//nolint:errcheck // best-effort error response before disconnect
		conn.WriteJSON(NewErrorFrame(authFrame.ID, ErrCodeUnauthorized, "authentication failed"))
		return fmt.Errorf("dwp: auth failed: %w", authErr)
	}

	// Negotiate codec.
	codec := s.defaultCodec
	if authReq.Format != "" {
		codec = GetCodec(authReq.Format)
	}

	// Create connection state.
	dwpConn := NewConnection(connID, identity, codec)
	s.conns.Add(dwpConn)
	defer func() {
		s.broker.RemoveSubscriber(connID)
		s.conns.Remove(connID)
		s.logger.Info("DWP WebSocket disconnected", slog.String("conn_id", connID))
	}()

	// Send auth response.
	resp, respErr := NewResponseFrame(authFrame.ID, AuthResponse{
		Format:    codec.Name(),
		SessionID: connID,
	})
	if respErr != nil {
		return fmt.Errorf("dwp: marshal auth response: %w", respErr)
	}
	if err := s.writeFrame(conn, codec, resp); err != nil {
		return err
	}

	s.logger.Info("DWP authenticated",
		slog.String("conn_id", connID),
		slog.String("subject", identity.Subject),
		slog.String("codec", codec.Name()),
	)

	// Create a subscriber for this connection and start a goroutine
	// to forward broker events to the WebSocket.
	sub := s.broker.Subscribe(connID)
	go s.forwardEvents(conn, codec, sub)

	// Frame processing loop.
	for {
		data, err := conn.Read()
		if err != nil {
			return nil // Connection closed.
		}

		dwpConn.Touch()

		frame, decErr := codec.Decode(data)
		if decErr != nil {
			errFrame := NewErrorFrame("", ErrCodeBadRequest, "invalid frame: "+decErr.Error())
			if writeErr := s.writeFrame(conn, codec, errFrame); writeErr != nil {
				s.logger.Warn("failed to write error frame", slog.String("error", writeErr.Error()))
			}
			continue
		}

		// Handle ping/pong.
		if frame.Type == FramePing {
			pong := &Frame{
				ID:        generateFrameID(),
				Type:      FramePong,
				CorrelID:  frame.ID,
				Timestamp: frame.Timestamp,
			}
			if writeErr := s.writeFrame(conn, codec, pong); writeErr != nil {
				s.logger.Warn("failed to write pong frame", slog.String("error", writeErr.Error()))
			}
			continue
		}

		// Check authorization for the method.
		if frame.Method != "" {
			reqScope := RequiredScope(frame.Method)
			if reqScope != "" && !identity.HasScope(reqScope) {
				errFrame := NewErrorFrame(frame.ID, ErrCodeForbidden, "insufficient permissions")
				if writeErr := s.writeFrame(conn, codec, errFrame); writeErr != nil {
					s.logger.Warn("failed to write forbidden frame", slog.String("error", writeErr.Error()))
				}
				continue
			}
		}

		// Handle credits replenishment.
		if frame.Credits > 0 {
			sub.AddCredits(int64(frame.Credits))
			continue
		}

		// Dispatch to handler.
		respFrame := s.handler.Handle(ctx.Context(), frame, dwpConn)
		if respFrame != nil {
			// Handle subscribe/unsubscribe side effects.
			if frame.Method == MethodSubscribe && respFrame.Type == FrameResponse {
				var subReq SubscribeRequest
				if json.Unmarshal(frame.Data, &subReq) == nil {
					s.broker.SubscribeTo(connID, subReq.Channel)
					dwpConn.AddSubscription(subReq.Channel)
				}
			} else if frame.Method == MethodUnsubscribe && respFrame.Type == FrameResponse {
				var unsubReq UnsubscribeRequest
				if json.Unmarshal(frame.Data, &unsubReq) == nil {
					s.broker.Unsubscribe(connID, unsubReq.Channel)
					dwpConn.RemoveSubscription(unsubReq.Channel)
				}
			}

			if writeErr := s.writeFrame(conn, codec, respFrame); writeErr != nil {
				s.logger.Warn("failed to write response frame", slog.String("error", writeErr.Error()))
			}
		}
	}
}

// forwardEvents reads from the subscriber channel and writes events
// to the WebSocket connection.
func (s *Server) forwardEvents(conn forge.Connection, codec Codec, sub *stream.Subscriber) {
	for evt := range sub.C() {
		evtFrame, err := NewEventFrame(evt.Topic, evt)
		if err != nil {
			continue
		}
		if writeErr := s.writeFrame(conn, codec, evtFrame); writeErr != nil {
			return // Connection gone.
		}
	}
}

// writeFrame encodes and writes a frame to a Forge connection.
func (s *Server) writeFrame(conn forge.Connection, codec Codec, frame *Frame) error {
	if codec.Name() == CodecNameJSON {
		return conn.WriteJSON(frame)
	}
	data, err := codec.Encode(frame)
	if err != nil {
		return err
	}
	return conn.Write(data)
}

// handleSSE serves read-only Server-Sent Events for clients that
// cannot establish WebSocket connections.
func (s *Server) handleSSE(ctx forge.Context, sseStream forge.Stream) error {
	// Get token from query parameter.
	token := ctx.Query("token")
	identity, err := s.auth.Authenticate(ctx.Context(), token)
	if err != nil {
		return fmt.Errorf("dwp: SSE auth failed: %w", err)
	}

	// Get channel from query parameter.
	channel := ctx.Query("channel")
	if channel == "" {
		return fmt.Errorf("dwp: SSE channel parameter required")
	}

	// Check subscribe permission.
	if !identity.HasScope(ScopeSubscribe) && !identity.HasScope(ScopeAll) {
		return fmt.Errorf("dwp: SSE insufficient permissions")
	}

	connID := fmt.Sprintf("sse-%s", generateFrameID())
	sub := s.broker.Subscribe(connID, channel)
	defer s.broker.RemoveSubscriber(connID)

	for {
		select {
		case evt, ok := <-sub.C():
			if !ok {
				return nil
			}
			if sendErr := sseStream.SendJSON(string(evt.Type), evt); sendErr != nil {
				return sendErr
			}
			if flushErr := sseStream.Flush(); flushErr != nil {
				return flushErr
			}
		case <-sseStream.Context().Done():
			return nil
		}
	}
}

// handleHTTPRPC handles one-shot HTTP RPC requests for simple operations.
func (s *Server) handleHTTPRPC(ctx forge.Context) error {
	// Parse the frame from the request body.
	var frame Frame
	if err := ctx.Bind(&frame); err != nil {
		return ctx.Status(400).JSON(NewErrorFrame("", ErrCodeBadRequest, "invalid request body"))
	}

	// Authenticate.
	token := frame.Token
	if token == "" {
		token = ctx.Header("Authorization")
	}
	identity, err := s.auth.Authenticate(ctx.Context(), token)
	if err != nil {
		return ctx.Status(401).JSON(NewErrorFrame(frame.ID, ErrCodeUnauthorized, "unauthorized"))
	}

	// Check authorization.
	reqScope := RequiredScope(frame.Method)
	if reqScope != "" && !identity.HasScope(reqScope) {
		return ctx.Status(403).JSON(NewErrorFrame(frame.ID, ErrCodeForbidden, "forbidden"))
	}

	// Create a temporary connection for scope.
	conn := NewConnection("rpc-"+generateFrameID(), identity, &JSONCodec{})

	// Dispatch.
	resp := s.handler.Handle(ctx.Context(), &frame, conn)
	if resp == nil {
		return ctx.NoContent(204)
	}

	status := 200
	if resp.Type == FrameErr && resp.Error != nil {
		status = resp.Error.Code
		if status < 100 || status > 599 {
			status = 500
		}
	}

	return ctx.Status(status).JSON(resp)
}
