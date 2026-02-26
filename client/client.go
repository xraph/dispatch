// Package client provides a Go client for connecting to a remote Dispatch
// instance via the Dispatch Wire Protocol (DWP) over WebSocket.
//
// Usage:
//
//	c, err := client.Dial("wss://api.example.com/dwp",
//	    client.WithToken("dk_..."),
//	)
//	defer c.Close()
//
//	// Enqueue a job.
//	job, err := c.Enqueue(ctx, "send-email", payload)
//
//	// Start a workflow and watch events.
//	run, err := c.StartWorkflow(ctx, "order-pipeline", input)
//	ch := c.Watch(ctx, run.RunID)
//	for evt := range ch {
//	    fmt.Printf("Step %s: %s\n", evt.StepName, evt.Type)
//	}
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/stream"
)

// Client is a DWP client that communicates with a remote Dispatch server.
type Client struct {
	url    string
	token  string
	format string
	logger *slog.Logger

	// Reconnection.
	reconnect  bool
	maxRetries int
	baseDelay  time.Duration

	// Connection state.
	conn      net.Conn
	mu        sync.Mutex
	closed    atomic.Bool
	sessionID string

	// Request-response correlation.
	pending sync.Map // frameID → chan *dwp.Frame

	// Subscriptions.
	subs sync.Map // channel → chan *stream.Event
}

// Dial connects to a DWP server and authenticates.
func Dial(url string, opts ...Option) (*Client, error) {
	return DialContext(context.Background(), url, opts...)
}

// DialContext connects to a DWP server with a context.
func DialContext(ctx context.Context, url string, opts ...Option) (*Client, error) {
	c := &Client{
		url:        url,
		format:     "json",
		logger:     slog.Default(),
		maxRetries: 5,
		baseDelay:  time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}

	if err := c.connect(ctx); err != nil {
		return nil, fmt.Errorf("dispatch/client: dial: %w", err)
	}

	// Start the read loop.
	go c.readLoop()

	return c, nil
}

// connect establishes the WebSocket connection and sends the auth frame.
// It reads the auth response directly since the readLoop hasn't started yet.
func (c *Client) connect(ctx context.Context) error {
	conn, _, _, err := ws.Dial(ctx, c.url)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}
	c.conn = conn

	// Send auth frame.
	authFrame := &dwp.Frame{
		ID:     dwp.GenerateFrameID(),
		Type:   dwp.FrameRequest,
		Method: dwp.MethodAuth,
		Token:  c.token,
	}
	authData, marshalErr := json.Marshal(dwp.AuthRequest{
		Token:  c.token,
		Format: c.format,
	})
	if marshalErr != nil {
		_ = conn.Close()
		return fmt.Errorf("marshal auth request: %w", marshalErr)
	}
	authFrame.Data = authData
	authFrame.Timestamp = time.Now().UTC()

	if writeErr := c.writeFrame(authFrame); writeErr != nil {
		_ = conn.Close()
		return fmt.Errorf("write auth frame: %w", writeErr)
	}

	// Read the auth response directly from the WebSocket.
	// We cannot use readLoop here because it hasn't been started yet
	// (DialContext starts it after connect returns).
	type readResult struct {
		resp *dwp.Frame
		err  error
	}
	resultCh := make(chan readResult, 1)

	go func() {
		data, readErr := wsutil.ReadServerText(conn)
		if readErr != nil {
			resultCh <- readResult{err: fmt.Errorf("read auth response: %w", readErr)}
			return
		}
		var frame dwp.Frame
		if unmarshalErr := json.Unmarshal(data, &frame); unmarshalErr != nil {
			resultCh <- readResult{err: fmt.Errorf("unmarshal auth response: %w", unmarshalErr)}
			return
		}
		resultCh <- readResult{resp: &frame}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			_ = conn.Close()
			return result.err
		}
		resp := result.resp
		if resp.Type == dwp.FrameErr {
			_ = conn.Close()
			msg := "unknown error"
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			return fmt.Errorf("auth failed: %s", msg)
		}
		// Extract session ID.
		var authResp dwp.AuthResponse
		if len(resp.Data) > 0 {
			if unmarshalErr := json.Unmarshal(resp.Data, &authResp); unmarshalErr != nil {
				c.logger.Warn("failed to unmarshal auth response", slog.String("error", unmarshalErr.Error()))
			}
		}
		c.sessionID = authResp.SessionID
		c.logger.Info("DWP client connected",
			slog.String("session_id", c.sessionID),
			slog.String("format", authResp.Format),
		)
		return nil
	case <-ctx.Done():
		_ = conn.Close()
		return ctx.Err()
	case <-time.After(10 * time.Second):
		_ = conn.Close()
		return fmt.Errorf("auth timeout")
	}
}

// readLoop reads frames from the WebSocket and dispatches them.
func (c *Client) readLoop() {
	for {
		if c.closed.Load() {
			return
		}

		data, err := wsutil.ReadServerText(c.conn)
		if err != nil {
			if c.closed.Load() {
				return
			}
			c.logger.Warn("DWP client read error", slog.String("error", err.Error()))
			if c.reconnect {
				c.tryReconnect()
			}
			return
		}

		var frame dwp.Frame
		if unmarshalErr := json.Unmarshal(data, &frame); unmarshalErr != nil {
			c.logger.Warn("DWP client: invalid frame", slog.String("error", unmarshalErr.Error()))
			continue
		}

		// Route the frame.
		switch frame.Type {
		case dwp.FrameResponse, dwp.FrameErr:
			// Correlate with pending request.
			if val, ok := c.pending.Load(frame.CorrelID); ok {
				ch := val.(chan *dwp.Frame) //nolint:errcheck // pending map always stores chan *dwp.Frame
				select {
				case ch <- &frame:
				default:
				}
			}
		case dwp.FrameEvent:
			// Route to subscription channel.
			if val, ok := c.subs.Load(frame.Channel); ok {
				ch := val.(chan *stream.Event) //nolint:errcheck // subs map always stores chan *stream.Event
				var evt stream.Event
				if json.Unmarshal(data, &evt) == nil {
					select {
					case ch <- &evt:
					default:
						// Drop if subscriber is slow.
					}
				}
			}
		case dwp.FramePong:
			// Ignore pong frames.
		}
	}
}

// tryReconnect attempts to reconnect with exponential backoff.
func (c *Client) tryReconnect() {
	delay := c.baseDelay
	for i := range c.maxRetries {
		c.logger.Info("DWP client reconnecting",
			slog.Int("attempt", i+1),
			slog.Duration("delay", delay),
		)
		time.Sleep(delay)

		if err := c.connect(context.Background()); err != nil {
			c.logger.Warn("DWP client reconnect failed", slog.String("error", err.Error()))
			delay = min(delay*2, 30*time.Second)
			continue
		}

		c.logger.Info("DWP client reconnected")
		go c.readLoop()
		return
	}
	c.logger.Error("DWP client: max reconnection attempts reached")
}

// request sends a request frame and waits for the correlated response.
func (c *Client) request(ctx context.Context, method string, data any) (*dwp.Frame, error) {
	frame := &dwp.Frame{
		ID:        dwp.GenerateFrameID(),
		Type:      dwp.FrameRequest,
		Method:    method,
		Timestamp: time.Now().UTC(),
	}

	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshal request data: %w", err)
		}
		frame.Data = raw
	}

	respCh := make(chan *dwp.Frame, 1)
	c.pending.Store(frame.ID, respCh)
	defer c.pending.Delete(frame.ID)

	if err := c.writeFrame(frame); err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		if resp.Type == dwp.FrameErr {
			msg := "unknown error"
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			return nil, fmt.Errorf("DWP error: %s", msg)
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// writeFrame JSON-encodes and sends a frame over the WebSocket.
func (c *Client) writeFrame(frame *dwp.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}
	return wsutil.WriteClientText(c.conn, data)
}

// SessionID returns the session ID assigned by the server.
func (c *Client) SessionID() string { return c.sessionID }

// Close closes the client connection.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil // already closed
	}

	// Close all subscription channels.
	c.subs.Range(func(key, val any) bool {
		ch := val.(chan *stream.Event) //nolint:errcheck // subs map always stores chan *stream.Event
		close(ch)
		c.subs.Delete(key)
		return true
	})

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
