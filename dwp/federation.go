package dwp

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

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/stream"
)

// ── Peer ──────────────────────────────────────

// PeerState represents the connection state of a federated peer.
type PeerState string

const (
	PeerStateConnected    PeerState = "connected"
	PeerStateDisconnected PeerState = "disconnected"
	PeerStateConnecting   PeerState = "connecting"
)

// Peer represents a remote Dispatch instance in the federation.
type Peer struct {
	// ID uniquely identifies this peer (typically hostname:port).
	ID string

	// URL is the WebSocket endpoint for the peer's DWP server.
	URL string

	// Token is the authentication token for the peer.
	Token string

	// State tracks the connection state.
	State PeerState

	// LastSeen is the timestamp of the last heartbeat.
	LastSeen time.Time

	// Metadata carries arbitrary peer information.
	Metadata map[string]string

	// conn is the underlying WebSocket connection.
	conn net.Conn

	// pending tracks request-response correlation.
	pending sync.Map // frameID → chan *Frame

	// mu protects state updates.
	mu sync.RWMutex
}

// ── Federation ─────────────────────────────────

// Federation manages server-to-server communication between Dispatch
// instances. It maintains a mesh of authenticated WebSocket connections
// to peers and provides methods for forwarding jobs and events.
type Federation struct {
	eng    *engine.Engine
	broker *stream.Broker
	auth   Authenticator
	logger *slog.Logger

	// Peers keyed by peer ID.
	peers sync.Map // peerID → *Peer

	// Local identity for outgoing connections.
	localID    string
	localToken string

	// Heartbeat settings.
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	// Reconnection settings.
	reconnectBackoff time.Duration
	maxReconnect     time.Duration

	// Metrics.
	jobsForwarded   atomic.Int64
	eventsForwarded atomic.Int64

	// Lifecycle.
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// FederationOption configures a Federation.
type FederationOption func(*Federation)

// WithLocalID sets the local peer ID for outgoing connections.
func WithLocalID(id string) FederationOption {
	return func(f *Federation) { f.localID = id }
}

// WithLocalToken sets the auth token used when connecting to peers.
func WithLocalToken(token string) FederationOption {
	return func(f *Federation) { f.localToken = token }
}

// WithHeartbeatInterval sets how often heartbeats are sent to peers.
func WithHeartbeatInterval(d time.Duration) FederationOption {
	return func(f *Federation) { f.heartbeatInterval = d }
}

// WithHeartbeatTimeout sets how long to wait before considering a peer dead.
func WithHeartbeatTimeout(d time.Duration) FederationOption {
	return func(f *Federation) { f.heartbeatTimeout = d }
}

// WithFederationAuth sets the authenticator for incoming peer connections.
func WithFederationAuth(auth Authenticator) FederationOption {
	return func(f *Federation) { f.auth = auth }
}

// NewFederation creates a new federation manager.
func NewFederation(eng *engine.Engine, broker *stream.Broker, logger *slog.Logger, opts ...FederationOption) *Federation {
	f := &Federation{
		eng:               eng,
		broker:            broker,
		logger:            logger,
		heartbeatInterval: 15 * time.Second,
		heartbeatTimeout:  45 * time.Second,
		reconnectBackoff:  2 * time.Second,
		maxReconnect:      60 * time.Second,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Start begins heartbeat monitoring and peer health checks.
func (f *Federation) Start(ctx context.Context) {
	ctx, f.cancel = context.WithCancel(ctx)

	// Heartbeat sender.
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		f.heartbeatLoop(ctx)
	}()

	// Peer health checker.
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		f.healthCheckLoop(ctx)
	}()
}

// Stop gracefully shuts down all peer connections.
func (f *Federation) Stop() {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()

	f.peers.Range(func(_, value any) bool {
		peer := value.(*Peer) //nolint:errcheck // sync.Map always stores *Peer
		f.disconnectPeer(peer)
		return true
	})
}

// ── Peer Management ────────────────────────────

// AddPeer registers and connects to a remote peer.
func (f *Federation) AddPeer(ctx context.Context, peerID, url, token string, metadata map[string]string) error {
	peer := &Peer{
		ID:       peerID,
		URL:      url,
		Token:    token,
		State:    PeerStateDisconnected,
		Metadata: metadata,
	}
	f.peers.Store(peerID, peer)

	return f.connectPeer(ctx, peer)
}

// RemovePeer disconnects and removes a peer.
func (f *Federation) RemovePeer(peerID string) {
	val, ok := f.peers.LoadAndDelete(peerID)
	if !ok {
		return
	}
	f.disconnectPeer(val.(*Peer)) //nolint:errcheck // sync.Map always stores *Peer
}

// GetPeer returns a peer by ID.
func (f *Federation) GetPeer(peerID string) (*Peer, bool) {
	val, ok := f.peers.Load(peerID)
	if !ok {
		return nil, false
	}
	return val.(*Peer), true //nolint:errcheck // sync.Map always stores *Peer
}

// Peers returns a snapshot of all peers.
func (f *Federation) Peers() []*Peer {
	var peers []*Peer
	f.peers.Range(func(_, value any) bool {
		peers = append(peers, value.(*Peer)) //nolint:errcheck // sync.Map always stores *Peer
		return true
	})
	return peers
}

// Stats returns federation metrics.
func (f *Federation) Stats() FederationStats {
	connected := 0
	total := 0
	f.peers.Range(func(_, value any) bool {
		total++
		peer := value.(*Peer) //nolint:errcheck // sync.Map always stores *Peer
		peer.mu.RLock()
		if peer.State == PeerStateConnected {
			connected++
		}
		peer.mu.RUnlock()
		return true
	})
	return FederationStats{
		TotalPeers:      total,
		ConnectedPeers:  connected,
		JobsForwarded:   f.jobsForwarded.Load(),
		EventsForwarded: f.eventsForwarded.Load(),
	}
}

// FederationStats contains federation metrics.
type FederationStats struct {
	TotalPeers      int   `json:"total_peers"`
	ConnectedPeers  int   `json:"connected_peers"`
	JobsForwarded   int64 `json:"jobs_forwarded"`
	EventsForwarded int64 `json:"events_forwarded"`
}

// ── Federation Operations ──────────────────────

// ForwardJob sends a job enqueue request to a specific peer.
func (f *Federation) ForwardJob(ctx context.Context, peerID, name string, payload json.RawMessage, opts ...job.Option) error {
	val, ok := f.peers.Load(peerID)
	if !ok {
		return fmt.Errorf("dwp/federation: peer %q not found", peerID)
	}
	peer := val.(*Peer) //nolint:errcheck // sync.Map always stores *Peer

	peer.mu.RLock()
	state := peer.State
	peer.mu.RUnlock()

	if state != PeerStateConnected {
		return fmt.Errorf("dwp/federation: peer %q not connected", peerID)
	}

	// Build the request.
	jobOpts := job.DefaultOptions()
	for _, opt := range opts {
		opt(&jobOpts)
	}

	data := FederationEnqueueRequest{
		Name:     name,
		Payload:  payload,
		Queue:    jobOpts.Queue,
		Priority: jobOpts.Priority,
		SourceID: f.localID,
	}

	resp, err := f.peerRequest(ctx, peer, MethodFederationEnqueue, data)
	if err != nil {
		return fmt.Errorf("dwp/federation: forward job to %q: %w", peerID, err)
	}

	if resp.Type == FrameErr {
		msg := "unknown error"
		if resp.Error != nil {
			msg = resp.Error.Message
		}
		return fmt.Errorf("dwp/federation: peer %q rejected job: %s", peerID, msg)
	}

	f.jobsForwarded.Add(1)
	f.logger.Info("job forwarded to peer",
		slog.String("peer", peerID),
		slog.String("job_name", name),
	)
	return nil
}

// ForwardEvent sends an event to a specific peer.
func (f *Federation) ForwardEvent(ctx context.Context, peerID, name string, payload json.RawMessage) error {
	val, ok := f.peers.Load(peerID)
	if !ok {
		return fmt.Errorf("dwp/federation: peer %q not found", peerID)
	}
	peer := val.(*Peer) //nolint:errcheck // sync.Map always stores *Peer

	peer.mu.RLock()
	state := peer.State
	peer.mu.RUnlock()

	if state != PeerStateConnected {
		return fmt.Errorf("dwp/federation: peer %q not connected", peerID)
	}

	data := FederationEventRequest{
		Name:     name,
		Payload:  payload,
		SourceID: f.localID,
	}

	resp, err := f.peerRequest(ctx, peer, MethodFederationEvent, data)
	if err != nil {
		return fmt.Errorf("dwp/federation: forward event to %q: %w", peerID, err)
	}

	if resp.Type == FrameErr {
		msg := "unknown error"
		if resp.Error != nil {
			msg = resp.Error.Message
		}
		return fmt.Errorf("dwp/federation: peer %q rejected event: %s", peerID, msg)
	}

	f.eventsForwarded.Add(1)
	return nil
}

// BroadcastJob sends a job to all connected peers.
func (f *Federation) BroadcastJob(ctx context.Context, name string, payload json.RawMessage, opts ...job.Option) []error {
	var errs []error
	f.peers.Range(func(key, _ any) bool {
		peerID := key.(string) //nolint:errcheck // sync.Map key is always string
		if err := f.ForwardJob(ctx, peerID, name, payload, opts...); err != nil {
			errs = append(errs, err)
		}
		return true
	})
	return errs
}

// BroadcastEvent sends an event to all connected peers.
func (f *Federation) BroadcastEvent(ctx context.Context, name string, payload json.RawMessage) []error {
	var errs []error
	f.peers.Range(func(key, _ any) bool {
		peerID := key.(string) //nolint:errcheck // sync.Map key is always string
		if err := f.ForwardEvent(ctx, peerID, name, payload); err != nil {
			errs = append(errs, err)
		}
		return true
	})
	return errs
}

// ── Connection Management ──────────────────────

func (f *Federation) connectPeer(ctx context.Context, peer *Peer) error {
	peer.mu.Lock()
	peer.State = PeerStateConnecting
	peer.mu.Unlock()

	conn, _, _, err := ws.Dial(ctx, peer.URL)
	if err != nil {
		peer.mu.Lock()
		peer.State = PeerStateDisconnected
		peer.mu.Unlock()
		return fmt.Errorf("dwp/federation: dial %q: %w", peer.ID, err)
	}

	// Authenticate with the peer.
	authFrame := &Frame{
		ID:     GenerateFrameID(),
		Type:   FrameRequest,
		Method: MethodAuth,
		Data:   mustMarshalJSON(AuthRequest{Token: peer.Token, Format: "json"}),
	}

	if writeErr := writeJSONFrame(conn, authFrame); writeErr != nil {
		conn.Close()
		peer.mu.Lock()
		peer.State = PeerStateDisconnected
		peer.mu.Unlock()
		return fmt.Errorf("dwp/federation: auth write %q: %w", peer.ID, writeErr)
	}

	// Read auth response.
	data, err := wsutil.ReadServerText(conn)
	if err != nil {
		conn.Close()
		peer.mu.Lock()
		peer.State = PeerStateDisconnected
		peer.mu.Unlock()
		return fmt.Errorf("dwp/federation: auth read %q: %w", peer.ID, err)
	}

	var resp Frame
	if err := json.Unmarshal(data, &resp); err != nil {
		conn.Close()
		peer.mu.Lock()
		peer.State = PeerStateDisconnected
		peer.mu.Unlock()
		return fmt.Errorf("dwp/federation: auth parse %q: %w", peer.ID, err)
	}

	if resp.Type == FrameErr {
		conn.Close()
		peer.mu.Lock()
		peer.State = PeerStateDisconnected
		peer.mu.Unlock()
		msg := "auth failed"
		if resp.Error != nil {
			msg = resp.Error.Message
		}
		return fmt.Errorf("dwp/federation: %s at %q", msg, peer.ID)
	}

	peer.mu.Lock()
	peer.conn = conn
	peer.State = PeerStateConnected
	peer.LastSeen = time.Now().UTC()
	peer.mu.Unlock()

	// Start read loop for this peer.
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		f.peerReadLoop(peer)
	}()

	f.logger.Info("federation peer connected",
		slog.String("peer_id", peer.ID),
		slog.String("url", peer.URL),
	)
	return nil
}

func (f *Federation) disconnectPeer(peer *Peer) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	if peer.conn != nil {
		peer.conn.Close()
		peer.conn = nil
	}
	peer.State = PeerStateDisconnected
}

func (f *Federation) peerReadLoop(peer *Peer) {
	for {
		peer.mu.RLock()
		conn := peer.conn
		state := peer.State
		peer.mu.RUnlock()

		if conn == nil || state != PeerStateConnected {
			return
		}

		data, err := wsutil.ReadServerText(conn)
		if err != nil {
			f.logger.Warn("federation peer read error",
				slog.String("peer_id", peer.ID),
				slog.String("error", err.Error()),
			)
			peer.mu.Lock()
			peer.State = PeerStateDisconnected
			peer.mu.Unlock()

			// Attempt reconnect.
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				f.reconnectPeer(peer)
			}()
			return
		}

		var frame Frame
		if err := json.Unmarshal(data, &frame); err != nil {
			continue
		}

		peer.mu.Lock()
		peer.LastSeen = time.Now().UTC()
		peer.mu.Unlock()

		// Route the frame.
		switch frame.Type {
		case FrameResponse, FrameErr:
			// Resolve pending request.
			if val, ok := peer.pending.LoadAndDelete(frame.CorrelID); ok {
				ch := val.(chan *Frame) //nolint:errcheck // pending map always stores chan *Frame
				ch <- &frame
			}
		case FramePong:
			// Heartbeat response — already updated LastSeen.
		}
	}
}

func (f *Federation) reconnectPeer(peer *Peer) {
	backoff := f.reconnectBackoff
	for {
		peer.mu.RLock()
		state := peer.State
		peer.mu.RUnlock()

		// Don't reconnect if peer was removed.
		if _, exists := f.peers.Load(peer.ID); !exists {
			return
		}

		if state == PeerStateConnected {
			return
		}

		f.logger.Info("federation reconnecting to peer",
			slog.String("peer_id", peer.ID),
			slog.Duration("backoff", backoff),
		)

		time.Sleep(backoff)

		if err := f.connectPeer(context.Background(), peer); err != nil {
			backoff = min(backoff*2, f.maxReconnect)
			continue
		}
		return
	}
}

// ── Request/Response ───────────────────────────

func (f *Federation) peerRequest(ctx context.Context, peer *Peer, method string, data any) (*Frame, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	frameID := GenerateFrameID()
	frame := &Frame{
		ID:        frameID,
		Type:      FrameRequest,
		Method:    method,
		Data:      raw,
		Timestamp: time.Now().UTC(),
	}

	ch := make(chan *Frame, 1)
	peer.pending.Store(frameID, ch)
	defer peer.pending.Delete(frameID)

	peer.mu.RLock()
	conn := peer.conn
	peer.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("peer not connected")
	}

	if err := writeJSONFrame(conn, frame); err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("request timed out")
	}
}

// ── Heartbeat ──────────────────────────────────

func (f *Federation) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(f.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.sendHeartbeats()
		}
	}
}

func (f *Federation) sendHeartbeats() {
	f.peers.Range(func(_, value any) bool {
		peer := value.(*Peer) //nolint:errcheck // sync.Map always stores *Peer
		peer.mu.RLock()
		conn := peer.conn
		state := peer.State
		peer.mu.RUnlock()

		if conn == nil || state != PeerStateConnected {
			return true
		}

		heartbeat := &Frame{
			ID:        GenerateFrameID(),
			Type:      FramePing,
			Method:    MethodFederationHeartbeat,
			Timestamp: time.Now().UTC(),
			Data:      mustMarshalJSON(FederationHeartbeatRequest{PeerID: f.localID}),
		}

		if err := writeJSONFrame(conn, heartbeat); err != nil {
			f.logger.Warn("federation heartbeat failed",
				slog.String("peer_id", peer.ID),
				slog.String("error", err.Error()),
			)
		}
		return true
	})
}

func (f *Federation) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(f.heartbeatTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.checkPeerHealth()
		}
	}
}

func (f *Federation) checkPeerHealth() {
	now := time.Now().UTC()
	f.peers.Range(func(_, value any) bool {
		peer := value.(*Peer) //nolint:errcheck // sync.Map always stores *Peer
		peer.mu.RLock()
		state := peer.State
		lastSeen := peer.LastSeen
		peer.mu.RUnlock()

		if state == PeerStateConnected && now.Sub(lastSeen) > f.heartbeatTimeout {
			f.logger.Warn("federation peer timed out",
				slog.String("peer_id", peer.ID),
				slog.Duration("since_last_seen", now.Sub(lastSeen)),
			)
			f.disconnectPeer(peer)

			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				f.reconnectPeer(peer)
			}()
		}
		return true
	})
}

// ── Incoming Federation Handlers ───────────────

// HandleFederationEnqueue processes an incoming federation.enqueue from a peer.
// This is called by the DWP handler for federation methods.
func (f *Federation) HandleFederationEnqueue(ctx context.Context, frame *Frame) *Frame {
	var req FederationEnqueueRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid federation enqueue: "+err.Error())
	}

	opts := make([]job.Option, 0, 2)
	if req.Queue != "" {
		opts = append(opts, job.WithQueue(req.Queue))
	}
	if req.Priority > 0 {
		opts = append(opts, job.WithPriority(req.Priority))
	}

	j, err := f.eng.EnqueueRaw(ctx, req.Name, req.Payload, opts...)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "federation enqueue failed: "+err.Error())
	}

	resp, respErr := NewResponseFrame(frame.ID, JobEnqueueResponse{
		JobID: j.ID.String(),
		Queue: j.Queue,
		State: string(j.State),
	})
	if respErr != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "marshal response: "+respErr.Error())
	}
	return resp
}

// HandleFederationEvent processes an incoming federation.event from a peer.
func (f *Federation) HandleFederationEvent(ctx context.Context, frame *Frame, conn *Connection) *Frame {
	var req FederationEventRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid federation event: "+err.Error())
	}

	appID := ""
	orgID := ""
	if conn.Identity != nil {
		appID = conn.Identity.AppID
		orgID = conn.Identity.OrgID
	}

	evt, err := f.eng.EventBus().Publish(ctx, req.Name, req.Payload, appID, orgID)
	if err != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "federation event failed: "+err.Error())
	}

	resp, respErr := NewResponseFrame(frame.ID, map[string]string{
		"event_id": evt.ID.String(),
		"name":     evt.Name,
		"source":   req.SourceID,
	})
	if respErr != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "marshal response: "+respErr.Error())
	}
	return resp
}

// HandleFederationHeartbeat processes an incoming federation.heartbeat.
func (f *Federation) HandleFederationHeartbeat(frame *Frame) *Frame {
	var req FederationHeartbeatRequest
	if err := json.Unmarshal(frame.Data, &req); err != nil {
		return NewErrorFrame(frame.ID, ErrCodeBadRequest, "invalid heartbeat: "+err.Error())
	}

	// Update the peer's LastSeen if we know about it.
	if val, ok := f.peers.Load(req.PeerID); ok {
		peer := val.(*Peer) //nolint:errcheck // sync.Map always stores *Peer
		peer.mu.Lock()
		peer.LastSeen = time.Now().UTC()
		peer.mu.Unlock()
	}

	resp, respErr := NewResponseFrame(frame.ID, map[string]string{
		"status":  "ok",
		"peer_id": f.localID,
	})
	if respErr != nil {
		return NewErrorFrame(frame.ID, ErrCodeInternal, "marshal response: "+respErr.Error())
	}
	return resp
}

// ── Federation Request/Response Types ──────────

// FederationEnqueueRequest is sent by a peer to enqueue a job remotely.
type FederationEnqueueRequest struct {
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload"`
	Queue    string          `json:"queue,omitempty"`
	Priority int             `json:"priority,omitempty"`
	SourceID string          `json:"source_id"` // ID of the originating peer
}

// FederationEventRequest is sent by a peer to publish an event remotely.
type FederationEventRequest struct {
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	SourceID string          `json:"source_id"`
}

// FederationHeartbeatRequest is a periodic liveness signal.
type FederationHeartbeatRequest struct {
	PeerID string `json:"peer_id"`
}

// ── Helpers ────────────────────────────────────

func writeJSONFrame(conn net.Conn, frame *Frame) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return wsutil.WriteClientText(conn, data)
}

func mustMarshalJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic("dwp/federation: marshal: " + err.Error())
	}
	return data
}
