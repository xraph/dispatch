package dwp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	log "github.com/xraph/go-utils/log"

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

	// State tracks the connection state. On a *Peer returned by
	// Federation.GetPeer or Federation.Peers it is a point-in-time copy.
	State PeerState

	// LastSeen is the timestamp of the last heartbeat. On a *Peer returned
	// by Federation.GetPeer or Federation.Peers it is a point-in-time copy.
	LastSeen time.Time

	// Metadata carries arbitrary peer information.
	Metadata map[string]string

	// conn is the underlying WebSocket connection.
	conn net.Conn

	// reconnecting is set while a reconnect goroutine owns this peer, so a
	// read error and a health-check timeout cannot both start one.
	reconnecting bool

	// pending tracks request-response correlation.
	pending sync.Map // frameID → chan *Frame

	// mu protects State, LastSeen, conn and reconnecting.
	mu sync.RWMutex

	// wmu serialises frame writes on conn. A WebSocket frame is written as
	// a header and a payload, so concurrent writers would interleave them.
	wmu sync.Mutex
}

// snapshot returns a copy of the peer's exported fields, taken under its
// lock, that callers can read without synchronisation.
func (p *Peer) snapshot() *Peer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return &Peer{
		ID:       p.ID,
		URL:      p.URL,
		Token:    p.Token,
		State:    p.State,
		LastSeen: p.LastSeen,
		Metadata: maps.Clone(p.Metadata),
	}
}

// writeFrame writes one frame to conn, serialised with every other writer
// on this peer.
func (p *Peer) writeFrame(conn net.Conn, frame *Frame) error {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	return writeJSONFrame(conn, frame)
}

// ErrFederationStopped is returned by Start and AddPeer once Stop has been
// called. A stopped Federation cannot be restarted; create a new one.
var ErrFederationStopped = errors.New("dwp/federation: federation stopped")

// ── Federation ─────────────────────────────────

// Federation manages server-to-server communication between Dispatch
// instances. It maintains a mesh of authenticated WebSocket connections
// to peers and provides methods for forwarding jobs and events.
type Federation struct {
	eng    *engine.Engine
	broker *stream.Broker
	auth   Authenticator
	logger log.Logger

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

	// handshakeTimeout bounds the dial-and-auth exchange with a peer.
	handshakeTimeout time.Duration

	// Lifecycle. mu guards started, stopped, stopLoops and every wg.Add,
	// so no goroutine is added once Stop has begun waiting. ctx is the
	// federation's own lifetime: it scopes peer read loops and reconnects
	// and is cancelled by Stop.
	mu        sync.Mutex
	started   bool
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
	stopLoops context.CancelFunc
	wg        sync.WaitGroup
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
func NewFederation(eng *engine.Engine, broker *stream.Broker, logger log.Logger, opts ...FederationOption) *Federation {
	f := &Federation{
		eng:               eng,
		broker:            broker,
		logger:            logger,
		heartbeatInterval: 15 * time.Second,
		heartbeatTimeout:  45 * time.Second,
		reconnectBackoff:  2 * time.Second,
		maxReconnect:      60 * time.Second,
		handshakeTimeout:  30 * time.Second,
	}
	f.ctx, f.cancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Start begins heartbeat monitoring and peer health checks. The loops run
// until ctx is cancelled or Stop is called. Calling Start again while the
// federation is running is a no-op; calling it after Stop returns
// ErrFederationStopped.
func (f *Federation) Start(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stopped {
		return ErrFederationStopped
	}
	if f.started {
		return nil
	}
	f.started = true

	ctx, f.stopLoops = context.WithCancel(ctx)

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
	return nil
}

// Stop shuts down the federation: it stops the heartbeat and health loops,
// cancels pending reconnects, closes every peer connection and waits for
// all federation goroutines to exit. Stop is final and safe to call more
// than once.
func (f *Federation) Stop() {
	f.mu.Lock()
	if !f.stopped {
		f.stopped = true
		f.cancel()
		if f.stopLoops != nil {
			f.stopLoops()
		}
	}
	f.mu.Unlock()

	// Closing the connections unblocks the read loops. No connection can be
	// installed after this point: connectPeer checks f.stopped under f.mu.
	f.peers.Range(func(_, value any) bool {
		peer := value.(*Peer) //nolint:errcheck // sync.Map always stores *Peer
		f.disconnectPeer(peer)
		return true
	})

	f.wg.Wait()
}

// goTracked runs fn in a goroutine tracked by f.wg, passing the federation
// context. It reports false, without running fn, once Stop has been called.
func (f *Federation) goTracked(fn func(ctx context.Context)) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped {
		return false
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		fn(f.ctx)
	}()
	return true
}

// isCurrent reports whether peer is still the registered peer for its ID.
func (f *Federation) isCurrent(peer *Peer) bool {
	val, ok := f.peers.Load(peer.ID)
	return ok && val.(*Peer) == peer //nolint:errcheck // sync.Map always stores *Peer
}

// ── Peer Management ────────────────────────────

// AddPeer registers and connects to a remote peer. A peer already
// registered under peerID is disconnected and replaced.
func (f *Federation) AddPeer(ctx context.Context, peerID, url, token string, metadata map[string]string) error {
	peer := &Peer{
		ID:       peerID,
		URL:      url,
		Token:    token,
		State:    PeerStateDisconnected,
		Metadata: metadata,
	}

	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return ErrFederationStopped
	}
	old, replaced := f.peers.Swap(peerID, peer)
	f.mu.Unlock()

	if replaced {
		f.disconnectPeer(old.(*Peer)) //nolint:errcheck // sync.Map always stores *Peer
	}

	return f.connectPeer(ctx, peer)
}

// RemovePeer disconnects and removes a peer.
func (f *Federation) RemovePeer(peerID string) {
	// Delete under f.mu so a concurrent connectPeer either installs its
	// connection before the removal (and it is closed below) or sees the
	// peer gone and discards the connection.
	f.mu.Lock()
	val, ok := f.peers.LoadAndDelete(peerID)
	f.mu.Unlock()
	if !ok {
		return
	}
	f.disconnectPeer(val.(*Peer)) //nolint:errcheck // sync.Map always stores *Peer
}

// GetPeer returns a snapshot of a peer by ID. The returned value is a copy
// taken under the peer's lock; it does not track later state changes.
func (f *Federation) GetPeer(peerID string) (*Peer, bool) {
	val, ok := f.peers.Load(peerID)
	if !ok {
		return nil, false
	}
	return val.(*Peer).snapshot(), true //nolint:errcheck // sync.Map always stores *Peer
}

// Peers returns a snapshot of all peers. Each element is a copy taken under
// the peer's lock; it does not track later state changes.
func (f *Federation) Peers() []*Peer {
	var peers []*Peer
	f.peers.Range(func(_, value any) bool {
		peers = append(peers, value.(*Peer).snapshot()) //nolint:errcheck // sync.Map always stores *Peer
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
		log.String("peer", peerID),
		log.String("job_name", name),
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

	fail := func(conn net.Conn, err error) error {
		if conn != nil {
			_ = conn.Close()
		}
		peer.mu.Lock()
		peer.State = PeerStateDisconnected
		peer.mu.Unlock()
		return err
	}

	// Stop must be able to interrupt a handshake in progress, so tie the
	// exchange to the federation context as well as the caller's.
	hctx, cancel := context.WithTimeout(ctx, f.handshakeTimeout)
	defer cancel()
	stopWithFed := context.AfterFunc(f.ctx, cancel)
	defer stopWithFed()

	conn, _, _, err := ws.Dial(hctx, peer.URL)
	if err != nil {
		return fail(nil, fmt.Errorf("dwp/federation: dial %q: %w", peer.ID, err))
	}

	// Closing the connection is the only way to unblock the auth read.
	stopClose := context.AfterFunc(hctx, func() { _ = conn.Close() })

	// Authenticate with the peer.
	authFrame := &Frame{
		ID:     GenerateFrameID(),
		Type:   FrameRequest,
		Method: MethodAuth,
		Data:   mustMarshalJSON(AuthRequest{Token: peer.Token, Format: "json"}),
	}

	if writeErr := writeJSONFrame(conn, authFrame); writeErr != nil {
		stopClose()
		return fail(conn, fmt.Errorf("dwp/federation: auth write %q: %w", peer.ID, writeErr))
	}

	// Read auth response.
	data, err := wsutil.ReadServerText(conn)
	if !stopClose() {
		// hctx fired: the connection was closed under the read, or is
		// about to be, so the result cannot be used either way.
		return fail(conn, fmt.Errorf("dwp/federation: auth read %q: %w", peer.ID, hctx.Err()))
	}
	if err != nil {
		return fail(conn, fmt.Errorf("dwp/federation: auth read %q: %w", peer.ID, err))
	}

	var resp Frame
	if err := json.Unmarshal(data, &resp); err != nil {
		return fail(conn, fmt.Errorf("dwp/federation: auth parse %q: %w", peer.ID, err))
	}

	if resp.Type == FrameErr {
		msg := "auth failed"
		if resp.Error != nil {
			msg = resp.Error.Message
		}
		return fail(conn, fmt.Errorf("dwp/federation: %s at %q", msg, peer.ID))
	}

	// Install the connection and start its read loop atomically with
	// respect to Stop and RemovePeer, which both hold f.mu while they
	// retire the federation or the peer.
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return fail(conn, ErrFederationStopped)
	}
	if !f.isCurrent(peer) {
		f.mu.Unlock()
		return fail(conn, fmt.Errorf("dwp/federation: peer %q removed", peer.ID))
	}

	peer.mu.Lock()
	if peer.conn != nil {
		// Should not happen while reconnects are exclusive, but never
		// orphan a live connection.
		_ = peer.conn.Close()
	}
	peer.conn = conn
	peer.State = PeerStateConnected
	peer.LastSeen = time.Now().UTC()
	peer.mu.Unlock()

	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		f.peerReadLoop(peer, conn)
	}()
	f.mu.Unlock()

	f.logger.Info("federation peer connected",
		log.String("peer_id", peer.ID),
		log.String("url", peer.URL),
	)
	return nil
}

func (f *Federation) disconnectPeer(peer *Peer) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	if peer.conn != nil {
		_ = peer.conn.Close()
		peer.conn = nil
	}
	peer.State = PeerStateDisconnected
}

// peerReadLoop reads frames from conn until it fails. If conn is still the
// peer's active connection at that point, the loop tears it down and asks
// for a reconnect; if someone else already replaced or closed it (health
// check, RemovePeer, Stop), they own what happens next.
func (f *Federation) peerReadLoop(peer *Peer, conn net.Conn) {
	for {
		data, err := wsutil.ReadServerText(conn)
		if err != nil {
			peer.mu.Lock()
			owned := peer.conn == conn
			if owned {
				_ = conn.Close()
				peer.conn = nil
				peer.State = PeerStateDisconnected
			}
			peer.mu.Unlock()

			if owned {
				f.logger.Warn("federation peer read error",
					log.String("peer_id", peer.ID),
					log.String("error", err.Error()),
				)
				f.startReconnect(peer)
			}
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

// startReconnect launches a reconnect for peer unless one is already
// running, so at most one goroutine dials a given peer at a time.
func (f *Federation) startReconnect(peer *Peer) {
	peer.mu.Lock()
	if peer.reconnecting {
		peer.mu.Unlock()
		return
	}
	peer.reconnecting = true
	peer.mu.Unlock()

	release := func() {
		peer.mu.Lock()
		peer.reconnecting = false
		peer.mu.Unlock()
	}

	if !f.goTracked(func(ctx context.Context) {
		defer release()
		f.reconnectPeer(ctx, peer)
	}) {
		release()
	}
}

// reconnectPeer redials peer with exponential backoff until it connects,
// the peer is removed, or ctx (the federation context) is cancelled.
func (f *Federation) reconnectPeer(ctx context.Context, peer *Peer) {
	backoff := f.reconnectBackoff
	for {
		f.logger.Info("federation reconnecting to peer",
			log.String("peer_id", peer.ID),
			log.Duration("backoff", backoff),
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		// Don't reconnect if the peer was removed or replaced.
		if !f.isCurrent(peer) {
			return
		}

		peer.mu.RLock()
		state := peer.State
		peer.mu.RUnlock()
		if state == PeerStateConnected {
			return
		}

		if err := f.connectPeer(ctx, peer); err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrFederationStopped) {
				return
			}
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

	if err := peer.writeFrame(conn, frame); err != nil {
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

		if err := peer.writeFrame(conn, heartbeat); err != nil {
			f.logger.Warn("federation heartbeat failed",
				log.String("peer_id", peer.ID),
				log.String("error", err.Error()),
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

		// Check and tear down in one critical section, so the read loop of
		// the closed connection sees it is no longer current and leaves
		// the reconnect to us.
		peer.mu.Lock()
		lastSeen := peer.LastSeen
		timedOut := peer.State == PeerStateConnected && now.Sub(lastSeen) > f.heartbeatTimeout
		if timedOut {
			if peer.conn != nil {
				_ = peer.conn.Close()
				peer.conn = nil
			}
			peer.State = PeerStateDisconnected
		}
		peer.mu.Unlock()

		if timedOut {
			f.logger.Warn("federation peer timed out",
				log.String("peer_id", peer.ID),
				log.Duration("since_last_seen", now.Sub(lastSeen)),
			)
			f.startReconnect(peer)
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
