package dwp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// ── Fake peer harness ──────────────────────────────

// fakePeerConn is the server side of one federation connection. Writes are
// serialised so a test handler may reply from several goroutines.
type fakePeerConn struct {
	conn net.Conn
	wmu  sync.Mutex
}

func (c *fakePeerConn) read() (*Frame, error) {
	data, err := wsutil.ReadClientText(c.conn)
	if err != nil {
		return nil, err
	}
	var f Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("invalid frame payload %q: %w", data, err)
	}
	return &f, nil
}

func (c *fakePeerConn) write(f *Frame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return wsutil.WriteServerText(c.conn, data)
}

// replyOK answers req with an empty success response.
func (c *fakePeerConn) replyOK(req *Frame) error {
	return c.write(&Frame{
		ID:        GenerateFrameID(),
		Type:      FrameResponse,
		CorrelID:  req.ID,
		Data:      json.RawMessage(`{"ok":true}`),
		Timestamp: time.Now().UTC(),
	})
}

// authenticate reads the auth frame and accepts it.
func (c *fakePeerConn) authenticate() error {
	req, err := c.read()
	if err != nil {
		return err
	}
	if req.Method != MethodAuth {
		return fmt.Errorf("first frame method = %q, want %q", req.Method, MethodAuth)
	}
	return c.replyOK(req)
}

// fakePeer is a minimal DWP endpoint for federation tests. handle is called
// once per accepted connection with its 1-based sequence number; the
// connection is closed when handle returns.
type fakePeer struct {
	srv      *httptest.Server
	accepted atomic.Int64
	open     atomic.Int64
	handle   func(n int64, c *fakePeerConn)
}

func newFakePeer(t *testing.T, handle func(n int64, c *fakePeerConn)) *fakePeer {
	t.Helper()
	p := &fakePeer{handle: handle}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		n := p.accepted.Add(1)
		p.open.Add(1)
		defer p.open.Add(-1)
		defer conn.Close()
		p.handle(n, &fakePeerConn{conn: conn})
	}))
	t.Cleanup(func() {
		p.srv.CloseClientConnections()
		p.srv.Close()
	})
	return p
}

func (p *fakePeer) url() string {
	return "ws" + strings.TrimPrefix(p.srv.URL, "http")
}

// serveRequests authenticates and then answers every request until the
// connection fails. Malformed frames are reported through bad.
func serveRequests(c *fakePeerConn, bad *atomic.Int64) {
	if err := c.authenticate(); err != nil {
		return
	}
	for {
		req, err := c.read()
		if err != nil {
			if !isClosedErr(err) {
				bad.Add(1)
			}
			return
		}
		if req.Type == FrameRequest {
			if err := c.replyOK(req); err != nil {
				return
			}
		}
	}
}

func isClosedErr(err error) bool {
	var closed wsutil.ClosedError
	if errors.As(err, &closed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return strings.Contains(err.Error(), "connection reset")
}

func newTestFederation(opts ...FederationOption) *Federation {
	all := make([]FederationOption, 0, 3+len(opts))
	all = append(all,
		WithLocalID("local"),
		WithHeartbeatInterval(time.Hour),
		WithHeartbeatTimeout(time.Hour),
	)
	return NewFederation(nil, nil, testLogger(), append(all, opts...)...)
}

// stopTimeout bounds Federation.Stop in tests; the old code hung forever.
const stopTimeout = 2 * time.Second

// stopWithin calls f.Stop and fails the test if it does not return within
// stopTimeout.
func stopWithin(t *testing.T, f *Federation) {
	t.Helper()
	d := stopTimeout
	done := make(chan struct{})
	go func() {
		f.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("Federation.Stop did not return within %s", d)
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ── Lifecycle ──────────────────────────────────────

func TestFederation_StopWithConnectedPeer(t *testing.T) {
	var bad atomic.Int64
	peer := newFakePeer(t, func(_ int64, c *fakePeerConn) { serveRequests(c, &bad) })

	f := newTestFederation()
	f.Start(t.Context())
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", nil); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	stopWithin(t, f)

	if s := f.Stats(); s.ConnectedPeers != 0 {
		t.Errorf("ConnectedPeers after Stop = %d, want 0", s.ConnectedPeers)
	}
}

func TestFederation_StopDuringReconnectBackoff(t *testing.T) {
	var bad atomic.Int64
	peer := newFakePeer(t, func(n int64, c *fakePeerConn) {
		if n == 1 {
			// Accept, then drop the connection to force a reconnect.
			_ = c.authenticate()
			return
		}
		serveRequests(c, &bad)
	})

	f := newTestFederation()
	f.reconnectBackoff = 10 * time.Second
	f.Start(t.Context())
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", nil); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	waitFor(t, 2*time.Second, "peer to drop", func() bool { return f.Stats().ConnectedPeers == 0 })

	stopWithin(t, f)

	if got := peer.accepted.Load(); got != 1 {
		t.Errorf("connections after Stop = %d, want 1 (no reconnect after Stop)", got)
	}
}

func TestFederation_StopDuringReconnectHandshake(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	peer := newFakePeer(t, func(n int64, c *fakePeerConn) {
		if n == 1 {
			_ = c.authenticate()
			return
		}
		// Swallow the auth frame and never answer it.
		_, _ = c.read()
		<-release
	})

	f := newTestFederation()
	f.reconnectBackoff = time.Millisecond
	f.Start(t.Context())
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", nil); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	waitFor(t, 2*time.Second, "reconnect attempt", func() bool { return peer.accepted.Load() >= 2 })

	stopWithin(t, f)
}

func TestFederation_DoubleStart(t *testing.T) {
	var pings atomic.Int64
	peer := newFakePeer(t, func(_ int64, c *fakePeerConn) {
		if err := c.authenticate(); err != nil {
			return
		}
		for {
			req, err := c.read()
			if err != nil {
				return
			}
			if req.Type == FramePing {
				pings.Add(1)
			}
		}
	})

	f := newTestFederation(WithHeartbeatInterval(20 * time.Millisecond))
	f.Start(t.Context())
	f.Start(t.Context())
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", nil); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	stopWithin(t, f)

	// No heartbeat loop may survive Stop.
	before := pings.Load()
	time.Sleep(100 * time.Millisecond)
	if after := pings.Load(); after != before {
		t.Errorf("heartbeats after Stop: %d -> %d", before, after)
	}
}

func TestFederation_StopIsFinal(t *testing.T) {
	var bad atomic.Int64
	peer := newFakePeer(t, func(_ int64, c *fakePeerConn) { serveRequests(c, &bad) })

	f := newTestFederation()
	if err := f.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.Start(t.Context()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	stopWithin(t, f)
	stopWithin(t, f) // idempotent

	if err := f.Start(t.Context()); !errors.Is(err, ErrFederationStopped) {
		t.Errorf("Start after Stop = %v, want ErrFederationStopped", err)
	}
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", nil); !errors.Is(err, ErrFederationStopped) {
		t.Errorf("AddPeer after Stop = %v, want ErrFederationStopped", err)
	}
	if got := peer.accepted.Load(); got != 0 {
		t.Errorf("connections after Stop = %d, want 0", got)
	}
}

// ── Peer health ────────────────────────────────────

func TestFederation_TimeoutReconnectsOnce(t *testing.T) {
	peer := newFakePeer(t, func(n int64, c *fakePeerConn) {
		if err := c.authenticate(); err != nil {
			return
		}
		if n == 1 {
			// Stay silent so the health check times the peer out, and keep
			// the socket open until the client closes it.
			for {
				if _, err := c.read(); err != nil {
					return
				}
			}
		}
		// Later connections keep the peer alive.
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					if c.write(&Frame{ID: GenerateFrameID(), Type: FramePong, Timestamp: time.Now().UTC()}) != nil {
						return
					}
				}
			}
		}()
		for {
			if _, err := c.read(); err != nil {
				return
			}
		}
	})

	f := newTestFederation(WithHeartbeatTimeout(200 * time.Millisecond))
	f.reconnectBackoff = 20 * time.Millisecond
	f.Start(t.Context())
	t.Cleanup(func() { stopWithin(t, f) })
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", nil); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	waitFor(t, 3*time.Second, "reconnect", func() bool { return peer.accepted.Load() >= 2 })
	// Give a duplicate reconnect time to show up.
	time.Sleep(300 * time.Millisecond)

	if got := peer.accepted.Load(); got != 2 {
		t.Errorf("connections accepted = %d, want 2 (one reconnect)", got)
	}
	if got := peer.open.Load(); got != 1 {
		t.Errorf("connections open = %d, want 1", got)
	}
	if s := f.Stats(); s.ConnectedPeers != 1 {
		t.Errorf("ConnectedPeers = %d, want 1", s.ConnectedPeers)
	}
}

// ── Writes ─────────────────────────────────────────

func TestFederation_ConcurrentWritesDoNotInterleave(t *testing.T) {
	var bad atomic.Int64
	peer := newFakePeer(t, func(_ int64, c *fakePeerConn) { serveRequests(c, &bad) })

	f := newTestFederation(WithHeartbeatInterval(time.Millisecond))
	f.Start(t.Context())
	t.Cleanup(func() { stopWithin(t, f) })
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", nil); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	const (
		workers = 32
		perW    = 25
	)
	payload := json.RawMessage(`{"blob":"` + strings.Repeat("x", 4096) + `"}`)

	// A desynchronised stream loses responses, so bound the wait rather than
	// sitting out the 30s request timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var failed atomic.Int64
	var errOnce sync.Once
	var firstErr error
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perW {
				var err error
				if (w+i)%2 == 0 {
					err = f.ForwardEvent(ctx, "remote", "evt", payload)
				} else {
					err = f.ForwardJob(ctx, "remote", "job", payload)
				}
				if err != nil {
					failed.Add(1)
					errOnce.Do(func() { firstErr = err })
				}
			}
		}()
	}
	wg.Wait()

	if n := failed.Load(); n != 0 {
		t.Errorf("%d of %d forwards failed, first: %v", n, workers*perW, firstErr)
	}
	if n := bad.Load(); n != 0 {
		t.Errorf("peer saw %d malformed frames", n)
	}
	if got := peer.accepted.Load(); got != 1 {
		t.Errorf("connections accepted = %d, want 1", got)
	}
}

// ── Snapshots ──────────────────────────────────────

func TestFederation_PeerSnapshotsDoNotRace(t *testing.T) {
	peer := newFakePeer(t, func(_ int64, c *fakePeerConn) {
		if err := c.authenticate(); err != nil {
			return
		}
		for {
			if err := c.write(&Frame{ID: GenerateFrameID(), Type: FramePong, Timestamp: time.Now().UTC()}); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	})

	f := newTestFederation()
	f.Start(t.Context())
	t.Cleanup(func() { stopWithin(t, f) })
	if err := f.AddPeer(t.Context(), "remote", peer.url(), "tok", map[string]string{"region": "eu"}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		p, ok := f.GetPeer("remote")
		if !ok {
			t.Fatal("GetPeer: not found")
		}
		_ = p.LastSeen
		_ = p.State
		for _, p := range f.Peers() {
			_ = p.LastSeen
			_ = p.State
		}
	}

	p, _ := f.GetPeer("remote")
	if p.State != PeerStateConnected || p.Metadata["region"] != "eu" || p.URL != peer.url() {
		t.Errorf("snapshot = %+v", p)
	}
}
