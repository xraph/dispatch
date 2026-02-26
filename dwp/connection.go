package dwp

import (
	"sync"
	"sync/atomic"
	"time"
)

// Connection represents an authenticated DWP connection.
type Connection struct {
	// ID uniquely identifies this connection.
	ID string

	// Identity is the authenticated identity for this connection.
	Identity *Identity

	// Codec is the negotiated wire format.
	Codec Codec

	// ConnectedAt records when the connection was established.
	ConnectedAt time.Time

	// LastActivity tracks the most recent frame received.
	LastActivity atomic.Value // time.Time

	// Subscriptions tracks active channel subscriptions.
	subscriptions map[string]struct{}
	mu            sync.RWMutex
}

// NewConnection creates a connection with the given ID and identity.
func NewConnection(id string, identity *Identity, codec Codec) *Connection {
	c := &Connection{
		ID:            id,
		Identity:      identity,
		Codec:         codec,
		ConnectedAt:   time.Now().UTC(),
		subscriptions: make(map[string]struct{}),
	}
	c.LastActivity.Store(time.Now().UTC())
	return c
}

// Touch updates the last activity timestamp.
func (c *Connection) Touch() {
	c.LastActivity.Store(time.Now().UTC())
}

// AddSubscription records a channel subscription.
func (c *Connection) AddSubscription(channel string) {
	c.mu.Lock()
	c.subscriptions[channel] = struct{}{}
	c.mu.Unlock()
}

// RemoveSubscription removes a channel subscription.
func (c *Connection) RemoveSubscription(channel string) {
	c.mu.Lock()
	delete(c.subscriptions, channel)
	c.mu.Unlock()
}

// Subscriptions returns a copy of active subscription channels.
func (c *Connection) Subscriptions() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.subscriptions))
	for ch := range c.subscriptions {
		out = append(out, ch)
	}
	return out
}

// ConnectionManager tracks active DWP connections.
type ConnectionManager struct {
	mu    sync.RWMutex
	conns map[string]*Connection
}

// NewConnectionManager creates an empty connection manager.
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		conns: make(map[string]*Connection),
	}
}

// Add registers a new connection.
func (cm *ConnectionManager) Add(conn *Connection) {
	cm.mu.Lock()
	cm.conns[conn.ID] = conn
	cm.mu.Unlock()
}

// Remove unregisters a connection.
func (cm *ConnectionManager) Remove(connID string) {
	cm.mu.Lock()
	delete(cm.conns, connID)
	cm.mu.Unlock()
}

// Get returns a connection by ID.
func (cm *ConnectionManager) Get(connID string) (*Connection, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	c, ok := cm.conns[connID]
	return c, ok
}

// Count returns the number of active connections.
func (cm *ConnectionManager) Count() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.conns)
}

// All returns a snapshot of all connections.
func (cm *ConnectionManager) All() []*Connection {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	out := make([]*Connection, 0, len(cm.conns))
	for _, c := range cm.conns {
		out = append(out, c)
	}
	return out
}
