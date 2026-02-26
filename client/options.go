package client

import (
	"log/slog"
	"time"
)

// Option configures a Client.
type Option func(*Client)

// WithToken sets the authentication token.
func WithToken(token string) Option {
	return func(c *Client) { c.token = token }
}

// WithFormat sets the wire format for frame encoding.
// Supported values: "json" (default), "msgpack".
func WithFormat(format string) Option {
	return func(c *Client) { c.format = format }
}

// WithLogger sets the structured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Client) { c.logger = logger }
}

// WithReconnect enables automatic reconnection with the given parameters.
func WithReconnect(maxRetries int, baseDelay time.Duration) Option {
	return func(c *Client) {
		c.reconnect = true
		c.maxRetries = maxRetries
		c.baseDelay = baseDelay
	}
}
