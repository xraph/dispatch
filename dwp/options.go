package dwp

import "log/slog"

// Option configures a DWP Server.
type Option func(*Server)

// WithAuth sets the authenticator for the DWP server.
// If not set, NoopAuthenticator is used (development mode).
func WithAuth(auth Authenticator) Option {
	return func(s *Server) { s.auth = auth }
}

// WithCodec sets the default codec for the DWP server.
// Clients can override via the auth frame's format field.
func WithCodec(codec Codec) Option {
	return func(s *Server) { s.defaultCodec = codec }
}

// WithLogger sets the logger for the DWP server.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) { s.logger = logger }
}

// WithPath sets the base path for DWP endpoints.
// Default is "/dwp".
func WithPath(path string) Option {
	return func(s *Server) { s.basePath = path }
}

// WithFederation attaches a federation manager to the server,
// enabling server-to-server DWP methods (federation.enqueue,
// federation.event, federation.heartbeat).
func WithFederation(f *Federation) Option {
	return func(s *Server) {
		s.handler.SetFederation(f)
	}
}
