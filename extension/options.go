package extension

import (
	"log/slog"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/ext"
	mw "github.com/xraph/dispatch/middleware"
)

// ExtOption configures the Dispatch Forge extension.
type ExtOption func(*Extension)

// WithStore sets the persistence backend via a dispatcher option.
func WithStore(s dispatch.Storer) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithStore(s))
	}
}

// WithConcurrency sets the maximum number of concurrent job processors.
func WithConcurrency(n int) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithConcurrency(n))
	}
}

// WithQueues sets the queues the dispatcher will poll.
func WithQueues(queues []string) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithQueues(queues))
	}
}

// WithExtension registers a dispatch extension (lifecycle hooks).
func WithExtension(x ext.Extension) ExtOption {
	return func(e *Extension) {
		e.exts = append(e.exts, x)
	}
}

// WithMiddleware adds job middleware to the dispatch engine.
func WithMiddleware(m mw.Middleware) ExtOption {
	return func(e *Extension) {
		e.mws = append(e.mws, m)
	}
}

// WithBackoff sets the retry backoff strategy.
func WithBackoff(b backoff.Strategy) ExtOption {
	return func(e *Extension) {
		e.bo = b
	}
}

// WithBasePath sets the URL prefix for all dispatch routes.
func WithBasePath(path string) ExtOption {
	return func(e *Extension) {
		e.config.BasePath = path
	}
}

// WithConfig sets the extension configuration directly.
func WithConfig(cfg Config) ExtOption {
	return func(e *Extension) {
		e.config = cfg
	}
}

// WithDisableRoutes disables the registration of HTTP routes.
func WithDisableRoutes() ExtOption {
	return func(e *Extension) {
		e.config.DisableRoutes = true
	}
}

// WithDisableMigrate disables auto-migration on start.
func WithDisableMigrate() ExtOption {
	return func(e *Extension) {
		e.config.DisableMigrate = true
	}
}

// WithRequireConfig requires config to be present in YAML files.
// If true and no config is found, Register returns an error.
func WithRequireConfig(require bool) ExtOption {
	return func(e *Extension) {
		e.config.RequireConfig = require
	}
}

// WithLogger sets the structured logger for the dispatch engine.
func WithLogger(l *slog.Logger) ExtOption {
	return func(e *Extension) {
		e.logger = l
	}
}

// WithGroveDatabase sets the name of the grove.DB to resolve from the DI container.
// The extension will auto-construct the appropriate store backend (postgres/sqlite/mongo)
// based on the grove driver type. Pass an empty string to use the default (unnamed) grove.DB.
func WithGroveDatabase(name string) ExtOption {
	return func(e *Extension) {
		e.config.GroveDatabase = name
		e.useGrove = true
	}
}

// WithGroveKV sets the name of the grove kv.Store to resolve from the DI container.
// The extension will auto-construct a Redis-backed store from the resolved kv.Store.
// Pass an empty string to use the default (unnamed) kv.Store.
func WithGroveKV(name string) ExtOption {
	return func(e *Extension) {
		e.config.GroveKV = name
		e.useGroveKV = true
	}
}
