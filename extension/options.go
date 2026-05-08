package extension

import (
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dwp"
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

// WithPollInterval overrides the worker poll interval (default 1s).
// Increase to reduce constant driver-pool pressure when running
// against a single shared mongo / postgres node.
func WithPollInterval(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithPollInterval(d))
	}
}

// WithHeartbeatInterval overrides the running-job heartbeat cadence
// (default 10s).
func WithHeartbeatInterval(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithHeartbeatInterval(d))
	}
}

// WithStaleJobThreshold overrides the no-heartbeat threshold and the
// matching reaper cadence (default 30s).
func WithStaleJobThreshold(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithStaleJobThreshold(d))
	}
}

// WithWorkerStoreCallTimeout caps a single worker store roundtrip
// (default 5s). Pass a negative duration to disable bounding (test-only).
func WithWorkerStoreCallTimeout(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithWorkerStoreCallTimeout(d))
	}
}

// WithCronTickInterval overrides the scheduler tick cadence (default 1s).
func WithCronTickInterval(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithCronTickInterval(d))
	}
}

// WithCronLeaderTTL overrides the leader election TTL (default 15s).
// Renewal happens at half this interval.
func WithCronLeaderTTL(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithCronLeaderTTL(d))
	}
}

// WithCronLockTTL overrides the per-entry distributed-lock TTL
// (default 30s).
func WithCronLockTTL(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithCronLockTTL(d))
	}
}

// WithCronStoreCallTimeout caps a single cron-scheduler store roundtrip
// (default 5s). Pass a negative duration to disable.
func WithCronStoreCallTimeout(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithCronStoreCallTimeout(d))
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
func WithLogger(l log.Logger) ExtOption {
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

// WithDWP enables the Dispatch Wire Protocol (DWP) for real-time
// client communication over WebSocket, SSE, and HTTP RPC.
// Options configure authentication, codec, and server behaviour.
func WithDWP(opts ...dwp.Option) ExtOption {
	return func(e *Extension) {
		e.enableDWP = true
		e.dwpOpts = append(e.dwpOpts, opts...)
	}
}
