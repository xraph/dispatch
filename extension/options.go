package extension

import (
	"slices"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/ext"
	mw "github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/resource"
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

// WithMaxPollInterval caps the worker fetcher's idle backoff (default 30s).
// Empty polls double the poll interval from PollInterval up to this value;
// new work or an in-process enqueue resets it.
func WithMaxPollInterval(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithMaxPollInterval(d))
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

// WithCronLeaderTTL overrides the leader election TTL (default 60s).
// Renewal happens at half this interval.
func WithCronLeaderTTL(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithCronLeaderTTL(d))
	}
}

// WithCronRefreshInterval overrides how often the cron leader re-lists
// entries from the store (default 30s). In-process registrations
// invalidate the cache immediately.
func WithCronRefreshInterval(d time.Duration) ExtOption {
	return func(e *Extension) {
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithCronRefreshInterval(d))
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

// WithArtifactBackend supplies the object store backing the artifact
// plane explicitly, bypassing container discovery.
//
// Use it outside Forge, or when the backend is not Trove.
func WithArtifactBackend(b artifact.Backend) ExtOption {
	return func(e *Extension) {
		e.artifactBackend = b
		e.config.Artifacts.Enabled = true
	}
}

// WithArtifactStore sets the store the artifact plane persists through.
// It defaults to the dispatcher's store when that store implements
// artifact.Store, which every bundled backend does.
func WithArtifactStore(s artifact.Store) ExtOption {
	return func(e *Extension) { e.artifactStore = s }
}

// WithArtifacts enables the artifact plane, resolving a Trove instance
// from the DI container. Pass a store name for multi-store Trove setups,
// or the empty string for the default instance.
func WithArtifacts(troveStore string) ExtOption {
	return func(e *Extension) {
		e.config.Artifacts.Enabled = true
		e.config.Artifacts.TroveStore = troveStore
	}
}

// WithArtifactCacheDir sets where staged artifacts are held on disk.
func WithArtifactCacheDir(dir string) ExtOption {
	return func(e *Extension) { e.config.Artifacts.Cache.Dir = dir }
}

// WithArtifactCacheBudget caps the bytes the staging cache may hold.
//
// With the resource model enabled this becomes the shared ledger's disk
// capacity, so it is the figure a job's declared disk requirement is
// admitted against rather than a private ceiling inside the cache.
func WithArtifactCacheBudget(bytes int64) ExtOption {
	return func(e *Extension) { e.config.Artifacts.Cache.Budget = bytes }
}

// WithResources turns on capacity detection and resource-aware
// admission.
//
// The extension then builds one resource.Manager over the detected
// capacity and hands the same instance to the staging cache and the
// worker pool: the cache holds a lease per cached entry and reclaims disk
// on demand, the pool admits every claimed job against what is actually
// free. Without this the pool dequeues unbounded and nothing changes.
//
// Detection is cgroup-first, so a container with a two-core quota
// advertises two cores rather than the host's sixty-four.
func WithResources() ExtOption {
	return func(e *Extension) { e.config.Resources.Enabled = true }
}

// WithCPUOvercommit multiplies the detected core count (default 1.0).
//
// CPU is compressible: exceeding it makes jobs slow, not dead. There is
// no memory equivalent, deliberately — overcommitting memory is how a box
// enters the OOM cascade this model prevents.
func WithCPUOvercommit(factor float64) ExtOption {
	return func(e *Extension) { e.config.Resources.CPUOvercommit = factor }
}

// WithMemoryFraction sets the share of detected memory to advertise
// (default 0.8), leaving the remainder for the Go runtime, the page
// cache, and everything else sharing the box.
func WithMemoryFraction(fraction float64) ExtOption {
	return func(e *Extension) { e.config.Resources.MemoryFraction = fraction }
}

// WithExplicitCapacity overrides detection per key and is the only way to
// declare a custom resource — nothing detects "fpga".
//
// Quantities are canonical units: cpu in millicores, memory and disk in
// bytes, gpu in milli-devices. Sets merge per key across calls.
func WithExplicitCapacity(sets ...resource.Set) ExtOption {
	return func(e *Extension) {
		for _, s := range sets {
			if e.config.Resources.Explicit == nil {
				e.config.Resources.Explicit = make(resource.Set, len(s))
			}

			for k, v := range s {
				e.config.Resources.Explicit[k] = v
			}
		}
	}
}

// WithWorkerCustomKeys narrows the custom resource keys this worker
// advertises at dequeue. Empty advertises every custom key it has
// capacity for, which is usually what you want; this exists so a worker
// draining a device can stop attracting work for it.
//
// Keys accumulate across calls and duplicates collapse, matching
// WithExplicitCapacity above. The slice is copied, so the caller keeps no
// handle on extension state.
func WithWorkerCustomKeys(keys ...string) ExtOption {
	return func(e *Extension) {
		for _, k := range keys {
			if !slices.Contains(e.config.Resources.CustomKeys, k) {
				e.config.Resources.CustomKeys = append(e.config.Resources.CustomKeys, k)
			}
		}
	}
}
