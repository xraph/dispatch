// Package extension provides the Forge extension adapter for Dispatch.
//
// It implements the forge.Extension interface to integrate Dispatch
// into a Forge application with automatic dependency discovery,
// route registration, and lifecycle management.
//
// Configuration can be provided programmatically via Option functions
// or via YAML configuration files under "extensions.dispatch" or "dispatch" keys.
package extension

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/grove/kv"
	"github.com/xraph/vessel"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/artifact/sweeper"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	mw "github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/security"
	mongostore "github.com/xraph/dispatch/store/mongo"
	pgstore "github.com/xraph/dispatch/store/postgres"
	redisstore "github.com/xraph/dispatch/store/redis"
	sqlitestore "github.com/xraph/dispatch/store/sqlite"
)

// ExtensionName is the name registered with Forge.
const ExtensionName = "dispatch"

// ExtensionDescription is the human-readable description.
const ExtensionDescription = "Durable execution engine for background jobs, workflows, and cron scheduling"

// ExtensionVersion is the semantic version.
const ExtensionVersion = "0.1.0"

// Ensure Extension implements forge.Extension at compile time.
var _ forge.Extension = (*Extension)(nil)

// Extension adapts Dispatch as a Forge extension. It implements the
// forge.Extension interface so Dispatch can be mounted into any Forge app.
type Extension struct {
	*forge.BaseExtension

	lifecycleOnce         sync.Once
	lifecycleGate         chan struct{}
	stopping              bool
	sweeperStopped        chan struct{}
	deliveryConfig        *DeliveryConfig
	publisher             *delivery.Publisher
	memoryAuditForTesting bool
	remoteAuth            security.Authenticator
	boundary              *security.Boundary
	durable               *runtime.Options
	durableHandlers       runtime.Options
	config                Config
	eng                   *engine.Engine
	apiHandler            *api.API
	dwpServer             *dwp.Server
	logger                log.Logger
	dispatchOpts          []dispatch.Option
	exts                  []ext.Extension
	mws                   []mw.Middleware
	dwpOpts               []dwp.Option
	bo                    backoff.Strategy
	useGrove              bool
	useGroveKV            bool
	enableDWP             bool

	// artifactBackend is an explicitly supplied backend, taking priority
	// over anything discovered in the container.
	artifactBackend artifact.Backend
	// artifactStore is the store the artifact service persists through.
	artifactStore artifact.Store
	artifacts     *artifact.Service
	artifactCache *cache.Cache
	sweeper       *sweeper.Sweeper

	// resources is the single admission ledger shared by the staging
	// cache and the worker pool. Nil when the resource model is off.
	resources resource.Manager
}

// New creates a Dispatch Forge extension with the given options.
func New(opts ...ExtOption) *Extension {
	e := &Extension{
		BaseExtension: forge.NewBaseExtension(ExtensionName, ExtensionVersion, ExtensionDescription),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Engine returns the underlying dispatch engine.
// This is nil until Register is called.
func (e *Extension) Engine() *engine.Engine { return e.eng }

// API returns the API handler.
func (e *Extension) API() *api.API { return e.apiHandler }

// Artifacts returns the artifact service, or nil when the plane is off.
func (e *Extension) Artifacts() *artifact.Service { return e.artifacts }

// ArtifactCache returns the staging cache, or nil when the plane is off.
func (e *Extension) ArtifactCache() *cache.Cache { return e.artifactCache }

// Resources returns the shared admission ledger, or nil when the
// resource model is off. It is the same instance the staging cache and
// the worker pool were built with — which is the point of exposing it.
func (e *Extension) Resources() resource.Manager { return e.resources }

// DWPServer returns the DWP server, or nil if DWP is not enabled.
func (e *Extension) DWPServer() *dwp.Server { return e.dwpServer }

// Register implements [forge.Extension]. It initializes the dispatcher,
// builds the engine, and optionally registers HTTP routes.
func (e *Extension) Register(fapp forge.App) error {
	if err := e.BaseExtension.Register(fapp); err != nil {
		return err
	}

	if err := e.loadConfiguration(); err != nil {
		return err
	}

	if err := e.init(fapp); err != nil {
		return err
	}

	// Register the engine in the DI container so other extensions can use it.
	if err := vessel.Provide(fapp.Container(), func() (*engine.Engine, error) {
		return e.eng, nil
	}); err != nil {
		return fmt.Errorf("dispatch: register engine in container: %w", err)
	}

	return nil
}

// init builds the dispatcher and engine.
func (e *Extension) init(fapp forge.App) error {
	// Resolve grove database store if configured (takes precedence over grove KV).
	if e.useGrove {
		groveDB, err := e.resolveGroveDB(fapp)
		if err != nil {
			return fmt.Errorf("dispatch: %w", err)
		}
		s, err := e.buildStoreFromGroveDB(groveDB)
		if err != nil {
			return err
		}
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithStore(s))
	} else if e.useGroveKV {
		kvStore, err := e.resolveGroveKV(fapp)
		if err != nil {
			return fmt.Errorf("dispatch: %w", err)
		}
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithStore(e.buildStoreFromGroveKV(kvStore)))
	} else if db, err := vessel.Inject[*grove.DB](fapp.Container()); err == nil {
		// Auto-discover default grove.DB from container (matches authsome/cortex pattern).
		s, err := e.buildStoreFromGroveDB(db)
		if err != nil {
			return err
		}
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithStore(s))
		e.Logger().Info("dispatch: auto-discovered grove.DB from container",
			forge.F("driver", db.Driver().Name()),
		)
	}

	logger := e.logger
	if logger == nil {
		logger = e.App().Logger()
	}

	// Build dispatcher options.
	opts := make([]dispatch.Option, 0, len(e.dispatchOpts)+1)
	opts = append(opts, e.dispatchOpts...)
	opts = append(opts, dispatch.WithLogger(logger))

	d, err := dispatch.New(opts...)
	if err != nil {
		return fmt.Errorf("dispatch: create dispatcher: %w", err)
	}

	// Build engine options.
	boCount := 0
	if e.bo != nil {
		boCount = 1
	}
	engOpts := make([]engine.Option, 0, len(e.exts)+len(e.mws)+boCount+1)
	engOpts = append(engOpts, engine.WithMetricFactory(fapp.Metrics()))
	for _, x := range e.exts {
		engOpts = append(engOpts, engine.WithExtension(x))
	}
	for _, m := range e.mws {
		engOpts = append(engOpts, engine.WithMiddleware(m))
	}
	if e.bo != nil {
		engOpts = append(engOpts, engine.WithBackoff(e.bo))
	}

	// Enable stream broker if DWP is requested (via option or config).
	if e.enableDWP || e.config.EnableDWP {
		engOpts = append(engOpts, engine.WithStreamBroker())
	}

	// Resolve the artifact store before the ledger, not after. The
	// ledger's disk capacity is the staging budget, and there is no
	// staging cache unless this resolves — a store that does not
	// implement artifact.Store turns `artifacts.enabled: true` into a
	// plane that is configured and never built.
	if e.artifactStore == nil {
		if as, ok := d.Store().(artifact.Store); ok {
			e.artifactStore = as
		}
	}

	// The admission ledger is built next, because both of the things that
	// follow have to be given the SAME instance: the staging cache holds
	// a lease per cached entry and registers itself as the ledger's disk
	// reclaimer, and the worker pool offers disk at dequeue as free PLUS
	// what that reclaimer could evict. Two managers and the second half
	// of that budget is permanently zero.
	e.resources = e.buildResourceManager()

	// The artifact plane is built before the engine, because the staging
	// middleware has to be in the chain the engine constructs.
	if e.artifactStore != nil {
		svc, artCache, aerr := e.buildArtifactPlane(fapp, e.resources)
		if aerr != nil {
			return aerr
		}

		if svc != nil {
			e.artifacts = svc
			e.artifactCache = artCache
			engOpts = append(engOpts, engine.WithArtifacts(svc, artCache))
		}
	}

	// Execution rungs beyond the in-process default are resolved after
	// the artifact plane, not before: resolveExecutionOptions checks
	// e.artifacts to decide whether a configured scratch directory will
	// actually be read (see its own comment), and that has to reflect
	// the artifact plane's FINAL state, not a guess made ahead of it.
	execOpts, execErr := e.resolveExecutionOptions()
	if execErr != nil {
		return execErr
	}
	engOpts = append(engOpts, execOpts...)

	if e.resources != nil {
		engOpts = append(engOpts, engine.WithResourceManager(e.resources))

		if keys := e.config.Resources.CustomKeys; len(keys) > 0 {
			engOpts = append(engOpts, engine.WithWorkerCustomKeys(keys))
		}
	}

	durableOptions, durableErr := e.durableOption()
	if durableErr != nil {
		return durableErr
	}
	engOpts = append(engOpts, durableOptions...)
	e.eng, err = engine.Build(d, engOpts...)
	if err != nil {
		return fmt.Errorf("dispatch: build engine: %w", err)
	}

	// Create the API handler.
	e.configureSecurity()
	e.eng.Dispatcher().BeforeStoreClose(e.stopDelivery)
	e.apiHandler = api.New(e.eng, fapp.Router(), api.WithSecurity(e.remoteAuth, *e.boundary))

	// Register HTTP routes unless disabled.
	if !e.config.DisableRoutes {
		basePath := e.config.BasePath
		if basePath == "" {
			basePath = "/dispatch"
		}
		if routeErr := e.apiHandler.RegisterRoutes(fapp.Router().Group(basePath)); routeErr != nil {
			return fmt.Errorf("dispatch: register routes: %w", routeErr)
		}
	}

	// Create DWP server if stream broker is available.
	if e.eng.StreamBroker() != nil {
		dwpOptList := make([]dwp.Option, 0, len(e.dwpOpts)+2)
		dwpOptList = append(dwpOptList, dwp.WithLogger(logger), dwp.WithAuth(&dwp.ForgeAuthenticator{Auth: e.remoteAuth}), dwp.WithSecurity(*e.boundary))
		if e.config.DWPBasePath != "" {
			dwpOptList = append(dwpOptList, dwp.WithPath(e.config.DWPBasePath))
		}
		dwpOptList = append(dwpOptList, e.dwpOpts...)

		handler := dwp.NewHandler(e.eng, e.eng.StreamBroker(), logger, *e.boundary)
		e.dwpServer = dwp.NewServer(e.eng.StreamBroker(), handler, dwpOptList...)

		if !e.config.DisableRoutes {
			e.dwpServer.RegisterRoutes(fapp.Router())
		}
	}

	return nil
}

// Start begins job processing and runs auto-migration if enabled.
func (e *Extension) Start(ctx context.Context) error {
	if err := e.lockLifecycle(ctx); err != nil {
		return err
	}
	defer e.unlockLifecycle()
	if e.stopping {
		return errors.New("dispatch: extension is stopping")
	}
	if e.IsStarted() {
		return nil
	}
	if e.eng == nil {
		return errors.New("dispatch: extension not initialized")
	}

	// Run migrations unless disabled.
	if !e.config.DisableMigrate {
		store := e.eng.Dispatcher().Store()
		if store != nil {
			if err := store.Migrate(ctx); err != nil {
				return fmt.Errorf("dispatch: migration failed: %w", err)
			}
		}
	}

	if err := e.prepareDelivery(ctx); err != nil {
		return err
	}
	if e.publisher != nil {
		if err := e.publisher.Start(); err != nil {
			return err
		}
	}

	// Sweep stale workers from prior instances. On a hard kill (SIGKILL,
	// pod restart, OOM), the previous worker's row stays in the cluster
	// store with is_leader possibly still set; the partial-unique index
	// then prevents the new instance from claiming leadership until
	// mongo's TTL sweeper runs (up to 60s). Doing the sweep ourselves
	// closes that gap immediately. The threshold is generous (max of
	// 5 minutes or 5×heartbeat) so we never evict a live worker that's
	// just slow to heartbeat during a cold start.
	if cls := e.eng.ClusterStore(); cls != nil {
		hb := e.eng.Dispatcher().Config().HeartbeatInterval
		threshold := 5 * hb
		if threshold < 5*time.Minute {
			threshold = 5 * time.Minute
		}
		logger := e.logger
		if logger == nil {
			logger = e.App().Logger()
		}
		if n, err := cls.DeleteStaleWorkers(ctx, threshold); err != nil {
			// Non-fatal: leader election will retry once mongo settles
			// and the TTL sweeper will eventually catch up.
			if logger != nil {
				logger.Warn("dispatch: stale worker sweep failed",
					log.String("error", err.Error()))
			}
		} else if n > 0 && logger != nil {
			logger.Info("dispatch: swept stale workers at startup",
				log.Int64("count", n))
		}
	}

	if err := e.eng.Start(ctx); err != nil {
		return errors.Join(err, e.stopDelivery(ctx))
	}
	if err := e.activateAudit(ctx); err != nil {
		return errors.Join(err, e.eng.Stop(ctx))
	}

	e.startSweeper(ctx)

	e.MarkStarted()
	return nil
}

// startSweeper begins reclaiming Dispatch-owned storage.
//
// It runs on the elected leader only, so a fleet does not race to delete
// the same objects, and it is skipped entirely when the artifact plane is
// off.
func (e *Extension) startSweeper(ctx context.Context) {
	if e.artifacts == nil || !e.artifacts.Enabled() {
		return
	}

	logger := e.logger
	if logger == nil {
		logger = e.App().Logger()
	}

	cfg := e.config.Artifacts

	e.sweeper = sweeper.New(e.artifactStore, e.artifacts.Backend(),
		sweeper.WithRetention(cfg.Retention),
		sweeper.WithPurgeGrace(cfg.PurgeGrace),
		sweeper.WithLogger(logger),
		sweeper.WithLeaderCheck(e.isClusterLeader),
	)

	if serr := e.sweeper.Start(ctx); serr != nil {
		logger.Warn("dispatch: could not start the artifact sweeper",
			log.String("error", serr.Error()))
	}
}

// isClusterLeader reports whether this instance holds cluster leadership.
// A single-instance deployment has no cluster store and is always the
// leader by default.
func (e *Extension) isClusterLeader() bool {
	cls := e.eng.ClusterStore()
	if cls == nil {
		return true
	}

	leader, err := cls.GetLeader(context.Background())
	if err != nil || leader == nil {
		return false
	}

	self := e.eng.WorkerID()
	if self.IsNil() {
		return false
	}

	return leader.ID.String() == self.String()
}

// Stop gracefully shuts down the dispatch engine.
func (e *Extension) Stop(ctx context.Context) error {
	if err := e.lockLifecycle(ctx); err != nil {
		return err
	}
	defer e.unlockLifecycle()
	e.stopping = true
	if e.eng == nil {
		e.MarkStopped()
		return nil
	}
	e.boundary.Audit.Deactivate()
	if e.sweeper != nil {
		if e.sweeperStopped == nil {
			e.sweeperStopped = make(chan struct{})
			go func() {
				if err := e.sweeper.Stop(context.Background()); err != nil {
					e.Logger().Warn("dispatch: artifact sweeper stop failed", forge.F("error", err.Error()))
				}
				close(e.sweeperStopped)
			}()
		}
		select {
		case <-e.sweeperStopped:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := e.eng.Stop(ctx)
	e.MarkStopped()
	return err
}

func (e *Extension) lockLifecycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.lifecycleOnce.Do(func() { e.lifecycleGate = make(chan struct{}, 1) })
	select {
	case e.lifecycleGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		e.unlockLifecycle()
		return err
	}
	return nil
}
func (e *Extension) unlockLifecycle() { <-e.lifecycleGate }

// Health implements [forge.Extension].
func (e *Extension) Health(ctx context.Context) error {
	if e.eng == nil {
		return errors.New("dispatch: extension not initialized")
	}

	return e.eng.Health(ctx)
}

// Handler returns the HTTP handler for all API routes.
// Convenience for standalone use outside Forge.
func (e *Extension) Handler() http.Handler {
	if e.apiHandler == nil {
		return http.NotFoundHandler()
	}
	return e.apiHandler.Handler()
}

// RegisterRoutes registers all dispatch API routes into a Forge router.
// It returns every route forge refused; see api.API.RegisterRoutes.
func (e *Extension) RegisterRoutes(router forge.Router) error {
	if e.apiHandler == nil {
		return nil
	}

	return e.apiHandler.RegisterRoutes(router)
}

// --- Config Loading (mirrors grove/shield extension pattern) ---

// loadConfiguration loads config from YAML files or programmatic sources.
func (e *Extension) loadConfiguration() error {
	programmaticConfig := e.config

	// Try loading from config file.
	fileConfig, configLoaded := e.tryLoadFromConfigFile()

	if !configLoaded {
		if programmaticConfig.RequireConfig {
			return errors.New("dispatch: configuration is required but not found in config files; " +
				"ensure 'extensions.dispatch' or 'dispatch' key exists in your config")
		}

		// Use programmatic config merged with defaults.
		e.config = e.mergeWithDefaults(programmaticConfig)
	} else {
		// Config loaded from YAML -- merge with programmatic options.
		e.config = e.mergeConfigurations(fileConfig, programmaticConfig)
	}

	// Enable grove resolution if YAML config specifies grove settings.
	if e.config.GroveDatabase != "" {
		e.useGrove = true
	}
	if e.config.GroveKV != "" {
		e.useGroveKV = true
	}

	e.Logger().Debug("dispatch: configuration loaded",
		forge.F("disable_routes", e.config.DisableRoutes),
		forge.F("disable_migrate", e.config.DisableMigrate),
		forge.F("base_path", e.config.BasePath),
		forge.F("grove_database", e.config.GroveDatabase),
		forge.F("grove_kv", e.config.GroveKV),
		forge.F("key_prefix", e.config.KeyPrefix),
	)

	return nil
}

// tryLoadFromConfigFile attempts to load config from YAML files.
func (e *Extension) tryLoadFromConfigFile() (Config, bool) {
	cm := e.App().Config()
	var cfg Config

	// Try "extensions.dispatch" first (namespaced pattern).
	if cm.IsSet("extensions.dispatch") {
		if err := cm.Bind("extensions.dispatch", &cfg); err == nil {
			e.Logger().Debug("dispatch: loaded config from file",
				forge.F("key", "extensions.dispatch"),
			)
			return cfg, true
		}
		e.Logger().Warn("dispatch: failed to bind extensions.dispatch config",
			forge.F("error", "bind failed"),
		)
	}

	// Try legacy "dispatch" key.
	if cm.IsSet("dispatch") {
		if err := cm.Bind("dispatch", &cfg); err == nil {
			e.Logger().Debug("dispatch: loaded config from file",
				forge.F("key", "dispatch"),
			)
			return cfg, true
		}
		e.Logger().Warn("dispatch: failed to bind dispatch config",
			forge.F("error", "bind failed"),
		)
	}

	return Config{}, false
}

// mergeWithDefaults fills zero-valued fields with defaults.
func (e *Extension) mergeWithDefaults(cfg Config) Config {
	defaults := DefaultConfig()
	if cfg.BasePath == "" {
		cfg.BasePath = defaults.BasePath
	}

	if cfg.Artifacts.Bucket == "" {
		cfg.Artifacts.Bucket = "dispatch-artifacts"
	}

	if cfg.Artifacts.EphemeralPrefix == "" {
		cfg.Artifacts.EphemeralPrefix = artifact.DefaultEphemeralPrefix
	}

	if cfg.Artifacts.Retention == 0 {
		cfg.Artifacts.Retention = 168 * time.Hour
	}

	if cfg.Artifacts.PurgeGrace == 0 {
		cfg.Artifacts.PurgeGrace = 24 * time.Hour
	}

	if cfg.Artifacts.Cache.Dir == "" {
		cfg.Artifacts.Cache.Dir = "/var/lib/dispatch/cache"
	}

	// Only filled in when the model is on. A zero CPUOvercommit on a
	// disabled config must stay zero, so a later `enabled: true` in YAML
	// cannot be silently reinterpreted as "someone chose these numbers".
	if cfg.Resources.Enabled {
		if cfg.Resources.CPUOvercommit <= 0 {
			cfg.Resources.CPUOvercommit = resource.DefaultCPUOvercommit
		}

		if cfg.Resources.MemoryFraction <= 0 {
			cfg.Resources.MemoryFraction = resource.DefaultMemoryFraction
		}
	}

	return cfg
}

// mergeConfigurations merges YAML config with programmatic options.
// YAML config takes precedence for most fields; programmatic bool flags fill gaps.
func (e *Extension) mergeConfigurations(yamlConfig, programmaticConfig Config) Config {
	if yamlConfig.Security.InstallationID == "" {
		yamlConfig.Security.InstallationID = programmaticConfig.Security.InstallationID
	}
	if yamlConfig.Security.PolicyTenant == "" {
		yamlConfig.Security.PolicyTenant = programmaticConfig.Security.PolicyTenant
	}
	if len(yamlConfig.Security.Providers) == 0 {
		yamlConfig.Security.Providers = append([]string(nil), programmaticConfig.Security.Providers...)
	}
	if programmaticConfig.Durable.Enabled {
		yamlConfig.Durable.Enabled = true
	}
	if yamlConfig.Durable.Namespace == "" {
		yamlConfig.Durable.Namespace = programmaticConfig.Durable.Namespace
	}
	if yamlConfig.Durable.Queue == "" {
		yamlConfig.Durable.Queue = programmaticConfig.Durable.Queue
	}
	if yamlConfig.Durable.BuildID == "" {
		yamlConfig.Durable.BuildID = programmaticConfig.Durable.BuildID
	}
	if yamlConfig.Durable.Owner == "" {
		yamlConfig.Durable.Owner = programmaticConfig.Durable.Owner
	}
	if yamlConfig.Durable.LeaseDuration == 0 {
		yamlConfig.Durable.LeaseDuration = programmaticConfig.Durable.LeaseDuration
	}
	if yamlConfig.Durable.PollInterval == 0 {
		yamlConfig.Durable.PollInterval = programmaticConfig.Durable.PollInterval
	}
	if yamlConfig.Durable.StoreTimeout == 0 {
		yamlConfig.Durable.StoreTimeout = programmaticConfig.Durable.StoreTimeout
	}
	if yamlConfig.Durable.Concurrency == 0 {
		yamlConfig.Durable.Concurrency = programmaticConfig.Durable.Concurrency
	}
	// Programmatic bool flags override when true.
	if programmaticConfig.DisableRoutes {
		yamlConfig.DisableRoutes = true
	}
	if programmaticConfig.DisableMigrate {
		yamlConfig.DisableMigrate = true
	}

	if programmaticConfig.EnableDWP {
		yamlConfig.EnableDWP = true
	}

	if programmaticConfig.Artifacts.Enabled {
		yamlConfig.Artifacts.Enabled = true
	}

	if yamlConfig.Artifacts.TroveStore == "" && programmaticConfig.Artifacts.TroveStore != "" {
		yamlConfig.Artifacts.TroveStore = programmaticConfig.Artifacts.TroveStore
	}

	if yamlConfig.Artifacts.Cache.Dir == "" && programmaticConfig.Artifacts.Cache.Dir != "" {
		yamlConfig.Artifacts.Cache.Dir = programmaticConfig.Artifacts.Cache.Dir
	}

	if yamlConfig.Artifacts.Cache.Budget == 0 && programmaticConfig.Artifacts.Cache.Budget != 0 {
		yamlConfig.Artifacts.Cache.Budget = programmaticConfig.Artifacts.Cache.Budget
	}

	yamlConfig.Resources = mergeResourceConfig(yamlConfig.Resources, programmaticConfig.Resources)
	yamlConfig.Execution = mergeExecutionConfig(yamlConfig.Execution, programmaticConfig.Execution)

	// String fields: YAML takes precedence.
	if yamlConfig.BasePath == "" && programmaticConfig.BasePath != "" {
		yamlConfig.BasePath = programmaticConfig.BasePath
	}
	if yamlConfig.GroveDatabase == "" && programmaticConfig.GroveDatabase != "" {
		yamlConfig.GroveDatabase = programmaticConfig.GroveDatabase
	}
	if yamlConfig.GroveKV == "" && programmaticConfig.GroveKV != "" {
		yamlConfig.GroveKV = programmaticConfig.GroveKV
	}
	if yamlConfig.KeyPrefix == "" && programmaticConfig.KeyPrefix != "" {
		yamlConfig.KeyPrefix = programmaticConfig.KeyPrefix
	}
	if yamlConfig.DWPBasePath == "" && programmaticConfig.DWPBasePath != "" {
		yamlConfig.DWPBasePath = programmaticConfig.DWPBasePath
	}

	// Fill remaining zeros with defaults.
	return e.mergeWithDefaults(yamlConfig)
}

// resolveGroveDB resolves a *grove.DB from the DI container.
// If GroveDatabase is set, it looks up the named DB; otherwise it uses the default.
func (e *Extension) resolveGroveDB(fapp forge.App) (*grove.DB, error) {
	if e.config.GroveDatabase != "" {
		db, err := vessel.InjectNamed[*grove.DB](fapp.Container(), e.config.GroveDatabase)
		if err != nil {
			return nil, fmt.Errorf("grove database %q not found in container: %w", e.config.GroveDatabase, err)
		}
		return db, nil
	}
	db, err := vessel.Inject[*grove.DB](fapp.Container())
	if err != nil {
		return nil, fmt.Errorf("default grove database not found in container: %w", err)
	}
	return db, nil
}

// buildStoreFromGroveDB constructs the appropriate store backend
// based on the grove driver type (pg, sqlite, mongo).
func (e *Extension) buildStoreFromGroveDB(db *grove.DB) (dispatch.Storer, error) {
	driverName := db.Driver().Name()
	switch driverName {
	case "pg":
		return pgstore.New(db), nil
	case "sqlite":
		return sqlitestore.New(db), nil
	case "mongo":
		return mongostore.New(db), nil
	default:
		return nil, fmt.Errorf("dispatch: unsupported grove driver %q", driverName)
	}
}

// buildStoreFromGroveKV wraps a grove KV store in the Redis backend,
// namespaced by the configured key prefix.
func (e *Extension) buildStoreFromGroveKV(kvStore *kv.Store) dispatch.Storer {
	return redisstore.New(kvStore, redisstore.WithKeyPrefix(e.config.KeyPrefix))
}

// resolveGroveKV resolves a *kv.Store from the DI container.
// If GroveKV is set, it looks up the named store; otherwise it uses the default.
func (e *Extension) resolveGroveKV(fapp forge.App) (*kv.Store, error) {
	if e.config.GroveKV != "" {
		s, err := vessel.InjectNamed[*kv.Store](fapp.Container(), e.config.GroveKV)
		if err != nil {
			return nil, fmt.Errorf("grove kv store %q not found in container: %w", e.config.GroveKV, err)
		}
		return s, nil
	}
	s, err := vessel.Inject[*kv.Store](fapp.Container())
	if err != nil {
		return nil, fmt.Errorf("default grove kv store not found in container: %w", err)
	}
	return s, nil
}
