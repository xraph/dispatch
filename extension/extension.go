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
	"log/slog"
	"net/http"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/grove/kv"
	"github.com/xraph/vessel"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	mw "github.com/xraph/dispatch/middleware"
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

	config       Config
	eng          *engine.Engine
	apiHandler   *api.API
	dwpServer    *dwp.Server
	logger       *slog.Logger
	dispatchOpts []dispatch.Option
	exts         []ext.Extension
	mws          []mw.Middleware
	dwpOpts      []dwp.Option
	bo           backoff.Strategy
	useGrove     bool
	useGroveKV   bool
	enableDWP    bool
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
		e.dispatchOpts = append(e.dispatchOpts, dispatch.WithStore(redisstore.New(kvStore)))
	}

	logger := e.logger
	if logger == nil {
		logger = slog.Default()
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

	e.eng, err = engine.Build(d, engOpts...)
	if err != nil {
		return fmt.Errorf("dispatch: build engine: %w", err)
	}

	// Create the API handler.
	e.apiHandler = api.New(e.eng, fapp.Router())

	// Register HTTP routes unless disabled.
	if !e.config.DisableRoutes {
		e.apiHandler.RegisterRoutes(fapp.Router())
	}

	// Create DWP server if stream broker is available.
	if e.eng.StreamBroker() != nil {
		dwpOptList := make([]dwp.Option, 0, len(e.dwpOpts)+2)
		dwpOptList = append(dwpOptList, dwp.WithLogger(logger))
		if e.config.DWPBasePath != "" {
			dwpOptList = append(dwpOptList, dwp.WithPath(e.config.DWPBasePath))
		}
		dwpOptList = append(dwpOptList, e.dwpOpts...)

		handler := dwp.NewHandler(e.eng, e.eng.StreamBroker(), logger)
		e.dwpServer = dwp.NewServer(e.eng.StreamBroker(), handler, dwpOptList...)

		if !e.config.DisableRoutes {
			e.dwpServer.RegisterRoutes(fapp.Router())
		}
	}

	return nil
}

// Start begins job processing and runs auto-migration if enabled.
func (e *Extension) Start(ctx context.Context) error {
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

	if err := e.eng.Start(ctx); err != nil {
		return err
	}

	e.MarkStarted()
	return nil
}

// Stop gracefully shuts down the dispatch engine.
func (e *Extension) Stop(ctx context.Context) error {
	if e.eng == nil {
		e.MarkStopped()
		return nil
	}
	err := e.eng.Stop(ctx)
	e.MarkStopped()
	return err
}

// Health implements [forge.Extension].
func (e *Extension) Health(ctx context.Context) error {
	if e.eng == nil {
		return errors.New("dispatch: extension not initialized")
	}

	store := e.eng.Dispatcher().Store()
	if store == nil {
		return errors.New("dispatch: no store configured")
	}

	return store.Ping(ctx)
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
func (e *Extension) RegisterRoutes(router forge.Router) {
	if e.apiHandler != nil {
		e.apiHandler.RegisterRoutes(router)
	}
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
	return cfg
}

// mergeConfigurations merges YAML config with programmatic options.
// YAML config takes precedence for most fields; programmatic bool flags fill gaps.
func (e *Extension) mergeConfigurations(yamlConfig, programmaticConfig Config) Config {
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
