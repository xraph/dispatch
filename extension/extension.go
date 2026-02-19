package extension

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/xraph/forge"
	"github.com/xraph/vessel"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	mw "github.com/xraph/dispatch/middleware"
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
	config       Config
	eng          *engine.Engine
	apiHandler   *api.API
	logger       *slog.Logger
	dispatchOpts []dispatch.Option
	exts         []ext.Extension
	mws          []mw.Middleware
	bo           backoff.Strategy
}

// New creates a Dispatch Forge extension with the given options.
func New(opts ...ExtOption) *Extension {
	e := &Extension{}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Name returns the extension name.
func (e *Extension) Name() string { return ExtensionName }

// Description returns the extension description.
func (e *Extension) Description() string { return ExtensionDescription }

// Version returns the extension version.
func (e *Extension) Version() string { return ExtensionVersion }

// Dependencies returns the list of extension names this extension depends on.
func (e *Extension) Dependencies() []string { return []string{} }

// Engine returns the underlying dispatch engine.
// This is nil until Register is called.
func (e *Extension) Engine() *engine.Engine { return e.eng }

// API returns the API handler.
func (e *Extension) API() *api.API { return e.apiHandler }

// Register implements [forge.Extension]. It initializes the dispatcher,
// builds the engine, and optionally registers HTTP routes.
func (e *Extension) Register(fapp forge.App) error {
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

	return e.eng.Start(ctx)
}

// Stop gracefully shuts down the dispatch engine.
func (e *Extension) Stop(ctx context.Context) error {
	if e.eng == nil {
		return nil
	}
	return e.eng.Stop(ctx)
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
