package extension_test

import (
	"context"
	"testing"
	"time"

	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// ──────────────────────────────────────────────────
// Metadata
// ──────────────────────────────────────────────────

func TestExtension_Metadata(t *testing.T) {
	ext := extension.New()

	if ext.Name() != extension.ExtensionName {
		t.Errorf("Name() = %q, want %q", ext.Name(), extension.ExtensionName)
	}
	if ext.Description() != extension.ExtensionDescription {
		t.Errorf("Description() = %q, want %q", ext.Description(), extension.ExtensionDescription)
	}
	if ext.Version() != extension.ExtensionVersion {
		t.Errorf("Version() = %q, want %q", ext.Version(), extension.ExtensionVersion)
	}
	if deps := ext.Dependencies(); len(deps) != 0 {
		t.Errorf("Dependencies() = %v, want empty", deps)
	}
}

// ──────────────────────────────────────────────────
// Register → Engine + API initialized
// ──────────────────────────────────────────────────

func TestExtension_Register(t *testing.T) {
	s := memory.New()
	ext := extension.New(
		extension.WithStore(s),
	)

	fapp := forgetesting.NewTestApp("test-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if ext.Engine() == nil {
		t.Fatal("expected engine to be initialized after Register")
	}
	if ext.API() == nil {
		t.Fatal("expected API handler to be initialized after Register")
	}
}

// ──────────────────────────────────────────────────
// Full lifecycle: Register → Start → Health → Stop
// ──────────────────────────────────────────────────

func TestExtension_Lifecycle(t *testing.T) {
	s := memory.New()
	ext := extension.New(
		extension.WithStore(s),
		extension.WithConcurrency(2),
		extension.WithQueues([]string{"default"}),
	)

	fapp := forgetesting.NewTestApp("lifecycle-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Start — runs migration and starts the engine.
	ctx := context.Background()
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Health should pass.
	if err := ext.Health(ctx); err != nil {
		t.Errorf("Health: %v", err)
	}

	// Stop gracefully.
	stopCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := ext.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ──────────────────────────────────────────────────
// Register + Enqueue via engine
// ──────────────────────────────────────────────────

func TestExtension_RegisterAndEnqueue(t *testing.T) {
	s := memory.New()
	ext := extension.New(
		extension.WithStore(s),
	)

	fapp := forgetesting.NewTestApp("enqueue-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	eng := ext.Engine()
	engine.Register(eng, job.NewDefinition("test-job", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	j, err := engine.Enqueue(context.Background(), eng, "test-job", struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if j.Name != "test-job" {
		t.Errorf("job.Name = %q, want %q", j.Name, "test-job")
	}
	if j.State != job.StatePending {
		t.Errorf("job.State = %q, want %q", j.State, job.StatePending)
	}
}

// ──────────────────────────────────────────────────
// Start before Register fails
// ──────────────────────────────────────────────────

func TestExtension_StartBeforeRegister(t *testing.T) {
	ext := extension.New()

	err := ext.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when starting before Register")
	}
}

// ──────────────────────────────────────────────────
// Health before Register fails
// ──────────────────────────────────────────────────

func TestExtension_HealthBeforeRegister(t *testing.T) {
	ext := extension.New()

	err := ext.Health(context.Background())
	if err == nil {
		t.Fatal("expected error when checking health before Register")
	}
}

// ──────────────────────────────────────────────────
// Stop before Register is safe (no-op)
// ──────────────────────────────────────────────────

func TestExtension_StopBeforeRegister(t *testing.T) {
	ext := extension.New()

	err := ext.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop before Register should be no-op, got: %v", err)
	}
}

// ──────────────────────────────────────────────────
// Register without store fails
// ──────────────────────────────────────────────────

func TestExtension_RegisterNoStore(t *testing.T) {
	ext := extension.New()
	fapp := forgetesting.NewTestApp("no-store-app", "0.1.0")

	err := ext.Register(fapp)
	if err == nil {
		t.Fatal("expected error when registering without a store")
	}
}

// ──────────────────────────────────────────────────
// DisableRoutes option
// ──────────────────────────────────────────────────

func TestExtension_DisableRoutes(t *testing.T) {
	s := memory.New()
	ext := extension.New(
		extension.WithStore(s),
		extension.WithDisableRoutes(),
	)

	fapp := forgetesting.NewTestApp("no-routes-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Engine should be initialized even without routes.
	if ext.Engine() == nil {
		t.Fatal("expected engine even with DisableRoutes")
	}
}

// ──────────────────────────────────────────────────
// DisableMigrate option
// ──────────────────────────────────────────────────

func TestExtension_DisableMigrate(t *testing.T) {
	s := memory.New()
	ext := extension.New(
		extension.WithStore(s),
		extension.WithDisableMigrate(),
	)

	fapp := forgetesting.NewTestApp("no-migrate-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := context.Background()
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := ext.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ──────────────────────────────────────────────────
// WithConfig option
// ──────────────────────────────────────────────────

func TestExtension_WithConfig(t *testing.T) {
	s := memory.New()
	ext := extension.New(
		extension.WithStore(s),
		extension.WithConfig(extension.Config{
			BasePath:       "/custom",
			DisableRoutes:  true,
			DisableMigrate: true,
			Dispatch:       dispatch.Config{Concurrency: 8},
		}),
	)

	fapp := forgetesting.NewTestApp("config-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if ext.Engine() == nil {
		t.Fatal("expected engine with custom config")
	}
}

// ──────────────────────────────────────────────────
// Handler returns working HTTP handler (standalone)
// ──────────────────────────────────────────────────

func TestExtension_Handler(t *testing.T) {
	s := memory.New()
	ext := extension.New(
		extension.WithStore(s),
		extension.WithDisableRoutes(), // Disable auto-registration so Handler() can register.
	)

	fapp := forgetesting.NewTestApp("handler-app", "0.1.0")

	if err := ext.Register(fapp); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h := ext.Handler()
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}

// ──────────────────────────────────────────────────
// Handler before Register returns NotFound
// ──────────────────────────────────────────────────

func TestExtension_HandlerBeforeRegister(t *testing.T) {
	ext := extension.New()

	h := ext.Handler()
	if h == nil {
		t.Fatal("expected non-nil handler even before Register (should be NotFoundHandler)")
	}
}
