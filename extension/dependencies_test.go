package extension_test

import (
	"context"
	"slices"
	"testing"

	"github.com/xraph/forge"
	forgetesting "github.com/xraph/forge/testing"
	"github.com/xraph/vessel"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/store/memory"
)

// fakeGrove stands in for the grove extension. It only records whether
// Dispatch had already registered its engine by the time grove's own
// Register ran, which is the ordering the dependency exists to prevent.
type fakeGrove struct {
	*forge.BaseExtension
	dispatchFirst bool
}

func newFakeGrove() *fakeGrove {
	return &fakeGrove{BaseExtension: forge.NewBaseExtension(extension.GroveExtensionName, "0.0.0", "fake grove")}
}

func (g *fakeGrove) Register(app forge.App) error {
	if err := g.BaseExtension.Register(app); err != nil {
		return err
	}
	if _, err := vessel.Inject[*engine.Engine](app.Container()); err == nil {
		g.dispatchFirst = true
	}
	return nil
}

func TestExtension_Dependencies_KeepsSetDependencies(t *testing.T) {
	ext := extension.New()
	ext.SetDependencies([]string{"other"})

	deps := ext.Dependencies()
	if !slices.Equal(deps, []string{"other", extension.GroveExtensionName}) {
		t.Fatalf("Dependencies() = %v, want [other %s]", deps, extension.GroveExtensionName)
	}

	ext.SetDependencies([]string{extension.GroveExtensionName})
	if deps := ext.Dependencies(); len(deps) != 1 {
		t.Fatalf("Dependencies() = %v, want grove listed once", deps)
	}
}

// Dispatch is registered before grove on purpose. Without the declared
// dependency Forge keeps registration order and Dispatch would run first.
func TestExtension_StartsAfterGrove(t *testing.T) {
	app := forgetesting.NewTestApp("dispatch-after-grove", "0.1.0")
	grove := newFakeGrove()

	if err := app.RegisterExtension(extension.New(extension.WithStore(memory.New()))); err != nil {
		t.Fatalf("register dispatch: %v", err)
	}
	if err := app.RegisterExtension(grove); err != nil {
		t.Fatalf("register grove: %v", err)
	}

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	if grove.dispatchFirst {
		t.Fatal("dispatch registered before grove; the grove dependency was not honoured")
	}
}

// An app that never registers grove must still start: the declared
// dependency is optional as far as Forge is concerned.
func TestExtension_StartsWithoutGrove(t *testing.T) {
	app := forgetesting.NewTestApp("dispatch-without-grove", "0.1.0")

	if err := app.RegisterExtension(extension.New(extension.WithStore(memory.New()))); err != nil {
		t.Fatalf("register dispatch: %v", err)
	}

	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start without grove: %v", err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })
}
