package extension

import (
	"fmt"

	"github.com/xraph/forge"
	trovelib "github.com/xraph/trove"
	"github.com/xraph/vessel"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/cache"
	troveadapter "github.com/xraph/dispatch/artifact/trove"
)

// resolveArtifactBackend finds the object store backing the artifact
// plane, in three tiers: an explicitly supplied backend, a named Trove
// store from configuration, then the default Trove instance in the
// container.
//
// This mirrors how resolveGroveDB discovers a database, and shares its
// most important property: when nothing is found it returns nil rather
// than an error, so an application that never asked for artifacts runs
// exactly as it did before.
//
// Trove's own extension registers *trove.Trove both unnamed and once per
// named store, so multi-store setups are covered by the named lookup
// without Dispatch importing trove/extension at all.
func (e *Extension) resolveArtifactBackend(fapp forge.App) (artifact.Backend, error) {
	if e.artifactBackend != nil {
		return e.artifactBackend, nil
	}

	if !e.config.Artifacts.Enabled {
		return nil, nil //nolint:nilnil // no backend is a valid, disabled state
	}

	opts := []troveadapter.Option{}

	if name := e.config.Artifacts.TroveStore; name != "" {
		t, err := vessel.InjectNamed[*trovelib.Trove](fapp.Container(), name)
		if err != nil {
			return nil, fmt.Errorf("dispatch: trove store %q not found in container: %w", name, err)
		}

		return troveadapter.New(t, append(opts, troveadapter.WithName(name))...), nil
	}

	t, err := vessel.Inject[*trovelib.Trove](fapp.Container())
	if err != nil {
		// Artifacts were requested but no Trove is mounted. That is a
		// configuration mistake worth reporting rather than silently
		// disabling a feature the operator asked for.
		return nil, fmt.Errorf(
			"dispatch: artifacts are enabled but no *trove.Trove is registered in the container; "+
				"mount the trove extension or supply a backend with WithArtifactBackend: %w", err)
	}

	e.Logger().Info("dispatch: auto-discovered trove from container")

	return troveadapter.New(t, opts...), nil
}

// buildArtifactPlane constructs the artifact service and staging cache,
// returning nils when no backend is configured.
func (e *Extension) buildArtifactPlane(fapp forge.App) (*artifact.Service, *cache.Cache, error) {
	backend, err := e.resolveArtifactBackend(fapp)
	if err != nil {
		return nil, nil, err
	}

	if backend == nil {
		return nil, nil, nil
	}

	cfg := e.config.Artifacts

	svc := artifact.NewService(e.artifactStore, backend,
		artifact.WithDefaultBucket(cfg.Bucket),
		artifact.WithEphemeralPrefix(cfg.EphemeralPrefix),
		artifact.WithRetention(cfg.Retention),
	)

	cacheOpts := []cache.Option{}
	if e.logger != nil {
		cacheOpts = append(cacheOpts, cache.WithLogger(e.logger))
	}
	if cfg.Cache.Budget > 0 {
		cacheOpts = append(cacheOpts, cache.WithBudget(cfg.Cache.Budget))
	}

	c, err := cache.New(cfg.Cache.Dir, backend, cacheOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("dispatch: create staging cache: %w", err)
	}

	if perr := vessel.Provide(fapp.Container(), func() (*artifact.Service, error) {
		return svc, nil
	}); perr != nil {
		return nil, nil, fmt.Errorf("dispatch: register artifact service in container: %w", perr)
	}

	if perr := vessel.Provide(fapp.Container(), func() (*cache.Cache, error) {
		return c, nil
	}); perr != nil {
		return nil, nil, fmt.Errorf("dispatch: register staging cache in container: %w", perr)
	}

	return svc, c, nil
}
