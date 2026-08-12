package engine

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/artifact/staging"
	"github.com/xraph/dispatch/job"
)

// WithArtifacts enables the artifact plane.
//
// The staging middleware is appended to the chain, so declared inputs are
// materialised before a handler runs and every cache lease is released
// afterwards. Without this option Dispatch behaves exactly as it did
// before artifacts existed.
func WithArtifacts(svc *artifact.Service, c *cache.Cache) Option {
	return func(eng *Engine) {
		if svc == nil || !svc.Enabled() {
			return
		}

		eng.artifacts = svc
		eng.artifactCache = c

		eng.mws = append(eng.mws, staging.Middleware(svc, c, func(name string) []artifact.InputSpec {
			return eng.registry.Inputs(name)
		}, staging.WithLogger(eng.logger)))
	}
}

// Artifacts returns the artifact service, or nil when the plane is off.
func (eng *Engine) Artifacts() *artifact.Service { return eng.artifacts }

// ArtifactCache returns the staging cache, or nil when the plane is off.
func (eng *Engine) ArtifactCache() *cache.Cache { return eng.artifactCache }

// ValidateArtifactInputs checks a definition's declarations against the
// engine's staging capacity.
//
// A definition whose declared inputs could never fit the cache budget is
// rejected here, at registration, rather than failing every time such a
// job runs. The point is to surface the mistake on a developer's machine
// instead of at 3am in production.
func (eng *Engine) ValidateArtifactInputs(name string, specs []artifact.InputSpec) error {
	if len(specs) == 0 {
		return nil
	}

	if err := artifact.ValidateInputs(specs); err != nil {
		return fmt.Errorf("job %q: %w", name, err)
	}

	if eng.artifacts == nil {
		return fmt.Errorf(
			"job %q declares artifact inputs but no artifact backend is configured: %w",
			name, artifact.ErrNoBackend)
	}

	if eng.artifactCache == nil {
		return nil
	}

	// A zero total means at least one declaration is unbounded, so the
	// requirement is unknown rather than known-too-large.
	total := artifact.TotalMaxSize(specs)
	if total == 0 {
		return nil
	}

	if budget := eng.artifactCache.Budget(); total > budget {
		return fmt.Errorf(
			"job %q declares up to %d bytes of artifact inputs, which exceeds the %d byte staging budget: %w",
			name, total, budget, cache.ErrBudgetExceeded)
	}

	return nil
}

// Bind attaches an artifact to a declared input at enqueue.
//
// Bindings are validated against the job's declarations before the job is
// persisted, so an oversized or undeclared input is a caller error rather
// than a job that fails on a worker.
func Bind(name string, ref artifact.Ref) job.Option {
	return func(o *job.Options) {
		if o.Bindings == nil {
			o.Bindings = make(map[string]artifact.Ref)
		}

		o.Bindings[name] = ref
	}
}

// applyBindings validates a job's bindings and records them on the job.
func (eng *Engine) applyBindings(_ context.Context, j *job.Job, bindings map[string]artifact.Ref) error {
	if len(bindings) == 0 {
		return nil
	}

	if eng.artifacts == nil {
		return fmt.Errorf("job %q binds artifacts but the artifact plane is not enabled: %w",
			j.Name, artifact.ErrNoBackend)
	}

	specs := eng.registry.Inputs(j.Name)

	if err := staging.Validate(specs, bindings); err != nil {
		return fmt.Errorf("job %q: %w", j.Name, err)
	}

	return staging.SetBindings(j, bindings)
}
