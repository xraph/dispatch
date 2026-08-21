package staging

import (
	"context"
	"errors"
	"fmt"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
)

// SpecLookup resolves a job name to its declared inputs. The engine
// supplies one backed by the job registry.
type SpecLookup func(jobName string) []artifact.InputSpec

// Options configures the middleware.
type Options struct {
	Logger log.Logger
}

// Option configures the middleware.
type Option func(*Options)

// WithLogger sets the logger.
func WithLogger(l log.Logger) Option {
	return func(o *Options) { o.Logger = l }
}

// Middleware stages a job's declared inputs, exposes them through an
// artifact.Accessor on the context, and releases every cache lease when
// the handler returns.
//
// It slots into the existing middleware chain, so nothing in the executor
// changes. That also makes it the natural boundary for out-of-process
// execution later: staging runs outside, and the handler sees files
// rather than credentials.
func Middleware(
	svc *artifact.Service,
	c *cache.Cache,
	lookup SpecLookup,
	opts ...Option,
) middleware.Middleware {
	cfg := Options{Logger: log.NewNoopLogger()}
	for _, opt := range opts {
		opt(&cfg)
	}

	return func(ctx context.Context, j *job.Job, next middleware.Handler) error {
		if svc == nil || !svc.Enabled() {
			return next(ctx)
		}

		specs := lookup(j.Name)

		bindings, err := GetBindings(j)
		if err != nil {
			return err
		}

		if len(specs) == 0 && len(bindings) == 0 {
			// Nothing declared and nothing bound. Still hand the handler an
			// accessor so it can create outputs.
			return next(artifact.WithAccessor(ctx, newAccessor(svc, j, nil)))
		}

		if verr := Validate(specs, bindings); verr != nil {
			return verr
		}

		staged, release, serr := stageInputs(ctx, svc, c, specs, bindings, cfg.Logger)

		// Release before returning whatever happens — including a panic
		// unwinding through here — or a failed job would pin cache entries
		// until the process restarted.
		defer release()

		if serr != nil {
			return serr
		}

		return next(artifact.WithAccessor(ctx, newAccessor(svc, j, staged)))
	}
}

// newAccessor builds the handler-facing accessor for a job.
//
// The attempt comes from RetryCount, which is what scopes committed
// outputs to this execution and lets IfAbsent see earlier ones.
func newAccessor(svc *artifact.Service, j *job.Job, inputs map[string]staged) artifact.Accessor {
	if inputs == nil {
		inputs = map[string]staged{}
	}

	return &accessor{
		svc:     svc,
		owner:   artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()},
		attempt: j.RetryCount,
		inputs:  inputs,
	}
}

// stageInputs materialises every declared input, returning the staged set
// and a release function that is safe to call even after a failure.
func stageInputs(
	ctx context.Context,
	svc *artifact.Service,
	c *cache.Cache,
	specs []artifact.InputSpec,
	bindings Bindings,
	logger log.Logger,
) (inputs map[string]staged, release func(), err error) {
	out := make(map[string]staged, len(bindings))

	var releases []func()

	release = func() {
		for _, r := range releases {
			r()
		}
	}

	for _, spec := range specs {
		ref, ok := bindings[spec.Name]
		if !ok {
			// Absent and optional: Validate already rejected the required case.
			continue
		}

		if spec.Mode == artifact.StageModeLazy || c == nil {
			out[spec.Name] = staged{ref: ref, mode: spec.Mode}

			continue
		}

		path, hash, rel, err := c.Stage(ctx, ref)
		if err != nil {
			// Preserve the permanent classification so the executor fails
			// the job fast rather than retrying a fetch that can never
			// succeed. A deleted input and one we are not authorized to
			// read are equally hopeless.
			if errors.Is(err, dispatch.ErrPermanent) {
				return nil, release, fmt.Errorf("stage input %q: %w", spec.Name, err)
			}

			return nil, release, fmt.Errorf("dispatch/artifact/staging: stage input %q: %w", spec.Name, err)
		}

		releases = append(releases, rel)
		out[spec.Name] = staged{ref: ref, path: path, mode: spec.Mode}

		recordHash(ctx, svc, ref, hash, logger)
	}

	return out, release, nil
}

// recordHash persists a content hash learned during staging.
//
// Registration deliberately skips hashing to keep enqueue cheap, so this
// is where an artifact's content_hash gets filled in. It is best effort:
// failing to record it costs a future dedupe opportunity, never
// correctness, so it must not fail the job.
func recordHash(
	ctx context.Context,
	svc *artifact.Service,
	ref artifact.Ref,
	hash string,
	logger log.Logger,
) {
	if hash == "" || ref.ContentHash == hash || ref.ID.IsNil() {
		return
	}

	a, err := svc.Store().GetArtifact(ctx, ref.ID)
	if err != nil {
		logger.Debug("dispatch/artifact/staging: could not load artifact to record hash",
			log.String("artifact_id", ref.ID.String()),
			log.String("error", err.Error()),
		)

		return
	}

	if a.ContentHash == hash {
		return
	}

	a.ContentHash = hash

	if err := svc.Store().UpdateArtifact(ctx, a); err != nil {
		logger.Debug("dispatch/artifact/staging: could not record content hash",
			log.String("artifact_id", ref.ID.String()),
			log.String("error", err.Error()),
		)
	}
}

// LinkInputs records which artifacts a job consumed, so lineage survives
// after the job finishes.
func LinkInputs(ctx context.Context, svc *artifact.Service, j *job.Job, bindings Bindings) error {
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}

	for name, ref := range bindings {
		if err := svc.Link(ctx, ref, owner, artifact.RoleInput, name, j.RetryCount); err != nil {
			return err
		}
	}

	return nil
}
