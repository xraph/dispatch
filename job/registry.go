package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/resource"
)

// ResourceDecl is what a definition declares about its resource needs,
// captured at registration.
//
// The declaration has to be reachable by job name because the typed
// definition is gone by the time a job is enqueued through EnqueueRaw,
// exactly as with Inputs.
type ResourceDecl struct {
	// Requests is the declared requirement. It is a floor: the engine may
	// raise it, and a per-enqueue override replaces it per key.
	Requests resource.Set
	// Limits is the declared enforcement ceiling, if any.
	Limits resource.Set
	// Func computes the requirement from the enqueue-time request.
	Func resource.ResourceFunc
	// Class is the opaque scheduling class for the isolation backend.
	Class string
}

// IsZero reports whether the definition declares nothing about
// resources, which is how every job behaves before this feature is used.
func (d ResourceDecl) IsZero() bool {
	return d.Requests.IsZero() && d.Limits.IsZero() && d.Func == nil && d.Class == ""
}

// HandlerFunc is a type-erased job handler that accepts raw JSON payload.
// The typed Definition[T] is converted to a HandlerFunc at registration
// time by closing over JSON unmarshal + the typed handler.
type HandlerFunc func(ctx context.Context, payload []byte) error

// Registry maps job names to type-erased handler functions.
// It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc

	// inputs holds each job's artifact declarations. The staging
	// middleware needs them keyed by job name, because by the time a job
	// is executing the typed definition is long gone.
	inputs map[string][]artifact.InputSpec

	// resources holds each job's resource declaration, for the same
	// reason: enqueue works from a job name and a payload.
	resources map[string]ResourceDecl
}

// NewRegistry creates an empty job registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers:  make(map[string]HandlerFunc),
		inputs:    make(map[string][]artifact.InputSpec),
		resources: make(map[string]ResourceDecl),
	}
}

// RegisterDefinition registers a typed job definition. The generic handler
// is wrapped in a closure that JSON-unmarshals the payload into T before
// calling the typed handler.
//
// This is a package-level generic function because Go does not allow
// generic methods on non-generic receiver types.
func RegisterDefinition[T any](r *Registry, def *Definition[T]) {
	handler := func(ctx context.Context, payload []byte) error {
		var t T
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &t); err != nil {
				return fmt.Errorf("unmarshal payload for job %q: %w", def.Name, err)
			}
		}
		return def.Handler(ctx, t)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[def.Name] = handler

	if len(def.Opts.Inputs) > 0 {
		specs := make([]artifact.InputSpec, len(def.Opts.Inputs))
		copy(specs, def.Opts.Inputs)
		r.inputs[def.Name] = specs
	}

	// The sets are cloned, not aliased: a definition's Options stay
	// reachable by the caller, and a resolved requirement must not change
	// under a job that was already enqueued.
	decl := ResourceDecl{
		Requests: def.Opts.Resources.Clone(),
		Limits:   def.Opts.ResourceLimits.Clone(),
		Func:     def.Opts.ResourceFunc,
		Class:    def.Opts.ResourceClass,
	}

	if !decl.IsZero() {
		r.resources[def.Name] = decl
	}
}

// Resources returns the resource declaration for a job, or the zero
// ResourceDecl when it declares none.
func (r *Registry) Resources(name string) ResourceDecl {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.resources[name]
}

// Inputs returns the artifact declarations for a job, or nil when it
// declares none.
func (r *Registry) Inputs(name string) []artifact.InputSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.inputs[name]
}

// Get returns the handler for the given job name.
// Returns false if no handler is registered.
func (r *Registry) Get(name string) (HandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

// Names returns all registered job names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}
