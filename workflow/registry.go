package workflow

import (
	"encoding/json"
	"fmt"
	"sync"
)

// RunnerFunc is a type-erased workflow runner that accepts raw JSON input.
// The typed Definition[T] is converted to a RunnerFunc at registration
// time by closing over JSON unmarshal + the typed handler.
type RunnerFunc func(wf *Workflow, input []byte) error

// Registry maps workflow names to type-erased runner functions.
// It is safe for concurrent use.
type Registry struct {
	mu      sync.RWMutex
	runners map[string]RunnerFunc
}

// NewRegistry creates an empty workflow registry.
func NewRegistry() *Registry {
	return &Registry{
		runners: make(map[string]RunnerFunc),
	}
}

// RegisterDefinition registers a typed workflow definition. The generic
// handler is wrapped in a closure that JSON-unmarshals the input into T
// before calling the typed handler.
//
// This is a package-level generic function because Go does not allow
// generic methods on non-generic receiver types.
func RegisterDefinition[T any](r *Registry, def *Definition[T]) {
	runner := func(wf *Workflow, input []byte) error {
		var t T
		if len(input) > 0 {
			if err := json.Unmarshal(input, &t); err != nil {
				return fmt.Errorf("unmarshal input for workflow %q: %w", def.Name, err)
			}
		}
		return def.Handler(wf, t)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.runners[def.Name] = runner
}

// Get returns the runner for the given workflow name.
// Returns false if no runner is registered.
func (r *Registry) Get(name string) (RunnerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.runners[name]
	return fn, ok
}

// Names returns all registered workflow names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.runners))
	for name := range r.runners {
		names = append(names, name)
	}
	return names
}
