package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// HandlerFunc is a type-erased job handler that accepts raw JSON payload.
// The typed Definition[T] is converted to a HandlerFunc at registration
// time by closing over JSON unmarshal + the typed handler.
type HandlerFunc func(ctx context.Context, payload []byte) error

// Registry maps job names to type-erased handler functions.
// It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewRegistry creates an empty job registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
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
