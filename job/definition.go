package job

import "context"

// Definition is a typed job definition with a handler function.
// T is the payload type (must be JSON-serializable).
type Definition[T any] struct {
	// Name is the unique identifier for this job type.
	Name string

	// Handler is the function that processes the job payload.
	Handler func(ctx context.Context, payload T) error

	// Opts configures retries, queue, priority, and timeout.
	Opts Options
}

// NewDefinition creates a typed job definition.
func NewDefinition[T any](name string, handler func(ctx context.Context, payload T) error, opts ...Option) *Definition[T] {
	def := &Definition[T]{
		Name:    name,
		Handler: handler,
		Opts:    DefaultOptions(),
	}
	for _, opt := range opts {
		opt(&def.Opts)
	}
	return def
}
