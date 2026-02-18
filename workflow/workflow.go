// Package workflow defines workflow definitions, runs, steps, checkpoints,
// and the workflow store interface.
package workflow

// Definition is a typed workflow definition with a handler function.
// T is the input type (must be JSON-serializable for Run.Input storage).
type Definition[T any] struct {
	// Name is the unique identifier for this workflow type.
	Name string

	// Handler is the function that executes the workflow logic.
	// It receives a *Workflow which provides Step, Parallel,
	// WaitForEvent, and Sleep methods.
	Handler func(wf *Workflow, input T) error
}

// NewWorkflow creates a typed workflow definition.
func NewWorkflow[T any](name string, handler func(wf *Workflow, input T) error) *Definition[T] {
	return &Definition[T]{
		Name:    name,
		Handler: handler,
	}
}
