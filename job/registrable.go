package job

import "github.com/xraph/dispatch/exec"

// Registrable is a job definition that can register itself into a Registry
// without the caller knowing its payload type.
//
// Go forbids generic methods, but a method on a generic type is legal, so
// Definition[T] can satisfy this non-generic interface. That is what lets
// definitions with different payload types live in one slice — and a slice
// is what an out-of-process entrypoint can be handed, since it cannot be
// given the engine that would otherwise do the registering.
type Registrable interface {
	// Register adds this definition's handler to the registry.
	Register(r *Registry)

	// JobName returns the name the definition registers under.
	JobName() string

	// Policy returns the execution declaration, so a caller can check
	// that the deployment can satisfy it before registering anything.
	Policy() exec.Policy
}

// Register adds the definition's handler to the registry.
func (d *Definition[T]) Register(r *Registry) { RegisterDefinition(r, d) }

// JobName returns the name this definition registers under.
func (d *Definition[T]) JobName() string { return d.Name }

// Policy returns this definition's execution declaration.
func (d *Definition[T]) Policy() exec.Policy { return d.Opts.Execution }
