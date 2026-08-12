package artifact

import (
	"context"
	"io"
)

// Accessor is the handler-facing face of the artifact plane. A handler
// obtains one with From(ctx).
type Accessor interface {
	// Path returns the local file path of a staged input. It returns an
	// empty string for an input that was not declared, was not bound, or
	// was declared lazy.
	Path(name string) string

	// Open streams an input's bytes. It works for both staging modes:
	// a path-staged input reads from local disk, a lazy one from the
	// backend.
	Open(ctx context.Context, name string) (io.ReadCloser, error)

	// Ref returns the artifact bound to an input.
	Ref(name string) (Ref, bool)

	// Create begins writing an output owned by the running job. Outputs
	// are imperative rather than declared so a handler can produce a
	// number of them it only discovers at run time.
	Create(ctx context.Context, name string, opts ...CreateOption) (*CommitWriter, error)

	// Existing returns an artifact a previous attempt committed under this
	// name, letting a retried handler skip work it already did.
	Existing(ctx context.Context, name string) (Ref, bool)
}

type accessorKey struct{}

// WithAccessor attaches an Accessor to a context.
func WithAccessor(ctx context.Context, a Accessor) context.Context {
	return context.WithValue(ctx, accessorKey{}, a)
}

// From returns the Accessor for the running job.
//
// It never returns nil. When the artifact plane is disabled, or the job
// declared no inputs, it returns a no-op Accessor so a handler calling
// From(ctx).Path("x") gets an empty string rather than a panic.
func From(ctx context.Context) Accessor {
	if a, ok := ctx.Value(accessorKey{}).(Accessor); ok && a != nil {
		return a
	}

	return noopAccessor{}
}

// noopAccessor stands in when no artifact plane is configured.
type noopAccessor struct{}

func (noopAccessor) Path(string) string { return "" }

func (noopAccessor) Open(context.Context, string) (io.ReadCloser, error) {
	return nil, ErrNoBackend
}

func (noopAccessor) Ref(string) (Ref, bool) { return Ref{}, false }

func (noopAccessor) Create(context.Context, string, ...CreateOption) (*CommitWriter, error) {
	return nil, ErrNoBackend
}

func (noopAccessor) Existing(context.Context, string) (Ref, bool) { return Ref{}, false }
