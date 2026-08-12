package staging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/xraph/dispatch/artifact"
)

// staged is one input the middleware materialised for a handler.
type staged struct {
	ref  artifact.Ref
	path string
	mode artifact.StageMode
}

// accessor is the artifact.Accessor handed to a running handler. It
// closes over the job's owner and attempt so the handler does not have to
// know either.
type accessor struct {
	svc     *artifact.Service
	owner   artifact.OwnerRef
	attempt int
	inputs  map[string]staged
}

var _ artifact.Accessor = (*accessor)(nil)

// Path returns the local file path of a staged input.
func (a *accessor) Path(name string) string {
	in, ok := a.inputs[name]
	if !ok {
		return ""
	}

	return in.path
}

// Ref returns the artifact bound to an input.
func (a *accessor) Ref(name string) (artifact.Ref, bool) {
	in, ok := a.inputs[name]
	if !ok {
		return artifact.Ref{}, false
	}

	return in.ref, true
}

// Open streams an input's bytes.
//
// A path-staged input is read from local disk, so a handler that prefers
// a reader does not pay for a second download. A lazy one streams from
// the backend on demand.
func (a *accessor) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	in, ok := a.inputs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", artifact.ErrUnbound, name)
	}

	if in.path != "" {
		f, err := os.Open(in.path)
		if err != nil {
			return nil, fmt.Errorf("dispatch/artifact/staging: open staged %q: %w", name, err)
		}

		return f, nil
	}

	return a.svc.Open(ctx, in.ref)
}

// Create begins writing an output owned by the running job.
func (a *accessor) Create(
	ctx context.Context,
	name string,
	opts ...artifact.CreateOption,
) (*artifact.CommitWriter, error) {
	return a.svc.Create(ctx, a.owner, a.attempt, name, opts...)
}

// Existing reports an artifact a previous attempt committed under this
// name, which is what lets a retried handler skip work already done.
func (a *accessor) Existing(ctx context.Context, name string) (artifact.Ref, bool) {
	ref, err := a.svc.FindExisting(ctx, a.owner, name)
	if err != nil {
		if !errors.Is(err, artifact.ErrNotFound) {
			return artifact.Ref{}, false
		}

		return artifact.Ref{}, false
	}

	return ref, true
}
