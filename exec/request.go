package exec

import (
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// ErrInvalidRequest marks a Request that cannot be executed as given.
var ErrInvalidRequest = errors.New("invalid execution request")

// InputSlot maps a declared input name to its location within InputDir.
// The path is relative, so the same Request describes the inputs whether
// the sandbox mounts them at /dispatch/in or reads them where they lie.
type InputSlot struct {
	Name string
	Path string
}

// PriorOutput is an artifact an earlier attempt of this job committed.
//
// A sandbox keeps its artifact rows in memory and cannot query the store,
// so without these Accessor.Existing would always answer "no" and a
// retried handler would redo work it had already finished. The output
// would still be correct, which is exactly why this is worth carrying
// explicitly: nothing would fail, it would just quietly cost twice.
type PriorOutput struct {
	Name string
	Ref  artifact.Ref
}

// Request is one execution attempt, fully described. Everything the
// handler needs crosses the boundary in this value; nothing is inherited
// from the worker's environment.
type Request struct {
	JobID   id.JobID
	Name    string
	Payload []byte
	Attempt int

	// Deadline is when the attempt must be killed. Zero means no deadline.
	Deadline time.Time

	// Fingerprint identifies the handler set the caller expects.
	Fingerprint string

	// InputDir holds staged inputs and is read-only to the handler.
	InputDir string
	// OutputDir is where the handler writes artifacts.
	OutputDir string

	Inputs       []InputSlot
	PriorOutputs []PriorOutput

	Policy Policy

	// ScopeAppID and ScopeOrgID label the attempt for logs and metrics.
	// They are identifiers, never credentials.
	ScopeAppID string
	ScopeOrgID string

	// Env is passed to out-of-process rungs. It is constructed, never
	// inherited, so the sandbox does not receive the worker's environment.
	Env map[string]string
}

// Validate reports whether the request is well formed.
func (r *Request) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("%w: empty job name", ErrInvalidRequest)
	}
	if r.Attempt < 0 {
		return fmt.Errorf("%w: negative attempt %d", ErrInvalidRequest, r.Attempt)
	}

	return nil
}

// InputPath returns the relative path of a declared input, or an empty
// string when the request carries no such input.
func (r *Request) InputPath(name string) string {
	for _, in := range r.Inputs {
		if in.Name == name {
			return in.Path
		}
	}

	return ""
}
