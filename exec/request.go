package exec

import (
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
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
// handler needs crosses the boundary in this value. The environment is
// not inherited wholesale from the worker: an out-of-process rung
// constructs the child's environment from Env plus a small fixed
// allowlist (PATH, HOME, TMPDIR) copied from the worker's own — see Env
// below and exec/subprocess.Executor.buildEnv.
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

	// ResourceLimits is the job's resolved enforcement ceiling
	// (job.Job.ResourceLimits, see job.WithResourceLimits), carried
	// across the execution boundary so a rung that can enforce something
	// has the per-job numbers to enforce it with. A key absent or zero
	// here means the job declared no ceiling for that dimension; it is
	// not this type's business to say what a rung does about that —
	// exec/subprocess.Executor falls back to its own deployment-wide
	// default in that case, but exec itself has no opinion.
	ResourceLimits resource.Set

	// ScopeAppID and ScopeOrgID label the attempt for logs and metrics.
	// They are identifiers, never credentials.
	ScopeAppID string
	ScopeOrgID string

	// Env is passed to out-of-process rungs. It is not the worker's
	// os.Environ() handed through: exec/subprocess.Executor.buildEnv
	// builds the child's environment from this map plus its own
	// configured base, never starting from the worker's full environment.
	// It does still copy a fixed allowlist of PATH, HOME, and TMPDIR from
	// the worker's own environment ahead of this map — HOME in
	// particular is what locates ~/.aws and similar credential paths, so
	// the dedicated uid this rung requires, not environment exclusion
	// alone, is what actually keeps the child from reading them.
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
