package artifact

import (
	"errors"

	"github.com/xraph/dispatch"
)

// Both permanent sentinels below unwrap to dispatch.ErrPermanent, so the
// executor sends a job that hits one straight to the dead letter queue
// instead of retrying it. A caller chooses how precisely to match:
//
//	errors.Is(err, artifact.ErrNotFound)  // the object is gone
//	errors.Is(err, dispatch.ErrPermanent) // don't retry -- also true
//
// Match dispatch.ErrPermanent when the question is whether to retry, and
// the specific sentinel when the answer changes what you do. Code that asks
// "is this missing" in order to decide "should I retry" is asking the wrong
// question: a permission failure is just as permanent, and answering it
// with a retry loop wastes the same budget a deleted input would.
var (
	// ErrNotFound means the artifact or its underlying object does not
	// exist. Retrying a fetch of something that no longer exists cannot
	// succeed, so it unwraps to dispatch.ErrPermanent.
	ErrNotFound error = &categoryError{
		msg:    "dispatch/artifact: not found",
		parent: dispatch.ErrPermanent,
	}

	// ErrPermissionDenied means the backend refused the operation as
	// unauthorized. Nothing changes until the credentials or the backend's
	// access policy do, so it unwraps to dispatch.ErrPermanent too.
	ErrPermissionDenied error = &categoryError{
		msg:    "dispatch/artifact: permission denied",
		parent: dispatch.ErrPermanent,
	}

	// ErrExists means an artifact already exists for this owner, name,
	// and a prior attempt. Create with IfAbsent returns it so a retried
	// handler can skip recomputation.
	ErrExists = errors.New("dispatch/artifact: already exists")

	// ErrSizeExceeded means a bound artifact is larger than the input
	// declaration's MaxSize.
	ErrSizeExceeded = errors.New("dispatch/artifact: size exceeds declared maximum")

	// ErrImmutable means an attempt was made to delete or overwrite a
	// durable artifact through a path reserved for ephemeral ones.
	ErrImmutable = errors.New("dispatch/artifact: durable artifacts are immutable")

	// ErrNoBackend means no storage backend is configured. Every artifact
	// operation is a no-op in this state and Dispatch behaves exactly as
	// it did before the artifact plane existed.
	ErrNoBackend = errors.New("dispatch/artifact: no backend configured")

	// ErrUnbound means a required input declaration has no binding on the
	// job being executed.
	ErrUnbound = errors.New("dispatch/artifact: required input not bound")

	// ErrUndeclared means a binding was supplied for a name the job
	// definition does not declare.
	ErrUndeclared = errors.New("dispatch/artifact: binding has no matching declaration")
)

// categoryError is a sentinel that belongs to a broader category, so that
// errors.Is matches both the specific sentinel and its parent. errors.New
// cannot express this because it produces a leaf with nothing to unwrap.
type categoryError struct {
	msg    string
	parent error
}

func (e *categoryError) Error() string { return e.msg }

// Unwrap returns the broader category this sentinel belongs to.
func (e *categoryError) Unwrap() error { return e.parent }
