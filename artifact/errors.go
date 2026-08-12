package artifact

import "errors"

var (
	// ErrNotFound means the artifact or its underlying object does not
	// exist. Staging treats this as permanent: retrying a fetch of
	// something that no longer exists cannot succeed.
	ErrNotFound = errors.New("dispatch/artifact: not found")

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
