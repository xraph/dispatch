package exec

// Status classifies how an execution attempt ended.
//
// A bare error cannot express this. In-process, a handler returning an
// error and a handler dying are the same value; out-of-process they are
// different events needing different handling, and only some of them are
// the handler's fault.
type Status string

const (
	// StatusOK means the handler ran and returned nil.
	StatusOK Status = "ok"

	// StatusHandlerError means the handler ran and returned an error.
	// This is a business failure and follows the normal retry path.
	StatusHandlerError Status = "handler_error"

	// StatusTimeout means the deadline expired and the sandbox was
	// killed. Unlike a cancelled context, this is enforced.
	StatusTimeout Status = "timeout"

	// StatusOOMKilled means a memory limit was hit. The handler did not
	// choose this and may succeed with a larger allocation.
	StatusOOMKilled Status = "oom_killed"

	// StatusKilled means the process died on a signal — a SIGSEGV from a
	// memory-unsafe parser, or a seccomp trap. It is security-relevant.
	StatusKilled Status = "killed"

	// StatusLaunchFailed means the sandbox never started: an image pull
	// failure, an exhausted quota, a missing runtime. The handler never
	// ran, so this is infrastructure rather than work.
	StatusLaunchFailed Status = "launch_failed"
)

// IsFailure reports whether the status represents anything other than
// success.
func (s Status) IsFailure() bool { return s != StatusOK }

// CountsAgainstRetries reports whether an attempt ending in this status
// should consume the job's retry budget.
//
// Launch failures do not. An ImagePullBackOff or a FailedScheduling says
// nothing about the work, and burning three retries on one bad node would
// send healthy jobs to the DLQ.
func (s Status) CountsAgainstRetries() bool {
	switch s {
	case StatusHandlerError, StatusTimeout, StatusOOMKilled, StatusKilled:
		return true
	case StatusOK, StatusLaunchFailed:
		return false
	default:
		return true
	}
}
