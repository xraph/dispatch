package dispatch

import "errors"

var (
	// Store errors.
	ErrNoStore         = errors.New("dispatch: no store configured")
	ErrStoreClosed     = errors.New("dispatch: store closed")
	ErrMigrationFailed = errors.New("dispatch: migration failed")

	// Not found errors.
	ErrJobNotFound      = errors.New("dispatch: job not found")
	ErrWorkflowNotFound = errors.New("dispatch: workflow not found")
	ErrRunNotFound      = errors.New("dispatch: run not found")
	ErrCronNotFound     = errors.New("dispatch: cron entry not found")
	ErrDLQNotFound      = errors.New("dispatch: dlq entry not found")
	ErrEventNotFound    = errors.New("dispatch: event not found")
	ErrWorkerNotFound   = errors.New("dispatch: worker not found")

	// Conflict errors.
	ErrJobAlreadyExists = errors.New("dispatch: job already exists")
	ErrDuplicateCron    = errors.New("dispatch: duplicate cron entry")

	// State errors.
	ErrInvalidState       = errors.New("dispatch: invalid state transition")
	ErrMaxRetriesExceeded = errors.New("dispatch: max retries exceeded")

	// ErrPermanent marks a failure that retrying cannot resolve. A job
	// whose handler returns an error wrapping it skips its remaining
	// attempts and goes straight to the dead letter queue.
	//
	// Handlers wrap it to decline a retry they know is pointless:
	//
	//	if !validPayload(p) {
	//	    return fmt.Errorf("malformed payload: %w", dispatch.ErrPermanent)
	//	}
	//
	// The artifact plane wraps it for a missing or forbidden input, which
	// is where it earns its keep: a job whose input was deleted would
	// otherwise spend every attempt, and the whole backoff schedule
	// between them, rediscovering that the object is still gone.
	//
	// Only mark a failure permanent when retrying is certain to fail the
	// same way. Anything unrecognized stays retryable on purpose: a job
	// retried needlessly costs some compute, while one dead-lettered by
	// mistake loses work that would have succeeded.
	ErrPermanent = errors.New("dispatch: permanent failure")

	// Cluster errors.
	ErrLeadershipLost = errors.New("dispatch: leadership lost")
	ErrNotLeader      = errors.New("dispatch: not the leader")
)
