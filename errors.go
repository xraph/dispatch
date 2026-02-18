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

	// Cluster errors.
	ErrLeadershipLost = errors.New("dispatch: leadership lost")
	ErrNotLeader      = errors.New("dispatch: not the leader")
)
