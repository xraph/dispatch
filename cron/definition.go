package cron

// Definition is a typed cron definition. T is the payload type
// (must be JSON-serializable).
type Definition[T any] struct {
	// Name is the unique identifier for this cron entry.
	Name string

	// Schedule is a cron expression (e.g., "*/5 * * * *" or "@every 30s").
	Schedule string

	// JobName is the name of the job to enqueue on each tick.
	JobName string

	// Payload is the default payload to enqueue with the job.
	Payload T

	// Queue overrides the default job queue (optional).
	Queue string
}
