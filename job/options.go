package job

import "time"

// Options configures per-job behavior such as retries, queue, and priority.
type Options struct {
	// MaxRetries is the maximum number of retry attempts before sending to DLQ.
	MaxRetries int

	// Queue is the queue name this job should be enqueued to.
	Queue string

	// Priority determines dequeue ordering. Higher values are processed first.
	Priority int

	// Timeout is the maximum duration a job may run before being cancelled.
	Timeout time.Duration

	// RunAt schedules the job for future execution. Zero means immediate.
	RunAt time.Time
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		MaxRetries: 3,
		Queue:      "default",
		Priority:   0,
		Timeout:    5 * time.Minute,
	}
}

// Option is a functional option for configuring a job definition.
type Option func(*Options)

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(n int) Option {
	return func(o *Options) {
		o.MaxRetries = n
	}
}

// WithQueue sets the queue name for the job.
func WithQueue(q string) Option {
	return func(o *Options) {
		o.Queue = q
	}
}

// WithPriority sets the job priority. Higher values are processed first.
func WithPriority(p int) Option {
	return func(o *Options) {
		o.Priority = p
	}
}

// WithTimeout sets the maximum execution duration for the job.
func WithTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.Timeout = d
	}
}

// WithRunAt schedules the job for execution at a specific time.
func WithRunAt(t time.Time) Option {
	return func(o *Options) {
		o.RunAt = t
	}
}
