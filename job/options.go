package job

import (
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/resource"
)

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

	// LeaseTTL is how long this job's lease survives without renewal.
	// Zero means the worker pool's default. Set it above the expected gap
	// between heartbeats, not above the expected runtime — a lease is a
	// liveness window, not a time limit.
	LeaseTTL time.Duration

	// RunAt schedules the job for future execution. Zero means immediate.
	RunAt time.Time

	// Inputs declares the artifacts this job consumes. Declaring them,
	// rather than burying refs in the opaque payload, is what lets the
	// engine size the job before scheduling it, validate bindings at
	// enqueue, and stage the bytes before the handler runs.
	Inputs []artifact.InputSpec

	// Bindings maps declared input names to the artifacts supplied at
	// enqueue. The engine validates them against Inputs before the job is
	// persisted.
	Bindings map[string]artifact.Ref

	// Resources declares what this job needs. It is the floor: the
	// engine may raise it via ResourceFunc or a configured estimator,
	// and a per-enqueue WithResources call overrides both.
	Resources resource.Set

	// ResourceLimits is the enforcement ceiling. When unset, memory and
	// the other incompressible keys default to their request and CPU is
	// left unbounded.
	ResourceLimits resource.Set

	// ResourceFunc computes the requirement from the enqueue-time
	// request, for jobs whose footprint scales with their input. It runs
	// once, in the enqueuing process, and never on the scheduling path.
	ResourceFunc resource.ResourceFunc

	// ResourceClass is an opaque scheduling class the isolation backend
	// interprets. Core never reads it.
	ResourceClass string
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

// WithArtifactInputs declares the artifact inputs a job consumes.
//
// The engine validates every binding against these declarations at
// enqueue and stages the declared inputs before the handler runs.
func WithArtifactInputs(specs ...artifact.InputSpec) Option {
	return func(o *Options) {
		o.Inputs = append(o.Inputs, specs...)
	}
}

// WithResources declares the resources a job needs. Multiple sets are
// merged per key, so the common form reads as a list:
//
//	job.WithResources(resource.CPUs(4), resource.MemoryGB(16))
//
// Passed at enqueue instead of on the definition, it overrides every
// other source, including a configured estimator.
func WithResources(sets ...resource.Set) Option {
	return func(o *Options) {
		for _, s := range sets {
			if o.Resources == nil {
				o.Resources = make(resource.Set, len(s))
			}

			for k, v := range s {
				o.Resources[k] = v
			}
		}
	}
}

// WithResourceLimits sets the enforcement ceiling explicitly.
func WithResourceLimits(sets ...resource.Set) Option {
	return func(o *Options) {
		for _, s := range sets {
			if o.ResourceLimits == nil {
				o.ResourceLimits = make(resource.Set, len(s))
			}

			for k, v := range s {
				o.ResourceLimits[k] = v
			}
		}
	}
}

// WithResourceFunc computes the requirement from the job's input.
//
// This is what lets one definition serve a 40 MB model and a 4 GB one:
// the artifact plane knows the input size at enqueue, so the function
// sees it before the job is ever scheduled.
func WithResourceFunc(fn resource.ResourceFunc) Option {
	return func(o *Options) { o.ResourceFunc = fn }
}

// WithResourceClass sets an opaque scheduling class for the isolation
// backend. Core stores and forwards it without interpretation.
func WithResourceClass(class string) Option {
	return func(o *Options) { o.ResourceClass = class }
}

// WithLeaseTTL sets how long this job's lease survives without renewal.
//
// A lease TTL is a liveness window, not a time limit: it should be a small
// multiple of the heartbeat interval regardless of how long the work takes.
// Non-positive durations are ignored, because a zero TTL would expire the
// lease the instant it was granted and the job would be reclaimed before
// its first heartbeat.
func WithLeaseTTL(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.LeaseTTL = d
		}
	}
}
