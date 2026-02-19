// Package job defines the job entity, state machine, typed definitions,
// and store interface.
//
// # Job Entity
//
// A [Job] represents a unit of work. It embeds [dispatch.Entity] for
// timestamps, carries a typed payload (JSON), and progresses through a
// state machine:
//
//	pending → running → completed
//	pending → running → retrying → running → ...
//	pending → running → failed
//	pending → running → failed → dlq
//	pending → cancelled
//
// Fields of note:
//   - Queue: which queue the job belongs to (default: "default")
//   - Priority: higher values are dequeued first
//   - MaxRetries / RetryCount: controls retry budget
//   - RunAt: earliest time the job may be dequeued
//   - Timeout: per-job execution deadline (zero = unlimited)
//
// # Defining a Job
//
// Use [Definition] with a typed handler. The payload is JSON-serialized
// at enqueue time and deserialized before the handler runs:
//
//	var SendEmail = job.NewDefinition("send_email",
//	    func(ctx context.Context, input EmailInput) error {
//	        return mailer.Send(input.To, input.Subject, input.Body)
//	    },
//	)
//
// # Registry
//
// [Registry] maps job names to type-erased [HandlerFunc] values.
// Register definitions at startup via [RegisterDefinition]:
//
//	job.RegisterDefinition(registry, SendEmail)
//	job.RegisterDefinition(registry, GenerateReport)
//
// The engine package provides higher-level engine.Register and
// engine.Enqueue wrappers.
package job
