// Package engine wires all Dispatch subsystems together and provides
// the primary application-level API for registering and enqueuing work.
//
// The engine package exists to break a fundamental import cycle: the root
// dispatch package defines Entity (imported by job, workflow, cron, etc.)
// and therefore cannot import those packages back. Engine sits above all
// subsystem packages and below the application layer.
//
// # Building an Engine
//
//	d, err := dispatch.New(
//	    dispatch.WithStore(pgStore),
//	    dispatch.WithConcurrency(20),
//	)
//
//	eng := engine.Build(d,
//	    engine.WithExtension(myExtension),
//	    engine.WithMiddleware(middleware.Logging(logger)),
//	    engine.WithBackoff(backoff.Exponential()),
//	    engine.WithQueueConfig(queue.Config{
//	        Name:      "critical",
//	        RateLimit: 100,
//	    }),
//	)
//
// # Registering Work
//
//	// Jobs
//	engine.Register(eng, SendEmail)
//
//	// Workflows
//	engine.RegisterWorkflow(eng, ProcessOrder)
//
//	// Crons
//	engine.RegisterCron(ctx, eng, "daily-report", "0 9 * * *", GenerateReport, ReportInput{})
//
// # Enqueuing Jobs
//
//	engine.Enqueue(ctx, eng, SendEmail, EmailInput{To: "user@example.com"})
//
//	// With options
//	engine.EnqueueAt(ctx, eng, SendEmail, input, time.Now().Add(5*time.Minute))
//	engine.EnqueueWithPriority(ctx, eng, SendEmail, input, 10)
//
// # Options
//
//   - [WithExtension] — register a lifecycle extension
//   - [WithMiddleware] — add a middleware to the execution chain
//   - [WithBackoff] — set the retry backoff strategy
//   - [WithQueueConfig] — configure per-queue rate limits and concurrency
//   - [WithTracerProvider] — set the OpenTelemetry tracer provider
//   - [WithMetricFactory] — set the metrics factory
package engine
