// Package queue defines the queue abstraction with priority ordering
// and per-queue / per-tenant rate limiting.
//
// Queues are named channels that group related jobs. Jobs carry a Queue field
// that determines which queue they belong to. The dispatcher polls the queues
// listed in [dispatch.Config.Queues] (default: ["default"]).
//
// # Per-Queue Configuration
//
// Use [Config] to set per-queue rate limits and concurrency caps:
//
//	queue.Config{
//	    Name:           "email",
//	    MaxConcurrency: 5,      // max 5 concurrent email jobs
//	    RateLimit:      10,     // max 10 jobs/s dequeued from this queue
//	    RateBurst:      20,     // allow bursts up to 20
//	}
//
// Pass configs when building the engine:
//
//	engine.Build(d,
//	    engine.WithQueueConfig(
//	        queue.Config{Name: "critical", MaxConcurrency: 20},
//	        queue.Config{Name: "bulk", RateLimit: 5, RateBurst: 10},
//	    ),
//	)
//
// # Manager
//
// [Manager] enforces per-queue and per-tenant limits at dequeue time.
// It uses a token-bucket rate limiter (golang.org/x/time/rate) and an
// active-count gate for concurrency limits.
//
//	m := queue.NewManager(configs...)
//	if m.Acquire(queueName, tenantID) {
//	    defer m.Release(queueName, tenantID)
//	    // process the job
//	}
//
// Queues without a [Config] have no limits beyond the pool-wide concurrency.
package queue
