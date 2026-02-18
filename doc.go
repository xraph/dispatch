// Package dispatch provides a composable, extensible durable execution engine
// for Go. It offers library-first background jobs, workflow orchestration,
// lifecycle hooks, and distributed workers.
//
// Dispatch is designed as a library, not a service. Import it, configure a
// store, and register jobs or workflows as ordinary Go functions.
//
// # Quick Start
//
//	d, err := dispatch.New(
//	    dispatch.WithStore(pgStore),
//	    dispatch.WithConcurrency(20),
//	)
//
// # Architecture
//
// Dispatch follows a composable store pattern where each subsystem (job,
// workflow, cron, dlq, event, cluster) defines its own store interface.
// A single backend implements all of them.
//
// All entity IDs use TypeID — type-prefixed, K-sortable, UUIDv7-based,
// compile-time safe identifiers.
package dispatch
