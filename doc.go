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
// workflow, cron, dlq, event, cluster, artifact) defines its own store
// interface. A single backend implements all of them.
//
// # Artifacts
//
// Jobs that process large files use the artifact plane rather than the
// payload column: an artifact is a tracked reference to an object in
// external storage, declared as a job input and staged to a
// content-addressed local cache before the handler runs. See the artifact
// package. It is opt-in — with no backend configured, Dispatch behaves as
// it did before artifacts existed.
//
// All entity IDs use TypeID — type-prefixed, K-sortable, UUIDv7-based,
// compile-time safe identifiers.
package dispatch
