// Package dlq provides the dead letter queue for jobs that have exhausted
// their retry budget. It supports inspection, replay, and purging.
//
// When a job fails and MaxRetries has been reached, the executor calls
// [Service.Push] to move it into the DLQ. The original payload, error
// message, and retry counts are preserved for debugging.
//
// # Entry
//
// A [Entry] captures:
//   - JobID / JobName / Queue: original job identity
//   - Payload: the raw JSON payload at time of failure
//   - Error: the final error message
//   - RetryCount / MaxRetries: exhausted retry budget
//   - FailedAt: when the terminal failure occurred
//   - ReplayedAt: set when the entry is replayed (nil if not yet replayed)
//
// # Service
//
// [Service] wraps the DLQ store with high-level operations:
//
//	svc := dlq.NewService(store, jobStore)
//
//	// Push is called automatically by the executor on terminal failure.
//	svc.Push(ctx, failedJob, err)
//
//	// Access the underlying store for list/get/purge/count.
//	svc.DLQStore().ListDLQ(ctx, dlq.ListOpts{Limit: 50})
//	svc.DLQStore().PurgeDLQ(ctx)
//
// # Replay
//
// Replaying an entry re-enqueues the original job with the same payload.
// Use the admin API (POST /v1/dlq/:entryId/replay) or call the store
// directly. Replay sets ReplayedAt on the DLQ entry.
//
// # Admin API
//
// The DLQ is exposed via the HTTP admin API:
//   - GET  /v1/dlq               — list entries
//   - GET  /v1/dlq/:entryId      — get a single entry
//   - POST /v1/dlq/:entryId/replay — replay one entry
//   - POST /v1/dlq/purge         — purge all entries
//   - GET  /v1/dlq/count         — entry count
package dlq
