// Package cron provides distributed cron scheduling with leader election
// and per-tenant support.
//
// Cron entries are stored in the database and fired only by the cluster
// leader. This guarantees at-most-once firing even when multiple Dispatch
// instances are running.
//
// # Entry
//
// An [Entry] represents a recurring job schedule:
//   - Schedule: standard cron expression (e.g., "0 9 * * 1-5")
//   - JobName: the registered job definition to enqueue when fired
//   - Queue: target queue (defaults to "default")
//   - Payload: static JSON payload passed to every triggered job
//   - ScopeAppID / ScopeOrgID: tenant scoping
//   - Enabled: whether the entry fires
//   - LockedBy / LockedUntil: distributed lock fields (managed internally)
//
// # Registering a Cron
//
// Use engine.RegisterCron to add a cron entry at startup:
//
//	engine.RegisterCron(ctx, eng, "daily-report", "0 9 * * *",
//	    GenerateReport, ReportInput{Format: "pdf"})
//
// # Enable / Disable
//
// Cron entries can be enabled or disabled at runtime via the admin API
// (POST /v1/crons/:cronId/enable and POST /v1/crons/:cronId/disable).
// Operator code goes through the engine's EnableCron, DisableCron,
// DeleteCron and TriggerCron. Enabling computes the next fire time from
// now, so an entry that was off past its old one does not fire a catch-up.
// [NextFires] previews a schedule's next fire times.
//
// # Scheduler
//
// The [Scheduler] evaluates due entries on every tick, acquires a distributed
// lock on each entry, re-reads it, and fires only if the store still has it
// enabled and due. It enqueues the corresponding job and records LastRunAt
// and NextRunAt with targeted writes that never touch Enabled, so a fire
// cannot undo an operator's disable. The [ext.CronFired] extension hook
// fires after each enqueue.
package cron
