// Package id provides TypeID-based identity types for all Dispatch entities.
//
// Every entity in Dispatch gets a type-prefixed, K-sortable, UUIDv7-based
// identifier. IDs are compile-time safe — you cannot pass a JobID where a
// RunID is expected.
//
// Examples:
//
//	job_01h2xcejqtf2nbrexx3vqjhp41
//	wfrun_01h2xcejqtf2nbrexx3vqjhp41
//	cron_01h455vb4pex5vsknk084sn02q
package id

import "go.jetify.com/typeid"

// ──────────────────────────────────────────────────
// Prefix types — each entity has its own prefix
// ──────────────────────────────────────────────────

// JobPrefix is the TypeID prefix for jobs.
type JobPrefix struct{}

// Prefix returns "job".
func (JobPrefix) Prefix() string { return "job" }

// WorkflowPrefix is the TypeID prefix for workflow definitions.
type WorkflowPrefix struct{}

// Prefix returns "wf".
func (WorkflowPrefix) Prefix() string { return "wf" }

// RunPrefix is the TypeID prefix for workflow runs.
type RunPrefix struct{}

// Prefix returns "wfrun".
func (RunPrefix) Prefix() string { return "wfrun" }

// CheckpointPrefix is the TypeID prefix for workflow checkpoints.
type CheckpointPrefix struct{}

// Prefix returns "ckpt".
func (CheckpointPrefix) Prefix() string { return "ckpt" }

// CronPrefix is the TypeID prefix for cron entries.
type CronPrefix struct{}

// Prefix returns "cron".
func (CronPrefix) Prefix() string { return "cron" }

// DLQPrefix is the TypeID prefix for dead letter queue entries.
type DLQPrefix struct{}

// Prefix returns "dlq".
func (DLQPrefix) Prefix() string { return "dlq" }

// EventPrefix is the TypeID prefix for events.
type EventPrefix struct{}

// Prefix returns "evt".
func (EventPrefix) Prefix() string { return "evt" }

// WorkerPrefix is the TypeID prefix for workers.
type WorkerPrefix struct{}

// Prefix returns "wkr".
func (WorkerPrefix) Prefix() string { return "wkr" }

// ──────────────────────────────────────────────────
// Typed ID aliases — compile-time safe
// ──────────────────────────────────────────────────

// JobID is a type-safe identifier for jobs (prefix: "job").
type JobID = typeid.TypeID[JobPrefix]

// WorkflowID is a type-safe identifier for workflow definitions (prefix: "wf").
type WorkflowID = typeid.TypeID[WorkflowPrefix]

// RunID is a type-safe identifier for workflow runs (prefix: "wfrun").
type RunID = typeid.TypeID[RunPrefix]

// CheckpointID is a type-safe identifier for workflow checkpoints (prefix: "ckpt").
type CheckpointID = typeid.TypeID[CheckpointPrefix]

// CronID is a type-safe identifier for cron entries (prefix: "cron").
type CronID = typeid.TypeID[CronPrefix]

// DLQID is a type-safe identifier for dead letter queue entries (prefix: "dlq").
type DLQID = typeid.TypeID[DLQPrefix]

// EventID is a type-safe identifier for events (prefix: "evt").
type EventID = typeid.TypeID[EventPrefix]

// WorkerID is a type-safe identifier for workers (prefix: "wkr").
type WorkerID = typeid.TypeID[WorkerPrefix]

// AnyID is a TypeID that accepts any valid prefix. Use for cases where
// the prefix is dynamic or unknown at compile time.
type AnyID = typeid.AnyID

// ──────────────────────────────────────────────────
// Constructors
// ──────────────────────────────────────────────────

// NewJobID returns a new random JobID.
func NewJobID() JobID { return must(typeid.New[JobID]()) }

// NewRunID returns a new random RunID.
func NewRunID() RunID { return must(typeid.New[RunID]()) }

// NewCheckpointID returns a new random CheckpointID.
func NewCheckpointID() CheckpointID { return must(typeid.New[CheckpointID]()) }

// NewCronID returns a new random CronID.
func NewCronID() CronID { return must(typeid.New[CronID]()) }

// NewDLQID returns a new random DLQID.
func NewDLQID() DLQID { return must(typeid.New[DLQID]()) }

// NewEventID returns a new random EventID.
func NewEventID() EventID { return must(typeid.New[EventID]()) }

// NewWorkerID returns a new random WorkerID.
func NewWorkerID() WorkerID { return must(typeid.New[WorkerID]()) }

// ──────────────────────────────────────────────────
// Parsing (type-safe: ParseJobID("cron_01h...") fails)
// ──────────────────────────────────────────────────

// ParseJobID parses a string into a JobID. Returns an error if the prefix
// is not "job" or the suffix is invalid.
func ParseJobID(s string) (JobID, error) { return typeid.Parse[JobID](s) }

// ParseRunID parses a string into a RunID.
func ParseRunID(s string) (RunID, error) { return typeid.Parse[RunID](s) }

// ParseCheckpointID parses a string into a CheckpointID.
func ParseCheckpointID(s string) (CheckpointID, error) { return typeid.Parse[CheckpointID](s) }

// ParseCronID parses a string into a CronID.
func ParseCronID(s string) (CronID, error) { return typeid.Parse[CronID](s) }

// ParseDLQID parses a string into a DLQID.
func ParseDLQID(s string) (DLQID, error) { return typeid.Parse[DLQID](s) }

// ParseEventID parses a string into an EventID.
func ParseEventID(s string) (EventID, error) { return typeid.Parse[EventID](s) }

// ParseWorkerID parses a string into a WorkerID.
func ParseWorkerID(s string) (WorkerID, error) { return typeid.Parse[WorkerID](s) }

// ParseAny parses a string into an AnyID, accepting any valid prefix.
func ParseAny(s string) (AnyID, error) { return typeid.FromString(s) }

// ──────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
