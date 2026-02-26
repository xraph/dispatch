// Package id defines TypeID-based identity types for all Dispatch entities.
//
// Every entity in Dispatch uses a single ID struct with a prefix that identifies
// the entity type. IDs are K-sortable (UUIDv7-based), globally unique,
// and URL-safe in the format "prefix_suffix".
package id

import (
	"database/sql/driver"
	"fmt"

	"go.jetify.com/typeid/v2"
)

// Prefix identifies the entity type encoded in a TypeID.
type Prefix string

// Prefix constants for all Dispatch entity types.
const (
	PrefixJob        Prefix = "job"
	PrefixWorkflow   Prefix = "wf"
	PrefixRun        Prefix = "wfrun"
	PrefixCheckpoint Prefix = "ckpt"
	PrefixCron       Prefix = "cron"
	PrefixDLQ        Prefix = "dlq"
	PrefixEvent      Prefix = "evt"
	PrefixWorker     Prefix = "wkr"
)

// ID is the primary identifier type for all Dispatch entities.
// It wraps a TypeID providing a prefix-qualified, globally unique,
// sortable, URL-safe identifier in the format "prefix_suffix".
//
//nolint:recvcheck // Value receivers for read-only methods, pointer receivers for UnmarshalText/Scan.
type ID struct {
	inner typeid.TypeID
	valid bool
}

// Nil is the zero-value ID.
var Nil ID

// New generates a new globally unique ID with the given prefix.
// It panics if prefix is not a valid TypeID prefix (programming error).
func New(prefix Prefix) ID {
	tid, err := typeid.Generate(string(prefix))
	if err != nil {
		panic(fmt.Sprintf("id: invalid prefix %q: %v", prefix, err))
	}

	return ID{inner: tid, valid: true}
}

// Parse parses a TypeID string (e.g., "job_01h2xcejqtf2nbrexx3vqjhp41")
// into an ID. Returns an error if the string is not valid.
func Parse(s string) (ID, error) {
	if s == "" {
		return Nil, fmt.Errorf("id: parse %q: empty string", s)
	}

	tid, err := typeid.Parse(s)
	if err != nil {
		return Nil, fmt.Errorf("id: parse %q: %w", s, err)
	}

	return ID{inner: tid, valid: true}, nil
}

// ParseWithPrefix parses a TypeID string and validates that its prefix
// matches the expected value.
func ParseWithPrefix(s string, expected Prefix) (ID, error) {
	parsed, err := Parse(s)
	if err != nil {
		return Nil, err
	}

	if parsed.Prefix() != expected {
		return Nil, fmt.Errorf("id: expected prefix %q, got %q", expected, parsed.Prefix())
	}

	return parsed, nil
}

// MustParse is like Parse but panics on error. Use for hardcoded ID values.
func MustParse(s string) ID {
	parsed, err := Parse(s)
	if err != nil {
		panic(fmt.Sprintf("id: must parse %q: %v", s, err))
	}

	return parsed
}

// MustParseWithPrefix is like ParseWithPrefix but panics on error.
func MustParseWithPrefix(s string, expected Prefix) ID {
	parsed, err := ParseWithPrefix(s, expected)
	if err != nil {
		panic(fmt.Sprintf("id: must parse with prefix %q: %v", expected, err))
	}

	return parsed
}

// ──────────────────────────────────────────────────
// Type aliases for backward compatibility
// ──────────────────────────────────────────────────

// JobID is a type-safe identifier for jobs (prefix: "job").
type JobID = ID

// WorkflowID is a type-safe identifier for workflow definitions (prefix: "wf").
type WorkflowID = ID

// RunID is a type-safe identifier for workflow runs (prefix: "wfrun").
type RunID = ID

// CheckpointID is a type-safe identifier for workflow checkpoints (prefix: "ckpt").
type CheckpointID = ID

// CronID is a type-safe identifier for cron entries (prefix: "cron").
type CronID = ID

// DLQID is a type-safe identifier for dead letter queue entries (prefix: "dlq").
type DLQID = ID

// EventID is a type-safe identifier for events (prefix: "evt").
type EventID = ID

// WorkerID is a type-safe identifier for workers (prefix: "wkr").
type WorkerID = ID

// AnyID is a type alias that accepts any valid prefix.
type AnyID = ID

// ──────────────────────────────────────────────────
// Convenience constructors
// ──────────────────────────────────────────────────

// NewJobID generates a new unique job ID.
func NewJobID() ID { return New(PrefixJob) }

// NewWorkflowID generates a new unique workflow ID.
func NewWorkflowID() ID { return New(PrefixWorkflow) }

// NewRunID generates a new unique run ID.
func NewRunID() ID { return New(PrefixRun) }

// NewCheckpointID generates a new unique checkpoint ID.
func NewCheckpointID() ID { return New(PrefixCheckpoint) }

// NewCronID generates a new unique cron ID.
func NewCronID() ID { return New(PrefixCron) }

// NewDLQID generates a new unique DLQ ID.
func NewDLQID() ID { return New(PrefixDLQ) }

// NewEventID generates a new unique event ID.
func NewEventID() ID { return New(PrefixEvent) }

// NewWorkerID generates a new unique worker ID.
func NewWorkerID() ID { return New(PrefixWorker) }

// ──────────────────────────────────────────────────
// Convenience parsers
// ──────────────────────────────────────────────────

// ParseJobID parses a string and validates the "job" prefix.
func ParseJobID(s string) (ID, error) { return ParseWithPrefix(s, PrefixJob) }

// ParseWorkflowID parses a string and validates the "wf" prefix.
func ParseWorkflowID(s string) (ID, error) { return ParseWithPrefix(s, PrefixWorkflow) }

// ParseRunID parses a string and validates the "wfrun" prefix.
func ParseRunID(s string) (ID, error) { return ParseWithPrefix(s, PrefixRun) }

// ParseCheckpointID parses a string and validates the "ckpt" prefix.
func ParseCheckpointID(s string) (ID, error) { return ParseWithPrefix(s, PrefixCheckpoint) }

// ParseCronID parses a string and validates the "cron" prefix.
func ParseCronID(s string) (ID, error) { return ParseWithPrefix(s, PrefixCron) }

// ParseDLQID parses a string and validates the "dlq" prefix.
func ParseDLQID(s string) (ID, error) { return ParseWithPrefix(s, PrefixDLQ) }

// ParseEventID parses a string and validates the "evt" prefix.
func ParseEventID(s string) (ID, error) { return ParseWithPrefix(s, PrefixEvent) }

// ParseWorkerID parses a string and validates the "wkr" prefix.
func ParseWorkerID(s string) (ID, error) { return ParseWithPrefix(s, PrefixWorker) }

// ParseAny parses a string into an ID without type checking the prefix.
func ParseAny(s string) (ID, error) { return Parse(s) }

// ──────────────────────────────────────────────────
// ID methods
// ──────────────────────────────────────────────────

// String returns the full TypeID string representation (prefix_suffix).
// Returns an empty string for the Nil ID.
func (i ID) String() string {
	if !i.valid {
		return ""
	}

	return i.inner.String()
}

// Prefix returns the prefix component of this ID.
func (i ID) Prefix() Prefix {
	if !i.valid {
		return ""
	}

	return Prefix(i.inner.Prefix())
}

// IsNil reports whether this ID is the zero value.
func (i ID) IsNil() bool {
	return !i.valid
}

// MarshalText implements encoding.TextMarshaler.
func (i ID) MarshalText() ([]byte, error) {
	if !i.valid {
		return []byte{}, nil
	}

	return []byte(i.inner.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (i *ID) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		*i = Nil

		return nil
	}

	parsed, err := Parse(string(data))
	if err != nil {
		return err
	}

	*i = parsed

	return nil
}

// Value implements driver.Valuer for database storage.
// Returns nil for the Nil ID so that optional foreign key columns store NULL.
func (i ID) Value() (driver.Value, error) {
	if !i.valid {
		return nil, nil //nolint:nilnil // nil is the canonical NULL for driver.Valuer
	}

	return i.inner.String(), nil
}

// Scan implements sql.Scanner for database retrieval.
func (i *ID) Scan(src any) error {
	if src == nil {
		*i = Nil

		return nil
	}

	switch v := src.(type) {
	case string:
		if v == "" {
			*i = Nil

			return nil
		}

		return i.UnmarshalText([]byte(v))
	case []byte:
		if len(v) == 0 {
			*i = Nil

			return nil
		}

		return i.UnmarshalText(v)
	default:
		return fmt.Errorf("id: cannot scan %T into ID", src)
	}
}
