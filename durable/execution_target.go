package durable

import (
	"errors"
	"fmt"
)

// RunSelection distinguishes an explicit run from a workflow identity lookup.
type RunSelection string

const (
	RunExplicit RunSelection = ""
	RunCurrent  RunSelection = "current"
	RunLatest   RunSelection = "latest"
)

// ErrAmbiguousRun means a migrated workflow has several closed runs but no saved
// latest identity. Query an explicit run until a new start establishes latest.
var ErrAmbiguousRun = errors.New("durable: latest run is unknown for migrated history")

// ExecutionTarget selects one snapshot. Current and latest require an empty RunID.
// Latest includes closed runs and follows successful creation, not timestamps.
type ExecutionTarget struct {
	Key
	Selection RunSelection `json:"selection,omitempty"`
}

func (r ExecutionTarget) Validate() error {
	switch r.Selection {
	case RunExplicit:
		return r.Key.Validate()
	case RunCurrent, RunLatest:
		if r.RunID != "" {
			return fmt.Errorf("%w: selected run cannot include RunID", ErrInvalid)
		}
		key := r.Key
		key.RunID = "selected"
		return key.Validate()
	default:
		return fmt.Errorf("%w: unknown run selection", ErrInvalid)
	}
}
