package operatorhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

// WriteStageDiagnostic emits fixed categories without provider messages or credentials.
func WriteStageDiagnostic(writer io.Writer, stage string, started time.Time, err error, status *delivery.Status) {
	if writer == nil {
		return
	}
	switch stage {
	case "route_load", "route_validate", "publisher_start", "http_shutdown", "worker_stop", "publisher_drain", "host_close":
	default:
		stage = "unknown"
	}
	category := "complete"
	switch {
	case errors.Is(err, ErrMissingChronicleRoute):
		category = "missing_route"
	case errors.Is(err, ErrChronicleOwnership), errors.Is(err, durable.ErrRequestConflict):
		category = "ownership_conflict"
	case errors.Is(err, sinkhost.ErrInvalidProfile), errors.Is(err, durable.ErrInvalid):
		category = "invalid_profile"
	case errors.Is(err, delivery.ErrIncompleteShutdown):
		category = "incomplete"
	case errors.Is(err, context.DeadlineExceeded):
		category = "deadline"
	case errors.Is(err, context.Canceled):
		category = "canceled"
	case err != nil:
		category = "unavailable"
	}
	record := struct {
		Stage             string `json:"stage"`
		Category          string `json:"category"`
		DurationMS        int64  `json:"duration_ms"`
		Incomplete        bool   `json:"incomplete"`
		Deadline          bool   `json:"deadline"`
		Canceled          bool   `json:"canceled"`
		CountsKnown       bool   `json:"counts_known"`
		CountsBeforeDrain bool   `json:"counts_before_drain"`
		Pending           int64  `json:"pending"`
		InFlight          int    `json:"in_flight"`
	}{Stage: stage, Category: category, DurationMS: max(0, time.Since(started).Milliseconds()), Incomplete: errors.Is(err, delivery.ErrIncompleteShutdown), Deadline: errors.Is(err, context.DeadlineExceeded), Canceled: errors.Is(err, context.Canceled)}
	if status != nil {
		record.CountsKnown = true
		record.CountsBeforeDrain = stage == "publisher_drain" && (record.Deadline || record.Canceled)
		record.Pending = min(max(status.Pending, 0), 1000000)
		record.InFlight = min(max(status.InFlight, 0), 100)
	}
	// Diagnostics are best effort and never replace the underlying shutdown result.
	if writeErr := json.NewEncoder(writer).Encode(record); writeErr != nil {
		return // A failed diagnostic sink cannot change the underlying shutdown result.
	}
}

func (h *Host) diagnose(stage string, started time.Time, err error, status *delivery.Status) {
	if h.lifecycle != nil {
		WriteStageDiagnostic(h.lifecycle.options.DiagnosticWriter, stage, started, err, status)
	}
}
