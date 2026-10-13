package operatorhost

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

func TestShutdownDiagnosticsSafeCategories(t *testing.T) {
	for _, tc := range []struct {
		err      error
		category string
	}{
		{errors.Join(delivery.ErrIncompleteShutdown, context.DeadlineExceeded), "incomplete"},
		{ErrMissingChronicleRoute, "missing_route"},
		{ErrChronicleOwnership, "ownership_conflict"},
		{sinkhost.ErrInvalidProfile, "invalid_profile"},
		{errors.New("postgres://secret@private/payload"), "unavailable"},
	} {
		var output bytes.Buffer
		WriteStageDiagnostic(&output, "publisher_drain", time.Now(), tc.err, &delivery.Status{Pending: 2000000, InFlight: 200})
		if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "payload") || !strings.Contains(output.String(), `"category":"`+tc.category+`"`) {
			t.Fatal("unsafe or unclassified diagnostic")
		}
		if !strings.Contains(output.String(), `"pending":1000000`) || !strings.Contains(output.String(), `"in_flight":100`) {
			t.Fatal("unbounded diagnostic counts")
		}
	}
}

func TestHostCloseWaitingCallerKeepsDeadline(t *testing.T) {
	h := &Host{}
	if err := h.lockClose(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() { <-h.closeGate }()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := h.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("concurrent close ignored caller deadline")
	}
}

func TestHostCloseCanceledFreeGate(t *testing.T) {
	h := &Host{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A nil audit service would panic if a canceled caller reached deactivation.
	for range 20 {
		if err := h.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal("canceled free gate changed host state")
		}
	}
	if err := h.lockClose(t.Context()); err != nil {
		t.Fatal("canceled call retained gate")
	}
	<-h.closeGate
}
