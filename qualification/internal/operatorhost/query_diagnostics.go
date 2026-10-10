package operatorhost

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/xraph/dispatch/durable"
)

const queryDiagnosticPrefix = "dispatch-query-rejection "
const queryDiagnosticLimit = 4096
const queryDiagnosticRecords = 32

// queryDiagnostics retains bounded private records, independently of audit delivery.
type queryDiagnostics struct {
	mu      sync.Mutex
	records []string
}

func queryDiagnosticLine(d durable.QueryRejectionDiagnostic) string {
	safe, _ := durable.QueryRejectionDetails(durable.NewQueryRejection(durable.ErrQueryRetention, d))
	raw, err := json.Marshal(safe)
	if err != nil || len(raw) > queryDiagnosticLimit {
		return queryDiagnosticPrefix + `{"stage":"unknown","reason":"unknown"}`
	}
	return queryDiagnosticPrefix + string(raw)
}

func (h *lifecycleHost) observeQueryRejection(_ context.Context, d durable.QueryRejectionDiagnostic) {
	line := queryDiagnosticLine(d)
	h.diagnostics.mu.Lock()
	defer h.diagnostics.mu.Unlock()
	if len(h.diagnostics.records) == queryDiagnosticRecords {
		copy(h.diagnostics.records, h.diagnostics.records[1:])
		h.diagnostics.records = h.diagnostics.records[:queryDiagnosticRecords-1]
	}
	h.diagnostics.records = append(h.diagnostics.records, line)
	if h.options.DiagnosticWriter != nil {
		if _, err := h.options.DiagnosticWriter.Write([]byte(line + "\n")); err != nil {
			// The in-memory record remains available. Diagnostics do not change acceptance.
			return
		}
	}
}

func (h *lifecycleHost) queryDiagnosticSnapshot() []string {
	h.diagnostics.mu.Lock()
	defer h.diagnostics.mu.Unlock()
	return append([]string(nil), h.diagnostics.records...)
}

// queryTime uses the same store authority that accepts the proof. A later clock
// rollback can still refuse it. Neither a read error nor rollback permits a fallback.
func (h *lifecycleHost) queryTime(ctx context.Context, target durable.QueryRuntimeTarget) (time.Time, error) {
	fail := func(cause error, reason string) (time.Time, error) {
		return time.Time{}, durable.NewQueryRejection(cause, durable.QueryRejectionDiagnostic{Stage: "host_clock", Reason: reason, Target: target})
	}
	store, ok := h.host.Store.(durable.QueryRuntimeStore)
	if !ok {
		return fail(durable.ErrQueryRetention, "clock_read")
	}
	facts, err := store.InspectQueryRetention(ctx, target.BuildTarget)
	if err != nil {
		return fail(err, "clock_read")
	}
	if facts.BuildTarget != target.BuildTarget || facts.ObservedAt.IsZero() || !facts.ObservedAt.Equal(durable.Timestamp(facts.ObservedAt)) {
		return fail(durable.ErrQueryRetention, "clock_sample")
	}
	return facts.ObservedAt, nil
}
