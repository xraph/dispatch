package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// ErrHandoffPending means worker heartbeats are suspended until an uncertain
// ownership transfer resolves. Retry DeferCompletion to recover its handle.
var ErrHandoffPending = errors.New("durable runtime: asynchronous handoff response is unresolved")

// AsyncActivityHandle transfers callback authority for one activity attempt.
// Treat it as a credential. JSON carries the secret for explicit delivery;
// ordinary formatting redacts every field. Keep the handle immutable.
type AsyncActivityHandle struct {
	Version int               `json:"version"`
	Key     durable.Key       `json:"key"`
	BuildID string            `json:"build_id"`
	Token   durable.TaskToken `json:"token"`
	Secret  string            `json:"secret"`
	// InitialHeartbeatSequence is informational. Coordinate later sequences
	// separately across callback producers; do not update this handle.
	InitialHeartbeatSequence int64 `json:"initial_heartbeat_sequence"`
}

func (h AsyncActivityHandle) String() string   { return "AsyncActivityHandle{redacted}" }
func (h AsyncActivityHandle) GoString() string { return h.String() }

// Validate checks the handle's shape. Stores verify its current authority.
func (h AsyncActivityHandle) Validate() error {
	if err := h.Key.Validate(); err != nil {
		return err
	}
	if err := h.Token.Validate(); err != nil {
		return err
	}
	// Builds use the durable store's 512-byte identifier limit. The 200-byte
	// workflow command limit does not apply to deployment routing.
	if h.Version != 1 || !validIdentifier(h.BuildID, 512) || h.Token.LeaseKind != durable.LeaseAsync || h.InitialHeartbeatSequence < 0 {
		return fmt.Errorf("%w: invalid asynchronous activity handle", durable.ErrInvalid)
	}
	_, err := durable.HashAsyncSecret(h.Secret)
	return err
}

// AsyncCompletionRequest completes or fails an asynchronous attempt. RequestID
// must stay stable on retries and be unique among result requests in the run.
// Output and Failure are mutually exclusive; an empty successful output is valid.
type AsyncCompletionRequest struct {
	Handle    AsyncActivityHandle `json:"handle"`
	RequestID string              `json:"request_id"`
	Output    []byte              `json:"output,omitempty"`
	Failure   *ApplicationError   `json:"failure,omitempty"`
}

// AsyncHeartbeatRequest records consecutive progress. RequestID is unique among
// heartbeat requests in the run. Reuse the entire request after an unknown ACK.
type AsyncHeartbeatRequest struct {
	Handle    AsyncActivityHandle
	RequestID string
	Sequence  int64
	Details   []byte
}

// DeferCompletion durably hands this attempt to an external callback before
// returning its handle. A confirmed handoff suppresses subsequent handler results.
// Calling again during the handler returns the same handle. A finite persisted
// overall, attempt or heartbeat deadline is required.
// After an unknown response, retry to recover the handle. Renewal reconciles
// sent pending requests; ordinary heartbeats return ErrHandoffPending meanwhile.
func (a ActivityInfo) DeferCompletion(ctx context.Context) (AsyncActivityHandle, error) {
	if a.deferCompletion == nil {
		return AsyncActivityHandle{}, fmt.Errorf("%w: asynchronous handoff requires a running version 2 activity", durable.ErrInvalid)
	}
	return a.deferCompletion(ctx)
}
