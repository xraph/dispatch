package durable

import "fmt"

// EventSignalReceived records an accepted signal independently of consumption.
const EventSignalReceived = "workflow.signal_received"

// Signal is the versioned history payload for one accepted message.
type Signal struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Name    string `json:"name"`
	Input   []byte `json:"input,omitempty"`
}

// SignalRequest targets one run, or the current open run when RunID is empty.
// RequestID is unique across both signal APIs within a namespace/workflow ID.
// Repeat the whole original request after an unknown response.
type SignalRequest struct {
	Key
	RequestID string `json:"request_id"`
	BuildID   string `json:"build_id"`
	Name      string `json:"name"`
	Input     []byte `json:"input,omitempty"`
}

// SignalWithStartRequest accepts a signal on the open run or atomically creates
// Start.RunID first. Start.RequestID identifies this entire operation. An existing
// run preserves its type, input and queue, and must match Start.BuildID.
type SignalWithStartRequest struct {
	Start StartRequest `json:"start"`
	Name  string       `json:"name"`
	Input []byte       `json:"input,omitempty"`
}

// SignalReceipt returns the actual target and original acceptance coordinates.
// It does not assert that workflow code consumed or processed the signal.
type SignalReceipt struct {
	Key
	Receipt
	Started bool `json:"started"`
}

// Validate bounds a message before it reaches storage or replay.
func (s Signal) Validate() error {
	if s.Version != 1 || !identifier(s.ID) || !identifier(s.Name) || len(s.Name) > 200 || len(s.Input) > 1<<20 {
		return fmt.Errorf("%w: invalid signal version, identity, name or input size", ErrInvalid)
	}
	return nil
}

// Validate allows an empty run selector but requires explicit namespace/build.
func (r SignalRequest) Validate() error {
	if !identifier(r.Namespace) || !identifier(r.WorkflowID) || (r.RunID != "" && !identifier(r.RunID)) || !identifier(r.BuildID) {
		return fmt.Errorf("%w: invalid signal target or build", ErrInvalid)
	}
	return (Signal{Version: 1, ID: r.RequestID, Name: r.Name, Input: r.Input}).Validate()
}

// Validate requires a complete proposed start identity, even when an open run wins.
func (r SignalWithStartRequest) Validate() error {
	if err := r.Start.Validate(); err != nil {
		return err
	}
	return (Signal{Version: 1, ID: r.Start.RequestID, Name: r.Name, Input: r.Input}).Validate()
}
