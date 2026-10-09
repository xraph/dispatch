package runtime

import (
	"bytes"

	"github.com/xraph/dispatch/durable"
)

// CommandSignal records a receive operation. It never creates a polled task.
const CommandSignal durable.TaskKind = "signal"

// Signal history separates durable acceptance from workflow consumption.
const (
	EventSignalReceived = durable.EventSignalReceived
	EventSignalConsumed = durable.EventSignalConsumed
)

// SignalConsumption binds one accepted message to a stable receive command.
type SignalConsumption = durable.SignalConsumption

// ReceiveSignal waits for the oldest unconsumed message with this name.
// Use a unique, stable command ID for each receive. Repeated Get on the same
// future returns the same copied input. Closing a workflow leaves any unread
// signals in history; acceptance alone does not promise processing.
func (w *Workflow) ReceiveSignal(id, name string) *Future {
	return w.schedule(Command{ID: id, Kind: CommandSignal, Name: name})
}

func (w *Workflow) receiveSignal(id, name string) (recordedOutcome, bool) {
	queue := w.history.signalQueues[name]
	offset := w.history.signalOffsets[name]
	if offset >= len(queue) {
		return recordedOutcome{}, false
	}
	w.checkEventCapacity()
	message := w.history.signals[queue[offset]]
	w.history.signalOffsets[name] = offset + 1
	w.signals = append(w.signals, SignalConsumption{Version: 1, CommandID: id, SignalID: message.value.ID})
	result := recordedOutcome{value: Outcome{Version: 1, CommandID: id, Output: bytes.Clone(message.value.Input)}, at: message.at, sequence: message.sequence}
	w.history.outcomes[id] = result
	return result, true
}
