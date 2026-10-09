package runtime

import (
	"bytes"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

type recordedSignal struct {
	value durable.Signal
	at    time.Time
}

func parseSignal(history *replayHistory, commands map[string]Command, event durable.Event) error {
	if event.Type == EventSignalReceived {
		var signal durable.Signal
		if err := decode(event.Payload, &signal); err != nil {
			return err
		}
		_, duplicate := history.signals[signal.ID]
		if signal.Validate() != nil || duplicate {
			return fmt.Errorf("%w: invalid or duplicate signal at event %d", ErrHistory, event.Sequence)
		}
		history.signals[signal.ID] = recordedSignal{value: signal, at: event.Time}
		history.signalQueues[signal.Name] = append(history.signalQueues[signal.Name], signal.ID)
		return nil
	}
	var consumed SignalConsumption
	if err := decode(event.Payload, &consumed); err != nil {
		return err
	}
	command, known := commands[consumed.CommandID]
	message, received := history.signals[consumed.SignalID]
	_, duplicate := history.outcomes[consumed.CommandID]
	if consumed.Version != 1 || !known || command.Kind != CommandSignal || !received || duplicate || command.Name != message.value.Name {
		return fmt.Errorf("%w: invalid signal consumption at event %d", ErrHistory, event.Sequence)
	}
	queue, offset := history.signalQueues[command.Name], history.signalOffsets[command.Name]
	if offset >= len(queue) || queue[offset] != consumed.SignalID {
		return fmt.Errorf("%w: signal consumed twice or out of order", ErrHistory)
	}
	history.signalOffsets[command.Name] = offset + 1
	history.outcomes[command.ID] = recordedOutcome{value: Outcome{Version: 1, CommandID: command.ID, Output: bytes.Clone(message.value.Input)}, at: message.at}
	return nil
}
