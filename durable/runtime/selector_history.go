package runtime

import (
	"fmt"
	"slices"

	"github.com/xraph/dispatch/durable"
)

func validateSelectionCommand(command Command) error {
	if command.Version != 1 || len(command.Candidates) == 0 || len(command.Candidates) > 1000 || command.Name != "" || command.Queue != "" || len(command.Input) != 0 || command.Delay != 0 || !command.Deadline.IsZero() || command.ActivityOptions != nil {
		return fmt.Errorf("%w: invalid selection command", durable.ErrInvalid)
	}
	seen := make(map[string]bool, len(command.Candidates))
	for _, id := range command.Candidates {
		if !validID(id) || seen[id] {
			return fmt.Errorf("%w: invalid selection candidate", durable.ErrInvalid)
		}
		seen[id] = true
	}
	return nil
}

func validateSelectionReferences(command Command, commands map[string]Command) error {
	for _, id := range command.Candidates {
		prior, ok := commands[id]
		if !ok || prior.Index >= command.Index || prior.Kind == CommandSelect {
			return fmt.Errorf("%w: selection candidate is not a prior effect or receive", ErrHistory)
		}
	}
	return nil
}

func parseSelection(history *replayHistory, commands map[string]Command, event durable.Event) error {
	var selected Selection
	if err := decode(event.Payload, &selected); err != nil {
		return err
	}
	command, known := commands[selected.CommandID]
	_, duplicate := history.selections[selected.CommandID]
	_, ready := history.outcomes[selected.FutureID]
	if selected.Version != 1 || !known || command.Kind != CommandSelect || duplicate || !slices.Contains(command.Candidates, selected.FutureID) || !ready {
		return fmt.Errorf("%w: invalid selection at event %d", ErrHistory, event.Sequence)
	}
	history.selections[selected.CommandID] = selected
	return nil
}
