package runtime

import (
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// ActivityHandoff records transfer of one attempt without exposing credentials.
type ActivityHandoff struct {
	Version   int                  `json:"version"`
	CommandID string               `json:"command_id"`
	Attempt   int64                `json:"attempt"`
	Epoch     int64                `json:"epoch"`
	Heartbeat *HeartbeatCheckpoint `json:"heartbeat"`
}

func parseActivityHandoff(history *replayHistory, commands map[string]Command, event durable.Event) error {
	var handoff ActivityHandoff
	if err := decode(event.Payload, &handoff); err != nil {
		return err
	}
	command, exists := commands[handoff.CommandID]
	prior := history.attempts[handoff.CommandID]
	_, completed := history.outcomes[handoff.CommandID]
	if !exists || command.Kind != durable.TaskActivity || command.Version != 2 || completed || prior.failed || prior.handoff != nil || !prior.value.HeartbeatEnabled ||
		handoff.Version != 1 || handoff.Attempt < 1 || handoff.Attempt != prior.value.Attempt || handoff.Epoch != prior.value.Epoch {
		return fmt.Errorf("%w: invalid asynchronous activity handoff", ErrHistory)
	}
	if err := validateHeartbeat(prior, handoff.Heartbeat, event.Time); err != nil {
		return err
	}
	current := prior
	current.value.Heartbeat = handoff.Heartbeat
	deadline, _, err := activityDeadline(command, history.scheduled[handoff.CommandID], current)
	if err != nil || deadline.IsZero() || !event.Time.Before(deadline) {
		return fmt.Errorf("%w: asynchronous handoff requires a future activity deadline", ErrHistory)
	}
	prior.handoff = cloneHeartbeat(handoff.Heartbeat)
	history.attempts[handoff.CommandID] = prior
	return nil
}
