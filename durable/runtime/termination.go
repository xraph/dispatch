package runtime

import (
	"bytes"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

func parseTermination(history *replayHistory, event durable.Event) error {
	var termination durable.ExecutionTermination
	if err := decode(event.Payload, &termination); err != nil {
		return err
	}
	if termination.Validate() != nil {
		return fmt.Errorf("%w: invalid workflow termination", ErrHistory)
	}
	history.termination = &termination
	history.terminal = durable.StateTerminated
	return nil
}

func evaluateForcedClosure(execution durable.Execution, events []durable.Event, handler WorkflowFunc) (Decision, *Workflow, error) {
	prefix := execution
	prefix.State, prefix.Output, prefix.LastSequence = durable.StateRunning, nil, execution.LastSequence-1
	// Full history validation already checked the retry pair. Neither event is
	// part of the running prefix used to reconstruct pre-timeout query closures.
	if prefix.LastSequence > 0 && events[prefix.LastSequence-1].Type == durable.EventWorkflowRetryScheduled {
		prefix.LastSequence--
		prefix.NextRunID = ""
	}
	history, err := parseHistory(prefix, events[:prefix.LastSequence])
	if err != nil {
		return Decision{}, nil, err
	}
	var w *Workflow
	if history.executionCancellation != nil {
		_, w, err = evaluateExecutionCancellation(prefix, events[:prefix.LastSequence], history, handler, true)
		if err != nil {
			return Decision{}, nil, err
		}
	} else {
		w = &Workflow{execution: execution, key: execution.Key, buildID: execution.BuildID, now: execution.AvailableAt(), history: history, ids: make(map[string]bool), freezeNormal: true}
		_, _ = invoke(w, handler, bytes.Clone(execution.Input)) //nolint:errcheck // Forced closure replaces the application result; replay faults are checked below.
		if replayErr := checkWorkflowReplay(w); replayErr != nil {
			return Decision{}, nil, replayErr
		}
	}
	return Decision{State: execution.State}, w, nil
}
