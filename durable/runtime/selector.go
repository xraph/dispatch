package runtime

import (
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// CommandSelect records an ordered set of futures without creating a polled task.
const CommandSelect durable.TaskKind = "select"

// EventSelected records the winning future in the workflow decision transaction.
const EventSelected = "workflow.selected"

// Selection binds one stable selection command to its ready candidate.
type Selection struct {
	Version   int    `json:"version"`
	CommandID string `json:"command_id"`
	FutureID  string `json:"future_id"`
}

// Select returns a ready candidate, or yields until one is ready. Give each call
// a stable, unique command ID and 1 through 1000 distinct futures from this
// evaluation. New choices use history order; replay retains the saved winner.
// Select consumes its winning signal and advances Now. Get reads its result or
// failure. Losing futures remain active, and a later Select may reuse a winner.
func (w *Workflow) Select(id string, futures ...*Future) *Future {
	w.checkOperation()
	if len(futures) == 0 || len(futures) > 1000 {
		w.stop(fmt.Errorf("%w: selection needs 1 through 1000 futures", durable.ErrInvalid))
	}
	ids := make([]string, len(futures))
	seen := make(map[string]bool, len(futures))
	for i, future := range futures {
		if future == nil || future.workflow != w || !w.ids[future.id] || seen[future.id] {
			w.stop(fmt.Errorf("%w: selection future is nil, foreign or repeated", durable.ErrInvalid))
		}
		ids[i] = future.id
		seen[future.id] = true
	}
	w.schedule(Command{ID: id, Kind: CommandSelect, Candidates: ids})
	if prior, ok := w.history.selections[id]; ok {
		for _, future := range futures {
			if future.id == prior.FutureID {
				w.advance(w.history.outcomes[future.id])
				return future
			}
		}
		w.stop(fmt.Errorf("%w: selection winner is absent", ErrNondeterministic))
	}
	var winner *Future
	var result recordedOutcome
	for _, future := range futures {
		candidate, ready := w.peek(future)
		if ready && (winner == nil || candidate.sequence < result.sequence) {
			winner, result = future, candidate
		}
	}
	if winner == nil {
		w.blocked = true
		panic(flowControl{})
	}
	if _, saved := w.history.outcomes[winner.id]; !saved {
		result, _ = w.receiveSignal(winner.id, winner.signalName)
	}
	w.checkEventCapacity()
	w.selections = append(w.selections, Selection{Version: 1, CommandID: id, FutureID: winner.id})
	w.advance(result)
	return winner
}

func (w *Workflow) peek(future *Future) (recordedOutcome, bool) {
	if result, ok := w.history.outcomes[future.id]; ok {
		return result, true
	}
	if future.signalName != "" {
		queue, offset := w.history.signalQueues[future.signalName], w.history.signalOffsets[future.signalName]
		if offset < len(queue) {
			message := w.history.signals[queue[offset]]
			return recordedOutcome{at: message.at, sequence: message.sequence}, true
		}
	}
	return recordedOutcome{}, false
}

func (w *Workflow) advance(result recordedOutcome) {
	if result.at.After(w.now) {
		w.now = result.at
	}
}

func (w *Workflow) checkEventCapacity() {
	if w.freezeNormal {
		return
	}
	// Reserve the final store event for workflow state.
	if len(w.commands)+len(w.signals)+len(w.selections)+w.cancellationCount >= 999 {
		w.stop(fmt.Errorf("%w: more than 999 decision events", durable.ErrInvalid))
	}
}
