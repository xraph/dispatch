package runtime

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xraph/dispatch/durable"
)

// Workflow provides replayable decision primitives. Use these methods only from
// the handler's goroutine. Schedule multiple futures before Get for parallel work.
type Workflow struct {
	key                 durable.Key
	buildID             string
	now                 time.Time
	history             replayHistory
	cursor              int
	commands            []Command
	signals             []SignalConsumption
	selections          []Selection
	ids                 map[string]bool
	blocked             bool
	fault               error
	queries             map[string]QueryFunc
	querying            bool
	acknowledgmentCount int
	cancellationHandler WorkflowCancellationFunc
	freezeNormal        bool
	cancelling          bool
}

// Future represents a recorded command's eventual result.
// Get yields the workflow decision if its result has not been recorded.
type Future struct {
	workflow   *Workflow
	id         string
	signalName string
	kind       durable.TaskKind
}

type flowControl struct{}

// Now returns logical time: run creation, advanced by outcomes consumed by Get or Select.
// It never reads the worker's wall clock.
func (w *Workflow) Now() time.Time { return w.now }

// Activity schedules an external operation. Empty queue uses the workflow queue.
// The ID must be unique within this run and stable when the handler is replayed.
func (w *Workflow) Activity(id, name, queue string, input []byte) *Future {
	return w.schedule(Command{ID: id, Kind: durable.TaskActivity, Name: name, Queue: queue, Input: bytes.Clone(input)})
}

// ActivityWithOptions captures a retry policy in a version 2 activity command.
// Use this for new workflows; changing saved options is nondeterministic replay.
func (w *Workflow) ActivityWithOptions(id, name, queue string, input []byte, options ActivityOptions) *Future {
	w.checkOperation()
	normalized, err := normalizeActivityOptions(options)
	if err != nil {
		w.stop(err)
	}
	return w.schedule(Command{Version: 2, ID: id, Kind: durable.TaskActivity, Name: name, Queue: queue, Input: bytes.Clone(input), ActivityOptions: &normalized})
}

// Timer schedules a durable delay from Now, rounded up to store clock precision.
// No worker or goroutine needs to stay alive while this future is pending.
func (w *Workflow) Timer(id string, delay time.Duration) *Future {
	deadline := w.now.Add(delay)
	if truncated := durable.Timestamp(deadline); truncated.Before(deadline) {
		deadline = truncated.Add(time.Microsecond)
	}
	return w.schedule(Command{ID: id, Kind: durable.TaskTimer, Delay: delay, Deadline: deadline})
}

func (w *Workflow) stop(err error) {
	w.fault = err
	panic(flowControl{})
}

func (w *Workflow) checkOperation() {
	if w.querying {
		w.stop(ErrQueryMutation)
	}
	if w.blocked || w.fault != nil {
		w.stop(fmt.Errorf("%w: workflow continued after an unresolved future", durable.ErrInvalid))
	}
}

func (w *Workflow) schedule(command Command) *Future {
	w.checkOperation()
	if command.Version == 0 {
		command.Version = 1
	}
	command.Index = int64(w.cursor + 1)
	if err := command.validate(); err != nil {
		w.stop(err)
	}
	if w.ids[command.ID] {
		w.stop(fmt.Errorf("%w: duplicate command ID %q", durable.ErrInvalid, command.ID))
	}
	if w.freezeNormal && w.cursor >= len(w.history.commands) {
		w.blocked = true
		panic(flowControl{})
	}
	w.ids[command.ID] = true
	if w.cursor < len(w.history.commands) {
		prior := w.history.commands[w.cursor]
		if !sameCommand(prior, command) {
			w.stop(fmt.Errorf("%w: command %d (%s)", ErrNondeterministic, command.Index, command.ID))
		}
	} else {
		if command.Kind == CommandCancel || command.Kind == CommandChild || command.Kind == CommandCancelChild {
			w.acknowledgmentCount++
		}
		w.checkEventCapacity()
		w.commands = append(w.commands, command)
	}
	w.cursor++
	future := &Future{workflow: w, id: command.ID, kind: command.Kind}
	if command.Kind == CommandSignal {
		future.signalName = command.Name
	}
	return future
}

// Get returns a copy of a saved result, or a typed recorded activity failure.
// An unresolved future yields internally; do not recover this control flow in
// workflow code or perform work in a defer. The handler is replayed to resume.
func (f *Future) Get() ([]byte, error) {
	w := f.workflow
	w.checkOperation()
	result, ok := w.history.outcomes[f.id]
	if !ok && f.signalName != "" {
		result, ok = w.receiveSignal(f.id, f.signalName)
	}
	if !ok {
		w.blocked = true
		panic(flowControl{})
	}
	w.advance(result)
	if result.child != nil {
		return nil, cloneChildError(result.child)
	}
	if result.workflowCancellation != nil {
		c := result.workflowCancellation
		return nil, &WorkflowCancelledError{RequestID: c.RequestID, Reason: c.Reason}
	}
	if result.cancellation != nil {
		c := result.cancellation
		return nil, &CancelledError{CommandID: c.TargetID, Attempt: c.Attempt, Heartbeat: cloneHeartbeat(c.Heartbeat)}
	}
	if result.value.Failure != nil {
		failure := *result.value.Failure
		if result.value.Heartbeat != nil {
			return nil, &ActivityError{Failure: &failure, Attempt: result.value.Attempt, Timeout: result.value.Timeout, Heartbeat: cloneHeartbeat(result.value.Heartbeat)}
		}
		return nil, &failure
	}
	return bytes.Clone(result.value.Output), nil
}

func validID(value string) bool {
	return validIdentifier(value, 200)
}

func validIdentifier(value string, limit int) bool {
	return value != "" && len(value) <= limit && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, 0) && utf8.ValidString(value)
}

func (c Command) validate() error {
	if (c.Version != 1 && c.Version != 2) || c.Index < 1 || !validID(c.ID) || (c.Queue != "" && !validID(c.Queue)) {
		return fmt.Errorf("%w: invalid command version, index, ID or queue", durable.ErrInvalid)
	}
	if c.Kind != CommandSelect && len(c.Candidates) != 0 {
		return fmt.Errorf("%w: only selection commands accept candidates", durable.ErrInvalid)
	}
	if c.Kind != CommandCancel && c.Kind != CommandCancelChild && c.TargetID != "" {
		return fmt.Errorf("%w: only cancellation commands accept a target", durable.ErrInvalid)
	}
	if c.Kind != CommandChild && c.Child != nil {
		return fmt.Errorf("%w: only child commands accept child metadata", durable.ErrInvalid)
	}
	switch c.Kind {
	case CommandChild:
		return validateChildCommand(c)
	case CommandCancel, CommandCancelChild:
		return validateCancellationCommand(c)
	case CommandSelect:
		return validateSelectionCommand(c)
	case durable.TaskActivity:
		if c.Version == 1 && c.ActivityOptions != nil {
			return fmt.Errorf("%w: legacy activity cannot set options", durable.ErrInvalid)
		}
		if c.Version == 2 {
			if c.ActivityOptions == nil {
				return fmt.Errorf("%w: activity options required", durable.ErrInvalid)
			}
			normalized, err := normalizeActivityOptions(*c.ActivityOptions)
			if err != nil || !sameActivityOptions(c.ActivityOptions, &normalized) {
				return fmt.Errorf("%w: activity options must be normalized", durable.ErrInvalid)
			}
		}
		if !validID(c.Name) || c.Delay != 0 || !c.Deadline.IsZero() {
			return fmt.Errorf("%w: invalid activity command", durable.ErrInvalid)
		}
	case CommandSignal:
		if c.Version != 1 || !validID(c.Name) || c.ActivityOptions != nil || c.Queue != "" || len(c.Input) != 0 || c.Delay != 0 || !c.Deadline.IsZero() {
			return fmt.Errorf("%w: invalid signal receive command", durable.ErrInvalid)
		}
	case durable.TaskTimer:
		if c.Version != 1 || c.ActivityOptions != nil || c.Delay <= 0 || c.Deadline.IsZero() || c.Deadline.Year() < 1 || c.Deadline.Year() > 9999 ||
			c.Name != "" || c.Queue != "" || len(c.Input) != 0 {
			return fmt.Errorf("%w: invalid timer command", durable.ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown command kind", durable.ErrInvalid)
	}
	return nil
}

func sameCommand(a, b Command) bool {
	return a.Version == b.Version && a.Index == b.Index && a.ID == b.ID && a.Kind == b.Kind &&
		a.Name == b.Name && a.Queue == b.Queue && bytes.Equal(a.Input, b.Input) &&
		a.Delay == b.Delay && a.Deadline.Equal(b.Deadline) && sameActivityOptions(a.ActivityOptions, b.ActivityOptions) && slices.Equal(a.Candidates, b.Candidates) && a.TargetID == b.TargetID && sameChildCommand(a.Child, b.Child)
}

func sameChildCommand(a, b *ChildCommand) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
