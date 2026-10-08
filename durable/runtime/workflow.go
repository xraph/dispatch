package runtime

import (
	"bytes"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xraph/dispatch/durable"
)

// Workflow provides replayable decision primitives. Use these methods only from
// the handler's goroutine. Schedule multiple futures before Get for parallel work.
type Workflow struct {
	now      time.Time
	history  replayHistory
	cursor   int
	commands []Command
	ids      map[string]bool
	blocked  bool
	fault    error
}

// Future represents a recorded command's eventual result.
// Get yields the workflow decision if its result has not been recorded.
type Future struct {
	workflow *Workflow
	id       string
}

type flowControl struct{}

// Now returns logical time: run creation, advanced by outcomes consumed by Get.
// It never reads the worker's wall clock.
func (w *Workflow) Now() time.Time { return w.now }

// Activity schedules an external operation. Empty queue uses the workflow queue.
// The ID must be unique within this run and stable when the handler is replayed.
func (w *Workflow) Activity(id, name, queue string, input []byte) *Future {
	return w.schedule(Command{ID: id, Kind: durable.TaskActivity, Name: name, Queue: queue, Input: bytes.Clone(input)})
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

func (w *Workflow) schedule(command Command) *Future {
	if w.blocked || w.fault != nil {
		w.stop(fmt.Errorf("%w: workflow continued after an unresolved future", durable.ErrInvalid))
	}
	command.Version, command.Index = 1, int64(w.cursor+1)
	if err := command.validate(); err != nil {
		w.stop(err)
	}
	if w.ids[command.ID] {
		w.stop(fmt.Errorf("%w: duplicate command ID %q", durable.ErrInvalid, command.ID))
	}
	w.ids[command.ID] = true
	if w.cursor < len(w.history.commands) {
		prior := w.history.commands[w.cursor]
		if !sameCommand(prior, command) {
			w.stop(fmt.Errorf("%w: command %d (%s)", ErrNondeterministic, command.Index, command.ID))
		}
	} else {
		// Reserve one event in the store's batch for workflow state.
		if len(w.commands) >= 999 {
			w.stop(fmt.Errorf("%w: more than 999 commands in one decision", durable.ErrInvalid))
		}
		w.commands = append(w.commands, command)
	}
	w.cursor++
	return &Future{workflow: w, id: command.ID}
}

// Get returns a copy of a saved result, or a typed recorded activity failure.
// An unresolved future yields internally; do not recover this control flow in
// workflow code or perform work in a defer. The handler is replayed to resume.
func (f *Future) Get() ([]byte, error) {
	w := f.workflow
	if w.blocked || w.fault != nil {
		w.stop(fmt.Errorf("%w: workflow continued after an unresolved future", durable.ErrInvalid))
	}
	result, ok := w.history.outcomes[f.id]
	if !ok {
		w.blocked = true
		panic(flowControl{})
	}
	if result.at.After(w.now) {
		w.now = result.at
	}
	if result.value.Failure != nil {
		failure := *result.value.Failure
		return nil, &failure
	}
	return bytes.Clone(result.value.Output), nil
}

func validID(value string) bool {
	return value != "" && len(value) <= 200 && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, 0) && utf8.ValidString(value)
}

func (c Command) validate() error {
	if c.Version != 1 || c.Index < 1 || !validID(c.ID) || (c.Queue != "" && !validID(c.Queue)) {
		return fmt.Errorf("%w: invalid command version, index, ID or queue", durable.ErrInvalid)
	}
	switch c.Kind {
	case durable.TaskActivity:
		if !validID(c.Name) || c.Delay != 0 || !c.Deadline.IsZero() {
			return fmt.Errorf("%w: invalid activity command", durable.ErrInvalid)
		}
	case durable.TaskTimer:
		if c.Delay <= 0 || c.Deadline.IsZero() || c.Deadline.Year() < 1 || c.Deadline.Year() > 9999 ||
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
		a.Delay == b.Delay && a.Deadline.Equal(b.Deadline)
}
