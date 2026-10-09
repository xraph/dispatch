package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

const (
	CommandChild                 durable.TaskKind = "child"
	CommandCancelChild           durable.TaskKind = "cancel_child"
	TaskChildDelivery            durable.TaskKind = "child_delivery"
	EventChildStartFailed                         = "workflow.child_start_failed"
	EventChildCancellationFailed                  = "workflow.child_cancellation_failed"
	EventWorkflowTimedOut                         = durable.EventWorkflowTimedOut
	FailureChildStartConflict                     = "child_start_conflict"
)

// ChildOptions captures routing and parent-close behavior in the saved command.
// Empty build and queue inherit the parent. The default close policy terminates.
type ChildOptions struct {
	WorkflowID        string
	BuildID           string
	Queue             string
	ParentClosePolicy durable.ParentClosePolicy
	RunTimeout        time.Duration
	ExecutionTimeout  time.Duration
}

// ChildCommand retains a child's exact identity and build across parent replay.
type ChildCommand struct {
	Key               durable.Key               `json:"key"`
	BuildID           string                    `json:"build_id"`
	ParentClosePolicy durable.ParentClosePolicy `json:"parent_close_policy"`
	RunTimeout        time.Duration             `json:"run_timeout,omitempty"`
	ExecutionTimeout  time.Duration             `json:"execution_timeout,omitempty"`
}

var (
	ErrChildStart      = errors.New("durable runtime: child could not start")
	ErrChildCancelled  = errors.New("durable runtime: child cancelled")
	ErrChildTerminated = errors.New("durable runtime: child terminated")
	ErrChildTimedOut   = errors.New("durable runtime: child timed out")
)

// ChildWorkflowError preserves the child identity, lifecycle and application cause.
// Empty State means creation failed before a child execution existed.
type ChildWorkflowError struct {
	Child   durable.Key
	State   durable.State
	Failure *ApplicationError
	Timeout *durable.ExecutionTimeout
}

func (e *ChildWorkflowError) Error() string {
	state := string(e.State)
	if state == "" {
		state = "could not start"
	}
	message := fmt.Sprintf("child workflow %q (%s) %s", e.Child.WorkflowID, e.Child.RunID, state)
	if e.Failure != nil {
		message += ": " + e.Failure.Message
	}
	return message
}
func (e *ChildWorkflowError) Unwrap() error {
	if e.Failure != nil {
		return e.Failure
	}
	return nil
}
func (e *ChildWorkflowError) Is(target error) bool {
	switch e.State {
	case "":
		return target == ErrChildStart
	case durable.StateCancelled:
		return target == ErrChildCancelled
	case durable.StateTerminated:
		return target == ErrChildTerminated
	case durable.StateTimedOut:
		return target == ErrChildTimedOut
	default:
		return false
	}
}
func cloneChildError(e *ChildWorkflowError) *ChildWorkflowError {
	if e == nil {
		return nil
	}
	result := *e
	if e.Failure != nil {
		failure := *e.Failure
		result.Failure = &failure
	}
	if e.Timeout != nil {
		timeout := *e.Timeout
		result.Timeout = &timeout
	}
	return &result
}

// ChildStartFailure records a definitive conflict without adopting another run.
type ChildStartFailure struct {
	Version   int               `json:"version"`
	CommandID string            `json:"command_id"`
	Child     durable.Key       `json:"child"`
	Failure   *ApplicationError `json:"failure"`
}

// ChildCancellationFailure preserves an earlier creation failure. No child
// cancellation was accepted when the target execution could not be created.
type ChildCancellationFailure struct {
	Version   int         `json:"version"`
	CommandID string      `json:"command_id"`
	TargetID  string      `json:"target_id"`
	Child     durable.Key `json:"child"`
}

// ChildWorkflow schedules a separate execution atomically with this decision.
// Await Started before returning when an abandoned child must outlive its parent.
func (w *Workflow) ChildWorkflow(id, name string, input []byte, options ChildOptions) *Future {
	w.checkOperation()
	if err := w.key.Validate(); err != nil {
		w.stop(err)
	}
	identity, err := durable.Fingerprint("child-workflow", struct {
		Parent    durable.Key
		CommandID string
	}{w.key, id})
	if err != nil {
		w.stop(err)
	}
	if options.WorkflowID == "" {
		options.WorkflowID = "child-" + identity
	}
	if options.BuildID == "" {
		options.BuildID = w.buildID
	}
	if options.ParentClosePolicy == "" {
		options.ParentClosePolicy = durable.ParentCloseTerminate
	}
	child := &ChildCommand{Key: durable.Key{Namespace: w.key.Namespace, WorkflowID: options.WorkflowID, RunID: identity}, BuildID: options.BuildID, ParentClosePolicy: options.ParentClosePolicy, RunTimeout: options.RunTimeout, ExecutionTimeout: options.ExecutionTimeout}
	if child.Key.WorkflowID == w.key.WorkflowID {
		w.stop(fmt.Errorf("%w: child workflow identity matches parent", durable.ErrInvalid))
	}
	return w.schedule(Command{ID: id, Kind: CommandChild, Name: name, Queue: options.Queue, Input: bytes.Clone(input), Child: child})
}

// Started yields until the child start or definitive creation failure is saved.
// It never claims that the child has completed.
func (f *Future) Started() (durable.Key, error) {
	w := f.workflow
	w.checkOperation()
	if f.kind != CommandChild {
		w.stop(fmt.Errorf("%w: Started requires a child future", durable.ErrInvalid))
	}
	start, ok := w.history.children[f.id]
	if !ok {
		w.blocked = true
		panic(flowControl{})
	}
	w.advance(recordedOutcome{at: start.at, sequence: start.sequence})
	if start.failure != nil {
		return durable.Key{}, cloneChildError(start.failure)
	}
	return start.value.Child, nil
}

// CancelChild requests cancellation and returns a future for its acknowledgment.
// The child's own future remains pending until its actual terminal result arrives.
func (w *Workflow) CancelChild(id string, target *Future) *Future {
	w.checkOperation()
	if target == nil || target.workflow != w || !w.ids[target.id] || target.kind != CommandChild {
		w.stop(fmt.Errorf("%w: cancellation requires a child from this evaluation", durable.ErrInvalid))
	}
	return w.schedule(Command{ID: id, Kind: CommandCancelChild, TargetID: target.id})
}

func validateChildCommand(c Command) error {
	if c.Version != 1 || c.Child == nil || c.Child.Key.Validate() != nil || !validIdentifier(c.Child.BuildID, 512) || !validID(c.Name) || len(c.Input) > 1<<20 || c.ActivityOptions != nil || c.Delay != 0 || !c.Deadline.IsZero() {
		return fmt.Errorf("%w: invalid child command", durable.ErrInvalid)
	}
	for _, timeout := range []time.Duration{c.Child.RunTimeout, c.Child.ExecutionTimeout} {
		if timeout != 0 && timeout < time.Microsecond {
			return durable.ErrInvalid
		}
	}
	switch c.Child.ParentClosePolicy {
	case durable.ParentCloseTerminate, durable.ParentCloseRequestCancel, durable.ParentCloseAbandon:
		return nil
	default:
		return durable.ErrInvalid
	}
}

func validateChildReference(command Command, commands map[string]Command, parent durable.Key) error {
	if command.Kind == CommandChild && (command.Child.Key.Namespace != parent.Namespace || command.Child.Key.WorkflowID == parent.WorkflowID) {
		return fmt.Errorf("%w: child identity is outside its parent", ErrHistory)
	}
	if command.Kind == CommandCancelChild {
		target, ok := commands[command.TargetID]
		if !ok || target.Kind != CommandChild || target.Index >= command.Index {
			return fmt.Errorf("%w: cancellation target is not a prior child", ErrHistory)
		}
	}
	return nil
}
