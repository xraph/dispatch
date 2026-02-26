package workflow

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"

	"golang.org/x/sync/errgroup"
)

// Step executes a named step function. If a checkpoint exists for
// this step name, the step is skipped (idempotent replay). Otherwise
// the function is executed and a checkpoint is saved on success.
// On failure, EmitWorkflowStepFailed is called and the error is returned.
func (w *Workflow) Step(name string, fn func(ctx context.Context) error) error {
	// Check for existing checkpoint (resume case).
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, name)
	if err != nil {
		return fmt.Errorf("workflow %s: get checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		w.logger.Debug("skipping checkpointed step",
			slog.String("run_id", w.run.ID.String()),
			slog.String("step", name),
		)
		return nil
	}

	// Execute the step.
	start := time.Now()
	stepErr := fn(w.ctx)
	elapsed := time.Since(start)

	if stepErr != nil {
		w.emitter.EmitStepFailed(w.ctx, w.run, name, stepErr)
		return fmt.Errorf("workflow %s step %q: %w", w.run.Name, name, stepErr)
	}

	// Save checkpoint (empty data since Step has no result).
	if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, name, []byte{}); saveErr != nil {
		return fmt.Errorf("workflow %s: save checkpoint %q: %w", w.run.Name, name, saveErr)
	}

	w.emitter.EmitStepCompleted(w.ctx, w.run, name, elapsed)
	return nil
}

// StepWithResult executes a named step that returns a typed value.
// The result is serialized via encoding/gob and saved as a checkpoint.
// On resume, the cached result is deserialized and returned without
// re-executing the step function.
//
// This is a package-level generic function because Go does not allow
// generic methods on non-generic receiver types.
func StepWithResult[T any](w *Workflow, name string, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	// Check for existing checkpoint.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, name)
	if err != nil {
		return zero, fmt.Errorf("workflow %s: get checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		// Deserialize cached result.
		var result T
		dec := gob.NewDecoder(bytes.NewReader(data))
		if decErr := dec.Decode(&result); decErr != nil {
			return zero, fmt.Errorf("workflow %s: decode checkpoint %q: %w", w.run.Name, name, decErr)
		}
		w.logger.Debug("returning checkpointed result",
			slog.String("run_id", w.run.ID.String()),
			slog.String("step", name),
		)
		return result, nil
	}

	// Execute the step.
	start := time.Now()
	result, stepErr := fn(w.ctx)
	elapsed := time.Since(start)

	if stepErr != nil {
		w.emitter.EmitStepFailed(w.ctx, w.run, name, stepErr)
		return zero, fmt.Errorf("workflow %s step %q: %w", w.run.Name, name, stepErr)
	}

	// Serialize result via gob.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if encErr := enc.Encode(result); encErr != nil {
		return zero, fmt.Errorf("workflow %s: encode checkpoint %q: %w", w.run.Name, name, encErr)
	}

	// Save checkpoint.
	if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, name, buf.Bytes()); saveErr != nil {
		return zero, fmt.Errorf("workflow %s: save checkpoint %q: %w", w.run.Name, name, saveErr)
	}

	w.emitter.EmitStepCompleted(w.ctx, w.run, name, elapsed)
	return result, nil
}

// Parallel executes multiple step functions concurrently using errgroup.
// If any step fails, the others are cancelled and the first error is returned.
// Each sub-step gets its own checkpoint with the name "parallel:<group>:<index>".
// A group-level checkpoint marks overall completion for resume.
func (w *Workflow) Parallel(groupName string, steps ...func(ctx context.Context) error) error {
	groupKey := "parallel:" + groupName

	// Check if entire group already completed.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, groupKey)
	if err != nil {
		return fmt.Errorf("workflow %s: get parallel checkpoint %q: %w", w.run.Name, groupName, err)
	}
	if data != nil {
		w.logger.Debug("skipping checkpointed parallel group",
			slog.String("run_id", w.run.ID.String()),
			slog.String("group", groupName),
		)
		return nil
	}

	g, gctx := errgroup.WithContext(w.ctx)

	for i, step := range steps {
		stepName := fmt.Sprintf("parallel:%s:%d", groupName, i)
		fn := step
		g.Go(func() error {
			// Check individual step checkpoint.
			d, chkErr := w.store.GetCheckpoint(gctx, w.run.ID, stepName)
			if chkErr != nil {
				return chkErr
			}
			if d != nil {
				return nil // already completed
			}

			start := time.Now()
			if fnErr := fn(gctx); fnErr != nil {
				w.emitter.EmitStepFailed(w.ctx, w.run, stepName, fnErr)
				return fnErr
			}

			elapsed := time.Since(start)
			if saveErr := w.store.SaveCheckpoint(gctx, w.run.ID, stepName, []byte{}); saveErr != nil {
				return saveErr
			}
			w.emitter.EmitStepCompleted(w.ctx, w.run, stepName, elapsed)
			return nil
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		return fmt.Errorf("workflow %s parallel %q: %w", w.run.Name, groupName, waitErr)
	}

	// Save group-level checkpoint.
	return w.store.SaveCheckpoint(w.ctx, w.run.ID, groupKey, []byte{})
}

// WaitForEvent blocks until an event matching the given name is published
// or the timeout expires. On success the event is acknowledged and checkpointed.
// On timeout, an empty checkpoint is saved and nil is returned.
// On resume from checkpoint, the cached event is returned without re-waiting.
func (w *Workflow) WaitForEvent(name string, timeout time.Duration) (*event.Event, error) {
	stepName := "wait:" + name

	// Check for checkpoint.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, stepName)
	if err != nil {
		return nil, fmt.Errorf("workflow %s: get wait checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		if len(data) == 0 {
			// Timeout case — no event was received.
			return nil, nil
		}
		// Decode cached event (JSON because event.Event contains typeid fields
		// which have no exported fields and cannot be gob-encoded).
		var evt event.Event
		if decErr := json.Unmarshal(data, &evt); decErr != nil {
			return nil, fmt.Errorf("workflow %s: decode wait checkpoint %q: %w", w.run.Name, name, decErr)
		}
		return &evt, nil
	}

	// Subscribe for event.
	evt, subErr := w.eventStore.SubscribeEvent(w.ctx, name, timeout)
	if subErr != nil {
		return nil, fmt.Errorf("workflow %s wait %q: %w", w.run.Name, name, subErr)
	}

	if evt == nil {
		// Timeout — save empty checkpoint.
		if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte{}); saveErr != nil {
			return nil, saveErr
		}
		return nil, nil
	}

	// Ack the event.
	if ackErr := w.eventStore.AckEvent(w.ctx, evt.ID); ackErr != nil {
		w.logger.Warn("failed to ack event",
			slog.String("event_id", evt.ID.String()),
			slog.String("error", ackErr.Error()),
		)
	}

	// Checkpoint the event (JSON because typeid fields don't support gob).
	evtData, encErr := json.Marshal(evt)
	if encErr != nil {
		return nil, fmt.Errorf("workflow %s: encode wait result %q: %w", w.run.Name, name, encErr)
	}
	if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, evtData); saveErr != nil {
		return nil, saveErr
	}

	return evt, nil
}

// Sleep pauses the workflow for the specified duration. On crash recovery,
// if a checkpoint exists for this sleep step, it is skipped immediately.
// The sleep can be interrupted by context cancellation.
func (w *Workflow) Sleep(name string, d time.Duration) error {
	stepName := "sleep:" + name

	// Check for checkpoint.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, stepName)
	if err != nil {
		return fmt.Errorf("workflow %s: get sleep checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		w.logger.Debug("skipping checkpointed sleep",
			slog.String("run_id", w.run.ID.String()),
			slog.String("step", name),
		)
		return nil
	}

	// Actually sleep.
	select {
	case <-time.After(d):
	case <-w.ctx.Done():
		return w.ctx.Err()
	}

	// Save checkpoint.
	return w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte{})
}

// ── Saga Compensations ──────────────────────────────

// StepWithCompensation executes a named step with an associated compensation
// function. If the step succeeds, the compensation is pushed onto a LIFO stack.
// When the workflow fails later, all registered compensations run in reverse
// order to undo completed work (saga pattern).
func (w *Workflow) StepWithCompensation(
	name string,
	execute func(ctx context.Context) error,
	compensate func(ctx context.Context) error,
) error {
	if err := w.Step(name, execute); err != nil {
		return err
	}
	w.compensations = append(w.compensations, Compensation{
		StepName:   name,
		Compensate: compensate,
	})
	return nil
}

// StepWithResultAndCompensation executes a named step that returns a typed
// value, with an associated compensation function. If the step succeeds,
// the result is checkpointed and the compensation is registered on the stack.
func StepWithResultAndCompensation[T any](
	w *Workflow,
	name string,
	execute func(ctx context.Context) (T, error),
	compensate func(ctx context.Context) error,
) (T, error) {
	result, err := StepWithResult(w, name, execute)
	if err != nil {
		return result, err
	}
	w.compensations = append(w.compensations, Compensation{
		StepName:   name,
		Compensate: compensate,
	})
	return result, nil
}

// ── Child Workflow Composition ──────────────────────

// RunChild starts a child workflow and blocks until it completes, returning
// the decoded result. The child run is linked to the parent via ParentRunID
// and the result is checkpointed for crash recovery.
//
// T is the input type, R is the result type (decoded from the child's JSON output).
func RunChild[T, R any](w *Workflow, name string, input T) (R, error) {
	var zero R
	stepName := "child:" + name

	// Check for existing checkpoint.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, stepName)
	if err != nil {
		return zero, fmt.Errorf("workflow %s: get child checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		var result R
		if decErr := json.Unmarshal(data, &result); decErr != nil {
			return zero, fmt.Errorf("workflow %s: decode child checkpoint %q: %w", w.run.Name, name, decErr)
		}
		w.logger.Debug("returning checkpointed child result",
			slog.String("run_id", w.run.ID.String()),
			slog.String("child", name),
		)
		return result, nil
	}

	if w.childStarter == nil {
		return zero, fmt.Errorf("workflow %s: child starter not configured", w.run.Name)
	}

	// Marshal input.
	inputData, marshalErr := json.Marshal(input)
	if marshalErr != nil {
		return zero, fmt.Errorf("workflow %s: marshal child input %q: %w", w.run.Name, name, marshalErr)
	}

	// Start child workflow (blocking).
	start := time.Now()
	childRun, startErr := w.childStarter.StartChildRaw(w.ctx, w.run.ID, name, inputData)
	elapsed := time.Since(start)

	if startErr != nil {
		w.emitter.EmitStepFailed(w.ctx, w.run, stepName, startErr)
		return zero, fmt.Errorf("workflow %s child %q: %w", w.run.Name, name, startErr)
	}

	// Check child result.
	if childRun.State == RunStateFailed {
		childErr := fmt.Errorf("child workflow %q failed: %s", name, childRun.Error)
		w.emitter.EmitStepFailed(w.ctx, w.run, stepName, childErr)
		return zero, childErr
	}

	// Decode child output.
	var result R
	if len(childRun.Output) > 0 {
		if decErr := json.Unmarshal(childRun.Output, &result); decErr != nil {
			return zero, fmt.Errorf("workflow %s: decode child output %q: %w", w.run.Name, name, decErr)
		}
	}

	// Checkpoint the result (JSON for consistency with child output format).
	chkData, encErr := json.Marshal(result)
	if encErr != nil {
		return zero, fmt.Errorf("workflow %s: encode child checkpoint %q: %w", w.run.Name, name, encErr)
	}
	if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, chkData); saveErr != nil {
		return zero, fmt.Errorf("workflow %s: save child checkpoint %q: %w", w.run.Name, name, saveErr)
	}

	w.emitter.EmitStepCompleted(w.ctx, w.run, stepName, elapsed)
	return result, nil
}

// SpawnChild starts a child workflow asynchronously (fire-and-forget)
// and returns the child's run ID. The child run is linked to the parent
// via ParentRunID and executes in a background goroutine. Use the Store's
// GetRun to check on its status later.
func SpawnChild[T any](w *Workflow, name string, input T) (id.RunID, error) {
	stepName := "spawn:" + name

	// Check for existing checkpoint (contains the child run ID string).
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, stepName)
	if err != nil {
		return id.Nil, fmt.Errorf("workflow %s: get spawn checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		runID, parseErr := id.ParseRunID(string(data))
		if parseErr != nil {
			return id.Nil, fmt.Errorf("workflow %s: decode spawn checkpoint %q: %w", w.run.Name, name, parseErr)
		}
		w.logger.Debug("returning checkpointed spawn ID",
			slog.String("run_id", w.run.ID.String()),
			slog.String("child", name),
			slog.String("child_run_id", runID.String()),
		)
		return runID, nil
	}

	if w.childStarter == nil {
		return id.Nil, fmt.Errorf("workflow %s: child starter not configured", w.run.Name)
	}

	// Marshal input.
	inputData, marshalErr := json.Marshal(input)
	if marshalErr != nil {
		return id.Nil, fmt.Errorf("workflow %s: marshal spawn input %q: %w", w.run.Name, name, marshalErr)
	}

	// Spawn child workflow (non-blocking).
	start := time.Now()
	childRun, spawnErr := w.childStarter.SpawnChildRaw(w.ctx, w.run.ID, name, inputData)
	elapsed := time.Since(start)

	if spawnErr != nil {
		w.emitter.EmitStepFailed(w.ctx, w.run, stepName, spawnErr)
		return id.Nil, fmt.Errorf("workflow %s spawn %q: %w", w.run.Name, name, spawnErr)
	}

	// Checkpoint the child run ID.
	if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte(childRun.ID.String())); saveErr != nil {
		return id.Nil, fmt.Errorf("workflow %s: save spawn checkpoint %q: %w", w.run.Name, name, saveErr)
	}

	w.emitter.EmitStepCompleted(w.ctx, w.run, stepName, elapsed)
	return childRun.ID, nil
}

// FanOut starts N child workflows in parallel and collects all results.
// Each child is independently checkpointed via RunChild. If any child
// fails, the remaining are cancelled and the error is returned.
func FanOut[T, R any](w *Workflow, name string, inputs []T) ([]R, error) {
	groupKey := "fanout:" + name

	// Check if entire group already completed.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, groupKey)
	if err != nil {
		return nil, fmt.Errorf("workflow %s: get fanout checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		var results []R
		if decErr := json.Unmarshal(data, &results); decErr != nil {
			return nil, fmt.Errorf("workflow %s: decode fanout checkpoint %q: %w", w.run.Name, name, decErr)
		}
		w.logger.Debug("returning checkpointed fanout results",
			slog.String("run_id", w.run.ID.String()),
			slog.String("group", name),
		)
		return results, nil
	}

	// Execute children in parallel.
	results := make([]R, len(inputs))
	g, gctx := errgroup.WithContext(w.ctx)

	for i, input := range inputs {
		childKey := fmt.Sprintf("%s:%d", name, i) // unique checkpoint key per child
		baseName := name                          // workflow registry name (base)
		inp := input
		idx := i
		g.Go(func() error {
			// Check for existing checkpoint for this indexed child.
			stepName := "child:" + childKey
			cpData, cpErr := w.store.GetCheckpoint(gctx, w.run.ID, stepName)
			if cpErr != nil {
				return fmt.Errorf("workflow %s: get fanout child checkpoint %q: %w", w.run.Name, childKey, cpErr)
			}
			if cpData != nil {
				var result R
				if decErr := json.Unmarshal(cpData, &result); decErr != nil {
					return fmt.Errorf("workflow %s: decode fanout child checkpoint %q: %w", w.run.Name, childKey, decErr)
				}
				results[idx] = result
				return nil
			}

			if w.childStarter == nil {
				return fmt.Errorf("workflow %s: child starter not configured", w.run.Name)
			}

			inputData, marshalErr := json.Marshal(inp)
			if marshalErr != nil {
				return fmt.Errorf("workflow %s: marshal fanout child input %q: %w", w.run.Name, childKey, marshalErr)
			}

			// Start child using the base workflow name (not the indexed key).
			childRun, startErr := w.childStarter.StartChildRaw(gctx, w.run.ID, baseName, inputData)
			if startErr != nil {
				return fmt.Errorf("workflow %s fanout child %q: %w", w.run.Name, childKey, startErr)
			}

			if childRun.State == RunStateFailed {
				return fmt.Errorf("fanout child %q failed: %s", childKey, childRun.Error)
			}

			var result R
			if len(childRun.Output) > 0 {
				if decErr := json.Unmarshal(childRun.Output, &result); decErr != nil {
					return fmt.Errorf("workflow %s: decode fanout child output %q: %w", w.run.Name, childKey, decErr)
				}
			}

			// Checkpoint individual child result.
			chk, encErr := json.Marshal(result)
			if encErr != nil {
				return fmt.Errorf("workflow %s: encode fanout child checkpoint %q: %w", w.run.Name, childKey, encErr)
			}
			if saveErr := w.store.SaveCheckpoint(gctx, w.run.ID, stepName, chk); saveErr != nil {
				return fmt.Errorf("workflow %s: save fanout child checkpoint %q: %w", w.run.Name, childKey, saveErr)
			}

			results[idx] = result
			return nil
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		return nil, fmt.Errorf("workflow %s fanout %q: %w", w.run.Name, name, waitErr)
	}

	// Checkpoint the collected results.
	chkData, encErr := json.Marshal(results)
	if encErr != nil {
		return nil, fmt.Errorf("workflow %s: encode fanout checkpoint %q: %w", w.run.Name, name, encErr)
	}
	if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, groupKey, chkData); saveErr != nil {
		return nil, fmt.Errorf("workflow %s: save fanout checkpoint %q: %w", w.run.Name, name, saveErr)
	}

	return results, nil
}

// ── Reactive Event Patterns ─────────────────────────

// WaitForAll blocks until events matching ALL specified names are received
// or the timeout expires. Each received event is individually checkpointed.
// Returns an error if any subscription times out.
func (w *Workflow) WaitForAll(names []string, timeout time.Duration) ([]*event.Event, error) {
	groupKey := "waitall:" + strings.Join(names, ",")

	// Check for group checkpoint.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, groupKey)
	if err != nil {
		return nil, fmt.Errorf("workflow %s: get waitall checkpoint: %w", w.run.Name, err)
	}
	if data != nil {
		var events []*event.Event
		if decErr := json.Unmarshal(data, &events); decErr != nil {
			return nil, fmt.Errorf("workflow %s: decode waitall checkpoint: %w", w.run.Name, decErr)
		}
		return events, nil
	}

	// Wait for each event in parallel.
	events := make([]*event.Event, len(names))
	g, gctx := errgroup.WithContext(w.ctx)

	for i, name := range names {
		idx := i
		evtName := name
		g.Go(func() error {
			stepName := "waitall:" + evtName

			// Check individual checkpoint.
			d, chkErr := w.store.GetCheckpoint(gctx, w.run.ID, stepName)
			if chkErr != nil {
				return chkErr
			}
			if len(d) > 0 {
				var evt event.Event
				if decErr := json.Unmarshal(d, &evt); decErr != nil {
					return decErr
				}
				events[idx] = &evt
				return nil
			}

			evt, subErr := w.eventStore.SubscribeEvent(gctx, evtName, timeout)
			if subErr != nil {
				return fmt.Errorf("wait for %q: %w", evtName, subErr)
			}
			if evt == nil {
				return fmt.Errorf("timeout waiting for event %q", evtName)
			}

			// Ack and checkpoint (best-effort: event already received).
			//nolint:errcheck // best-effort ack/checkpoint after successful receive
			w.eventStore.AckEvent(gctx, evt.ID)
			evtData, marshalErr := json.Marshal(evt)
			if marshalErr != nil {
				return fmt.Errorf("marshal event %q: %w", evtName, marshalErr)
			}
			//nolint:errcheck // best-effort checkpoint
			w.store.SaveCheckpoint(gctx, w.run.ID, stepName, evtData)

			events[idx] = evt
			return nil
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		return nil, fmt.Errorf("workflow %s waitall: %w", w.run.Name, waitErr)
	}

	// Checkpoint all events.
	allData, marshalErr := json.Marshal(events)
	if marshalErr != nil {
		return nil, fmt.Errorf("workflow %s: marshal waitall events: %w", w.run.Name, marshalErr)
	}
	//nolint:errcheck // best-effort group checkpoint
	w.store.SaveCheckpoint(w.ctx, w.run.ID, groupKey, allData)

	return events, nil
}

// WaitForAny blocks until an event matching ANY of the given names is
// received or the timeout expires. Returns the first event received,
// or nil on timeout.
func (w *Workflow) WaitForAny(names []string, timeout time.Duration) (*event.Event, error) {
	stepName := "waitany:" + strings.Join(names, ",")

	// Check for checkpoint.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, stepName)
	if err != nil {
		return nil, fmt.Errorf("workflow %s: get waitany checkpoint: %w", w.run.Name, err)
	}
	if data != nil {
		if len(data) == 0 {
			return nil, nil
		}
		var evt event.Event
		if decErr := json.Unmarshal(data, &evt); decErr != nil {
			return nil, fmt.Errorf("workflow %s: decode waitany checkpoint: %w", w.run.Name, decErr)
		}
		return &evt, nil
	}

	// Race: subscribe to all names, return the first event.
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()

	type result struct {
		evt *event.Event
		err error
	}
	ch := make(chan result, len(names))

	for _, name := range names {
		evtName := name
		go func() {
			evt, subErr := w.eventStore.SubscribeEvent(ctx, evtName, timeout)
			ch <- result{evt, subErr}
		}()
	}

	// Wait for first non-nil result or all completions.
	var firstEvt *event.Event
	var lastErr error
	for range names {
		select {
		case r := <-ch:
			if r.err != nil {
				lastErr = r.err
				continue
			}
			if r.evt != nil {
				firstEvt = r.evt
				cancel() // Cancel remaining subscriptions.
				goto done
			}
		case <-ctx.Done():
			//nolint:errcheck // best-effort timeout checkpoint
			w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte{})
			return nil, nil
		}
	}
done:

	if firstEvt == nil {
		//nolint:errcheck // best-effort nil-result checkpoint
		w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte{})
		if lastErr != nil {
			return nil, fmt.Errorf("workflow %s waitany: %w", w.run.Name, lastErr)
		}
		return nil, nil
	}

	// Ack and checkpoint (best-effort: event already received).
	//nolint:errcheck // best-effort ack after successful receive
	w.eventStore.AckEvent(w.ctx, firstEvt.ID)
	evtData, marshalErr := json.Marshal(firstEvt)
	if marshalErr != nil {
		return nil, fmt.Errorf("workflow %s: marshal waitany event: %w", w.run.Name, marshalErr)
	}
	//nolint:errcheck // best-effort checkpoint
	w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, evtData)

	return firstEvt, nil
}

// WaitForMatch blocks until an event matching the given name AND predicate
// is received, or the timeout expires. Events that don't match the predicate
// are skipped (not acknowledged). Returns nil on timeout.
func (w *Workflow) WaitForMatch(
	name string,
	timeout time.Duration,
	match func(*event.Event) bool,
) (*event.Event, error) {
	stepName := "waitmatch:" + name

	// Check for checkpoint.
	data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, stepName)
	if err != nil {
		return nil, fmt.Errorf("workflow %s: get waitmatch checkpoint %q: %w", w.run.Name, name, err)
	}
	if data != nil {
		if len(data) == 0 {
			return nil, nil
		}
		var evt event.Event
		if decErr := json.Unmarshal(data, &evt); decErr != nil {
			return nil, fmt.Errorf("workflow %s: decode waitmatch checkpoint %q: %w", w.run.Name, name, decErr)
		}
		return &evt, nil
	}

	// Poll for matching events within the timeout.
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			//nolint:errcheck // best-effort timeout checkpoint
			w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte{})
			return nil, nil
		}

		evt, subErr := w.eventStore.SubscribeEvent(w.ctx, name, remaining)
		if subErr != nil {
			return nil, fmt.Errorf("workflow %s waitmatch %q: %w", w.run.Name, name, subErr)
		}
		if evt == nil {
			// Timeout.
			//nolint:errcheck // best-effort timeout checkpoint
			w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte{})
			return nil, nil
		}

		if match(evt) {
			// Match found. Ack and checkpoint.
			//nolint:errcheck // best-effort ack after successful match
			w.eventStore.AckEvent(w.ctx, evt.ID)
			evtData, marshalErr := json.Marshal(evt)
			if marshalErr != nil {
				return nil, fmt.Errorf("workflow %s: marshal waitmatch event: %w", w.run.Name, marshalErr)
			}
			//nolint:errcheck // best-effort checkpoint
			w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, evtData)
			return evt, nil
		}

		// No match — skip and try again.
	}
}
