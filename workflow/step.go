package workflow

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/xraph/dispatch/event"

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
