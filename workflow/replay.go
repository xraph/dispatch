package workflow

import (
	"context"
	"fmt"
	"sort"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
)

// ReplayPlan says what replaying a run from a step does. PlanReplay
// returns it without changing anything; ReplayFrom returns the plan it
// acted on.
type ReplayPlan struct {
	RunID    id.RunID `json:"run_id"`
	FromStep string   `json:"from_step"`

	// Version is the run's stamped version, the one the replay runs on.
	// A run stamped 0 predates versioning and runs on version 1.
	Version int `json:"version"`

	// Reruns names the checkpointed steps after FromStep whose
	// checkpoints the replay deletes, so they run again, in checkpoint
	// order. Steps the run never reached are not listed but run too.
	Reruns []string `json:"reruns"`

	// State is the run's state when the plan was made. ReplayFrom
	// refuses a running run, so a caller can refuse it early from here.
	State RunState `json:"state"`

	// Generation fences the claim against the run used for this plan.
	Generation int64 `json:"generation"`
}

// PlanReplay reports what ReplayFrom would do for the run and step,
// without changing anything. It fails when the run does not exist
// (wrapping dispatch.ErrRunNotFound), when fromStep has no checkpoint,
// or when the run's stamped version is not registered (both wrapping
// dispatch.ErrInvalidState). A running run plans fine; its State says
// ReplayFrom would refuse it.
func (r *Runner) PlanReplay(ctx context.Context, runID id.RunID, fromStep string) (*ReplayPlan, error) {
	plan, _, err := r.planReplay(ctx, runID, fromStep)
	return plan, err
}

// planReplay is PlanReplay, also returning the stamped version's runner.
func (r *Runner) planReplay(ctx context.Context, runID id.RunID, fromStep string) (*ReplayPlan, RunnerFunc, error) {
	run, err := r.store.GetRun(ctx, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("get run %s: %w", runID, err)
	}

	checkpoints, err := r.store.ListCheckpoints(ctx, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("list checkpoints for run %s: %w", runID, err)
	}

	var target *Checkpoint
	for _, cp := range checkpoints {
		if cp.StepName == fromStep {
			target = cp
			break
		}
	}
	if target == nil {
		return nil, nil, fmt.Errorf("%w: run %s has no checkpoint for step %q",
			dispatch.ErrInvalidState, runID, fromStep)
	}

	version := stampedVersion(run)
	runner, ok := r.registry.GetVersion(run.Name, version)
	if !ok {
		return nil, nil, fmt.Errorf("%w: workflow %q version %d is not registered (run %s)",
			dispatch.ErrInvalidState, run.Name, version, runID)
	}

	// Reruns is what DeleteCheckpointsAfter removes on the durable
	// backends: every checkpoint created strictly after fromStep's, in
	// creation order, with the ID breaking a tie as GetTimeline does.
	later := make([]*Checkpoint, 0, len(checkpoints))
	for _, cp := range checkpoints {
		if cp.CreatedAt.After(target.CreatedAt) {
			later = append(later, cp)
		}
	}
	sort.SliceStable(later, func(i, j int) bool {
		if later[i].CreatedAt.Equal(later[j].CreatedAt) {
			return later[i].ID.String() < later[j].ID.String()
		}
		return later[i].CreatedAt.Before(later[j].CreatedAt)
	})
	reruns := make([]string, len(later))
	for i, cp := range later {
		reruns[i] = cp.StepName
	}

	return &ReplayPlan{
		RunID:      runID,
		FromStep:   fromStep,
		Version:    version,
		Reruns:     reruns,
		State:      run.State,
		Generation: run.ReplayGeneration,
	}, runner, nil
}

// stampedVersion is the version a run executes on. Run.Version 0 means
// version 1 (see Run.Version); Registry.GetVersion would read 0 as
// "latest", which for an old run is the wrong handler.
func stampedVersion(run *Run) int {
	if run.Version <= 0 {
		return 1
	}
	return run.Version
}

// ReplayFrom re-runs a finished run from a step. Checkpoints up to and
// including fromStep are kept, so those steps are skipped; every later
// checkpoint is deleted and those steps run again. The run executes on
// its stamped version, never a newer one registered since.
//
// The refusals are PlanReplay's, plus: a running run, or one another
// replay claims first, wraps dispatch.ErrInvalidState; once Shutdown has
// begun, ErrRunnerShutdown. Of two concurrent calls exactly one starts.
//
// The run executes on the runner's background launcher and ReplayFrom
// returns the plan as soon as it has started. The run keeps the values
// of ctx but not its cancellation: Shutdown cancels it instead.
func (r *Runner) ReplayFrom(ctx context.Context, runID id.RunID, fromStep string) (*ReplayPlan, error) {
	plan, runner, err := r.planReplay(ctx, runID, fromStep)
	if err != nil {
		return nil, err
	}
	if plan.State == RunStateRunning {
		return nil, fmt.Errorf("%w: run %s is %s", dispatch.ErrInvalidState, runID, plan.State)
	}

	// Count the replay as in flight before claiming the run, so Shutdown
	// either refuses it here, with the run untouched, or waits for it.
	if !r.track() {
		return nil, fmt.Errorf("replay run %s: %w", runID, ErrRunnerShutdown)
	}
	launched := false
	defer func() {
		if !launched {
			r.inflight.Done()
		}
	}()

	// The claim. Of two replays that both planned, one reopens the run
	// and the other is refused here.
	if reopenErr := r.store.ReopenRun(ctx, runID, plan.Generation); reopenErr != nil {
		return nil, fmt.Errorf("reopen run %s: %w", runID, reopenErr)
	}

	if delErr := r.store.DeleteCheckpointsAfter(ctx, runID, fromStep); delErr != nil {
		err = fmt.Errorf("delete checkpoints after %q for run %s: %w", fromStep, runID, delErr)
		r.failReopened(ctx, runID, err)
		return nil, err
	}

	stored, err := r.store.GetRun(ctx, runID)
	if err != nil {
		err = fmt.Errorf("get reopened run %s: %w", runID, err)
		r.failReopened(ctx, runID, err)
		return nil, err
	}
	// The background run writes to its own copy, never to a value a
	// store handed out to other readers.
	run := *stored

	r.emitter.EmitWorkflowStarted(ctx, &run)

	launched = true
	go func() {
		defer r.inflight.Done()
		bg, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		stop := context.AfterFunc(r.life, cancel)
		defer stop()
		r.executeRun(bg, &run, runner, run.Input)
	}()

	return plan, nil
}

// failReopened puts a run ReplayFrom reopened, but could not hand to an
// executor, back to failed with the reason. Without it the run would sit
// in running with nothing executing it until the next ResumeAll. The
// writes ignore ctx's cancellation, since a cancelled request is one way
// to get here. A failure is logged: the run then stays running, and the
// next ResumeAll resumes it from whatever checkpoints remain.
func (r *Runner) failReopened(ctx context.Context, runID id.RunID, cause error) {
	ctx = context.WithoutCancel(ctx)

	stored, err := r.store.GetRun(ctx, runID)
	if err != nil {
		r.logger.Error("replay not started and the run could not be marked failed",
			log.String("run_id", runID.String()),
			log.String("cause", cause.Error()),
			log.String("error", err.Error()),
		)
		return
	}

	failed := *stored
	now := time.Now().UTC()
	failed.State = RunStateFailed
	failed.Error = "replay not started: " + cause.Error()
	failed.CompletedAt = &now
	if updateErr := r.store.UpdateRun(ctx, &failed); updateErr != nil {
		r.logger.Error("replay not started and the run could not be marked failed",
			log.String("run_id", runID.String()),
			log.String("cause", cause.Error()),
			log.String("error", updateErr.Error()),
		)
	}
}
