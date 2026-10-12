package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// EnableCron turns a cron entry on and returns it as stored.
//
// The next fire time is computed from now, so an entry that was off past
// its old next_run_at does not fire a catch-up the moment it comes back.
// For an @every schedule that restarts the interval, and enabling an
// entry that is already on does the same. A schedule with no fire time in
// the next five years (30 February) is refused with a wrapped
// dispatch.ErrInvalidState, since the scheduler would have nothing to
// wait for. dispatch.ErrCronNotFound when the entry does not exist.
func (eng *Engine) EnableCron(ctx context.Context, cronID id.CronID) (*cron.Entry, error) {
	entry, err := eng.cronStore.GetCron(ctx, cronID)
	if err != nil {
		return nil, err
	}

	fires, _, err := cron.NextFires(entry.Schedule, time.Now().UTC(), 1)
	if err != nil {
		return nil, fmt.Errorf("enable cron %s: %w", cronID, err)
	}
	if len(fires) == 0 {
		return nil, fmt.Errorf("%w: cron %s schedule %q has no fire time in the next five years",
			dispatch.ErrInvalidState, cronID, entry.Schedule)
	}
	next := fires[0].UTC()

	if err := eng.cronStore.SetCronEnabled(ctx, cronID, true, &next); err != nil {
		return nil, err
	}
	return eng.cronToggled(ctx, cronID, ext.ActionCronEnabled)
}

// DisableCron turns a cron entry off and returns it as stored. Its
// next_run_at is left as it was. dispatch.ErrCronNotFound when the entry
// does not exist.
func (eng *Engine) DisableCron(ctx context.Context, cronID id.CronID) (*cron.Entry, error) {
	if err := eng.cronStore.SetCronEnabled(ctx, cronID, false, nil); err != nil {
		return nil, err
	}
	return eng.cronToggled(ctx, cronID, ext.ActionCronDisabled)
}

// DeleteCron removes a cron entry. dispatch.ErrCronNotFound when it does
// not exist.
func (eng *Engine) DeleteCron(ctx context.Context, cronID id.CronID) error {
	if err := eng.cronStore.DeleteCron(ctx, cronID); err != nil {
		return err
	}
	eng.invalidateCronCache()
	eng.extensions.EmitOperatorAction(ctx, ext.Action{Kind: ext.ActionCronDeleted, CronID: cronID})
	return nil
}

// TriggerCron enqueues the entry's job now, with its payload and queue,
// the same job a scheduled fire makes. The schedule is untouched:
// next_run_at and last_run_at stay as they were, so the entry still fires
// at its next scheduled time. A disabled entry can be triggered too, since
// running it by hand is exactly what an operator asks for here. Hooks see
// CronFired with the new job, as for a scheduled fire, and then the
// operator action. dispatch.ErrCronNotFound when the entry does not exist.
func (eng *Engine) TriggerCron(ctx context.Context, cronID id.CronID) (*job.Job, error) {
	entry, err := eng.cronStore.GetCron(ctx, cronID)
	if err != nil {
		return nil, err
	}

	var opts []job.Option
	if entry.Queue != "" {
		opts = append(opts, job.WithQueue(entry.Queue))
	}
	j, err := eng.EnqueueRaw(ctx, entry.JobName, entry.Payload, opts...)
	if err != nil {
		return nil, fmt.Errorf("trigger cron %s: %w", cronID, err)
	}

	eng.extensions.EmitCronFired(ctx, entry.Name, j.ID)
	eng.extensions.EmitOperatorAction(ctx, ext.Action{
		Kind:     ext.ActionCronTriggered,
		CronID:   cronID,
		NewJobID: j.ID,
	})
	return j, nil
}

// cronToggled finishes an enable or a disable once the write has landed:
// it makes the local scheduler re-list before its next tick, emits the
// action, and reads the entry back for the caller.
func (eng *Engine) cronToggled(ctx context.Context, cronID id.CronID, kind ext.ActionKind) (*cron.Entry, error) {
	eng.invalidateCronCache()
	eng.extensions.EmitOperatorAction(ctx, ext.Action{Kind: kind, CronID: cronID})
	return eng.cronStore.GetCron(ctx, cronID)
}

// invalidateCronCache reaches only this process's scheduler. A leader on
// another node keeps its cached list until its own refresh, which is why
// the scheduler re-reads an entry under its lock before firing it.
func (eng *Engine) invalidateCronCache() {
	if eng.scheduler != nil {
		eng.scheduler.InvalidateCronCache()
	}
}
