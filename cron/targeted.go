package cron

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// TargetedUpdater writes single fields so a scheduler fire can never
// overwrite an operator's enable or disable. The whole-row UpdateCronEntry
// write-back is what let a disabled cron come back: the scheduler read the
// entry while it was enabled, an operator disabled it, and the scheduler's
// post-fire write put the old row, enabled flag and all, back on top.
//
// Each method changes only the columns it names. Every other field,
// including the lock and last_run_at, reads back exactly as it was.
type TargetedUpdater interface {
	// SetCronEnabled sets enabled, sets next_run_at when nextRunAt is
	// non-nil (nil leaves it unchanged), and stamps updated_at. It
	// returns dispatch.ErrCronNotFound when the entry does not exist.
	SetCronEnabled(ctx context.Context, entryID id.CronID, enabled bool, nextRunAt *time.Time) error

	// UpdateCronNextRun sets next_run_at and stamps updated_at, and
	// nothing else; in particular it never touches enabled. It returns
	// dispatch.ErrCronNotFound when the entry does not exist.
	UpdateCronNextRun(ctx context.Context, entryID id.CronID, nextRunAt time.Time) error
}
