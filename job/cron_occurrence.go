package job

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/dispatch/id"
)

// ErrCronOccurrenceLost means a scheduled occurrence changed or was already committed.
var ErrCronOccurrenceLost = errors.New("dispatch: cron occurrence no longer owned")

// CronOccurrence identifies the exact scheduled occurrence and its lock holder.
type CronOccurrence struct {
	CronID             id.CronID
	WorkerID           id.WorkerID
	Due, Next, FiredAt time.Time
	Schedule           string
}

// CronOccurrenceStore atomically inserts a job and advances its cron entry.
// A stale due time, disabled entry or lost lock must return ErrCronOccurrenceLost.
type CronOccurrenceStore interface {
	EnqueueCronOccurrence(context.Context, *Job, CronOccurrence) error
}
