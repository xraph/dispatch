package dlq

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// ListOpts controls pagination and filtering for DLQ list queries.
type ListOpts struct {
	// Limit is the maximum number of entries to return. Zero means no limit.
	Limit int
	// Offset is the number of entries to skip.
	Offset int
	// Queue filters by queue name. Empty means all queues.
	Queue string
}

// Store defines the persistence contract for the dead letter queue.
type Store interface {
	// PushDLQ adds a failed job entry to the dead letter queue.
	PushDLQ(ctx context.Context, entry *Entry) error

	// ListDLQ returns DLQ entries matching the given options.
	ListDLQ(ctx context.Context, opts ListOpts) ([]*Entry, error)

	// GetDLQ retrieves a DLQ entry by ID.
	GetDLQ(ctx context.Context, entryID id.DLQID) (*Entry, error)

	// ReplayDLQ marks a DLQ entry as replayed. The actual re-enqueue is
	// handled at the service layer.
	ReplayDLQ(ctx context.Context, entryID id.DLQID) error

	// PurgeDLQ removes DLQ entries with FailedAt before the given time.
	// Returns the number of entries removed.
	PurgeDLQ(ctx context.Context, before time.Time) (int64, error)

	// CountDLQ returns the total number of entries in the dead letter queue.
	CountDLQ(ctx context.Context) (int64, error)
}
