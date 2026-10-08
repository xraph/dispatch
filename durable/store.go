package durable

import (
	"context"
	"errors"
	"time"
)

// Store errors let the coordinator distinguish conflicts from transport failures.
var (
	ErrInvalid          = errors.New("durable: invalid request")
	ErrNotFound         = errors.New("durable: execution not found")
	ErrExists           = errors.New("durable: identity already exists")
	ErrRequestConflict  = errors.New("durable: request ID reused with different content")
	ErrRevisionConflict = errors.New("durable: execution revision changed")
	ErrLeaseLost        = errors.New("durable: task lease lost")
	ErrClosed           = errors.New("durable: execution closed")
)

// Store is an explicit backend capability. Implementations must provide atomic
// transitions and durable receipts; implementing the older workflow.Store does
// not satisfy this contract. Memory implements it only for tests and development.
//
// Lease checks use the store's clock and reject expired grants before reclamation.
// A transport error can mean an unknown commit outcome. Retry the identical
// request to resolve that uncertainty through its receipt.
type Store interface {
	StartExecution(context.Context, StartRequest) (Receipt, error)
	GetExecution(context.Context, Key) (Execution, error)
	// ReadHistory returns events after the exclusive cursor, in sequence order.
	// Limits must be between 1 and 1000. A missing run is ErrNotFound.
	ReadHistory(ctx context.Context, key Key, after int64, limit int) ([]Event, error)
	// ClaimTask returns nil when no eligible task exists. Reclaims increment epoch.
	ClaimTask(context.Context, ClaimRequest) (*Task, error)
	RenewTask(context.Context, Key, TaskToken, time.Duration) (time.Time, error)
	CommitTransition(context.Context, CommitRequest) (Receipt, error)
}
