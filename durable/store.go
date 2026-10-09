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
	ErrTaskConflict     = errors.New("durable: task observation changed")
	ErrTaskDeadline     = errors.New("durable: task deadline expired")
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
	// Signal receipts are scoped by namespace/workflow across runs and survive
	// target closure. Acceptance records history and runnable work atomically.
	SignalExecution(context.Context, SignalRequest) (SignalReceipt, error)
	SignalWithStart(context.Context, SignalWithStartRequest) (SignalReceipt, error)
	// RequestCancelExecution records acceptance and a wakeup, not terminal state.
	RequestCancelExecution(context.Context, CancelExecutionRequest) (CancelExecutionReceipt, error)
	GetExecution(context.Context, Key) (Execution, error)
	// ResolveExecution fixes one snapshot without claiming tasks or writing history.
	ResolveExecution(context.Context, ExecutionTarget) (Execution, error)
	GetChildExecution(context.Context, Key, string) (ChildExecution, error)
	GetParentExecution(context.Context, Key) (ChildExecution, error)
	ListChildExecutions(context.Context, Key, string, int) ([]ChildExecution, error)
	ClaimChildDelivery(context.Context, ChildDeliveryClaimRequest) (*ChildDelivery, error)
	ApplyChildDelivery(context.Context, ChildDeliveryRequest) (ChildDeliveryReceipt, error)
	GetChildDelivery(context.Context, Key, string) (ChildDelivery, error)
	ListChildDeliveries(context.Context, Key, string, int) ([]ChildDelivery, error)
	// GetTask returns persisted state, including finished tasks, in one namespace.
	GetTask(context.Context, Key, string) (Task, error)
	// ReadHistory returns events after the exclusive cursor, in sequence order.
	// Limits must be between 1 and 1000. A missing run is ErrNotFound.
	ReadHistory(ctx context.Context, key Key, after int64, limit int) ([]Event, error)
	// ClaimTask returns nil when no eligible task exists. Reclaims increment epoch.
	ClaimTask(context.Context, ClaimRequest) (*Task, error)
	// ClaimTimeoutTask grants expired activity processing without consuming an execution attempt.
	ClaimTimeoutTask(context.Context, TimeoutClaimRequest) (*Task, error)
	RenewTask(context.Context, Key, TaskToken, time.Duration) (time.Time, error)
	// RecordHeartbeat changes task progress without appending workflow history.
	RecordHeartbeat(context.Context, HeartbeatRequest) (Receipt, error)
	CommitTransition(context.Context, CommitRequest) (Receipt, error)
	// LookupReceipt recovers a matching client intent without checking current
	// ownership or lifecycle. A missing receipt is false,nil; a missing run is
	// ErrNotFound. Errors return false and a zero receipt. A miss cannot rule out
	// an in-flight commit. Legacy receipts reject lookup with ErrRequestConflict.
	LookupReceipt(context.Context, ReceiptRequest) (Receipt, bool, error)
}
