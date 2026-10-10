package durable

import (
	"context"
	"time"
)

const WorkerDrainSchemaVersion = 1

const OperationRequestWorkerDrain LifecycleOperation = "worker.drain.request"

// WorkerProcessIdentity binds one process incarnation to its immutable routing.
// A restarted process needs a new RuntimeID even when InstanceID is reused.
type WorkerProcessIdentity struct {
	BuildTarget
	Queue      string
	RuntimeID  string
	InstanceID string
}

func (i WorkerProcessIdentity) Validate() error {
	if i.BuildTarget.Validate() != nil || !DeliveryIdentifier(i.Queue) || !DeliveryIdentifier(i.RuntimeID) || !DeliveryIdentifier(i.InstanceID) {
		return ErrInvalid
	}
	return nil
}

// WorkerDrainRequest persists acceptance before a trusted host invokes drain.
// Its receipt never establishes actual admission closure or handler quiescence.
type WorkerDrainRequest struct {
	WorkerProcessIdentity
	RequestID     string
	CommandDigest string
	OperationID   string
	Deadline      time.Time
}

func (r WorkerDrainRequest) Validate() error {
	if r.WorkerProcessIdentity.Validate() != nil || !DeliveryIdentifier(r.RequestID) || !DeliveryIdentifier(r.OperationID) || r.Deadline.IsZero() || (r.CommandDigest != "" && !validHex256(r.CommandDigest)) {
		return ErrInvalid
	}
	return nil
}

type WorkerDrainStore interface {
	RequestWorkerDrain(context.Context, WorkerDrainRequest) (LifecycleReceipt, error)
}

func (r LifecycleReceipt) validWorkerDrainResult() bool {
	if r.WorkerDrain == nil || r.WorkerDrain.Validate() != nil || r.WorkerDrain.NamespaceTarget != r.NamespaceTarget || r.WorkerDrain.RequestID != r.RequestID || r.WorkerDrain.CommandDigest != r.CommandDigest || !r.WorkerDrain.Deadline.After(r.AcceptedAt) || r.QueryRuntime != nil || r.QueryAbort != nil || r.Enrollment != nil || r.Build != nil {
		return false
	}
	digest, err := Fingerprint(string(OperationRequestWorkerDrain), *r.WorkerDrain)
	return err == nil && digest == r.RequestDigest
}
