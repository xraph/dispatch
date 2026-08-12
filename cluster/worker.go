package cluster

import (
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// WorkerState represents the lifecycle state of a worker.
type WorkerState string

const (
	// WorkerActive means the worker is healthy and processing jobs.
	WorkerActive WorkerState = "active"
	// WorkerDraining means the worker is finishing in-flight jobs
	// but not accepting new ones (graceful shutdown).
	WorkerDraining WorkerState = "draining"
	// WorkerDead means the worker has stopped responding and should
	// have its jobs reassigned.
	WorkerDead WorkerState = "dead"
)

// Worker represents a Dispatch worker instance in a distributed cluster.
type Worker struct {
	ID          id.WorkerID `json:"id"`
	Hostname    string      `json:"hostname"`
	Queues      []string    `json:"queues"`
	Concurrency int         `json:"concurrency"`
	State       WorkerState `json:"state"`
	// Capacity is what this worker can run at once. It is advisory:
	// empty means unknown, which disables the enqueue-time unschedulable
	// check rather than rejecting everything.
	Capacity    resource.Set      `json:"capacity,omitempty"`
	IsLeader    bool              `json:"is_leader"`
	LeaderUntil *time.Time        `json:"leader_until,omitempty"`
	LastSeen    time.Time         `json:"last_seen"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}
