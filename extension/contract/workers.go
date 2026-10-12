package contract

import (
	"context"
	"slices"
	"strings"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

type WorkerRow struct {
	ID                string              `json:"id"`
	Hostname          *string             `json:"hostname"`
	Queues            []string            `json:"queues"`
	Concurrency       int                 `json:"concurrency"`
	State             cluster.WorkerState `json:"state"`
	Self              bool                `json:"self"`
	IsLeader          bool                `json:"isLeader"`
	LeaderUntil       *string             `json:"leaderUntil"`
	LastSeen          *string             `json:"lastSeen"`
	CreatedAt         *string             `json:"createdAt"`
	HeartbeatAge      *Duration           `json:"heartbeatAge"`
	HeartbeatInterval *Duration           `json:"heartbeatInterval"`
	HeartbeatStatus   string              `json:"heartbeatStatus"`
	ClockSkew         bool                `json:"clockSkew"`
	Capacity          resource.Set        `json:"capacity"`
}
type WorkersPage struct {
	Page[WorkerRow]
	Enabled            bool      `json:"enabled"`
	LeaderID           *string   `json:"leaderId"`
	HeartbeatReference *Duration `json:"heartbeatReference"`
	SilentAfter        *Duration `json:"silentAfter"`
}
type ResourceLeaseRow struct {
	Owner      string       `json:"owner"`
	Held       resource.Set `json:"held"`
	AcquiredAt *string      `json:"acquiredAt"`
}
type LocalResources struct {
	Enabled     bool               `json:"enabled"`
	Capacity    resource.Set       `json:"capacity"`
	Free        resource.Set       `json:"free"`
	Reclaimable resource.Set       `json:"reclaimable"`
	Leases      []ResourceLeaseRow `json:"leases"`
}
type WorkerDetail struct {
	Enabled     bool           `json:"enabled"`
	Worker      *WorkerRow     `json:"worker"`
	LeaderID    *string        `json:"leaderId"`
	SilentAfter *Duration      `json:"silentAfter"`
	Resources   LocalResources `json:"resources"`
	AsOf        string         `json:"asOf"`
}

func projectWorker(w *cluster.Worker, self id.WorkerID, leader *string, interval, threshold time.Duration, at time.Time) WorkerRow {
	row := WorkerRow{ID: w.ID.String(), Hostname: nullable(w.Hostname), Queues: append([]string{}, w.Queues...), Concurrency: w.Concurrency, State: w.State,
		Self: w.ID == self, IsLeader: leader != nil && *leader == w.ID.String(), LeaderUntil: timestampPtr(w.LeaderUntil), LastSeen: timestamp(w.LastSeen),
		CreatedAt: timestamp(w.CreatedAt), HeartbeatStatus: "unknown", Capacity: resourceValues(w.Capacity)}
	if row.Self {
		d := duration(interval)
		row.HeartbeatInterval = &d
	}
	if !w.LastSeen.IsZero() {
		age := at.Sub(w.LastSeen)
		if age < 0 {
			row.ClockSkew = true
		} else {
			d := duration(age)
			row.HeartbeatAge = &d
			row.HeartbeatStatus = "recent"
			if age > threshold {
				row.HeartbeatStatus = "silent"
			}
		}
	}
	return row
}
func workerLeader(ctx context.Context, store cluster.Store) (*string, error) {
	leader, err := store.GetLeader(ctx)
	if err != nil {
		return nil, err
	}
	if leader == nil {
		return nil, nil
	}
	return nullable(leader.ID.String()), nil
}
func readWorkers(ctx context.Context, deps Deps) (WorkersPage, error) {
	at := time.Now()
	out := WorkersPage{Page: newPage([]WorkerRow{}, "", true, at)}
	store := deps.Engine.ClusterStore()
	if store == nil {
		return out, nil
	}
	workers, err := store.ListWorkers(ctx)
	if err != nil {
		return WorkersPage{}, err
	}
	leader, err := workerLeader(ctx, store)
	if err != nil {
		return WorkersPage{}, err
	}
	at = time.Now()
	settings := deps.Engine.Inspect()
	interval, threshold := duration(settings.WorkerHeartbeatInterval), duration(settings.WorkerStaleThreshold)
	out.Enabled = true
	out.LeaderID = leader
	out.HeartbeatReference = &interval
	out.SilentAfter = &threshold
	for _, worker := range workers {
		out.Items = append(out.Items, projectWorker(worker, deps.Engine.WorkerID(), leader, settings.WorkerHeartbeatInterval, settings.WorkerStaleThreshold, at))
	}
	slices.SortFunc(out.Items, func(a, b WorkerRow) int { return strings.Compare(a.ID, b.ID) })
	out.AsOf = at.UTC().Format(time.RFC3339Nano)
	return out, nil
}
func workersListHandler(deps Deps) func(context.Context, EmptyInput, fc.Principal) (WorkersPage, error) {
	return handle(deps, "workers.list", false, func(ctx context.Context, _ EmptyInput, _ fc.Principal) (WorkersPage, error) {
		return readWorkers(ctx, deps)
	})
}
func readLocalResources(deps Deps, self bool) LocalResources {
	out := LocalResources{Capacity: resource.Set{}, Free: resource.Set{}, Reclaimable: resource.Set{}, Leases: []ResourceLeaseRow{}}
	if !self {
		return out
	}
	manager := deps.Engine.Resources()
	if manager == nil {
		return out
	}
	out.Enabled = true
	out.Capacity = resourceValues(manager.Capacity())
	out.Free = resourceValues(manager.Free())
	out.Reclaimable = resourceValues(manager.Reclaimable())
	for _, lease := range manager.Leases() {
		out.Leases = append(out.Leases, ResourceLeaseRow{Owner: lease.Owner, Held: resourceValues(lease.Held), AcquiredAt: timestamp(lease.AcquiredAt)})
	}
	return out
}
func workersGetHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (WorkerDetail, error) {
	return handle(deps, "workers.get", false, func(ctx context.Context, input IDInput, _ fc.Principal) (WorkerDetail, error) {
		workerID, err := id.ParseWorkerID(input.ID)
		if err != nil || workerID.IsNil() {
			return WorkerDetail{}, badRequest("id must be a worker ID")
		}
		out := WorkerDetail{Resources: readLocalResources(deps, false), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}
		store := deps.Engine.ClusterStore()
		if store == nil {
			return out, nil
		}
		worker, err := store.GetWorker(ctx, workerID)
		if err != nil {
			return WorkerDetail{}, err
		}
		leader, err := workerLeader(ctx, store)
		if err != nil {
			return WorkerDetail{}, err
		}
		at := time.Now()
		settings := deps.Engine.Inspect()
		threshold := duration(settings.WorkerStaleThreshold)
		row := projectWorker(worker, deps.Engine.WorkerID(), leader, settings.WorkerHeartbeatInterval, settings.WorkerStaleThreshold, at)
		out.Enabled = true
		out.Worker = &row
		out.LeaderID = leader
		out.SilentAfter = &threshold
		out.Resources = readLocalResources(deps, row.Self)
		out.AsOf = at.UTC().Format(time.RFC3339Nano)
		return out, nil
	})
}
