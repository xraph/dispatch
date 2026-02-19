// Package cluster provides distributed worker coordination, consensus-based
// leader election, worker registration, and work stealing / rebalancing.
//
// When running multiple Dispatch instances, the cluster package coordinates
// which instance is the leader (responsible for cron firing, stale-job
// recovery, and work redistribution) and which are followers.
//
// # Worker Entity
//
// Each running Dispatch instance registers itself as a [Worker] with:
//   - a unique [id.WorkerID]
//   - its hostname
//   - the list of queues it polls
//   - its concurrency limit
//   - a state: [WorkerActive], [WorkerDraining], or [WorkerDead]
//
// Workers send periodic heartbeats. If a heartbeat is not received within
// the configured threshold, the worker is considered dead and its in-flight
// jobs are eligible for reassignment (work stealing).
//
// # Leader Election
//
// One worker at a time holds leadership. The leader:
//   - fires cron entries
//   - reclaims stale jobs from dead workers
//   - rebalances work across the cluster
//
// Leadership is managed by [Store.AcquireLeadership] using optimistic locking.
// If leadership is lost mid-operation, [dispatch.ErrLeadershipLost] is returned.
//
// # Kubernetes Consensus
//
// For K8s deployments use the cluster/k8s sub-package which uses Kubernetes
// leader election via client-go for consensus.
package cluster
