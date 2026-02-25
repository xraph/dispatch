package redis

import "fmt"

// Redis key naming conventions for dispatch data.
// All keys are prefixed with "dispatch:" to avoid collisions.

const keyPrefix = "dispatch:"

// ── Job keys ──

// jobKey returns the key for a job entity: dispatch:job:{id}
func jobKey(id string) string { return keyPrefix + "job:" + id }

// queueKey returns the Sorted Set key for a queue: dispatch:queue:{name}
func queueKey(name string) string { return keyPrefix + "queue:" + name }

// jobIDsKey is the Set tracking all job IDs for enumeration.
const jobIDsKey = keyPrefix + "job_ids"

// ── Workflow keys ──

// runKey returns the key for a workflow run entity: dispatch:run:{id}
func runKey(id string) string { return keyPrefix + "run:" + id }

// runIDsKey is the Set tracking all run IDs for enumeration.
const runIDsKey = keyPrefix + "run_ids"

// checkpointKey returns the key for a checkpoint: dispatch:checkpoint:{runID}:{step}
func checkpointKey(runID, step string) string {
	return fmt.Sprintf("%scheckpoint:%s:%s", keyPrefix, runID, step)
}

// checkpointIndexKey returns the Set key tracking checkpoints for a run.
func checkpointIndexKey(runID string) string {
	return keyPrefix + "checkpoint_idx:" + runID
}

// ── Cron keys ──

// cronKey returns the key for a cron entry entity: dispatch:cron:{id}
func cronKey(id string) string { return keyPrefix + "cron:" + id }

// cronIDsKey is the Set tracking all cron IDs for enumeration.
const cronIDsKey = keyPrefix + "cron_ids"

// cronNamesKey maps cron names to IDs for duplicate detection.
const cronNamesKey = keyPrefix + "cron_names"

// ── DLQ keys ──

// dlqKey returns the key for a DLQ entry entity: dispatch:dlq:{id}
func dlqKey(id string) string { return keyPrefix + "dlq:" + id }

// dlqIDsKey is the Set tracking all DLQ entry IDs for enumeration.
const dlqIDsKey = keyPrefix + "dlq_ids"

// ── Event keys ──

// eventKey returns the key for an event entity: dispatch:event:{id}
func eventKey(id string) string { return keyPrefix + "event:" + id }

// eventStreamKey returns the Stream key for an event name: dispatch:events:{name}
func eventStreamKey(name string) string { return keyPrefix + "events:" + name }

// ── Cluster keys ──

// workerKey returns the key for a worker entity: dispatch:worker:{id}
func workerKey(id string) string { return keyPrefix + "worker:" + id }

// workerIDsKey is the Set tracking all worker IDs for enumeration.
const workerIDsKey = keyPrefix + "worker_ids"

// leaderKey stores the current leader worker ID.
const leaderKey = keyPrefix + "leader"
