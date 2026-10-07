package redis

import "fmt"

// keys builds every Redis key and channel name the store touches. All of
// them sit under "dispatch:" so the store can share a Redis with other
// users of the same database; a tenant prefix on top of that lets several
// dispatch instances share one Redis without seeing each other's queues,
// cron locks or leadership.
type keys struct {
	prefix string
}

// base is the namespace every dispatch key lives under, tenant or not.
const base = "dispatch:"

func newKeys(prefix string) keys { return keys{prefix: prefix} }

// full composes the key that hits Redis. The tenant prefix goes outside
// the dispatch namespace (ws_acme:dispatch:job:1) so everything a tenant
// owns shares one leading segment, the same layout the tenant's other
// Redis keys already use; an empty prefix yields the historical key.
func (k keys) full(suffix string) string {
	return k.prefix + base + suffix
}

// ── Job keys ──

// job returns the key for a job entity.
func (k keys) job(id string) string { return k.full("job:" + id) }

// queue returns the Sorted Set key for a queue.
func (k keys) queue(name string) string { return k.full("queue:" + name) }

// jobIDs is the Set tracking all job IDs for enumeration.
func (k keys) jobIDs() string { return k.full("job_ids") }

// wakeChannel is the pub/sub channel that announces newly enqueued jobs.
func (k keys) wakeChannel() string { return k.full("jobs:wake") }

// ── Workflow keys ──

// run returns the key for a workflow run entity.
func (k keys) run(id string) string { return k.full("run:" + id) }

// runIDs is the Set tracking all run IDs for enumeration.
func (k keys) runIDs() string { return k.full("run_ids") }

// checkpoint returns the key for a checkpoint.
func (k keys) checkpoint(runID, step string) string {
	return k.full(fmt.Sprintf("checkpoint:%s:%s", runID, step))
}

// checkpointIndex returns the Set key tracking checkpoints for a run.
func (k keys) checkpointIndex(runID string) string {
	return k.full("checkpoint_idx:" + runID)
}

// ── Cron keys ──

// cron returns the key for a cron entry entity.
func (k keys) cron(id string) string { return k.full("cron:" + id) }

// cronIDs is the Set tracking all cron IDs for enumeration.
func (k keys) cronIDs() string { return k.full("cron_ids") }

// cronNames maps cron names to IDs for duplicate detection.
func (k keys) cronNames() string { return k.full("cron_names") }

// ── DLQ keys ──

// dlq returns the key for a DLQ entry entity.
func (k keys) dlq(id string) string { return k.full("dlq:" + id) }

// dlqIDs is the Set tracking all DLQ entry IDs for enumeration.
func (k keys) dlqIDs() string { return k.full("dlq_ids") }

// ── Event keys ──

// event returns the key for an event entity.
func (k keys) event(id string) string { return k.full("event:" + id) }

// eventStream returns the Stream key for an event name.
func (k keys) eventStream(name string) string { return k.full("events:" + name) }

// ── Cluster keys ──

// worker returns the key for a worker entity.
func (k keys) worker(id string) string { return k.full("worker:" + id) }

// workerIDs is the Set tracking all worker IDs for enumeration.
func (k keys) workerIDs() string { return k.full("worker_ids") }

// leader stores the current leader worker ID.
func (k keys) leader() string { return k.full("leader") }

// ── Artifact keys ──

// artifact returns the key for an artifact entity.
func (k keys) artifact(id string) string { return k.full("artifact:" + id) }

// artifactIDs is the Set tracking all artifact IDs for enumeration.
func (k keys) artifactIDs() string { return k.full("artifact_ids") }

// artifactGuard maps live storage coordinates to an artifact ID. It is
// claimed with SETNX so concurrent creates at the same coordinates resolve
// to one winner, and released on soft-delete so a purged key is reusable.
func (k keys) artifactGuard(backend, bucket, key string) string {
	return k.full(fmt.Sprintf("artifact_key:%s:%s:%s", backend, bucket, key))
}

// artifactEphemeral is the Sorted Set of ephemeral artifact IDs scored
// by creation time. Durable artifacts are never members, which is this
// backend's form of the SQL "lifecycle = 'ephemeral'" literal.
func (k keys) artifactEphemeral() string { return k.full("artifact_ephemeral") }

// artifactDeleted is the Sorted Set of soft-deleted artifact IDs scored
// by deletion time, driving the purge pass.
func (k keys) artifactDeleted() string { return k.full("artifact_deleted") }

// artifactLinks is the Set of link members pointing at an artifact.
func (k keys) artifactLinks(artifactID string) string {
	return k.full("artifact_links:" + artifactID)
}

// ownerLinks is the Hash of an owner's artifact links, keyed by
// "name\x00attempt".
func (k keys) ownerLinks(kind, ownerID string) string {
	return k.full(fmt.Sprintf("artifact_owner_links:%s:%s", kind, ownerID))
}

// ── Created-order index keys ──

// byCreated is the Sorted Set of every ID of one entity kind (entityJob,
// entityRun, entityDLQ, entityArtifact) scored by the Unix millisecond
// minted into the ID. The paged lists walk it newest first. The *_ids
// sets stay the source of truth for everything else, and a list call
// backfills the index from them whenever a set holds more members than
// its index.
func (k keys) byCreated(entity string) string { return k.full(entity + "_by_created") }
