package telemetry

import (
	"strings"
	"sync"
	"time"

	gu "github.com/xraph/go-utils/metrics"
)

// Metric names, as Dispatch registers them. A Forge app scrapes each one
// with its namespace in front: dispatch_jobs_enqueued_total is
// forge_dispatch_jobs_enqueued_total on /_/metrics.
const (
	MetricJobsEnqueued       = "dispatch_jobs_enqueued_total"
	MetricJobAttemptDuration = "dispatch_job_attempt_duration_seconds"
	MetricJobsDeadLettered   = "dispatch_jobs_dead_lettered_total"
	MetricJobQueueWait       = "dispatch_job_queue_wait_seconds"
	MetricJobPolls           = "dispatch_job_polls_total"
	MetricJobsRequeued       = "dispatch_jobs_requeued_total"
	MetricJobLeaseRenewals   = "dispatch_job_lease_renewals_total"
	MetricJobsReclaimed      = "dispatch_jobs_reclaimed_total"
	MetricJobsActive         = "dispatch_jobs_active"

	MetricCronFires    = "dispatch_cron_fires_total"
	MetricCronFireLag  = "dispatch_cron_fire_lag_seconds"
	MetricCronLeader   = "dispatch_cron_leader"
	MetricWorkflowRuns = "dispatch_workflow_run_duration_seconds"
	MetricWorkflowStep = "dispatch_workflow_step_duration_seconds"

	MetricDurableTaskDuration   = "dispatch_durable_task_duration_seconds"
	MetricDurablePolls          = "dispatch_durable_polls_total"
	MetricDurableReplayDuration = "dispatch_durable_replay_duration_seconds"

	MetricOutboxDeliveryDuration = "dispatch_outbox_delivery_duration_seconds"
	MetricOutboxDeliveryLag      = "dispatch_outbox_delivery_lag_seconds"

	MetricArtifactsSwept         = "dispatch_artifacts_swept_total"
	MetricArtifactsPurged        = "dispatch_artifacts_purged_total"
	MetricArtifactBytesReclaimed = "dispatch_artifact_bytes_reclaimed_total"
)

// Job attempt outcomes, the outcome label on
// dispatch_job_attempt_duration_seconds and the dispatch.job.outcome span
// attribute. Cancelled covers an operator's cancel and a worker shutting
// down mid-attempt; store_error means the store refused the terminal write.
const (
	OutcomeCompleted    = "completed"
	OutcomeRetry        = "retry"
	OutcomeDeadLettered = "dead_lettered"
	OutcomeRequeued     = "requeued"
	OutcomeLeaseLost    = "lease_lost"
	OutcomeCancelled    = "cancelled"
	OutcomeStoreError   = "store_error"
)

// Dead letter reasons, the reason label on dispatch_jobs_dead_lettered_total.
const (
	DeadLetterRetriesExhausted = "retries_exhausted"
	DeadLetterPermanent        = "permanent"
	DeadLetterLaunchFailures   = "launch_failures"
)

// Requeue reasons, the reason label on dispatch_jobs_requeued_total: a job
// this worker claimed and handed back without running it.
const (
	RequeueRateLimited = "rate_limited"
	RequeueNoCapacity  = "no_capacity"
	RequeueShutdown    = "shutdown"
)

// Shared result values.
const (
	ResultOK    = "ok"
	ResultError = "error"
)

// Poll results, the result label on dispatch_job_polls_total and
// dispatch_durable_polls_total.
const (
	PollWork  = "work"
	PollEmpty = "empty"
	PollError = "error"
)

// Lease renewal results, the result label on
// dispatch_job_lease_renewals_total.
const (
	RenewalOK    = "ok"
	RenewalLost  = "lost"
	RenewalError = "error"
)

// Cron fire results, the result label on dispatch_cron_fires_total.
const (
	CronFired   = "fired"
	CronLocked  = "locked"
	CronSkipped = "skipped"
	CronError   = "error"
)

// Durable task results, the result label on
// dispatch_durable_task_duration_seconds.
const (
	DurableOK         = "ok"
	DurableContention = "contention"
	DurableLeaseLost  = "lease_lost"
	DurableNoHandler  = "no_handler"
	DurableCancelled  = "cancelled"
	DurableError      = "error"
)

// Outbox delivery results, the result label on
// dispatch_outbox_delivery_duration_seconds. Rejected through
// invalid_receipt are the failure categories the publisher already persists
// on a retry; cancelled means Stop interrupted the attempt, which leaves the
// delivery to be claimed again.
const (
	OutboxAcknowledged   = "acknowledged"
	OutboxCancelled      = "cancelled"
	OutboxRejected       = "rejected"
	OutboxTimeout        = "timeout"
	OutboxConflict       = "conflict"
	OutboxUnavailable    = "unavailable"
	OutboxInvalidReceipt = "invalid_receipt"
)

// Artifact purge results, the result label on dispatch_artifacts_purged_total.
const (
	PurgeOK      = "ok"
	PurgeSkipped = "skipped"
)

var (
	jobOutcomes  = []string{OutcomeCompleted, OutcomeRetry, OutcomeDeadLettered, OutcomeRequeued, OutcomeLeaseLost, OutcomeCancelled, OutcomeStoreError}
	dlqReasons   = []string{DeadLetterRetriesExhausted, DeadLetterPermanent, DeadLetterLaunchFailures}
	requeues     = []string{RequeueRateLimited, RequeueNoCapacity, RequeueShutdown}
	pollResults  = []string{PollWork, PollEmpty, PollError}
	renewals     = []string{RenewalOK, RenewalLost, RenewalError}
	cronResults  = []string{CronFired, CronLocked, CronSkipped, CronError}
	okError      = []string{ResultOK, ResultError}
	runResults   = []string{"completed", "failed"}
	stepKinds    = []string{"step", "parallel", "fan_out", "child", "spawn", "wait_event", "wait_all", "wait_any", "wait_match", "sleep", "compensate"}
	taskKinds    = []string{"workflow", "activity", "timer", "timeout", "child_delivery", "execution_timeout"}
	taskResults  = []string{DurableOK, DurableContention, DurableLeaseLost, DurableNoHandler, DurableCancelled, DurableError}
	destinations = []string{"chronicle", "relay"}
	outboxResult = []string{OutboxAcknowledged, OutboxCancelled, OutboxRejected, OutboxTimeout, OutboxConflict, OutboxUnavailable, OutboxInvalidReceipt}
	sweepReasons = []string{"retention", "orphan"}
	purgeResults = []string{PurgeOK, PurgeSkipped}

	// attemptBuckets cover a job attempt or a workflow step, from a fast
	// in-process handler to an hour-long batch job.
	attemptBuckets = []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}
	// waitBuckets cover how long due work waits: queue wait, cron lag and
	// outbox lag.
	waitBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 900, 3600}
	// taskBuckets cover one durable task or one outbox delivery call.
	taskBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
)

// JobMetrics are the job pool, runner and enqueue instruments.
type JobMetrics struct {
	enqueued     *vec[gu.Counter]
	attempts     *vec[gu.Histogram]
	deadLettered *vec[gu.Counter]
	queueWait    *vec[gu.Histogram]
	polls        *vec[gu.Counter]
	requeued     *vec[gu.Counter]
	renewals     *vec[gu.Counter]
	reclaimed    gu.Counter
	active       gu.Gauge
}

// Jobs returns the job instruments, building them on the first call. It
// returns nil, which records nothing, when metrics are off.
func (t *Telemetry) Jobs() *JobMetrics {
	if t == nil || t.factory == nil {
		return nil
	}
	t.jobsOnce.Do(func() {
		f, queues := t.factory, t.queueValues()
		t.jobs = &JobMetrics{
			enqueued: counterVec(f, MetricJobsEnqueued, "Jobs written to the store, by queue",
				label{"queue", queues}),
			attempts: histogramVec(f, MetricJobAttemptDuration, "Job attempts from start to their terminal write, by queue and outcome", attemptBuckets,
				label{"queue", queues}, label{"outcome", jobOutcomes}),
			deadLettered: counterVec(f, MetricJobsDeadLettered, "Jobs moved to the dead letter queue, by queue and reason",
				label{"queue", queues}, label{"reason", dlqReasons}),
			queueWait: histogramVec(f, MetricJobQueueWait, "Time from a job becoming due to a worker starting it, by queue", waitBuckets,
				label{"queue", queues}),
			polls: counterVec(f, MetricJobPolls, "Job pool dequeue polls, by result",
				label{"result", pollResults}),
			requeued: counterVec(f, MetricJobsRequeued, "Claimed jobs handed back without running, by queue and reason",
				label{"queue", queues}, label{"reason", requeues}),
			renewals: counterVec(f, MetricJobLeaseRenewals, "Job lease renewals and heartbeats, by result",
				label{"result", renewals}),
			reclaimed: f.Counter(MetricJobsReclaimed, gu.WithDescription("Jobs whose lease expired, returned to pending by the reaper")),
			active:    f.Gauge(MetricJobsActive, gu.WithDescription("Job attempts running on this worker")),
		}
	})

	return t.jobs
}

// Enqueued counts one job written to the store.
func (m *JobMetrics) Enqueued(queue string) {
	if m != nil {
		m.enqueued.counter(queue)
	}
}

// Attempt records one finished job attempt.
func (m *JobMetrics) Attempt(queue, outcome string, d time.Duration) {
	if m != nil {
		m.attempts.observe(d.Seconds(), queue, outcome)
	}
}

// DeadLettered counts one job moved to the dead letter queue.
func (m *JobMetrics) DeadLettered(queue, reason string) {
	if m != nil {
		m.deadLettered.counter(queue, reason)
	}
}

// QueueWait records how long a claimed job waited past its due time.
func (m *JobMetrics) QueueWait(queue string, d time.Duration) {
	if m != nil {
		m.queueWait.observe(max(d, 0).Seconds(), queue)
	}
}

// Poll counts one dequeue poll.
func (m *JobMetrics) Poll(result string) {
	if m != nil {
		m.polls.counter(result)
	}
}

// Requeued counts one claimed job handed back without running.
func (m *JobMetrics) Requeued(queue, reason string) {
	if m != nil {
		m.requeued.counter(queue, reason)
	}
}

// Renewal counts one lease renewal or heartbeat.
func (m *JobMetrics) Renewal(result string) {
	if m != nil {
		m.renewals.counter(result)
	}
}

// Reclaimed counts jobs the reaper returned to pending.
func (m *JobMetrics) Reclaimed(n int) {
	if m != nil && n > 0 {
		m.reclaimed.Add(float64(n))
	}
}

// Active sets the number of attempts running on this worker.
func (m *JobMetrics) Active(n int) {
	if m != nil {
		m.active.Set(float64(n))
	}
}

// CronMetrics are the cron scheduler instruments.
type CronMetrics struct {
	fires  *vec[gu.Counter]
	lag    gu.Histogram
	leader gu.Gauge
}

// Cron returns the cron instruments, or nil when metrics are off.
func (t *Telemetry) Cron() *CronMetrics {
	if t == nil || t.factory == nil {
		return nil
	}
	t.cronOnce.Do(func() {
		f := t.factory
		t.cron = &CronMetrics{
			fires: counterVec(f, MetricCronFires, "Due cron entries this scheduler handled, by result",
				label{"result", cronResults}),
			lag: f.Histogram(MetricCronFireLag, gu.WithBuckets(waitBuckets...), gu.WithUnit("s"),
				gu.WithDescription("Time from a cron entry's due time to its job being enqueued")),
			leader: f.Gauge(MetricCronLeader, gu.WithDescription("1 while this scheduler holds cron leadership, 0 otherwise")),
		}
	})

	return t.cron
}

// Fire counts one due cron entry handled.
func (m *CronMetrics) Fire(result string) {
	if m != nil {
		m.fires.counter(result)
	}
}

// Lag records how late a cron entry fired.
func (m *CronMetrics) Lag(d time.Duration) {
	if m != nil {
		m.lag.Observe(max(d, 0).Seconds())
	}
}

// Leader records whether this scheduler leads.
func (m *CronMetrics) Leader(leading bool) {
	if m == nil {
		return
	}
	if leading {
		m.leader.Set(1)
		return
	}
	m.leader.Set(0)
}

// WorkflowMetrics are the checkpoint workflow instruments.
type WorkflowMetrics struct {
	runs  *vec[gu.Histogram]
	steps *vec[gu.Histogram]
}

// Workflows returns the checkpoint workflow instruments, or nil when
// metrics are off.
func (t *Telemetry) Workflows() *WorkflowMetrics {
	if t == nil || t.factory == nil {
		return nil
	}
	t.workflowOnce.Do(func() {
		f := t.factory
		t.workflow = &WorkflowMetrics{
			runs: histogramVec(f, MetricWorkflowRuns, "Checkpoint workflow runs, by result", attemptBuckets,
				label{"result", runResults}),
			steps: histogramVec(f, MetricWorkflowStep, "Checkpoint workflow steps that ran (not replayed), by kind and result", attemptBuckets,
				label{"kind", stepKinds}, label{"result", okError}),
		}
	})

	return t.workflow
}

// Run records one finished workflow run. result is completed or failed.
func (m *WorkflowMetrics) Run(result string, d time.Duration) {
	if m != nil {
		m.runs.observe(d.Seconds(), result)
	}
}

// Step records one workflow step that ran.
func (m *WorkflowMetrics) Step(kind, result string, d time.Duration) {
	if m != nil {
		m.steps.observe(d.Seconds(), kind, result)
	}
}

// DurableMetrics are the durable runtime instruments.
type DurableMetrics struct {
	tasks  *vec[gu.Histogram]
	polls  *vec[gu.Counter]
	replay gu.Histogram
}

// Durable returns the durable runtime instruments, or nil when metrics are
// off.
func (t *Telemetry) Durable() *DurableMetrics {
	if t == nil || t.factory == nil {
		return nil
	}
	t.durableOnce.Do(func() {
		f := t.factory
		t.durable = &DurableMetrics{
			tasks: histogramVec(f, MetricDurableTaskDuration, "Durable tasks from claim to finish, by kind and result", taskBuckets,
				label{"kind", taskKinds}, label{"result", taskResults}),
			polls: counterVec(f, MetricDurablePolls, "Durable task claim polls, by kind and result",
				label{"kind", taskKinds}, label{"result", pollResults}),
			replay: f.Histogram(MetricDurableReplayDuration, gu.WithBuckets(taskBuckets...), gu.WithUnit("s"),
				gu.WithDescription("Time to replay a workflow's history and evaluate its next decision")),
		}
	})

	return t.durable
}

// Task records one claimed durable task.
func (m *DurableMetrics) Task(kind, result string, d time.Duration) {
	if m != nil {
		m.tasks.observe(d.Seconds(), kind, result)
	}
}

// Poll counts one durable claim poll.
func (m *DurableMetrics) Poll(kind, result string) {
	if m != nil {
		m.polls.counter(kind, result)
	}
}

// Replay records one history replay.
func (m *DurableMetrics) Replay(d time.Duration) {
	if m != nil {
		m.replay.Observe(d.Seconds())
	}
}

// OutboxMetrics are the outbox publisher instruments.
type OutboxMetrics struct {
	deliveries *vec[gu.Histogram]
	lag        *vec[gu.Histogram]
}

// Outbox returns the outbox publisher instruments, or nil when metrics are
// off.
func (t *Telemetry) Outbox() *OutboxMetrics {
	if t == nil || t.factory == nil {
		return nil
	}
	t.outboxOnce.Do(func() {
		f := t.factory
		t.outbox = &OutboxMetrics{
			deliveries: histogramVec(f, MetricOutboxDeliveryDuration, "Outbox delivery attempts, by destination and result", taskBuckets,
				label{"destination", destinations}, label{"result", outboxResult}),
			lag: histogramVec(f, MetricOutboxDeliveryLag, "Time from an outbox intent being accepted to its sink acknowledging it, by destination", waitBuckets,
				label{"destination", destinations}),
		}
	})

	return t.outbox
}

// Delivery records one delivery attempt.
func (m *OutboxMetrics) Delivery(destination, result string, d time.Duration) {
	if m != nil {
		m.deliveries.observe(d.Seconds(), destination, result)
	}
}

// Lag records how long an acknowledged delivery waited since acceptance.
func (m *OutboxMetrics) Lag(destination string, d time.Duration) {
	if m != nil {
		m.lag.observe(max(d, 0).Seconds(), destination)
	}
}

// ArtifactMetrics are the artifact sweeper instruments.
type ArtifactMetrics struct {
	swept  *vec[gu.Counter]
	purged *vec[gu.Counter]
	bytes  gu.Counter
}

// Artifacts returns the artifact sweeper instruments, or nil when metrics
// are off.
func (t *Telemetry) Artifacts() *ArtifactMetrics {
	if t == nil || t.factory == nil {
		return nil
	}
	t.artifactsOnce.Do(func() {
		f := t.factory
		t.artifacts = &ArtifactMetrics{
			swept: counterVec(f, MetricArtifactsSwept, "Ephemeral artifacts marked for purge, by reason",
				label{"reason", sweepReasons}),
			purged: counterVec(f, MetricArtifactsPurged, "Artifacts the purge pass handled, by result",
				label{"result", purgeResults}),
			bytes: f.Counter(MetricArtifactBytesReclaimed, gu.WithDescription("Bytes of artifact storage the purge pass deleted")),
		}
	})

	return t.artifacts
}

// Swept counts artifacts marked for purge.
func (m *ArtifactMetrics) Swept(reason string, n int) {
	if m != nil && n > 0 {
		m.swept.add(float64(n), reason)
	}
}

// Purged counts artifacts the purge pass handled.
func (m *ArtifactMetrics) Purged(result string, n int) {
	if m != nil && n > 0 {
		m.purged.add(float64(n), result)
	}
}

// BytesReclaimed counts bytes the purge pass deleted.
func (m *ArtifactMetrics) BytesReclaimed(n int64) {
	if m != nil && n > 0 {
		m.bytes.Add(float64(n))
	}
}

// label is one label key and the only values it may take. A value outside
// the set is never recorded, so no metric can grow a series per job, tenant
// or free-text string.
type label struct {
	key    string
	values []string
}

// vec is a metric family with fixed label keys. Children are created on
// first use, through the factory, and cached.
type vec[T any] struct {
	labels   []label
	allowed  []map[string]struct{}
	build    func(map[string]string) T
	mu       sync.Mutex
	children map[string]T
}

func newVec[T any](mk func(map[string]string) T, labels ...label) *vec[T] {
	v := &vec[T]{labels: labels, build: mk, children: map[string]T{}}
	for _, l := range labels {
		set := make(map[string]struct{}, len(l.values))
		for _, value := range l.values {
			set[value] = struct{}{}
		}
		v.allowed = append(v.allowed, set)
	}

	return v
}

func (v *vec[T]) with(values ...string) (T, bool) {
	var zero T
	if len(values) != len(v.labels) {
		return zero, false
	}
	for i, value := range values {
		if _, ok := v.allowed[i][value]; !ok {
			return zero, false
		}
	}
	key := strings.Join(values, "\xff")

	v.mu.Lock()
	defer v.mu.Unlock()
	if child, ok := v.children[key]; ok {
		return child, true
	}
	lm := make(map[string]string, len(values))
	for i, value := range values {
		lm[v.labels[i].key] = value
	}
	child := v.build(lm)
	v.children[key] = child

	return child, true
}

func counterVec(f gu.MetricFactory, name, help string, labels ...label) *vec[gu.Counter] {
	return newVec(func(l map[string]string) gu.Counter {
		return f.Counter(name, gu.WithLabels(l), gu.WithDescription(help))
	}, labels...)
}

func histogramVec(f gu.MetricFactory, name, help string, buckets []float64, labels ...label) *vec[gu.Histogram] {
	return newVec(func(l map[string]string) gu.Histogram {
		return f.Histogram(name, gu.WithLabels(l), gu.WithBuckets(buckets...), gu.WithUnit("s"), gu.WithDescription(help))
	}, labels...)
}

func (v *vec[T]) counter(values ...string) { v.add(1, values...) }

func (v *vec[T]) add(delta float64, values ...string) {
	if child, ok := v.with(values...); ok {
		if c, isCounter := any(child).(gu.Counter); isCounter {
			c.Add(delta)
		}
	}
}

func (v *vec[T]) observe(value float64, values ...string) {
	if child, ok := v.with(values...); ok {
		if h, isHistogram := any(child).(gu.Histogram); isHistogram {
			h.Observe(value)
		}
	}
}
