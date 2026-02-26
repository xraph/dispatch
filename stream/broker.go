package stream

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Compile-time interface checks.
var (
	_ ext.Extension             = (*Broker)(nil)
	_ ext.JobEnqueued           = (*Broker)(nil)
	_ ext.JobStarted            = (*Broker)(nil)
	_ ext.JobCompleted          = (*Broker)(nil)
	_ ext.JobFailed             = (*Broker)(nil)
	_ ext.JobRetrying           = (*Broker)(nil)
	_ ext.JobDLQ                = (*Broker)(nil)
	_ ext.WorkflowStarted       = (*Broker)(nil)
	_ ext.WorkflowStepCompleted = (*Broker)(nil)
	_ ext.WorkflowStepFailed    = (*Broker)(nil)
	_ ext.WorkflowCompleted     = (*Broker)(nil)
	_ ext.WorkflowFailed        = (*Broker)(nil)
	_ ext.CronFired             = (*Broker)(nil)
	_ ext.Shutdown              = (*Broker)(nil)
)

// DefaultBufferSize is the default per-subscriber event buffer.
const DefaultBufferSize = 256

// DefaultCredits is the default initial credits for new subscribers.
const DefaultCredits int64 = 1000

// Broker is the real-time stream broker. It implements the ext.Extension
// interface to receive lifecycle events and fans them out to subscribers
// via topic-based pub/sub.
type Broker struct {
	topics *TopicRegistry
	logger *slog.Logger

	// Subscriber management.
	subscribers sync.Map // subscriberID → *Subscriber

	// Metrics.
	totalPublished atomic.Int64
	totalDropped   atomic.Int64

	// Config.
	bufferSize     int
	defaultCredits int64
}

// BrokerOption configures a Broker.
type BrokerOption func(*Broker)

// WithBufferSize sets the per-subscriber event buffer size.
func WithBufferSize(size int) BrokerOption {
	return func(b *Broker) { b.bufferSize = size }
}

// WithDefaultCredits sets the initial credits for new subscribers.
func WithDefaultCredits(credits int64) BrokerOption {
	return func(b *Broker) { b.defaultCredits = credits }
}

// NewBroker creates a new stream broker.
func NewBroker(logger *slog.Logger, opts ...BrokerOption) *Broker {
	b := &Broker{
		topics:         NewTopicRegistry(),
		logger:         logger,
		bufferSize:     DefaultBufferSize,
		defaultCredits: DefaultCredits,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Name implements ext.Extension.
func (b *Broker) Name() string { return "stream-broker" }

// Topics returns the topic registry for external use (e.g., DWP server).
func (b *Broker) Topics() *TopicRegistry { return b.topics }

// Subscribe creates a new subscriber on the given topics.
func (b *Broker) Subscribe(subscriberID string, topics ...string) *Subscriber {
	sub := NewSubscriber(subscriberID, b.bufferSize, b.defaultCredits)
	b.subscribers.Store(subscriberID, sub)
	for _, topic := range topics {
		b.topics.Subscribe(topic, sub)
	}
	return sub
}

// SubscribeTo adds an existing subscriber to additional topics.
func (b *Broker) SubscribeTo(subscriberID string, topics ...string) {
	val, ok := b.subscribers.Load(subscriberID)
	if !ok {
		return
	}
	sub := val.(*Subscriber) //nolint:errcheck // sync.Map always stores *Subscriber
	for _, topic := range topics {
		b.topics.Subscribe(topic, sub)
	}
}

// Unsubscribe removes a subscriber from specific topics.
func (b *Broker) Unsubscribe(subscriberID string, topics ...string) {
	for _, topic := range topics {
		b.topics.Unsubscribe(topic, subscriberID)
	}
}

// RemoveSubscriber removes a subscriber from all topics and closes it.
func (b *Broker) RemoveSubscriber(subscriberID string) {
	b.topics.UnsubscribeAll(subscriberID)
	if val, ok := b.subscribers.LoadAndDelete(subscriberID); ok {
		val.(*Subscriber).Close() //nolint:errcheck // sync.Map always stores *Subscriber
	}
}

// GetSubscriber returns a subscriber by ID.
func (b *Broker) GetSubscriber(subscriberID string) (*Subscriber, bool) {
	val, ok := b.subscribers.Load(subscriberID)
	if !ok {
		return nil, false
	}
	return val.(*Subscriber), true //nolint:errcheck // sync.Map always stores *Subscriber
}

// Stats returns broker statistics.
func (b *Broker) Stats() BrokerStats {
	count := 0
	b.subscribers.Range(func(_, _ any) bool {
		count++
		return true
	})
	return BrokerStats{
		TopicCount:      b.topics.TopicCount(),
		SubscriberCount: count,
		TotalPublished:  b.totalPublished.Load(),
		TotalDropped:    b.totalDropped.Load(),
	}
}

// BrokerStats contains broker metrics.
type BrokerStats struct {
	TopicCount      int   `json:"topic_count"`
	SubscriberCount int   `json:"subscriber_count"`
	TotalPublished  int64 `json:"total_published"`
	TotalDropped    int64 `json:"total_dropped"`
}

// publish creates an event and broadcasts it to all matching topics.
func (b *Broker) publish(evt *Event) {
	topics := resolveTopics(evt)
	delivered := b.topics.Broadcast(topics, evt)
	b.totalPublished.Add(int64(delivered))
}

// mustMarshal marshals data to JSON, panicking on error (programming error).
func mustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic("stream: marshal event data: " + err.Error())
	}
	return data
}

// ── Job lifecycle hooks ─────────────────────────────

func (b *Broker) OnJobEnqueued(_ context.Context, j *job.Job) error {
	b.publish(&Event{
		Type:      EventJobEnqueued,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic(j.ID.String()),
		Data: mustMarshal(JobEventData{
			JobID:      j.ID.String(),
			JobName:    j.Name,
			Queue:      j.Queue,
			ScopeAppID: j.ScopeAppID,
			ScopeOrgID: j.ScopeOrgID,
		}),
	})
	return nil
}

func (b *Broker) OnJobStarted(_ context.Context, j *job.Job) error {
	b.publish(&Event{
		Type:      EventJobStarted,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic(j.ID.String()),
		Data: mustMarshal(JobEventData{
			JobID:      j.ID.String(),
			JobName:    j.Name,
			Queue:      j.Queue,
			ScopeAppID: j.ScopeAppID,
			ScopeOrgID: j.ScopeOrgID,
		}),
	})
	return nil
}

func (b *Broker) OnJobCompleted(_ context.Context, j *job.Job, elapsed time.Duration) error {
	b.publish(&Event{
		Type:      EventJobCompleted,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic(j.ID.String()),
		Data: mustMarshal(JobEventData{
			JobID:      j.ID.String(),
			JobName:    j.Name,
			Queue:      j.Queue,
			ScopeAppID: j.ScopeAppID,
			ScopeOrgID: j.ScopeOrgID,
			ElapsedMs:  elapsed.Milliseconds(),
		}),
	})
	return nil
}

func (b *Broker) OnJobFailed(_ context.Context, j *job.Job, jobErr error) error {
	b.publish(&Event{
		Type:      EventJobFailed,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic(j.ID.String()),
		Data: mustMarshal(JobEventData{
			JobID:      j.ID.String(),
			JobName:    j.Name,
			Queue:      j.Queue,
			ScopeAppID: j.ScopeAppID,
			ScopeOrgID: j.ScopeOrgID,
			Error:      jobErr.Error(),
		}),
	})
	return nil
}

func (b *Broker) OnJobRetrying(_ context.Context, j *job.Job, attempt int, nextRunAt time.Time) error {
	b.publish(&Event{
		Type:      EventJobRetrying,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic(j.ID.String()),
		Data: mustMarshal(JobEventData{
			JobID:      j.ID.String(),
			JobName:    j.Name,
			Queue:      j.Queue,
			ScopeAppID: j.ScopeAppID,
			ScopeOrgID: j.ScopeOrgID,
			Attempt:    attempt,
			NextRunAt:  nextRunAt.Format(time.RFC3339),
		}),
	})
	return nil
}

func (b *Broker) OnJobDLQ(_ context.Context, j *job.Job, jobErr error) error {
	b.publish(&Event{
		Type:      EventJobDLQ,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic(j.ID.String()),
		Data: mustMarshal(JobEventData{
			JobID:      j.ID.String(),
			JobName:    j.Name,
			Queue:      j.Queue,
			ScopeAppID: j.ScopeAppID,
			ScopeOrgID: j.ScopeOrgID,
			Error:      jobErr.Error(),
		}),
	})
	return nil
}

// ── Workflow lifecycle hooks ────────────────────────

func (b *Broker) OnWorkflowStarted(_ context.Context, r *workflow.Run) error {
	b.publish(&Event{
		Type:      EventWorkflowStarted,
		Timestamp: time.Now().UTC(),
		Topic:     WorkflowTopic(r.ID.String()),
		Data: mustMarshal(WorkflowEventData{
			RunID:      r.ID.String(),
			Name:       r.Name,
			ScopeAppID: r.ScopeAppID,
			ScopeOrgID: r.ScopeOrgID,
		}),
	})
	return nil
}

func (b *Broker) OnWorkflowStepCompleted(_ context.Context, r *workflow.Run, stepName string, elapsed time.Duration) error {
	b.publish(&Event{
		Type:      EventWorkflowStepCompleted,
		Timestamp: time.Now().UTC(),
		Topic:     WorkflowTopic(r.ID.String()),
		Data: mustMarshal(WorkflowEventData{
			RunID:      r.ID.String(),
			Name:       r.Name,
			StepName:   stepName,
			ScopeAppID: r.ScopeAppID,
			ScopeOrgID: r.ScopeOrgID,
			ElapsedMs:  elapsed.Milliseconds(),
		}),
	})
	return nil
}

func (b *Broker) OnWorkflowStepFailed(_ context.Context, r *workflow.Run, stepName string, stepErr error) error {
	b.publish(&Event{
		Type:      EventWorkflowStepFailed,
		Timestamp: time.Now().UTC(),
		Topic:     WorkflowTopic(r.ID.String()),
		Data: mustMarshal(WorkflowEventData{
			RunID:      r.ID.String(),
			Name:       r.Name,
			StepName:   stepName,
			ScopeAppID: r.ScopeAppID,
			ScopeOrgID: r.ScopeOrgID,
			Error:      stepErr.Error(),
		}),
	})
	return nil
}

func (b *Broker) OnWorkflowCompleted(_ context.Context, r *workflow.Run, elapsed time.Duration) error {
	b.publish(&Event{
		Type:      EventWorkflowCompleted,
		Timestamp: time.Now().UTC(),
		Topic:     WorkflowTopic(r.ID.String()),
		Data: mustMarshal(WorkflowEventData{
			RunID:      r.ID.String(),
			Name:       r.Name,
			ScopeAppID: r.ScopeAppID,
			ScopeOrgID: r.ScopeOrgID,
			ElapsedMs:  elapsed.Milliseconds(),
		}),
	})
	return nil
}

func (b *Broker) OnWorkflowFailed(_ context.Context, r *workflow.Run, runErr error) error {
	b.publish(&Event{
		Type:      EventWorkflowFailed,
		Timestamp: time.Now().UTC(),
		Topic:     WorkflowTopic(r.ID.String()),
		Data: mustMarshal(WorkflowEventData{
			RunID:      r.ID.String(),
			Name:       r.Name,
			ScopeAppID: r.ScopeAppID,
			ScopeOrgID: r.ScopeOrgID,
			Error:      runErr.Error(),
		}),
	})
	return nil
}

// ── Cron lifecycle hooks ────────────────────────────

func (b *Broker) OnCronFired(_ context.Context, entryName string, jobID id.JobID) error {
	b.publish(&Event{
		Type:      EventCronFired,
		Timestamp: time.Now().UTC(),
		Data: mustMarshal(CronEventData{
			EntryName: entryName,
			JobID:     jobID.String(),
		}),
	})
	return nil
}

// ── Shutdown ────────────────────────────────────────

func (b *Broker) OnShutdown(_ context.Context) error {
	b.subscribers.Range(func(key, value any) bool {
		sub := value.(*Subscriber) //nolint:errcheck // sync.Map always stores *Subscriber
		sub.Close()
		b.subscribers.Delete(key)
		return true
	})
	b.logger.Info("stream broker shut down")
	return nil
}
