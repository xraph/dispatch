package stream

import (
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestBrokerSubscribeAndPublish(t *testing.T) {
	t.Parallel()

	b := NewBroker(testLogger())

	sub := b.Subscribe("sub-1", TopicJobs)

	evt := &Event{
		Type:      EventJobEnqueued,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic("job-123"),
		Data:      json.RawMessage(`{"job_id":"job-123"}`),
	}
	b.publish(evt)

	// Event should arrive on the subscriber channel.
	select {
	case received := <-sub.C():
		if received.Type != EventJobEnqueued {
			t.Errorf("Type = %q, want %q", received.Type, EventJobEnqueued)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestBrokerMultipleTopics(t *testing.T) {
	t.Parallel()

	b := NewBroker(testLogger())

	// Subscribe to firehose — should get everything.
	firehose := b.Subscribe("firehose-sub", TopicFirehose)

	// Subscribe to just jobs.
	jobsSub := b.Subscribe("jobs-sub", TopicJobs)

	// Publish a job event.
	evt := &Event{
		Type:      EventJobCompleted,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic("job-456"),
		Data:      json.RawMessage(`{}`),
	}
	b.publish(evt)

	// Both should receive the event.
	for _, sub := range []*Subscriber{firehose, jobsSub} {
		select {
		case <-sub.C():
			// ok
		case <-time.After(time.Second):
			t.Fatalf("subscriber %s timed out", sub.ID())
		}
	}
}

func TestBrokerWorkflowTopics(t *testing.T) {
	t.Parallel()

	b := NewBroker(testLogger())

	// Subscribe to specific workflow run.
	sub := b.Subscribe("wf-sub", WorkflowTopic("run-abc"))

	// Publish event to that run.
	evt := &Event{
		Type:      EventWorkflowStepCompleted,
		Timestamp: time.Now().UTC(),
		Topic:     WorkflowTopic("run-abc"),
		Data:      json.RawMessage(`{"step_name":"validate"}`),
	}
	b.publish(evt)

	select {
	case received := <-sub.C():
		if received.Type != EventWorkflowStepCompleted {
			t.Errorf("Type = %q, want %q", received.Type, EventWorkflowStepCompleted)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for workflow event")
	}

	// Publish event to different run — should NOT arrive.
	evt2 := &Event{
		Type:      EventWorkflowStarted,
		Timestamp: time.Now().UTC(),
		Topic:     WorkflowTopic("run-other"),
		Data:      json.RawMessage(`{}`),
	}
	b.publish(evt2)

	select {
	case <-sub.C():
		t.Fatal("should not receive event for different run")
	case <-time.After(50 * time.Millisecond):
		// ok — no event
	}
}

func TestBrokerUnsubscribe(t *testing.T) {
	t.Parallel()

	b := NewBroker(testLogger())

	sub := b.Subscribe("sub-rm", TopicFirehose)

	// Remove subscriber.
	b.RemoveSubscriber("sub-rm")

	evt := &Event{
		Type:      EventJobEnqueued,
		Timestamp: time.Now().UTC(),
		Topic:     JobTopic("j1"),
		Data:      json.RawMessage(`{}`),
	}
	b.publish(evt)

	// Channel should be closed.
	select {
	case _, ok := <-sub.C():
		if ok {
			t.Fatal("channel should be closed after RemoveSubscriber")
		}
	case <-time.After(100 * time.Millisecond):
		// ok
	}
}

func TestBrokerStats(t *testing.T) {
	t.Parallel()

	b := NewBroker(testLogger())

	_ = b.Subscribe("s1", TopicJobs)
	_ = b.Subscribe("s2", TopicWorkflows, TopicFirehose)

	stats := b.Stats()
	if stats.SubscriberCount != 2 {
		t.Errorf("SubscriberCount = %d, want 2", stats.SubscriberCount)
	}
	if stats.TopicCount < 2 {
		t.Errorf("TopicCount = %d, want >= 2", stats.TopicCount)
	}
}

func TestSubscriberCredits(t *testing.T) {
	t.Parallel()

	sub := NewSubscriber("credit-sub", 10, 2)

	evt := &Event{Type: EventJobEnqueued, Timestamp: time.Now().UTC(), Data: json.RawMessage(`{}`)}

	// Should accept 2 events (initial credits).
	if !sub.send(evt) {
		t.Fatal("first send should succeed")
	}
	if !sub.send(evt) {
		t.Fatal("second send should succeed")
	}

	// Third should fail — no credits.
	if sub.send(evt) {
		t.Fatal("third send should fail (no credits)")
	}

	// Replenish credits.
	sub.AddCredits(5)
	if sub.Credits() != 5 {
		t.Errorf("Credits = %d, want 5", sub.Credits())
	}

	if !sub.send(evt) {
		t.Fatal("send after credit replenishment should succeed")
	}
}

func TestSubscriberFilter(t *testing.T) {
	t.Parallel()

	sub := NewSubscriber("filter-sub", 10, 100)
	sub.SetFilter(func(e *Event) bool {
		return e.Type == EventJobFailed
	})

	// Should be rejected by filter.
	if sub.send(&Event{Type: EventJobCompleted, Timestamp: time.Now().UTC(), Data: json.RawMessage(`{}`)}) {
		t.Fatal("completed event should be filtered out")
	}

	// Should pass filter.
	if !sub.send(&Event{Type: EventJobFailed, Timestamp: time.Now().UTC(), Data: json.RawMessage(`{}`)}) {
		t.Fatal("failed event should pass filter")
	}
}

func TestTopicValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		topic string
		valid bool
	}{
		{TopicJobs, true},
		{TopicWorkflows, true},
		{TopicFirehose, true},
		{"job:job-123", true},
		{"workflow:run-abc", true},
		{"queue:default", true},
		{"invalid", false},
		{"unknown:entity", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			err := ValidateTopic(tt.topic)
			if tt.valid && err != nil {
				t.Errorf("ValidateTopic(%q) returned error: %v", tt.topic, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("ValidateTopic(%q) should return error", tt.topic)
			}
		})
	}
}

func TestTopicRegistry(t *testing.T) {
	t.Parallel()

	tr := NewTopicRegistry()

	sub1 := NewSubscriber("s1", 10, 100)
	sub2 := NewSubscriber("s2", 10, 100)

	tr.Subscribe("topic-a", sub1)
	tr.Subscribe("topic-a", sub2)
	tr.Subscribe("topic-b", sub1)

	if tr.TopicCount() != 2 {
		t.Errorf("TopicCount = %d, want 2", tr.TopicCount())
	}
	if tr.SubscriberCount("topic-a") != 2 {
		t.Errorf("SubscriberCount(topic-a) = %d, want 2", tr.SubscriberCount("topic-a"))
	}

	// Unsubscribe s2 from topic-a.
	tr.Unsubscribe("topic-a", "s2")
	if tr.SubscriberCount("topic-a") != 1 {
		t.Errorf("SubscriberCount(topic-a) = %d, want 1", tr.SubscriberCount("topic-a"))
	}

	// UnsubscribeAll for s1.
	tr.UnsubscribeAll("s1")
	if tr.TopicCount() != 0 {
		t.Errorf("TopicCount after UnsubscribeAll = %d, want 0", tr.TopicCount())
	}
}

func TestBroadcastDeduplication(t *testing.T) {
	t.Parallel()

	tr := NewTopicRegistry()
	sub := NewSubscriber("dedup-sub", 10, 100)

	// Subscribe to multiple topics.
	tr.Subscribe("topic-x", sub)
	tr.Subscribe("topic-y", sub)

	evt := &Event{Type: EventJobEnqueued, Timestamp: time.Now().UTC(), Data: json.RawMessage(`{}`)}

	delivered := tr.Broadcast([]string{"topic-x", "topic-y"}, evt)
	if delivered != 1 {
		t.Errorf("Broadcast delivered to %d subscribers, want 1 (deduplicated)", delivered)
	}
}

func TestResolveTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		evt      *Event
		expected []string
	}{
		{
			evt:      &Event{Type: EventJobEnqueued, Topic: "job:j1"},
			expected: []string{TopicFirehose, TopicJobs, "job:j1"},
		},
		{
			evt:      &Event{Type: EventWorkflowStarted, Topic: "workflow:r1"},
			expected: []string{TopicFirehose, TopicWorkflows, "workflow:r1"},
		},
		{
			evt:      &Event{Type: EventCronFired, Topic: ""},
			expected: []string{TopicFirehose},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.evt.Type), func(t *testing.T) {
			topics := resolveTopics(tt.evt)
			if len(topics) != len(tt.expected) {
				t.Errorf("got %d topics, want %d: %v", len(topics), len(tt.expected), topics)
				return
			}
			for i, topic := range topics {
				if topic != tt.expected[i] {
					t.Errorf("topic[%d] = %q, want %q", i, topic, tt.expected[i])
				}
			}
		})
	}
}
