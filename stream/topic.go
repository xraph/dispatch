package stream

import (
	"fmt"
	"strings"
	"sync"
)

// Topic names follow a pattern:
//
//	job:<jobID>       — events for a specific job
//	workflow:<runID>   — events for a specific workflow run
//	queue:<name>       — all events for a queue
//	jobs               — all job lifecycle events
//	workflows          — all workflow lifecycle events
//	firehose           — everything

const (
	TopicJobs      = "jobs"
	TopicWorkflows = "workflows"
	TopicFirehose  = "firehose"
)

// JobTopic returns the topic name for a specific job.
func JobTopic(jobID string) string { return "job:" + jobID }

// WorkflowTopic returns the topic name for a specific workflow run.
func WorkflowTopic(runID string) string { return "workflow:" + runID }

// QueueTopic returns the topic name for a queue.
func QueueTopic(queue string) string { return "queue:" + queue }

// TopicRegistry manages subscriber sets per topic.
// It is safe for concurrent use.
type TopicRegistry struct {
	mu     sync.RWMutex
	topics map[string]map[string]*Subscriber // topic → subscriberID → subscriber
}

// NewTopicRegistry creates an empty topic registry.
func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{
		topics: make(map[string]map[string]*Subscriber),
	}
}

// Subscribe adds a subscriber to a topic. Creates the topic if it
// doesn't exist.
func (tr *TopicRegistry) Subscribe(topic string, sub *Subscriber) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	subs, ok := tr.topics[topic]
	if !ok {
		subs = make(map[string]*Subscriber)
		tr.topics[topic] = subs
	}
	subs[sub.ID()] = sub
	sub.addTopic(topic)
}

// Unsubscribe removes a subscriber from a topic. Cleans up empty topics.
func (tr *TopicRegistry) Unsubscribe(topic, subscriberID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	subs, ok := tr.topics[topic]
	if !ok {
		return
	}
	if sub, exists := subs[subscriberID]; exists {
		sub.removeTopic(topic)
		delete(subs, subscriberID)
	}
	if len(subs) == 0 {
		delete(tr.topics, topic)
	}
}

// UnsubscribeAll removes a subscriber from all topics.
func (tr *TopicRegistry) UnsubscribeAll(subscriberID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	for topic, subs := range tr.topics {
		if sub, ok := subs[subscriberID]; ok {
			sub.removeTopic(topic)
			delete(subs, subscriberID)
		}
		if len(subs) == 0 {
			delete(tr.topics, topic)
		}
	}
}

// Publish sends an event to all subscribers on the given topic.
// Returns the number of subscribers that received the event.
func (tr *TopicRegistry) Publish(topic string, evt *Event) int {
	tr.mu.RLock()
	subs := tr.topics[topic]
	// Copy to avoid holding lock during send.
	targets := make([]*Subscriber, 0, len(subs))
	for _, s := range subs {
		targets = append(targets, s)
	}
	tr.mu.RUnlock()

	delivered := 0
	for _, s := range targets {
		if s.send(evt) {
			delivered++
		}
	}
	return delivered
}

// Broadcast sends an event to all subscribers on multiple topics.
// Deduplicates subscribers that are on more than one of the listed topics.
func (tr *TopicRegistry) Broadcast(topics []string, evt *Event) int {
	tr.mu.RLock()
	seen := make(map[string]*Subscriber)
	for _, topic := range topics {
		for id, sub := range tr.topics[topic] {
			seen[id] = sub
		}
	}
	tr.mu.RUnlock()

	delivered := 0
	for _, sub := range seen {
		if sub.send(evt) {
			delivered++
		}
	}
	return delivered
}

// TopicCount returns the number of active topics.
func (tr *TopicRegistry) TopicCount() int {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return len(tr.topics)
}

// SubscriberCount returns the number of subscribers on a topic.
func (tr *TopicRegistry) SubscriberCount(topic string) int {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return len(tr.topics[topic])
}

// resolveTopics returns all topics an event should be published to
// based on its type and data.
func resolveTopics(evt *Event) []string {
	topics := []string{TopicFirehose}

	evtType := string(evt.Type)
	if strings.HasPrefix(evtType, "job.") {
		topics = append(topics, TopicJobs)
	} else if strings.HasPrefix(evtType, "workflow.") {
		topics = append(topics, TopicWorkflows)
	}
	// Cron events only go to firehose (no additional topic).

	// Add entity-specific topic from the event's own topic field.
	if evt.Topic != "" {
		topics = append(topics, evt.Topic)
	}

	return topics
}

// ParseTopicEntity extracts the entity type and ID from a topic string.
// For example, "job:job_abc123" returns ("job", "job_abc123").
// Returns ("", "") for global topics like "jobs" or "firehose".
func ParseTopicEntity(topic string) (entityType, entityID string) {
	idx := strings.IndexByte(topic, ':')
	if idx < 0 {
		return "", ""
	}
	return topic[:idx], topic[idx+1:]
}

// ValidateTopic checks whether a topic string is valid.
func ValidateTopic(topic string) error {
	switch topic {
	case TopicJobs, TopicWorkflows, TopicFirehose:
		return nil
	}

	entityType, entityID := ParseTopicEntity(topic)
	if entityType == "" || entityID == "" {
		return fmt.Errorf("stream: invalid topic %q", topic)
	}

	switch entityType {
	case "job", "workflow", "queue":
		return nil
	default:
		return fmt.Errorf("stream: unknown topic entity type %q", entityType)
	}
}
