package stream

import (
	"sync"
	"sync/atomic"
)

// Subscriber receives events from topics it is subscribed to.
// It uses credit-based flow control: the subscriber grants credits
// indicating how many events it can receive. The broker stops
// sending when credits reach zero.
type Subscriber struct {
	// id uniquely identifies this subscriber.
	id string

	// ch is the buffered channel events are sent on.
	ch chan *Event

	// credits tracks remaining flow-control credits.
	// When zero, the broker skips this subscriber.
	credits atomic.Int64

	// topics tracks which topics this subscriber is on.
	topics map[string]struct{}
	mu     sync.RWMutex

	// filter is an optional predicate. If set, only events
	// matching the filter are delivered.
	filter func(*Event) bool

	// closed prevents double-close of the channel.
	closed atomic.Bool
}

// NewSubscriber creates a subscriber with the given buffer size
// and initial credits.
func NewSubscriber(id string, bufferSize int, initialCredits int64) *Subscriber {
	s := &Subscriber{
		id:     id,
		ch:     make(chan *Event, bufferSize),
		topics: make(map[string]struct{}),
	}
	s.credits.Store(initialCredits)
	return s
}

// ID returns the subscriber identifier.
func (s *Subscriber) ID() string { return s.id }

// C returns the read-only event channel.
func (s *Subscriber) C() <-chan *Event { return s.ch }

// AddCredits replenishes flow-control credits.
func (s *Subscriber) AddCredits(n int64) {
	s.credits.Add(n)
}

// Credits returns the current credit count.
func (s *Subscriber) Credits() int64 {
	return s.credits.Load()
}

// SetFilter sets an optional event filter predicate.
func (s *Subscriber) SetFilter(fn func(*Event) bool) {
	s.filter = fn
}

// addTopic records that this subscriber is on the given topic.
func (s *Subscriber) addTopic(topic string) {
	s.mu.Lock()
	s.topics[topic] = struct{}{}
	s.mu.Unlock()
}

// removeTopic removes a topic from the subscriber's tracked set.
func (s *Subscriber) removeTopic(topic string) {
	s.mu.Lock()
	delete(s.topics, topic)
	s.mu.Unlock()
}

// Topics returns a copy of all subscribed topic names.
func (s *Subscriber) Topics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.topics))
	for t := range s.topics {
		out = append(out, t)
	}
	return out
}

// send attempts to deliver an event to the subscriber.
// Returns false if the event was dropped (no credits, filter mismatch, or full buffer).
func (s *Subscriber) send(evt *Event) bool {
	if s.closed.Load() {
		return false
	}

	// Check filter.
	if s.filter != nil && !s.filter(evt) {
		return false
	}

	// Check credits.
	for {
		current := s.credits.Load()
		if current <= 0 {
			return false
		}
		if s.credits.CompareAndSwap(current, current-1) {
			break
		}
	}

	// Non-blocking send.
	select {
	case s.ch <- evt:
		return true
	default:
		// Buffer full, restore credit.
		s.credits.Add(1)
		return false
	}
}

// Close closes the subscriber channel. Safe to call multiple times.
func (s *Subscriber) Close() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
}
