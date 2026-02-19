package relayhook

// Option configures a Extension.
type Option func(*Extension)

// PayloadFunc builds a custom event payload for a specific event type.
// The args parameter contains the raw lifecycle arguments (job, error,
// duration, etc.) and the returned value becomes event.Event.Data.
type PayloadFunc func(args any) (any, error)

// WithEvents restricts the extension to emit only the listed event types.
// By default all 12 event types are enabled. Unknown types are silently
// ignored.
func WithEvents(events ...string) Option {
	return func(h *Extension) {
		h.enabled = make(map[string]bool, len(events))
		for _, e := range events {
			h.enabled[e] = true
		}
	}
}

// WithPayloadFunc registers a custom payload builder for the given event
// type. The function replaces the default JSON payload for that event.
func WithPayloadFunc(eventType string, fn PayloadFunc) Option {
	return func(h *Extension) {
		if h.payloads == nil {
			h.payloads = make(map[string]PayloadFunc)
		}
		h.payloads[eventType] = fn
	}
}
