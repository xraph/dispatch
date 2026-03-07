package audithook

import log "github.com/xraph/go-utils/log"

// Option configures an Extension.
type Option func(*Extension)

// WithActions restricts the extension to emit only the listed actions.
// By default all 12 actions are enabled. Unknown actions are silently ignored.
//
// Example:
//
//	audithook.New(emitter,
//	    audithook.WithActions(
//	        audithook.ActionJobCompleted,
//	        audithook.ActionJobFailed,
//	        audithook.ActionJobDLQ,
//	    ),
//	)
func WithActions(actions ...string) Option {
	return func(e *Extension) {
		e.enabled = make(map[string]bool, len(actions))
		for _, a := range actions {
			e.enabled[a] = true
		}
	}
}

// WithLogger sets a custom logger for the extension.
func WithLogger(l log.Logger) Option {
	return func(e *Extension) { e.logger = l }
}
