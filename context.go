package dispatch

import "context"

// Context is the execution context for Dispatch handlers.
// For Phase 2 it is a simple alias for context.Context.
// Scope is injected via forge.WithScope on the stdlib context.
// A richer dispatch-specific context (with step methods for workflows)
// is planned for a later phase.
type Context = context.Context
