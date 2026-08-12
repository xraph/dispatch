package exectest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/job"
)

// Job names the suite installs. Every executor under test must be able to
// run all of them.
//
// Artifact-carrying fixtures (writing outputs, reading staged inputs)
// arrive in Phase 2 alongside the suite cases that exercise them. Adding
// them now would ship handlers no case runs.
const (
	JobOK    = "exectest.ok"
	JobError = "exectest.error"
	JobPanic = "exectest.panic"
	JobSlow  = "exectest.slow"
	JobEcho  = "exectest.echo"
)

// ErrIntentional is what JobError returns, so tests can match it exactly.
var ErrIntentional = errors.New("intentional failure")

// EchoPayload is the payload JobEcho round-trips. Want, when non-zero, is
// the byte length the handler asserts Value has, which is how the suite
// proves a large payload crossed the boundary without truncation.
type EchoPayload struct {
	Value string `json:"value"`
	Want  int    `json:"want"`
}

// SlowPayload controls how long JobSlow sleeps.
type SlowPayload struct {
	SleepMillis int  `json:"sleep_millis"`
	IgnoreCtx   bool `json:"ignore_ctx"`
}

// Handlers returns the fixture handler set. Registering these is all an
// executor needs to be run through the suite.
func Handlers() []job.Registrable {
	return []job.Registrable{
		job.NewDefinition(JobOK, func(context.Context, struct{}) error {
			return nil
		}),
		job.NewDefinition(JobError, func(context.Context, struct{}) error {
			return ErrIntentional
		}),
		job.NewDefinition(JobPanic, func(context.Context, struct{}) error {
			panic("intentional panic")
		}),
		job.NewDefinition(JobSlow, func(ctx context.Context, p SlowPayload) error {
			d := time.Duration(p.SleepMillis) * time.Millisecond
			if p.IgnoreCtx {
				// Stands in for a native library that has stopped
				// honouring cancellation. Only a rung that can kill
				// will stop this.
				time.Sleep(d)
				return nil
			}
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
		// Echo proves the payload crossed the boundary intact. It
		// validates by round-tripping rather than by recording to a
		// package-level variable, which an out-of-process rung could
		// never observe anyway.
		job.NewDefinition(JobEcho, func(_ context.Context, p EchoPayload) error {
			if p.Value == "" {
				return errors.New("exectest: echo received an empty payload")
			}
			if p.Want != 0 && len(p.Value) != p.Want {
				return fmt.Errorf("exectest: echo got %d bytes, want %d", len(p.Value), p.Want)
			}

			return nil
		}),
	}
}

// HandlerNames returns the fixture job names, which is what a fingerprint
// is derived from.
func HandlerNames() []string {
	defs := Handlers()
	names := make([]string, 0, len(defs))
	for _, d := range defs {
		names = append(names, d.JobName())
	}

	return names
}
