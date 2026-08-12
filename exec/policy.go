package exec

import (
	"fmt"
	"time"
)

// DefaultGracePeriod is how long a sandbox is given to exit after being
// asked politely, before it is killed outright.
const DefaultGracePeriod = 30 * time.Second

// Level is the minimum isolation a job definition requires. The levels are
// ordered, so a deployment offering a stronger level satisfies a definition
// asking for a weaker one.
type Level int

const (
	// LevelNone runs the handler in the worker process. This is the
	// default and it provides no isolation of any kind.
	LevelNone Level = iota

	// LevelProcess runs the handler in a separate address space, so an
	// exploited parser cannot read the worker's credentials.
	LevelProcess

	// LevelSandboxed adds mount, network, PID, and user namespaces, a
	// seccomp filter, and dropped capabilities.
	LevelSandboxed

	// LevelVM adds an independent kernel — gVisor or Kata — so a Linux
	// privilege escalation is not by itself an escape.
	LevelVM
)

// String renders the level for configuration, logs, and errors.
func (l Level) String() string {
	switch l {
	case LevelNone:
		return "none"
	case LevelProcess:
		return "process"
	case LevelSandboxed:
		return "sandboxed"
	case LevelVM:
		return "vm"
	default:
		return fmt.Sprintf("Level(%d)", int(l))
	}
}

// Policy is a job definition's execution declaration. It states the minimum
// isolation the handler requires, not the executor it runs on: which rung
// satisfies the requirement is a deployment decision.
type Policy struct {
	// Level is the minimum isolation required.
	Level Level

	// GracePeriod is how long the sandbox has to exit after SIGTERM
	// before it is killed.
	GracePeriod time.Duration

	// AllowDowngrade permits running at a weaker level than Level when
	// the deployment cannot provide it. Without it, a deployment that
	// cannot satisfy the policy fails at registration rather than
	// silently running the handler unisolated.
	AllowDowngrade bool

	// Image overrides the container image for out-of-process rungs.
	// Empty means the worker's own image, which is the correct default
	// because the sandbox re-execs the same binary.
	Image string
}

// PolicyOption configures a Policy.
type PolicyOption func(*Policy)

// NewPolicy builds a Policy from options, starting from the defaults:
// no isolation and a 30-second grace period.
func NewPolicy(opts ...PolicyOption) Policy {
	p := Policy{
		Level:       LevelNone,
		GracePeriod: DefaultGracePeriod,
	}
	for _, opt := range opts {
		opt(&p)
	}

	return p
}

// Isolate sets the minimum isolation level the handler requires.
func Isolate(l Level) PolicyOption {
	return func(p *Policy) { p.Level = l }
}

// GracePeriod sets how long the sandbox has to exit cleanly after being
// signalled. Non-positive durations are ignored, because a zero grace
// period reduces the kill ladder to an immediate SIGKILL and loses any
// chance of a clean shutdown.
func GracePeriod(d time.Duration) PolicyOption {
	return func(p *Policy) {
		if d > 0 {
			p.GracePeriod = d
		}
	}
}

// AllowDowngrade permits running below the declared level when the
// deployment cannot satisfy it.
func AllowDowngrade() PolicyOption {
	return func(p *Policy) { p.AllowDowngrade = true }
}

// Image overrides the container image used by out-of-process rungs.
func Image(ref string) PolicyOption {
	return func(p *Policy) { p.Image = ref }
}
