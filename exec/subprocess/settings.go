package subprocess

// Settings describes process isolation without exposing its environment or arguments.
// Rlimits are requested ceilings. Kernel support still determines enforcement.
type Settings struct {
	UserConfigured                           bool
	UID, GID                                 int
	AllowSameUser, HasRlimits, StrictRlimits bool
	Rlimits                                  Rlimits
	ScratchDir                               string
}

// Settings returns safe inspection fields. Core dumps are always disabled by the shim.
func (e *Executor) Settings() Settings {
	limits := e.opts.rlimits
	limits.Core = 0
	return Settings{
		UserConfigured: e.opts.hasUser, UID: e.opts.uid, GID: e.opts.gid,
		AllowSameUser: e.opts.allowSameUser, HasRlimits: e.opts.hasRlimits,
		StrictRlimits: e.opts.strictRlimits, Rlimits: limits, ScratchDir: e.opts.scratchDir,
	}
}
