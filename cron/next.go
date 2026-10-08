package cron

import (
	"fmt"
	"strings"
	"time"
)

// NextFires returns the next n times schedule fires strictly after from,
// and the location the schedule is evaluated in.
//
// It parses with ParseSchedule, so it accepts exactly what the scheduler
// and engine.RegisterCron accept, and it steps the schedule the way the
// scheduler does, from a UTC instant. A schedule with no CRON_TZ= or TZ=
// prefix therefore runs in UTC whatever the process's local zone is, and
// a prefixed one runs in the zone it names. The returned times are in
// that location, so their wall clock reads as the schedule does.
//
// Fewer than n times come back when the schedule runs out. The parser
// searches five years ahead and then gives up, so "0 0 30 2 *" (30
// February) yields none and no error. n must be positive.
func NextFires(schedule string, from time.Time, n int) ([]time.Time, *time.Location, error) {
	if n <= 0 {
		return nil, nil, fmt.Errorf("cron: next fires: n must be positive, got %d", n)
	}

	sched, err := ParseSchedule(schedule)
	if err != nil {
		return nil, nil, err
	}
	loc := scheduleLocation(schedule)

	var fires []time.Time
	t := from.UTC()
	for len(fires) < n {
		t = sched.Next(t)
		if t.IsZero() {
			break
		}
		fires = append(fires, t.In(loc))
	}

	return fires, loc, nil
}

// scheduleLocation is the zone a schedule that ParseSchedule accepted is
// evaluated in. The parser reads a CRON_TZ= or TZ= prefix the same way:
// the zone name runs from the '=' to the first space. A schedule with no
// prefix, or one naming Local, is stepped from the scheduler's UTC clock,
// so it runs in UTC. An @every schedule is a fixed delay no zone changes,
// and it reports the prefix's zone only so a page can show its times
// there.
func scheduleLocation(schedule string) *time.Location {
	if !hasTZPrefix(schedule) {
		return time.UTC
	}
	eq := strings.Index(schedule, "=")
	sp := strings.Index(schedule, " ")
	loc, err := time.LoadLocation(schedule[eq+1 : sp])
	if err != nil || loc == time.Local {
		return time.UTC
	}
	return loc
}

func hasTZPrefix(expr string) bool {
	return strings.HasPrefix(expr, "TZ=") || strings.HasPrefix(expr, "CRON_TZ=")
}
