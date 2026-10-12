package cron_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/cron"
)

func mustLoad(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", name, err)
	}
	return loc
}

// utc2026 is a whole minute in 2026, in UTC.
func utc2026(month time.Month, day, hour, minute int) time.Time {
	return time.Date(2026, month, day, hour, minute, 0, 0, time.UTC)
}

func TestNextFires(t *testing.T) {
	tokyo := mustLoad(t, "Asia/Tokyo")

	cases := []struct {
		name     string
		schedule string
		from     time.Time
		n        int
		want     []time.Time // compared as instants
		wantLoc  string
	}{
		{
			name:     "five fields",
			schedule: "*/15 * * * *",
			from:     time.Date(2026, 10, 7, 10, 7, 30, 0, time.UTC),
			n:        3,
			want:     []time.Time{utc2026(10, 7, 10, 15), utc2026(10, 7, 10, 30), utc2026(10, 7, 10, 45)},
			wantLoc:  "UTC",
		},
		{
			name:     "strictly after from",
			schedule: "*/15 * * * *",
			from:     utc2026(10, 7, 10, 15),
			n:        1,
			want:     []time.Time{utc2026(10, 7, 10, 30)},
			wantLoc:  "UTC",
		},
		{
			name:     "every descriptor",
			schedule: "@every 90m",
			from:     utc2026(10, 7, 10, 0),
			n:        3,
			want:     []time.Time{utc2026(10, 7, 11, 30), utc2026(10, 7, 13, 0), utc2026(10, 7, 14, 30)},
			wantLoc:  "UTC",
		},
		{
			name:     "no prefix is UTC whatever zone from is in",
			schedule: "0 9 * * *",
			from:     time.Date(2026, 10, 7, 10, 0, 0, 0, tokyo), // 01:00 UTC
			n:        1,
			want:     []time.Time{utc2026(10, 7, 9, 0)},
			wantLoc:  "UTC",
		},
		{
			name:     "CRON_TZ prefix",
			schedule: "CRON_TZ=Asia/Tokyo 0 9 * * *",
			from:     utc2026(10, 7, 0, 30), // 09:30 in Tokyo
			n:        2,
			want:     []time.Time{utc2026(10, 8, 0, 0), utc2026(10, 9, 0, 0)},
			wantLoc:  "Asia/Tokyo",
		},
		{
			name:     "TZ prefix",
			schedule: "TZ=Europe/London 0 9 * * *",
			from:     utc2026(10, 7, 0, 0), // British Summer Time, UTC+1
			n:        1,
			want:     []time.Time{utc2026(10, 7, 8, 0)},
			wantLoc:  "Europe/London",
		},
		{
			// 8 March 2026 is the US spring-forward day: 02:30 does not
			// exist in New York that night, so the schedule skips it, and
			// the offset moves from -5 to -4 either side of the gap.
			name:     "DST gap in a named zone",
			schedule: "CRON_TZ=America/New_York 30 2 * * *",
			from:     utc2026(3, 6, 12, 0),
			n:        3,
			want:     []time.Time{utc2026(3, 7, 7, 30), utc2026(3, 9, 6, 30), utc2026(3, 10, 6, 30)},
			wantLoc:  "America/New_York",
		},
		{
			// The scheduler steps every schedule from a UTC instant, so a
			// schedule pinned to the process's local zone runs in UTC.
			name:     "TZ=Local runs in UTC like the scheduler",
			schedule: "TZ=Local 0 9 * * *",
			from:     utc2026(10, 7, 0, 0),
			n:        1,
			want:     []time.Time{utc2026(10, 7, 9, 0)},
			wantLoc:  "UTC",
		},
		{
			name:     "every with a prefix reports the zone",
			schedule: "CRON_TZ=Asia/Tokyo @every 1h",
			from:     utc2026(10, 7, 0, 0),
			n:        2,
			want:     []time.Time{utc2026(10, 7, 1, 0), utc2026(10, 7, 2, 0)},
			wantLoc:  "Asia/Tokyo",
		},
		{
			name:     "a schedule that never fires",
			schedule: "0 0 30 2 *", // 30 February
			from:     utc2026(10, 7, 0, 0),
			n:        3,
			want:     nil,
			wantLoc:  "UTC",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, loc, err := cron.NextFires(tc.schedule, tc.from, tc.n)
			if err != nil {
				t.Fatalf("NextFires(%q): %v", tc.schedule, err)
			}
			if loc == nil || loc.String() != tc.wantLoc {
				t.Fatalf("location = %v, want %s", loc, tc.wantLoc)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d fires %v, want %d %v", len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if !got[i].Equal(tc.want[i]) {
					t.Errorf("fire %d = %v, want %v", i, got[i], tc.want[i])
				}
				if got[i].Location().String() != tc.wantLoc {
					t.Errorf("fire %d is in %v, want it in %s", i, got[i].Location(), tc.wantLoc)
				}
			}
		})
	}
}

func TestNextFires_DSTOffsets(t *testing.T) {
	got, _, err := cron.NextFires("CRON_TZ=America/New_York 30 2 * * *", utc2026(3, 6, 12, 0), 2)
	if err != nil {
		t.Fatalf("NextFires: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d fires, want 2", len(got))
	}
	for i, wantOffset := range []int{-5 * 3600, -4 * 3600} {
		if h, m := got[i].Hour(), got[i].Minute(); h != 2 || m != 30 {
			t.Errorf("fire %d wall clock = %02d:%02d, want 02:30", i, h, m)
		}
		if _, off := got[i].Zone(); off != wantOffset {
			t.Errorf("fire %d offset = %d, want %d", i, off, wantOffset)
		}
	}
}

func TestNextFires_RejectsNonPositiveN(t *testing.T) {
	for _, n := range []int{0, -1} {
		got, loc, err := cron.NextFires("@every 1m", utc2026(10, 7, 0, 0), n)
		if err == nil {
			t.Errorf("NextFires(n=%d) error = nil, want one", n)
		}
		if got != nil || loc != nil {
			t.Errorf("NextFires(n=%d) = %v, %v, want nil, nil", n, got, loc)
		}
	}
}

func TestNextFires_RejectsBadSchedules(t *testing.T) {
	for _, schedule := range []string{"", "not-a-cron", "61 * * * *", "CRON_TZ=Not/AZone 0 9 * * *", "CRON_TZ=UTC"} {
		if _, _, err := cron.NextFires(schedule, utc2026(10, 7, 0, 0), 1); err == nil {
			t.Errorf("NextFires(%q) error = nil, want one", schedule)
		}
	}
}

// TestParseSchedule_TZPrefixWithoutSchedule pins the guard in front of
// robfig's parser, which slices a CRON_TZ= or TZ= prefix up to the first
// space and panics when there is none.
func TestParseSchedule_TZPrefixWithoutSchedule(t *testing.T) {
	for _, expr := range []string{"CRON_TZ=UTC", "TZ=Europe/London", "CRON_TZ=UTC\t0 9 * * *"} {
		if _, err := cron.ParseSchedule(expr); err == nil {
			t.Errorf("ParseSchedule(%q) error = nil, want one", expr)
		}
	}
}
