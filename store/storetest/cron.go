package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
)

// CronStore is what the cron suite requires: the base cron store plus the
// targeted writes the scheduler and the operator actions use.
type CronStore interface {
	cron.Store
	cron.TargetedUpdater
}

// RunCronSuite pins the targeted cron writes: each changes only the fields
// it names. The load-bearing case is DisableSurvivesAFire, the regression
// that let a disabled cron come back when the scheduler wrote its whole
// stale row after firing.
//
// newStore may return a shared store, so every case registers its own
// entry under a unique name and never lists or counts.
func RunCronSuite(t *testing.T, newStore func(t *testing.T) CronStore) {
	t.Helper()

	cases := []struct {
		name string
		fn   func(t *testing.T, s CronStore)
	}{
		{"SetCronEnabledLeavesOtherFieldsAlone", testSetCronEnabledLeavesOthers},
		{"SetCronEnabledNilNextRunKeepsNextRun", testSetCronEnabledNilNextRun},
		{"UpdateCronNextRunLeavesOtherFieldsAlone", testUpdateCronNextRunLeavesOthers},
		{"DisableSurvivesAFire", testCronDisableSurvivesAFire},
		{"TargetedWritesUnknownEntry", testCronTargetedUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

// registerCron stores an entry with every field set to something distinct,
// takes its lock, and returns it as the store reads it back. Comparing
// store reads with store reads keeps each backend's time precision out of
// the comparison.
func registerCron(t *testing.T, s CronStore, enabled bool) *cron.Entry {
	t.Helper()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	lastRun := now.Add(-5 * time.Minute)
	nextRun := now.Add(5 * time.Minute)

	cronID := id.NewCronID()
	e := &cron.Entry{
		Entity:     dispatch.NewEntity(),
		ID:         cronID,
		Name:       "storetest-cron-" + cronID.String(),
		Schedule:   "*/5 * * * *",
		JobName:    "cron-suite-job",
		Queue:      "cron-suite",
		Payload:    []byte(`{"report":"daily"}`),
		ScopeAppID: "app_cron",
		ScopeOrgID: "org_cron",
		LastRunAt:  &lastRun,
		NextRunAt:  &nextRun,
		Enabled:    enabled,
	}
	if err := s.RegisterCron(ctx, e); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}

	locked, err := s.AcquireCronLock(ctx, e.ID, id.NewWorkerID(), time.Hour)
	if err != nil {
		t.Fatalf("AcquireCronLock: %v", err)
	}
	if !locked {
		t.Fatal("AcquireCronLock on a fresh entry = false, want true")
	}

	return mustGetCron(t, s, e.ID)
}

// mustGetCron reads an entry and returns a copy of it, so a store that
// hands out its own pointer cannot change a snapshot behind the test.
func mustGetCron(t *testing.T, s CronStore, entryID id.CronID) *cron.Entry {
	t.Helper()

	got, err := s.GetCron(context.Background(), entryID)
	if err != nil {
		t.Fatalf("GetCron(%s): %v", entryID, err)
	}
	snapshot := *got

	return &snapshot
}

func timePtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return a.Equal(*b)
}

func fmtTimePtr(p *time.Time) string {
	if p == nil {
		return "nil"
	}

	return p.Format(time.RFC3339Nano)
}

// assertCronUntouched checks every field neither targeted write may
// change. Enabled, NextRunAt and UpdatedAt are each case's to check.
func assertCronUntouched(t *testing.T, label string, before, after *cron.Entry) {
	t.Helper()

	if after.ID != before.ID {
		t.Errorf("%s: ID = %s, want %s", label, after.ID, before.ID)
	}
	if after.Name != before.Name {
		t.Errorf("%s: Name = %q, want %q", label, after.Name, before.Name)
	}
	if after.Schedule != before.Schedule {
		t.Errorf("%s: Schedule = %q, want %q", label, after.Schedule, before.Schedule)
	}
	if after.JobName != before.JobName {
		t.Errorf("%s: JobName = %q, want %q", label, after.JobName, before.JobName)
	}
	if after.Queue != before.Queue {
		t.Errorf("%s: Queue = %q, want %q", label, after.Queue, before.Queue)
	}
	if !bytes.Equal(after.Payload, before.Payload) {
		t.Errorf("%s: Payload = %q, want %q", label, after.Payload, before.Payload)
	}
	if after.ScopeAppID != before.ScopeAppID || after.ScopeOrgID != before.ScopeOrgID {
		t.Errorf("%s: scope = %q/%q, want %q/%q", label,
			after.ScopeAppID, after.ScopeOrgID, before.ScopeAppID, before.ScopeOrgID)
	}
	if !timePtrEqual(after.LastRunAt, before.LastRunAt) {
		t.Errorf("%s: LastRunAt = %s, want %s", label, fmtTimePtr(after.LastRunAt), fmtTimePtr(before.LastRunAt))
	}
	if after.LockedBy != before.LockedBy {
		t.Errorf("%s: LockedBy = %q, want %q", label, after.LockedBy, before.LockedBy)
	}
	if !timePtrEqual(after.LockedUntil, before.LockedUntil) {
		t.Errorf("%s: LockedUntil = %s, want %s", label, fmtTimePtr(after.LockedUntil), fmtTimePtr(before.LockedUntil))
	}
	if !after.CreatedAt.Equal(before.CreatedAt) {
		t.Errorf("%s: CreatedAt = %v, want %v", label, after.CreatedAt, before.CreatedAt)
	}
	if after.UpdatedAt.Before(before.UpdatedAt) {
		t.Errorf("%s: UpdatedAt went backwards: %v, was %v", label, after.UpdatedAt, before.UpdatedAt)
	}
}

func testSetCronEnabledLeavesOthers(t *testing.T, s CronStore) {
	ctx := context.Background()
	before := registerCron(t, s, false)
	next := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)

	if err := s.SetCronEnabled(ctx, before.ID, true, &next); err != nil {
		t.Fatalf("SetCronEnabled(true): %v", err)
	}

	after := mustGetCron(t, s, before.ID)
	if !after.Enabled {
		t.Error("Enabled = false after SetCronEnabled(true)")
	}
	if !timePtrEqual(after.NextRunAt, &next) {
		t.Errorf("NextRunAt = %s, want %s", fmtTimePtr(after.NextRunAt), fmtTimePtr(&next))
	}
	assertCronUntouched(t, "SetCronEnabled(true)", before, after)
}

func testSetCronEnabledNilNextRun(t *testing.T, s CronStore) {
	ctx := context.Background()
	before := registerCron(t, s, true)

	if err := s.SetCronEnabled(ctx, before.ID, false, nil); err != nil {
		t.Fatalf("SetCronEnabled(false, nil): %v", err)
	}

	after := mustGetCron(t, s, before.ID)
	if after.Enabled {
		t.Error("Enabled = true after SetCronEnabled(false)")
	}
	if !timePtrEqual(after.NextRunAt, before.NextRunAt) {
		t.Errorf("NextRunAt = %s, want it left at %s", fmtTimePtr(after.NextRunAt), fmtTimePtr(before.NextRunAt))
	}
	assertCronUntouched(t, "SetCronEnabled(false, nil)", before, after)
}

func testUpdateCronNextRunLeavesOthers(t *testing.T, s CronStore) {
	ctx := context.Background()
	before := registerCron(t, s, true)
	next := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Millisecond)

	if err := s.UpdateCronNextRun(ctx, before.ID, next); err != nil {
		t.Fatalf("UpdateCronNextRun: %v", err)
	}

	after := mustGetCron(t, s, before.ID)
	if !after.Enabled {
		t.Error("Enabled = false after UpdateCronNextRun on an enabled entry")
	}
	if !timePtrEqual(after.NextRunAt, &next) {
		t.Errorf("NextRunAt = %s, want %s", fmtTimePtr(after.NextRunAt), fmtTimePtr(&next))
	}
	assertCronUntouched(t, "UpdateCronNextRun", before, after)
}

// testCronDisableSurvivesAFire is the regression. An operator disables an
// enabled entry, then the scheduler, which read the entry before the
// disable, records its next fire. The entry must stay disabled.
func testCronDisableSurvivesAFire(t *testing.T, s CronStore) {
	ctx := context.Background()
	entry := registerCron(t, s, true)

	if err := s.SetCronEnabled(ctx, entry.ID, false, nil); err != nil {
		t.Fatalf("SetCronEnabled(false): %v", err)
	}
	disabled := mustGetCron(t, s, entry.ID)
	if disabled.Enabled {
		t.Fatal("Enabled = true right after SetCronEnabled(false)")
	}

	// What the scheduler does after a fire.
	next := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Millisecond)
	if err := s.UpdateCronNextRun(ctx, entry.ID, next); err != nil {
		t.Fatalf("UpdateCronNextRun: %v", err)
	}

	after := mustGetCron(t, s, entry.ID)
	if after.Enabled {
		t.Fatal("a disabled cron came back enabled after the scheduler recorded its next run")
	}
	if !timePtrEqual(after.NextRunAt, &next) {
		t.Errorf("NextRunAt = %s, want %s", fmtTimePtr(after.NextRunAt), fmtTimePtr(&next))
	}
	assertCronUntouched(t, "fire after disable", disabled, after)
}

func testCronTargetedUnknown(t *testing.T, s CronStore) {
	ctx := context.Background()
	next := time.Now().UTC()

	if err := s.SetCronEnabled(ctx, id.NewCronID(), true, &next); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Errorf("SetCronEnabled(unknown) error = %v, want ErrCronNotFound", err)
	}
	if err := s.UpdateCronNextRun(ctx, id.NewCronID(), next); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Errorf("UpdateCronNextRun(unknown) error = %v, want ErrCronNotFound", err)
	}
}
