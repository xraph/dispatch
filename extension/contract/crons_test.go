package contract

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

func seedCron(t *testing.T, d Deps, name, schedule string, enabled bool) *cron.Entry {
	t.Helper()
	next := time.Now().UTC().Add(-time.Hour)
	entry := &cron.Entry{Entity: dispatch.NewEntity(), ID: id.NewCronID(), Name: name, Schedule: schedule, JobName: "mail.send", Queue: "mail",
		Payload: []byte(`{"n":9007199254740993}`), NextRunAt: &next, Enabled: enabled}
	if err := d.Store.RegisterCron(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	return entry
}
func TestCronProjectionKeepsUTCInstantsAndScheduleZone(t *testing.T) {
	entry := &cron.Entry{ID: id.NewCronID(), Schedule: "CRON_TZ=America/New_York 0 9 * * *"}
	at := time.Date(2026, 3, 7, 13, 0, 0, 0, time.UTC)
	row := projectCron(entry, at)
	if row.Location == nil || *row.Location != "America/New_York" || len(row.NextFires) != 5 ||
		row.NextFires[0] != "2026-03-07T14:00:00Z" || row.NextFires[1] != "2026-03-08T13:00:00Z" {
		t.Fatalf("DST projection = %+v", row)
	}
	if row.Queue != nil || row.EffectiveQueue != "default" || row.LockedBy != nil || row.ScopeAppID != nil || row.ScopeOrgID != nil {
		t.Fatalf("nulls/defaults = %+v", row)
	}
}
func runCronDomain(t *testing.T, s store.Store) {
	t.Helper()
	recorder := &actionRecorder{}
	d := contractDeps(t, s, engine.WithExtension(recorder))
	ctx := context.Background()
	p := fc.Principal{User: &dashauth.UserInfo{Subject: "operator"}}
	first := seedCron(t, d, "alpha", "CRON_TZ=America/New_York 0 9 * * *", false)
	second := seedCron(t, d, "beta", "@every 1h", true)
	page, err := cronsListHandler(d)(ctx, EmptyInput{}, p)
	if err != nil || len(page.Items) != 2 || page.Items[0].ID != first.ID.String() || page.Items[1].ID != second.ID.String() || !page.Complete || page.NextCursor != nil {
		t.Fatalf("list = %+v, %v", page, err)
	}
	detail, err := cronsGetHandler(d)(ctx, IDInput{ID: first.ID.String()}, p)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(detail)
	if !strings.Contains(string(raw), "9007199254740993") || strings.Contains(string(raw), "job_name") || len(detail.NextFires) != 5 {
		t.Fatalf("detail = %s", raw)
	}
	now := time.Now()
	enabled, err := cronToggleHandler(d, true)(ctx, IDInput{ID: first.ID.String()}, p)
	if err != nil || !enabled.Enabled || enabled.NextRunAt == nil {
		t.Fatalf("enable = %+v, %v", enabled, err)
	}
	next, err := time.Parse(time.RFC3339Nano, *enabled.NextRunAt)
	if err != nil || !next.After(now) {
		t.Fatalf("enable caught up: %s, %v", *enabled.NextRunAt, err)
	}
	disabled, err := cronToggleHandler(d, false)(ctx, IDInput{ID: second.ID.String()}, p)
	if err != nil || disabled.Enabled {
		t.Fatalf("disable = %+v, %v", disabled, err)
	}
	before, err := d.Store.GetCron(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	fired, err := cronsRunNowHandler(d)(ctx, IDInput{ID: second.ID.String()}, p)
	if err != nil || fired.Job.Queue != "mail" || fired.Job.State != job.StatePending {
		t.Fatalf("run now = %+v, %v", fired, err)
	}
	after, err := d.Store.GetCron(ctx, second.ID)
	if err != nil || after.Enabled || !reflect.DeepEqual(before.NextRunAt, after.NextRunAt) || !reflect.DeepEqual(before.LastRunAt, after.LastRunAt) {
		t.Fatalf("schedule changed: before=%+v, after=%+v, %v", before, after, err)
	}
	jobID, err := id.ParseJobID(fired.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	storedJob, err := d.Store.GetJob(ctx, jobID)
	if err != nil || string(storedJob.Payload) != string(second.Payload) {
		t.Fatalf("payload = %+v, %v", storedJob, err)
	}
	if _, deleteErr := cronsDeleteHandler(d)(ctx, IDInput{ID: second.ID.String()}, p); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if _, getErr := cronsGetHandler(d)(ctx, IDInput{ID: second.ID.String()}, p); !errors.Is(getErr, fc.ErrNotFound) {
		t.Fatalf("deleted = %v", getErr)
	}
	if _, getErr := cronsGetHandler(d)(ctx, IDInput{ID: id.NewJobID().String()}, p); !errors.Is(getErr, fc.ErrBadRequest) {
		t.Fatalf("invalid = %v", getErr)
	}
	if len(recorder.actions) != 4 {
		t.Fatalf("actions = %+v", recorder.actions)
	}
	for _, a := range recorder.actions {
		if a.Actor != "operator" {
			t.Fatalf("actor = %+v", a)
		}
	}
}
func TestCronDomainMemoryAndSQLite(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runCronDomain(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) { runCronDomain(t, sqliteContractStore(t)) })
}
func TestCronInvalidStoredScheduleRemainsInspectableAndRemovable(t *testing.T) {
	for _, schedule := range []string{"not a schedule", "0 0 30 2 *"} {
		t.Run(schedule, func(t *testing.T) {
			d := contractDeps(t, memory.New())
			e := seedCron(t, d, "invalid", schedule, false)
			detail, err := cronsGetHandler(d)(context.Background(), IDInput{ID: e.ID.String()}, fc.Principal{})
			if err != nil || detail.ScheduleError == nil || detail.NextFires == nil || len(detail.NextFires) != 0 {
				t.Fatalf("invalid record = %+v, %v", detail, err)
			}
			_, err = cronToggleHandler(d, true)(context.Background(), IDInput{ID: e.ID.String()}, fc.Principal{})
			var ce *fc.Error
			if !errors.As(err, &ce) || ce.Code != fc.CodeConflict || ce.Details["state"] != "disabled" {
				t.Fatalf("enable refusal = %v", err)
			}
			if _, deleteErr := cronsDeleteHandler(d)(context.Background(), IDInput{ID: e.ID.String()}, fc.Principal{}); deleteErr != nil {
				t.Fatal(deleteErr)
			}
		})
	}
}

type failingCronStore struct{ store.Store }

func (s failingCronStore) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	if _, ok := ctx.Deadline(); !ok {
		panic("missing cron query deadline")
	}
	return nil, errors.New("private database diagnostics")
}
func TestCronReadFailureIsRedacted(t *testing.T) {
	d := contractDeps(t, failingCronStore{Store: memory.New()})
	_, err := cronsListHandler(d)(context.Background(), EmptyInput{}, fc.Principal{})
	if !errors.Is(err, fc.ErrInternal) || strings.Contains(err.Error(), "private") {
		t.Fatalf("read failure = %v", err)
	}
}
