package contract

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"
	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

func seedDLQ(t *testing.T, d Deps, name, queue, app, org string, failed time.Time, replayed bool) *dlq.Entry {
	t.Helper()
	e := &dlq.Entry{ID: id.NewDLQID(), JobID: id.NewJobID(), JobName: name, Queue: queue, Payload: []byte(`{"n":9007199254740993}`),
		Error: "handler failed", RetryCount: 3, MaxRetries: 3, ScopeAppID: app, ScopeOrgID: org, FailedAt: failed, CreatedAt: failed,
		Priority: 7, Timeout: time.Minute, LeaseTTL: time.Hour, Resources: resource.Set{"cpu": 1}}
	if replayed {
		at := failed.Add(time.Minute)
		e.ReplayedAt = &at
	}
	if err := d.Store.PushDLQ(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	return e
}
func runDLQDomain(t *testing.T, s store.Store) {
	t.Helper()
	recorder := &actionRecorder{}
	d := contractDeps(t, s, engine.WithExtension(recorder))
	p := fc.Principal{User: &dashauth.UserInfo{Subject: "operator"}, Claims: map[string]any{"scope_app_id": "app-a", "scope_org_id": "org-a"}}
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []*dlq.Entry{
		seedDLQ(t, d, "mail.a", "mail", "app-a", "org-a", base, false),
		seedDLQ(t, d, "mail.b", "mail", "app-b", "org-b", base.Add(time.Hour), true),
		seedDLQ(t, d, "report.a", "reports", "app-a", "org-a", base.Add(2*time.Hour), false),
		seedDLQ(t, d, "mail.c", "mail", "", "", base.Add(3*time.Hour), false),
	}
	var got []string
	cursor := ""
	for {
		page, err := dlqListHandler(d)(ctx, DLQListInput{Limit: 2, Cursor: cursor}, p)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Items {
			got = append(got, row.ID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
		if len(got) > len(rows) {
			t.Fatal("cursor repeated entries")
		}
	}
	want := make([]string, 0, len(rows))
	for _, row := range rows {
		want = append(want, row.ID.String())
	}
	slices.Sort(want)
	slices.Reverse(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all scopes/order = %v, want %v", got, want)
	}
	replayed := true
	page, err := dlqListHandler(d)(ctx, DLQListInput{Queue: "mail", NamePrefix: "mail.", ScopeAppID: "app-b", ScopeOrgID: "org-b", Replayed: &replayed}, p)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != rows[1].ID.String() {
		t.Fatalf("filtered = %+v, %v", page, err)
	}
	unreplayed := false
	for _, tc := range []struct {
		input DLQListInput
		ids   []string
	}{
		{DLQListInput{Queue: "reports"}, []string{rows[2].ID.String()}},
		{DLQListInput{NamePrefix: "report."}, []string{rows[2].ID.String()}},
		{DLQListInput{ScopeAppID: "app-a"}, []string{rows[2].ID.String(), rows[0].ID.String()}},
		{DLQListInput{ScopeOrgID: "org-b"}, []string{rows[1].ID.String()}},
		{DLQListInput{ScopeAppID: "app-a", ScopeOrgID: "org-b"}, []string{}},
		{DLQListInput{Replayed: &unreplayed}, []string{rows[3].ID.String(), rows[2].ID.String(), rows[0].ID.String()}},
	} {
		filtered, filterErr := dlqListHandler(d)(ctx, tc.input, p)
		if filterErr != nil {
			t.Fatal(filterErr)
		}
		actual := []string{}
		for _, row := range filtered.Items {
			actual = append(actual, row.ID)
		}
		if !reflect.DeepEqual(actual, tc.ids) {
			t.Fatalf("filter %+v = %v, want %v", tc.input, actual, tc.ids)
		}
	}
	counts, err := dlqCountsHandler(d)(ctx, DLQCountsInput{Queue: "mail", Replayed: &unreplayed}, p)
	if err != nil || counts.Count != 2 {
		t.Fatalf("counts = %+v, %v", counts, err)
	}
	detail, err := dlqGetHandler(d)(ctx, IDInput{ID: rows[3].ID.String()}, p)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(detail)
	if detail.ScopeAppID != nil || detail.ScopeOrgID != nil || detail.ReplayedJobID != nil || detail.LeaseTTL.MS != 3600000 ||
		!strings.Contains(string(raw), "9007199254740993") || strings.Contains(string(raw), "job_id") {
		t.Fatalf("detail = %s", raw)
	}
	preview, err := dlqPurgeHandler(d, true)(ctx, BeforeInput{Before: rows[1].FailedAt.Format(time.RFC3339Nano)}, p)
	if err != nil || preview.Count != 1 {
		t.Fatalf("strict cutoff = %+v, %v", preview, err)
	}
	for _, input := range []DLQListInput{{Limit: -1}, {Cursor: "invalid"}} {
		if _, listErr := dlqListHandler(d)(ctx, input, p); !errors.Is(listErr, fc.ErrBadRequest) {
			t.Fatalf("input=%+v: %v", input, listErr)
		}
	}
	if _, getErr := dlqGetHandler(d)(ctx, IDInput{ID: id.NewJobID().String()}, p); !errors.Is(getErr, fc.ErrBadRequest) {
		t.Fatalf("wrong kind = %v", getErr)
	}
	if _, getErr := dlqGetHandler(d)(ctx, IDInput{ID: id.NewDLQID().String()}, p); !errors.Is(getErr, fc.ErrNotFound) {
		t.Fatalf("missing = %v", getErr)
	}
	for _, before := range []string{"", "invalid", "0001-01-01T00:00:00Z"} {
		if _, purgeErr := dlqPurgeHandler(d, false)(ctx, BeforeInput{Before: before}, p); !errors.Is(purgeErr, fc.ErrBadRequest) {
			t.Fatalf("before=%q: %v", before, purgeErr)
		}
	}
	replay, err := dlqReplayHandler(d)(ctx, IDInput{ID: rows[0].ID.String()}, p)
	if err != nil || replay.Job.ID == rows[0].JobID.String() || replay.Job.State != job.StatePending || *replay.Job.ScopeAppID != "app-a" {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	stored, err := d.Store.GetDLQ(ctx, rows[0].ID)
	if err != nil || stored.ReplayedJobID == nil || stored.ReplayedJobID.String() != replay.Job.ID {
		t.Fatalf("claim = %+v, %v", stored, err)
	}
	_, err = dlqReplayHandler(d)(ctx, IDInput{ID: rows[0].ID.String()}, p)
	var ce *fc.Error
	if !errors.As(err, &ce) || ce.Code != fc.CodeConflict {
		t.Fatalf("duplicate = %v", err)
	}
	detailsJSON, _ := json.Marshal(ce.Details)
	if !strings.Contains(string(detailsJSON), `"state":"replayed"`) {
		t.Fatalf("conflict details = %s", detailsJSON)
	}
	if _, deleteErr := dlqDeleteHandler(d)(ctx, IDInput{ID: rows[1].ID.String()}, p); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if _, getErr := d.Store.GetDLQ(ctx, rows[1].ID); !errors.Is(getErr, dispatch.ErrDLQNotFound) {
		t.Fatal(getErr)
	}
	bulk, err := dlqReplayAllHandler(d)(ctx, DLQReplayAllInput{Queue: "mail", Limit: 1}, p)
	if err != nil || bulk.Replayed != 1 || bulk.Conflicts != 0 || bulk.Errors != 0 || bulk.Interrupted || bulk.Failure != nil || bulk.Limit != 1 {
		t.Fatalf("bulk = %+v, %v", bulk, err)
	}
	if _, bulkErr := dlqReplayAllHandler(d)(ctx, DLQReplayAllInput{Limit: -1}, p); !errors.Is(bulkErr, fc.ErrBadRequest) {
		t.Fatalf("negative bulk limit = %v", bulkErr)
	}
	before := BeforeInput{Before: base.Add(48 * time.Hour).Format(time.RFC3339Nano)}
	preview, err = dlqPurgeHandler(d, true)(ctx, before, p)
	if err != nil || preview.Count != 3 {
		t.Fatalf("preview = %+v, %v", preview, err)
	}
	purged, err := dlqPurgeHandler(d, false)(ctx, before, p)
	if err != nil || purged.Count != preview.Count {
		t.Fatalf("purge = %+v, %v", purged, err)
	}
	if len(recorder.actions) != 4 {
		t.Fatalf("actions = %+v", recorder.actions)
	}
	for _, action := range recorder.actions {
		if action.Actor != "operator" {
			t.Fatalf("actor = %+v", action)
		}
	}
}
func TestDLQDomainMemoryAndSQLite(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runDLQDomain(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) { runDLQDomain(t, sqliteContractStore(t)) })
}

type replayFailureStore struct {
	store.Store
	failEnqueue      bool
	interruptListing bool
	pages            int
}

func (s *replayFailureStore) EnqueueJob(ctx context.Context, j *job.Job) error {
	if s.failEnqueue {
		return errors.New("private backend password")
	}
	return s.Store.EnqueueJob(ctx, j)
}
func (s *replayFailureStore) ListDLQPage(ctx context.Context, o dlq.PageOpts) (dlq.Page, error) {
	if s.interruptListing {
		s.pages++
		if s.pages > 1 {
			return dlq.Page{}, errors.New("private backend address")
		}
		o.Limit = 1
	}
	return s.Store.ListDLQPage(ctx, o)
}
func TestDLQBulkKeepsPartialProgressAndRedactsFailures(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "entry failure", true: "later page failure"}[interrupt], func(t *testing.T) {
			s := &replayFailureStore{Store: memory.New(), failEnqueue: !interrupt, interruptListing: interrupt}
			d := contractDeps(t, s)
			logger := &errorRecorder{Logger: forge.NewNoopLogger()}
			d.Logger = logger
			seedDLQ(t, d, "one", "mail", "", "", time.Now(), false)
			seedDLQ(t, d, "two", "mail", "", "", time.Now(), false)
			result, err := dlqReplayAllHandler(d)(context.Background(), DLQReplayAllInput{}, testPrincipal())
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(result)
			if strings.Contains(string(raw), "private") || logger.calls != 1 {
				t.Fatalf("result = %s, logs=%d", raw, logger.calls)
			}
			if interrupt {
				if result.Replayed != 1 || !result.Interrupted || result.Failure == nil || result.Failure.Code != fc.CodeInternal {
					t.Fatalf("partial = %+v", result)
				}
			} else if result.Errors != 2 || result.Replayed != 0 || result.Interrupted || result.Failure != nil {
				t.Fatalf("failures = %+v", result)
			}
		})
	}
}

type incompleteDLQStore struct{ store.Store }

func (s incompleteDLQStore) ListDLQPage(ctx context.Context, _ dlq.PageOpts) (dlq.Page, error) {
	if _, ok := ctx.Deadline(); !ok {
		return dlq.Page{}, errors.New("missing deadline")
	}
	return dlq.Page{NextCursor: "next-window", Complete: false}, nil
}
func TestDLQListPreservesIncompleteSearch(t *testing.T) {
	d := contractDeps(t, incompleteDLQStore{Store: memory.New()})
	page, err := dlqListHandler(d)(context.Background(), DLQListInput{}, testPrincipal())
	if err != nil || page.Items == nil || page.Complete || page.NextCursor == nil || *page.NextCursor != "next-window" {
		t.Fatalf("page = %+v, %v", page, err)
	}
}
