package contract

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
	sqlitestore "github.com/xraph/dispatch/store/sqlite"
)

func sqliteContractStore(t *testing.T) store.Store {
	t.Helper()
	drv := sqlitedriver.New()
	if err := drv.Open(context.Background(), filepath.Join(t.TempDir(), "contract.db")); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return sqlitestore.New(db)
}

func contractDeps(t *testing.T, s store.Store, options ...engine.Option) Deps {
	t.Helper()
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(d, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })
	return Deps{Engine: eng, Store: s, Security: testBoundary()}
}

func seedJob(t *testing.T, d Deps, name string, state job.State, app, org, queue string) *job.Job {
	t.Helper()
	j, err := d.Engine.EnqueueRaw(context.Background(), name, []byte(`{"n":1}`), job.WithQueue(queue))
	if err != nil {
		t.Fatal(err)
	}
	j.State = state
	j.ScopeAppID = app
	j.ScopeOrgID = org
	if err := d.Store.UpdateJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	return j
}

func runJobDomain(t *testing.T, s store.Store) {
	t.Helper()
	d := contractDeps(t, s)
	rows := []*job.Job{
		seedJob(t, d, "mail.a", job.StatePending, "app-a", "org-a", "mail"),
		seedJob(t, d, "mail.b", job.StateFailed, "app-b", "org-b", "mail"),
		seedJob(t, d, "report.a", job.StateCompleted, "app-a", "org-a", "reports"),
		seedJob(t, d, "mail.c", job.StateRetrying, "", "", "mail"),
	}
	principal := fc.Principal{User: testPrincipal().User, Claims: map[string]any{"scope_app_id": "app-a", "scope_org_id": "org-a"}}
	list := jobsListHandler(d)
	var got []string
	cursor := ""
	for {
		page, err := list(context.Background(), JobsListInput{Limit: 2, Cursor: cursor}, principal)
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
			t.Fatal("cursor repeated rows")
		}
	}
	want := make([]string, 0, len(rows))
	for _, j := range rows {
		want = append(want, j.ID.String())
	}
	slices.Sort(want)
	slices.Reverse(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("operator-wide identity/order = %v, want %v", got, want)
	}
	filtered, err := list(context.Background(), JobsListInput{States: []job.State{job.StateFailed}, Queue: "mail", NamePrefix: "mail.", ScopeAppID: "app-b", ScopeOrgID: "org-b"}, principal)
	if err != nil || len(filtered.Items) != 1 || filtered.Items[0].ID != rows[1].ID.String() {
		t.Fatalf("filtered = %+v, %v", filtered, err)
	}
	scoped, err := list(context.Background(), JobsListInput{ScopeAppID: "app-a"}, principal)
	if err != nil || len(scoped.Items) != 2 {
		t.Fatalf("app filter = %+v, %v", scoped, err)
	}
	gotScoped := make([]string, 0, len(scoped.Items))
	for _, row := range scoped.Items {
		gotScoped = append(gotScoped, row.ID)
	}
	wantScoped := []string{rows[0].ID.String(), rows[2].ID.String()}
	slices.Sort(wantScoped)
	slices.Reverse(wantScoped)
	if !reflect.DeepEqual(gotScoped, wantScoped) {
		t.Fatalf("app filter identities = %v, want %v", gotScoped, wantScoped)
	}
	counts, err := jobsCountsHandler(d)(context.Background(), QueueInput{Queue: "mail"}, principal)
	if err != nil || counts.Total != 3 || counts.Counts[job.StateFailed] != 1 || counts.Counts[job.StateCompleted] != 0 {
		t.Fatalf("counts = %+v, %v", counts, err)
	}
	for _, input := range []JobsListInput{{Limit: -1}, {States: []job.State{"unknown"}}, {Cursor: "invalid"}} {
		if _, listErr := list(context.Background(), input, principal); !errors.Is(listErr, fc.ErrBadRequest) {
			t.Fatalf("bad input %+v: %v", input, listErr)
		}
	}
	detail, err := jobsGetHandler(d)(context.Background(), IDInput{ID: rows[3].ID.String()}, principal)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	if detail.ScopeAppID != nil || detail.ScopeOrgID != nil || detail.WorkerID != nil || detail.Artifacts.Enabled ||
		detail.Artifacts.Links == nil || !strings.Contains(string(data), `"scopeAppId":null`) || strings.Contains(string(data), "scope_app_id") {
		t.Fatalf("detail = %s", data)
	}
	if _, err := jobsGetHandler(d)(context.Background(), IDInput{ID: id.NewJobID().String()}, principal); !errors.Is(err, fc.ErrNotFound) {
		t.Fatalf("missing = %v", err)
	}
	if _, err := jobsGetHandler(d)(context.Background(), IDInput{ID: id.NewRunID().String()}, principal); !errors.Is(err, fc.ErrBadRequest) {
		t.Fatalf("wrong ID kind = %v", err)
	}
}

func TestJobDomainMemoryAndSQLite(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runJobDomain(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) { runJobDomain(t, sqliteContractStore(t)) })
}

type actionRecorder struct{ actions []ext.Action }

func (*actionRecorder) Name() string { return "contract-actions" }
func (r *actionRecorder) OnOperatorAction(_ context.Context, a ext.Action) error {
	r.actions = append(r.actions, a)
	return nil
}

func TestJobActionsUseEngineAndReturnCurrentState(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.Store{"memory": func(*testing.T) store.Store { return memory.New() }, "sqlite": sqliteContractStore} {
		t.Run(name, func(t *testing.T) {
			recorder := &actionRecorder{}
			d := contractDeps(t, factory(t), engine.WithExtension(recorder))
			principal := fc.Principal{User: &dashauth.UserInfo{Subject: "operator"}}
			pending := seedJob(t, d, "cancel", job.StatePending, "", "", "mail")
			cancel := jobActionHandler(d, "jobs.cancel", d.Engine.CancelJob)
			result, err := cancel(context.Background(), IDInput{ID: pending.ID.String()}, principal)
			if err != nil || result.Job.State != job.StateCancelled {
				t.Fatalf("cancel = %+v, %v", result, err)
			}
			_, err = cancel(context.Background(), IDInput{ID: pending.ID.String()}, principal)
			var ce *fc.Error
			if !errors.As(err, &ce) || ce.Code != fc.CodeConflict {
				t.Fatalf("second cancel = %v", err)
			}
			data, _ := json.Marshal(ce.Details)
			if !strings.Contains(string(data), `"state":"cancelled"`) {
				t.Fatalf("conflict details = %s", data)
			}
			failed := seedJob(t, d, "retry", job.StateFailed, "app-a", "org-a", "mail")
			if pushErr := d.Engine.DLQService().Push(context.Background(), failed, errors.New("job failed")); pushErr != nil {
				t.Fatal(pushErr)
			}
			detail, err := jobsGetHandler(d)(context.Background(), IDInput{ID: failed.ID.String()}, principal)
			if err != nil || detail.DLQEntryID == nil {
				t.Fatalf("missing DLQ link: %+v, %v", detail, err)
			}
			retry := jobActionHandler(d, "jobs.retry", d.Engine.RetryJob)
			if _, retryErr := retry(context.Background(), IDInput{ID: failed.ID.String()}, principal); retryErr != nil {
				t.Fatal(retryErr)
			}
			entry, err := d.Store.GetDLQByJobID(context.Background(), failed.ID)
			if err != nil || entry.ReplayedAt == nil {
				t.Fatalf("retry did not claim DLQ: %+v, %v", entry, err)
			}
			if _, err := retry(context.Background(), IDInput{ID: failed.ID.String()}, principal); !errors.Is(err, fc.ErrConflict) {
				t.Fatalf("duplicate retry = %v", err)
			}
			if len(recorder.actions) != 2 {
				t.Fatalf("actions = %v", recorder.actions)
			}
			for _, action := range recorder.actions {
				if action.Actor != "operator" {
					t.Fatalf("actor = %q", action.Actor)
				}
			}
		})
	}
}

type incompleteJobsStore struct{ store.Store }

func (s incompleteJobsStore) ListJobs(ctx context.Context, _ job.ListJobsOpts) (job.Page, error) {
	if _, ok := ctx.Deadline(); !ok {
		return job.Page{}, errors.New("unbounded list")
	}
	return job.Page{Jobs: []*job.Job{}, NextCursor: id.NewJobID().String(), Complete: false}, nil
}
func TestJobListPreservesIncompleteSearch(t *testing.T) {
	d := contractDeps(t, memory.New())
	d.Store = incompleteJobsStore{d.Store}
	page, err := jobsListHandler(d)(context.Background(), JobsListInput{}, testPrincipal())
	if err != nil || page.Complete || page.NextCursor == nil || page.Items == nil {
		t.Fatalf("incomplete page = %+v, %v", page, err)
	}
}
