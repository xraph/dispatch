package contract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"
	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/paging"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

func TestDependenciesRequireEngineAndStore(t *testing.T) {
	if (Deps{}).validate() == nil {
		t.Fatal("accepted missing engine")
	}
	d, err := dispatch.New(dispatch.WithStore(memory.New()))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if (Deps{Engine: eng}).validate() == nil {
		t.Fatal("accepted missing store")
	}
	if err := (Deps{Engine: eng, Store: memory.New()}).validate(); err != nil {
		t.Fatal(err)
	}
}

func TestWirePayloadsPreserveJSONAndDistinguishGob(t *testing.T) {
	source := []byte(`{"id":9007199254740993}`)
	payload := projectPayload(source, false)
	source[0] = '['
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"kind":"json","json":{"id":9007199254740993},"jsonText":"{\"id\":9007199254740993}"}` {
		t.Fatalf("payload = %s", encoded)
	}
	for _, checkpoint := range []bool{false, true} {
		p := projectPayload([]byte{0, 255}, checkpoint)
		want := "binary"
		if checkpoint {
			want = "gob"
		}
		if p.Kind != want || p.Bytes == nil || *p.Bytes != 2 || p.JSON != nil {
			t.Fatalf("opaque = %+v", p)
		}
	}
	empty, err := json.Marshal(projectPayload(nil, false))
	if err != nil || string(empty) != `{"kind":"binary","bytes":0}` {
		t.Fatalf("empty = %s, %v", empty, err)
	}
}

func TestWireNullsUTCAndCursorCoverage(t *testing.T) {
	if nullable("") != nil || timestamp(time.Time{}) != nil || timestampPtr(nil) != nil {
		t.Fatal("absent values are not null")
	}
	local := time.Date(2026, 10, 8, 9, 0, 0, 0, time.FixedZone("offset", -5*3600))
	if got := *timestamp(local); got != "2026-10-08T14:00:00Z" {
		t.Fatal(got)
	}
	d := duration(90 * time.Second)
	if d.Text != "1m30s" || d.MS != 90000 {
		t.Fatalf("duration = %+v", d)
	}
	page := newPage[string](nil, "next", true, local)
	if page.Items == nil || page.NextCursor == nil || !page.Complete {
		t.Fatalf("page = %+v", page)
	}
	// Complete is search coverage. It does not consume a nonempty cursor.
	if *page.NextCursor != "next" {
		t.Fatal("lost next cursor")
	}
	for limit, want := range map[int]int{0: 50, 1: 1, 200: 200, 201: 200} {
		got, err := pageLimit(limit)
		if err != nil || got != want {
			t.Fatalf("limit %d = %d, %v", limit, got, err)
		}
	}
	if _, err := pageLimit(-1); !errors.Is(err, fc.ErrBadRequest) {
		t.Fatalf("negative limit = %v", err)
	}
}

type errorRecorder struct {
	forge.Logger
	calls int
}

func (l *errorRecorder) Error(string, ...forge.Field) { l.calls++ }

func TestErrorMappingRedactsInternalAndLogsIntent(t *testing.T) {
	conflict := stateConflict("completed")
	encoded, marshalErr := json.Marshal(conflict)
	if marshalErr != nil || !strings.Contains(string(encoded), `"details":{"state":"completed"}`) || !errors.Is(conflict, fc.ErrConflict) {
		t.Fatalf("state conflict = %s, %v", encoded, marshalErr)
	}
	logger := &errorRecorder{Logger: forge.NewNoopLogger()}
	deps := Deps{Logger: logger}
	internal := deps.mapError("jobs.list", errors.New("secret-database-password"))
	if !errors.Is(internal, fc.ErrInternal) || strings.Contains(internal.Error(), "secret") || logger.calls != 1 {
		t.Fatalf("internal = %v, logs=%d", internal, logger.calls)
	}
	cases := []struct{ err, want error }{
		{dispatch.ErrJobNotFound, fc.ErrNotFound}, {dispatch.ErrRunNotFound, fc.ErrNotFound},
		{dispatch.ErrDLQNotFound, fc.ErrNotFound}, {dispatch.ErrCronNotFound, fc.ErrNotFound},
		{dispatch.ErrWorkerNotFound, fc.ErrNotFound}, {paging.ErrInvalidCursor, fc.ErrBadRequest},
		{dispatch.ErrInvalidState, fc.ErrConflict}, {dispatch.ErrDLQAlreadyReplayed, fc.ErrConflict},
		{resource.ErrUnschedulable, fc.ErrConflict}, {workflow.ErrRunnerShutdown, fc.ErrUnavailable},
		{context.Canceled, fc.ErrUnavailable}, {context.DeadlineExceeded, fc.ErrUnavailable},
	}
	for _, tc := range cases {
		if got := deps.mapError("test", fmt.Errorf("driver detail: %w", tc.err)); !errors.Is(got, tc.want) || strings.Contains(got.Error(), "driver detail") {
			t.Fatalf("%v mapped to %v", tc.err, got)
		}
	}
	if logger.calls != 1 {
		t.Fatalf("expected refusals logged as internal: %d", logger.calls)
	}
}

func TestHandleBoundsRequestsPreservesEarlierDeadlineAndSetsActor(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := parent.Deadline()
	principal := fc.Principal{User: &dashauth.UserInfo{Subject: "operator"}, Claims: map[string]any{"sub": "not-the-actor", "scope_app_id": "not-a-filter"}}
	fn := handle(Deps{}, "test", true, func(ctx context.Context, _ struct{}, _ fc.Principal) (string, error) {
		got, ok := ctx.Deadline()
		if !ok || !got.Equal(deadline) {
			t.Fatalf("deadline = %v", got)
		}
		return ext.ActorFrom(ctx), nil
	})
	got, err := fn(parent, struct{}{}, principal)
	if err != nil || got != "operator" {
		t.Fatalf("actor = %q, %v", got, err)
	}
	query := handle(Deps{}, "query", false, func(ctx context.Context, _ struct{}, _ fc.Principal) (bool, error) {
		until, ok := ctx.Deadline()
		return ok && time.Until(until) <= queryTimeout, nil
	})
	if bounded, err := query(context.Background(), struct{}{}, fc.Principal{}); err != nil || !bounded {
		t.Fatalf("unbounded query: %v", err)
	}
}
