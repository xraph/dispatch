package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// fixture is an engine on a memory store with the REST api mounted on a
// fresh forge router, the way API.Handler mounts it in production.
type fixture struct {
	eng *engine.Engine
	s   *memory.Store
	h   http.Handler
}

func newFixture(t *testing.T, opts ...engine.Option) *fixture {
	t.Helper()

	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d, opts...)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })

	return &fixture{eng: eng, s: s, h: api.New(eng, nil).Handler()}
}

// do sends one request through the router. A non-empty body goes as JSON.
func (f *fixture) do(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, target, r)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)

	return rec
}

// wantStatus stops the test when rec answered anything but want.
func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()

	if rec.Code != want {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, want, rec.Body.String())
	}
}

// decode reads the whole body as one T. It uses json.Unmarshal rather
// than a Decoder on purpose: a body written twice fails here instead of
// passing on its first half.
func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()

	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %T: %v; body %s", v, err, rec.Body.String())
	}

	return v
}

// jobInState enqueues a job and moves it straight to state, without a
// worker.
func jobInState(t *testing.T, f *fixture, state job.State, opts ...job.Option) *job.Job {
	t.Helper()

	j, err := f.eng.EnqueueRaw(context.Background(), "api-job", []byte(`{"n":1}`), opts...)
	if err != nil {
		t.Fatalf("EnqueueRaw: %v", err)
	}
	if state == job.StatePending {
		return j
	}

	j.State = state
	if err := f.s.UpdateJob(context.Background(), j); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	return j
}

// failedWithEntry puts a job in failed and gives it a dead letter entry,
// the way the runner leaves a job that ran out of retries.
func failedWithEntry(t *testing.T, f *fixture, opts ...job.Option) (*job.Job, *dlq.Entry) {
	t.Helper()
	ctx := context.Background()

	j := jobInState(t, f, job.StateFailed, opts...)
	if err := f.eng.DLQService().Push(ctx, j, errors.New("boom")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	entry, err := f.s.GetDLQByJobID(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}

	return j, entry
}

func storedJob(t *testing.T, f *fixture, jobID id.JobID) *job.Job {
	t.Helper()

	j, err := f.s.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	return j
}
