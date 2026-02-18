package job_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"

	"github.com/xraph/dispatch/job"
)

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := job.NewRegistry()

	var got emailPayload
	def := job.NewDefinition("send-email", func(_ context.Context, p emailPayload) error {
		got = p
		return nil
	})

	job.RegisterDefinition(r, def)

	h, ok := r.Get("send-email")
	if !ok {
		t.Fatal("expected handler to be registered")
	}

	payload, _ := json.Marshal(emailPayload{To: "alice@example.com", Subject: "Hello"})
	err := h(context.Background(), payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.To != "alice@example.com" {
		t.Errorf("To = %q, want %q", got.To, "alice@example.com")
	}
	if got.Subject != "Hello" {
		t.Errorf("Subject = %q, want %q", got.Subject, "Hello")
	}
}

func TestRegistry_GetUnknown(t *testing.T) {
	r := job.NewRegistry()
	_, ok := r.Get("nonexistent")
	if ok {
		t.Fatal("expected no handler for unregistered job")
	}
}

func TestRegistry_Names(t *testing.T) {
	r := job.NewRegistry()

	job.RegisterDefinition(r, job.NewDefinition("job-a", func(_ context.Context, _ struct{}) error { return nil }))
	job.RegisterDefinition(r, job.NewDefinition("job-b", func(_ context.Context, _ struct{}) error { return nil }))
	job.RegisterDefinition(r, job.NewDefinition("job-c", func(_ context.Context, _ struct{}) error { return nil }))

	names := r.Names()
	sort.Strings(names)
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}
	expected := []string{"job-a", "job-b", "job-c"}
	for i, want := range expected {
		if names[i] != want {
			t.Errorf("names[%d] = %q, want %q", i, names[i], want)
		}
	}
}

func TestRegistry_InvalidJSON(t *testing.T) {
	r := job.NewRegistry()
	job.RegisterDefinition(r, job.NewDefinition("typed-job", func(_ context.Context, _ emailPayload) error {
		t.Fatal("handler should not be called with invalid JSON")
		return nil
	}))

	h, _ := r.Get("typed-job")
	err := h(context.Background(), []byte(`{invalid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestRegistry_EmptyPayload(t *testing.T) {
	r := job.NewRegistry()
	called := false
	job.RegisterDefinition(r, job.NewDefinition("no-payload", func(_ context.Context, _ struct{}) error {
		called = true
		return nil
	}))

	h, _ := r.Get("no-payload")
	err := h(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler not called with empty payload")
	}
}

func TestRegistry_HandlerError(t *testing.T) {
	r := job.NewRegistry()
	want := errors.New("handler failed")
	job.RegisterDefinition(r, job.NewDefinition("failing", func(_ context.Context, _ struct{}) error {
		return want
	}))

	h, _ := r.Get("failing")
	err := h(context.Background(), nil)
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestRegistry_OverwriteHandler(t *testing.T) {
	r := job.NewRegistry()

	job.RegisterDefinition(r, job.NewDefinition("overwrite", func(_ context.Context, _ struct{}) error {
		return errors.New("old")
	}))
	job.RegisterDefinition(r, job.NewDefinition("overwrite", func(_ context.Context, _ struct{}) error {
		return errors.New("new")
	}))

	h, _ := r.Get("overwrite")
	err := h(context.Background(), nil)
	if err == nil || err.Error() != "new" {
		t.Fatalf("expected 'new' error, got %v", err)
	}
}
