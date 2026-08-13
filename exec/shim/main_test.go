package shim_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/shim"
	"github.com/xraph/dispatch/exec/wire"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

type shimPayload struct {
	Mode string `json:"mode"`
}

func shimHandlers(t *testing.T) []job.Registrable {
	t.Helper()

	return []job.Registrable{
		job.NewDefinition("shim.ok", func(context.Context, shimPayload) error { return nil }),
		job.NewDefinition("shim.err", func(context.Context, shimPayload) error {
			return errors.New("handler said no")
		}),
		job.NewDefinition("shim.permanent", func(context.Context, shimPayload) error {
			return dispatch.ErrPermanent
		}),
	}
}

func runShim(t *testing.T, req *exec.Request, defs []job.Registrable) *exec.Result {
	t.Helper()

	var in, out bytes.Buffer
	if err := wire.Encode(&in, &wire.Frame{Kind: wire.KindRequest, Request: req}); err != nil {
		t.Fatalf("Encode() = %v", err)
	}
	if err := shim.Run(context.Background(), &in, &out, defs); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	f, err := wire.Decode(&out)
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}

	return f.Result
}

func req(t *testing.T, name string) *exec.Request {
	t.Helper()
	raw, _ := json.Marshal(shimPayload{Mode: "x"})

	return &exec.Request{
		JobID:     id.NewJobID(),
		Name:      name,
		Payload:   raw,
		OutputDir: t.TempDir(),
	}
}

func TestRun(t *testing.T) {
	defs := shimHandlers(t)

	tests := []struct {
		name       string
		job        string
		wantStatus exec.Status
		wantPerm   bool
	}{
		{"success", "shim.ok", exec.StatusOK, false},
		{"handler error", "shim.err", exec.StatusHandlerError, false},
		{"permanent crosses as a flag", "shim.permanent", exec.StatusHandlerError, true},
		{"unknown handler is a launch failure", "shim.absent", exec.StatusLaunchFailed, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runShim(t, req(t, tt.job), defs)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (err %q)", got.Status, tt.wantStatus, got.HandlerErr)
			}
			if got.Permanent != tt.wantPerm {
				t.Errorf("Permanent = %v, want %v", got.Permanent, tt.wantPerm)
			}
			if got.Cause != nil {
				t.Error("Cause must be nil across the boundary")
			}
		})
	}
}

func TestRunFingerprintMismatch(t *testing.T) {
	r := req(t, "shim.ok")
	r.Fingerprint = "not-the-right-fingerprint"

	got := runShim(t, r, shimHandlers(t))
	if got.Status != exec.StatusLaunchFailed {
		t.Fatalf("Status = %q, want %q", got.Status, exec.StatusLaunchFailed)
	}
}

func TestRunMatchingFingerprintPasses(t *testing.T) {
	defs := shimHandlers(t)
	names := make([]string, 0, len(defs))
	for _, d := range defs {
		names = append(names, d.JobName())
	}

	r := req(t, "shim.ok")
	r.Fingerprint = exec.Fingerprint(names)

	if got := runShim(t, r, defs); got.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want %q", got.Status, exec.StatusOK)
	}
}

func TestRunRecordsOutputs(t *testing.T) {
	dir := t.TempDir()
	defs := []job.Registrable{
		job.NewDefinition("shim.writes", func(ctx context.Context, _ shimPayload) error {
			w, err := artifact.From(ctx).Create(ctx, "mesh.glb")
			if err != nil {
				return err
			}
			if _, werr := w.Write([]byte("meshbytes")); werr != nil {
				return werr
			}
			_, err = w.Commit(ctx)

			return err
		}),
	}

	r := req(t, "shim.writes")
	r.OutputDir = dir

	got := runShim(t, r, defs)
	if got.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want ok (err %q)", got.Status, got.HandlerErr)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].Name != "mesh.glb" {
		t.Fatalf("Outputs = %+v, want one named mesh.glb", got.Outputs)
	}
	if got.Outputs[0].Size != int64(len("meshbytes")) {
		t.Errorf("Size = %d, want %d", got.Outputs[0].Size, len("meshbytes"))
	}
}

// TestRunSeedsPriorOutputsForExisting proves that a PriorOutput on the
// request makes Accessor.Existing answer true for that name — the whole
// point of seeding — while a name that was never seeded still answers
// false, so the seed is not mistaken for a wildcard "everything exists".
func TestRunSeedsPriorOutputsForExisting(t *testing.T) {
	defs := []job.Registrable{
		job.NewDefinition("shim.checks_existing", func(ctx context.Context, _ shimPayload) error {
			if _, ok := artifact.From(ctx).Existing(ctx, "mesh.glb"); !ok {
				return errors.New("expected mesh.glb to resolve as an existing prior output")
			}
			if _, ok := artifact.From(ctx).Existing(ctx, "unseeded.glb"); ok {
				return errors.New("unseeded.glb must not resolve; nothing seeded it")
			}

			return nil
		}),
	}

	r := req(t, "shim.checks_existing")
	r.PriorOutputs = []exec.PriorOutput{
		{
			Name: "mesh.glb",
			Ref: artifact.Ref{
				ID:      id.NewArtifactID(),
				Backend: "localfs",
				Bucket:  "shim",
				Key:     "ephemeral/job/prior-attempt/0/mesh.glb",
				Size:    9,
			},
		},
	}

	got := runShim(t, r, defs)
	if got.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want ok (err %q)", got.Status, got.HandlerErr)
	}
}

// TestRunResolvesDeclaredInput proves the accessor's Path and Open work
// against a declared, staged input, and that Ref reports the documented
// Phase 2 limitation (no ref travels with an out-of-process InputSlot)
// rather than panicking or lying.
func TestRunResolvesDeclaredInput(t *testing.T) {
	inputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "in.txt"), []byte("input bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}

	defs := []job.Registrable{
		job.NewDefinition("shim.reads_input", func(ctx context.Context, _ shimPayload) error {
			acc := artifact.From(ctx)

			if got := acc.Path("in.txt"); got != filepath.Join(inputDir, "in.txt") {
				return errors.New("Path() did not resolve to the staged file")
			}
			if got := acc.Path("undeclared.txt"); got != "" {
				return errors.New("Path() of an undeclared input must be empty")
			}

			rc, err := acc.Open(ctx, "in.txt")
			if err != nil {
				return err
			}
			defer rc.Close()

			buf := make([]byte, len("input bytes"))
			if _, err := rc.Read(buf); err != nil {
				return err
			}
			if string(buf) != "input bytes" {
				return errors.New("Open() did not read the staged file's bytes")
			}

			if _, ok := acc.Ref("in.txt"); ok {
				return errors.New("Ref() must report false: Phase 2 does not stage a ref for InputSlot")
			}

			if _, err := acc.Open(ctx, "undeclared.txt"); !errors.Is(err, artifact.ErrUnbound) {
				return errors.New("Open() of an undeclared input must wrap artifact.ErrUnbound")
			}

			return nil
		}),
	}

	r := req(t, "shim.reads_input")
	r.InputDir = inputDir
	r.Inputs = []exec.InputSlot{{Name: "in.txt", Path: "in.txt"}}

	got := runShim(t, r, defs)
	if got.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want ok (err %q)", got.Status, got.HandlerErr)
	}
}
