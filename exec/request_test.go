package exec_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
)

func validRequest() *exec.Request {
	return &exec.Request{
		JobID:    id.NewJobID(),
		Name:     "tessellate.model",
		Payload:  []byte(`{"detail":3}`),
		Attempt:  0,
		Deadline: time.Now().Add(time.Hour),
	}
}

func TestRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*exec.Request)
		wantErr error
	}{
		{
			name:   "valid",
			mutate: func(*exec.Request) {},
		},
		{
			name:    "missing name",
			mutate:  func(r *exec.Request) { r.Name = "" },
			wantErr: exec.ErrInvalidRequest,
		},
		{
			name:    "negative attempt",
			mutate:  func(r *exec.Request) { r.Attempt = -1 },
			wantErr: exec.ErrInvalidRequest,
		},
		{
			name:   "zero deadline is allowed",
			mutate: func(r *exec.Request) { r.Deadline = time.Time{} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
			tt.mutate(req)

			err := req.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRequest_InputPathLookup(t *testing.T) {
	req := validRequest()
	req.Inputs = []exec.InputSlot{{Name: "model", Path: "model/scene.ifc"}}

	if got := req.InputPath("model"); got != "model/scene.ifc" {
		t.Errorf("InputPath(model) = %q, want %q", got, "model/scene.ifc")
	}
	if got := req.InputPath("absent"); got != "" {
		t.Errorf("InputPath(absent) = %q, want empty", got)
	}
}
