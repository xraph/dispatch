package artifact_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
)

// TestPermanentCategory pins the relationships the retry path depends on.
// Code deciding whether to retry matches ErrPermanent; code deciding what to
// report matches the specific sentinel. Both must keep working.
func TestPermanentCategory(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"not found matches itself", artifact.ErrNotFound, artifact.ErrNotFound, true},
		{"not found is permanent", artifact.ErrNotFound, dispatch.ErrPermanent, true},
		{"permission denied matches itself", artifact.ErrPermissionDenied, artifact.ErrPermissionDenied, true},
		{"permission denied is permanent", artifact.ErrPermissionDenied, dispatch.ErrPermanent, true},

		// The two permanent conditions are distinct causes.
		{"not found is not permission denied", artifact.ErrNotFound, artifact.ErrPermissionDenied, false},
		{"permission denied is not not-found", artifact.ErrPermissionDenied, artifact.ErrNotFound, false},

		// The category does not imply any particular cause.
		{"permanent is not not-found", dispatch.ErrPermanent, artifact.ErrNotFound, false},

		// ErrExists is control flow for IfAbsent, not a permanent failure.
		{"exists is not permanent", artifact.ErrExists, dispatch.ErrPermanent, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errors.Is(tt.err, tt.target); got != tt.want {
				t.Fatalf("errors.Is(%v, %v) = %v, want %v", tt.err, tt.target, got, tt.want)
			}
		})
	}
}

// TestPermanentSurvivesWrapping is the property the call sites rely on: the
// classification has to survive the layers of context added between the
// backend and the executor.
func TestPermanentSurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("stage input %q: %w",
		"model", fmt.Errorf("stage models/gone.ifc: %w", artifact.ErrNotFound))

	if !errors.Is(err, dispatch.ErrPermanent) {
		t.Fatalf("wrapped ErrNotFound is not permanent: %v", err)
	}

	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("wrapped ErrNotFound lost its specific sentinel: %v", err)
	}
}

// TestErrNotFoundMessageUnchanged guards the rendered text, since ErrNotFound
// moved from errors.New to a category type and anything logging or comparing
// the message should not notice.
func TestErrNotFoundMessageUnchanged(t *testing.T) {
	if got := artifact.ErrNotFound.Error(); got != "dispatch/artifact: not found" {
		t.Fatalf("ErrNotFound.Error() = %q, want %q", got, "dispatch/artifact: not found")
	}
}
