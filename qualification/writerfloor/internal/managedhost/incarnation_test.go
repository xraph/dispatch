package managedhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xraph/ctrlplane/managed"
)

func TestRestoreIncarnationSurvivesReopenAndSerializesRotation(t *testing.T) {
	directory, pathErr := filepath.EvalSymlinks(t.TempDir())
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	state, err := OpenRestoreIncarnation(directory, "authority")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.ReadIncarnation(t.Context(), "authority"); !errors.Is(err, managed.ErrUnavailable) {
		t.Fatal("missing state admitted", err)
	}
	first, err := state.Rotate(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRestoreIncarnation(directory, "authority")
	if err != nil {
		t.Fatal(err)
	}
	if current, readErr := reopened.ReadIncarnation(t.Context(), "authority"); readErr != nil || current != first {
		t.Fatal("reopen changed identity", readErr)
	}
	var group sync.WaitGroup
	outcomes := make(chan error, 2)
	for _, journal := range []*RestoreIncarnation{state, reopened} {
		group.Go(func() { _, rotateErr := journal.Rotate(t.Context(), first); outcomes <- rotateErr })
	}
	group.Wait()
	close(outcomes)
	successes, conflicts := 0, 0
	for outcome := range outcomes {
		if outcome == nil {
			successes++
		} else if errors.Is(outcome, managed.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(outcome)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatal("rotation did not serialize", successes, conflicts)
	}
	current, err := state.ReadIncarnation(t.Context(), "authority")
	if err != nil || current == first {
		t.Fatal("restore retained old identity", err)
	}
	if _, err = reopened.Rotate(t.Context(), ""); !errors.Is(err, managed.ErrConflict) {
		t.Fatal("reinitialization admitted", err)
	}
	if _, err = state.ReadIncarnation(t.Context(), "another"); !errors.Is(err, managed.ErrAuthority) {
		t.Fatal("authority substitution admitted", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = state.Rotate(ctx, current); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled rotation accepted", err)
	}
}
