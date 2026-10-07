package paging_test

import (
	"errors"
	"testing"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/paging"
)

func TestLimit(t *testing.T) {
	for in, want := range map[int]int{-3: paging.DefaultLimit, 0: paging.DefaultLimit, 1: 1, 500: 500} {
		if got := paging.Limit(in); got != want {
			t.Errorf("Limit(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestCursor(t *testing.T) {
	jobID := id.NewJobID()

	got, err := paging.Cursor("", id.PrefixJob)
	if err != nil || !got.IsNil() {
		t.Fatalf("empty cursor = (%v, %v), want (nil ID, nil)", got, err)
	}

	got, err = paging.Cursor(jobID.String(), id.PrefixJob)
	if err != nil || got.String() != jobID.String() {
		t.Fatalf("job cursor = (%v, %v), want (%s, nil)", got, err, jobID)
	}

	for _, raw := range []string{"abc", id.NewRunID().String()} {
		if _, err := paging.Cursor(raw, id.PrefixJob); !errors.Is(err, paging.ErrInvalidCursor) {
			t.Errorf("Cursor(%q, job) error = %v, want ErrInvalidCursor", raw, err)
		}
	}
}
