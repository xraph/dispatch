package trove

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	trovelib "github.com/xraph/trove"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
)

// TestTranslate covers the mapping from Trove's classification onto the
// artifact plane's. What is *not* translated matters as much as what is: an
// error left alone is retried, so translating a transient condition would
// dead-letter work that would have succeeded, and failing to translate a
// permanent one burns the whole retry budget.
func TestTranslate(t *testing.T) {
	tests := []struct {
		name string
		err  error

		wantPermanent bool
		wantSentinel  error // nil means "returned unchanged"
	}{
		{
			name:          "object not found",
			err:           fmt.Errorf(`memdriver: object "in.bin" not found in bucket "art": %w`, trovelib.ErrObjectNotFound),
			wantPermanent: true,
			wantSentinel:  artifact.ErrNotFound,
		},
		{
			name:          "bucket not found",
			err:           fmt.Errorf(`memdriver: bucket "art" not found: %w`, trovelib.ErrBucketNotFound),
			wantPermanent: true,
			wantSentinel:  artifact.ErrNotFound,
		},
		{
			name:          "general not found",
			err:           fmt.Errorf("cas: hash not found: %w", trovelib.ErrNotFound),
			wantPermanent: true,
			wantSentinel:  artifact.ErrNotFound,
		},
		{
			name:          "permission denied",
			err:           fmt.Errorf(`s3driver: permission denied for object "in.bin": %w`, trovelib.ErrPermissionDenied),
			wantPermanent: true,
			wantSentinel:  artifact.ErrPermissionDenied,
		},

		// A quota may be granted later, so the job should back off rather
		// than die. This is the case a blunt "permanent unless recognized"
		// rule would get wrong.
		{
			name:          "quota exceeded is retryable",
			err:           fmt.Errorf("s3driver: quota exceeded: %w", trovelib.ErrQuotaExceeded),
			wantPermanent: false,
		},
		{
			name:          "unclassified is retryable",
			err:           errors.New("dial tcp 10.0.0.1:443: connection refused"),
			wantPermanent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translate(tt.err)

			if errors.Is(got, dispatch.ErrPermanent) != tt.wantPermanent {
				t.Fatalf("translate(%v) permanent = %v, want %v",
					tt.err, !tt.wantPermanent, tt.wantPermanent)
			}

			if tt.wantSentinel == nil {
				if !errors.Is(got, tt.err) {
					t.Fatalf("translate returned %v, want the original error unchanged", got)
				}

				return
			}

			if !errors.Is(got, tt.wantSentinel) {
				t.Fatalf("translate(%v) = %v, want %v", tt.err, got, tt.wantSentinel)
			}
		})
	}
}

func TestTranslateNil(t *testing.T) {
	if got := translate(nil); got != nil {
		t.Fatalf("translate(nil) = %v, want nil", got)
	}
}

// TestTranslatePreservesDriverMessage covers what the substring fallback used
// to do and the sentinel branch did not: keep what the driver said. A DLQ
// entry reading only "not found" cannot tell an operator which object, on
// which driver, went missing.
func TestTranslatePreservesDriverMessage(t *testing.T) {
	driverErr := fmt.Errorf(
		`memdriver: object "input.bin" not found in bucket "artifacts": %w`,
		trovelib.ErrObjectNotFound)

	got := translate(driverErr)

	if !strings.Contains(got.Error(), `object "input.bin" not found in bucket "artifacts"`) {
		t.Fatalf("translate dropped the driver message: %q", got.Error())
	}

	// The Trove sentinel stays reachable underneath, so a diagnostic can
	// still ask which resource was missing.
	if !errors.Is(got, trovelib.ErrObjectNotFound) {
		t.Fatalf("translate broke the chain to the Trove sentinel: %v", got)
	}
}
