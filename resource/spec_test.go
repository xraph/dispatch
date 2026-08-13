package resource_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xraph/dispatch/resource"
)

// stubEstimator returns a fixed Set, or an error when set.
type stubEstimator struct {
	out resource.Set
	err error
}

func (s stubEstimator) Estimate(context.Context, resource.Request) (resource.Set, error) {
	return s.out, s.err
}

func TestResolvePrecedence(t *testing.T) {
	tests := []struct {
		name string
		in   resource.ResolveInput
		want resource.Set
	}{
		{
			name: "nothing declared resolves to zero",
			in:   resource.ResolveInput{},
			want: resource.Set{},
		},
		{
			name: "queue default overlays global default per key",
			in: resource.ResolveInput{
				GlobalDefault: resource.Set{resource.CPU: 1000, resource.Memory: 1 << 30},
				QueueDefault:  resource.Set{resource.Memory: 4 << 30},
			},
			want: resource.Set{resource.CPU: 1000, resource.Memory: 4 << 30},
		},
		{
			name: "declaration beats queue default",
			in: resource.ResolveInput{
				QueueDefault: resource.Set{resource.Memory: 4 << 30},
				Declared:     resource.Set{resource.Memory: 16 << 30},
			},
			want: resource.Set{resource.Memory: 16 << 30},
		},
		{
			name: "estimator beats declaration but only on keys it returns",
			in: resource.ResolveInput{
				Declared:  resource.Set{resource.CPU: 4000, resource.Memory: 16 << 30},
				Estimator: stubEstimator{out: resource.Set{resource.Memory: 6 << 30}},
			},
			want: resource.Set{resource.CPU: 4000, resource.Memory: 6 << 30},
		},
		{
			name: "enqueue override beats the estimator",
			in: resource.ResolveInput{
				Declared:  resource.Set{resource.Memory: 16 << 30},
				Estimator: stubEstimator{out: resource.Set{resource.Memory: 6 << 30}},
				Override:  resource.Set{resource.Memory: 48 << 30},
			},
			want: resource.Set{resource.Memory: 48 << 30},
		},
		{
			name: "resource func beats declaration and is fed InputBytes",
			in: resource.ResolveInput{
				Declared: resource.Set{resource.CPU: 4000},
				Request:  resource.Request{InputBytes: 2 << 30},
				Func: func(_ context.Context, r resource.Request) (resource.Set, error) {
					return resource.MemoryBytes(r.InputBytes * 3), nil
				},
			},
			want: resource.Set{resource.CPU: 4000, resource.Memory: 6 << 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resource.Resolve(context.Background(), tt.in)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if len(got.Requests) != len(tt.want) {
				t.Fatalf("got %v, want %v", got.Requests, tt.want)
			}
			for k, v := range tt.want {
				if got.Requests[k] != v {
					t.Errorf("key %q: got %d, want %d", k, got.Requests[k], v)
				}
			}
		})
	}
}

func TestResolveEstimatorErrorFallsBack(t *testing.T) {
	got, err := resource.Resolve(context.Background(), resource.ResolveInput{
		Declared:  resource.Set{resource.Memory: 8 << 30},
		Estimator: stubEstimator{err: errors.New("rollup unavailable")},
	})
	if err != nil {
		t.Fatalf("an estimator error must never fail enqueue: %v", err)
	}
	if got.Requests[resource.Memory] != 8<<30 {
		t.Errorf("got %v, want the declaration preserved", got.Requests)
	}
}

// TestResolveReportsSourceErrors pins the other half of "never fails an
// enqueue": it must not fail SILENTLY either.
//
// Both dynamic sources run once per enqueue, in the enqueuing process,
// so one that is misconfigured is misconfigured for every job of that
// name from then on. Falling back to the declaration with no signal
// means the fleet under-sizes those jobs indefinitely and the symptom —
// OOM kills under real input — points at the handler, not at the
// estimator that stopped answering.
func TestResolveReportsSourceErrors(t *testing.T) {
	funcErr := errors.New("input service unreachable")
	estErr := errors.New("rollup unavailable")

	reported := map[string]error{}

	got, err := resource.Resolve(context.Background(), resource.ResolveInput{
		Declared: resource.Set{resource.Memory: 8 << 30},
		Func: func(context.Context, resource.Request) (resource.Set, error) {
			return nil, funcErr
		},
		Estimator: stubEstimator{err: estErr},
		OnError: func(source string, err error) {
			reported[source] = err
		},
	})
	if err != nil {
		t.Fatalf("a reported source error must still never fail enqueue: %v", err)
	}

	if got.Requests[resource.Memory] != 8<<30 {
		t.Errorf("got %v, want the declaration preserved", got.Requests)
	}

	if !errors.Is(reported[resource.SourceFunc], funcErr) {
		t.Errorf("func error reported as %v, want %v", reported[resource.SourceFunc], funcErr)
	}

	if !errors.Is(reported[resource.SourceEstimator], estErr) {
		t.Errorf("estimator error reported as %v, want %v",
			reported[resource.SourceEstimator], estErr)
	}

	// A source that succeeds reports nothing.
	reported = map[string]error{}

	if _, err = resource.Resolve(context.Background(), resource.ResolveInput{
		Estimator: stubEstimator{out: resource.Set{resource.Memory: 1 << 30}},
		OnError:   func(source string, err error) { reported[source] = err },
	}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if len(reported) != 0 {
		t.Errorf("a successful estimator reported %v, want nothing", reported)
	}
}

func TestResolveRejectsUnschedulable(t *testing.T) {
	_, err := resource.Resolve(context.Background(), resource.ResolveInput{
		Declared:    resource.Set{resource.Memory: 64 << 30},
		MaxCapacity: resource.Set{resource.Memory: 32 << 30},
	})
	if !errors.Is(err, resource.ErrUnschedulable) {
		t.Fatalf("got %v, want ErrUnschedulable", err)
	}
	if !strings.Contains(err.Error(), resource.Memory) {
		t.Errorf("error must name the dimension that does not fit: %v", err)
	}
}

func TestResolveSkipsCapacityCheckWhenUnknown(t *testing.T) {
	// A single-process engine with no registered workers has no known
	// capacity. Rejecting on an empty MaxCapacity would reject everything.
	_, err := resource.Resolve(context.Background(), resource.ResolveInput{
		Declared: resource.Set{resource.Memory: 64 << 30},
	})
	if err != nil {
		t.Fatalf("empty MaxCapacity must disable the check, got %v", err)
	}
}

func TestResolveLimitsDefaultToRequestsExceptCPU(t *testing.T) {
	got, err := resource.Resolve(context.Background(), resource.ResolveInput{
		Declared: resource.Set{resource.CPU: 4000, resource.Memory: 8 << 30},
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got.Limits[resource.Memory] != 8<<30 {
		t.Errorf("memory limit should default to the request, got %v", got.Limits)
	}
	if _, ok := got.Limits[resource.CPU]; ok {
		t.Errorf("CPU limit should be unset (burstable), got %v", got.Limits)
	}
}
