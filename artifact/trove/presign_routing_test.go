package trove_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	trovelib "github.com/xraph/trove"
	trovedriver "github.com/xraph/trove/driver"
	"github.com/xraph/trove/drivers/memdriver"

	"github.com/xraph/dispatch/artifact"
	troveadapter "github.com/xraph/dispatch/artifact/trove"
)

type routedSigner struct {
	trovedriver.Driver
	name  string
	calls int
}

func (s *routedSigner) PresignGet(_ context.Context, bucket, key string, ttl time.Duration) (string, error) {
	s.calls++
	return fmt.Sprintf("https://%s.example/%s/%s?ttl=%s", s.name, bucket, key, ttl), nil
}

func (s *routedSigner) PresignPut(ctx context.Context, bucket, key string, ttl time.Duration) (string, error) {
	return s.PresignGet(ctx, bucket, key, ttl)
}

func TestTrovePresignUsesReadRoute(t *testing.T) {
	for _, test := range []struct {
		name          string
		defaultSigns  bool
		routedSigns   bool
		defaultBucket bool
	}{
		{name: "both sign", defaultSigns: true, routedSigns: true},
		{name: "routed signs", routedSigns: true},
		{name: "default cannot substitute", defaultSigns: true},
		{name: "default bucket resolves before routing", defaultSigns: true, routedSigns: true, defaultBucket: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			makeDriver := func(name string, signing bool) (trovedriver.Driver, *routedSigner) {
				t.Helper()
				drv := memdriver.New()
				if err := drv.Open(ctx, "mem://"); err != nil {
					t.Fatal(err)
				}
				if err := drv.CreateBucket(ctx, testBucket); err != nil {
					t.Fatal(err)
				}
				if _, err := drv.Put(ctx, testBucket, "shared.bin", strings.NewReader(name)); err != nil {
					t.Fatal(err)
				}
				signer := &routedSigner{Driver: drv, name: name}
				if signing {
					return signer, signer
				}
				return drv, signer
			}
			defaultDriver, defaultSigner := makeDriver("default", test.defaultSigns)
			routedDriver, routed := makeDriver("routed", test.routedSigns)
			tr, err := trovelib.Open(defaultDriver, trovelib.WithDefaultBucket(testBucket), trovelib.WithBackend("routed", routedDriver), trovelib.WithRouteFunc(func(bucket, key string) string {
				if bucket == testBucket && key == "shared.bin" {
					return "routed"
				}
				return ""
			}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if closeErr := tr.Close(ctx); closeErr != nil {
					t.Error(closeErr)
				}
			})
			backend := troveadapter.New(tr)
			ref := artifact.Ref{Bucket: testBucket, Key: "shared.bin"}
			if test.defaultBucket {
				ref.Bucket = ""
			}
			if available := artifact.SupportsPresign(backend, ref); available != test.routedSigns {
				t.Errorf("download available = %t, want %t", available, test.routedSigns)
			}
			reader, err := backend.Open(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(reader)
			if closeErr := reader.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if readErr != nil || string(data) != "routed" {
				t.Fatalf("read = %q, %v", data, readErr)
			}
			url, err := backend.PresignGet(ctx, ref, time.Minute)
			if test.routedSigns {
				if err != nil || url != "https://routed.example/dispatch/shared.bin?ttl=1m0s" || routed.calls != 1 {
					t.Errorf("routed download = %q, %v; calls = %d", url, err, routed.calls)
				}
			} else if err == nil || url != "" {
				t.Errorf("unsupported routed driver signed = %q, %v", url, err)
			}
			if defaultSigner.calls != 0 {
				t.Errorf("default driver signed %d times", defaultSigner.calls)
			}
		})
	}
}
