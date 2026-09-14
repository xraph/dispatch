package extension

import (
	"testing"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/drivers/redisdriver"

	redisstore "github.com/xraph/dispatch/store/redis"
)

// unopenedKV returns a Redis-driver KV store that never dials. Building the
// dispatch store needs only the driver type, so the tests below can pin the
// wiring without a Redis on the box.
func unopenedKV(t *testing.T) *kv.Store {
	t.Helper()
	s, err := kv.Open(redisdriver.New())
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	return s
}

func TestBuildStoreFromGroveKV_appliesKeyPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
	}{
		{name: "no prefix keeps the historical keys", prefix: ""},
		{name: "tenant prefix reaches the redis store", prefix: "ws_acme:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(WithKVKeyPrefix(tt.prefix))
			s, ok := e.buildStoreFromGroveKV(unopenedKV(t)).(*redisstore.Store)
			if !ok {
				t.Fatal("grove KV must build a redis store")
			}
			if got := s.KeyPrefix(); got != tt.prefix {
				t.Fatalf("store key prefix = %q, want %q", got, tt.prefix)
			}
		})
	}
}

func TestMergeConfigurations_keyPrefixYAMLWins(t *testing.T) {
	e := New()
	tests := []struct {
		name         string
		yaml         string
		programmatic string
		want         string
	}{
		{name: "yaml set, programmatic empty", yaml: "ws_yaml:", programmatic: "", want: "ws_yaml:"},
		{name: "yaml empty, programmatic set", yaml: "", programmatic: "ws_go:", want: "ws_go:"},
		{name: "both set, yaml wins", yaml: "ws_yaml:", programmatic: "ws_go:", want: "ws_yaml:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := e.mergeConfigurations(Config{KeyPrefix: tt.yaml}, Config{KeyPrefix: tt.programmatic})
			if got.KeyPrefix != tt.want {
				t.Fatalf("merged KeyPrefix = %q, want %q", got.KeyPrefix, tt.want)
			}
		})
	}
}
