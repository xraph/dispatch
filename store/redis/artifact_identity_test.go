//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
	redisstore "github.com/xraph/dispatch/store/redis"
)

func TestArtifactReadsRejectMismatchedIdentity(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)
	client := redisdriver.UnwrapClient(kvStore)
	a := &artifact.Artifact{ID: id.NewArtifactID(), Backend: "memory", Bucket: "inputs", Key: "owned", Lifecycle: artifact.Durable, CreatedAt: time.Now()}
	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatal(err)
	}
	key := "dispatch:artifact:" + a.ID.String()
	raw, err := client.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]json.RawMessage
	if decodeErr := json.Unmarshal(raw, &record); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	record["id"], err = json.Marshal(id.NewArtifactID().String())
	if err != nil {
		t.Fatal(err)
	}
	corrupt, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, key, corrupt, 0).Err(); err != nil {
		t.Fatal(err)
	}
	for name, read := range map[string]func() error{
		"get":  func() error { _, err := s.GetArtifact(ctx, a.ID); return err },
		"list": func() error { _, err := s.ListArtifacts(ctx, artifact.ListOpts{}); return err },
		"page": func() error { _, err := s.ListArtifactsPage(ctx, artifact.PageOpts{}); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); err == nil {
				t.Fatal("mismatched record was returned")
			}
		})
	}
}
