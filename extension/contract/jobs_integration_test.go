//go:build integration

package contract

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/mongodriver"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch/store"
	mongostore "github.com/xraph/dispatch/store/mongo"
	pgstore "github.com/xraph/dispatch/store/postgres"
	redisstore "github.com/xraph/dispatch/store/redis"
)

func postgresContractStore(t *testing.T) store.Store {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcpg.Run(ctx, "postgres:16-alpine", tcpg.WithDatabase("dispatch_contract"), tcpg.WithUsername("test"), tcpg.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if terminateErr := ctr.Terminate(ctx); terminateErr != nil {
			t.Error(terminateErr)
		}
	})
	uri, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	drv := pgdriver.New()
	if openErr := drv.Open(ctx, uri); openErr != nil {
		t.Fatal(openErr)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return pgstore.New(db)
}

func redisContractStore(t *testing.T) store.Store {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if terminateErr := ctr.Terminate(ctx); terminateErr != nil {
			t.Error(terminateErr)
		}
	})
	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	drv := redisdriver.New()
	if openErr := drv.Open(ctx, uri); openErr != nil {
		t.Fatal(openErr)
	}
	db, err := kv.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return redisstore.New(db)
}

func mongoContractStore(t *testing.T) store.Store {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if terminateErr := ctr.Terminate(ctx); terminateErr != nil {
			t.Error(terminateErr)
		}
	})
	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	drv := mongodriver.New()
	if openErr := drv.Open(ctx, uri, mongodriver.WithDatabase("dispatch_contract")); openErr != nil {
		t.Fatal(openErr)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return mongostore.New(db)
}

func TestJobDomainOtherBackends(t *testing.T) {
	t.Run("postgres", func(t *testing.T) { runJobDomain(t, postgresContractStore(t)) })
	t.Run("redis", func(t *testing.T) { runJobDomain(t, redisContractStore(t)) })
	t.Run("mongo", func(t *testing.T) { runJobDomain(t, mongoContractStore(t)) })
}
