package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/drivers/mongodriver/mongomigrate"
	"github.com/xraph/grove/migrate"
)

// Migrations is the grove migration group for the Dispatch mongo store.
var Migrations = migrate.NewGroup("dispatch")

func init() {
	Migrations.MustRegister(
		&migrate.Migration{
			Name:    "create_dispatch_jobs",
			Version: "20240101000001",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*jobModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colJobs, []mongo.IndexModel{
					{Keys: bson.D{{Key: "queue", Value: 1}, {Key: "state", Value: 1}, {Key: "priority", Value: -1}, {Key: "run_at", Value: 1}}},
					{Keys: bson.D{{Key: "state", Value: 1}}},
					{Keys: bson.D{{Key: "scope_app_id", Value: 1}, {Key: "scope_org_id", Value: 1}}},
					{Keys: bson.D{{Key: "state", Value: 1}, {Key: "heartbeat_at", Value: 1}}},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}
				return mexec.DropCollection(ctx, (*jobModel)(nil))
			},
		},
		&migrate.Migration{
			Name:    "create_dispatch_workflow_runs",
			Version: "20240101000002",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*workflowRunModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colWorkflowRuns, []mongo.IndexModel{
					{Keys: bson.D{{Key: "state", Value: 1}}},
					{Keys: bson.D{{Key: "created_at", Value: 1}}},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}
				return mexec.DropCollection(ctx, (*workflowRunModel)(nil))
			},
		},
		&migrate.Migration{
			Name:    "create_dispatch_checkpoints",
			Version: "20240101000003",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*checkpointModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colCheckpoints, []mongo.IndexModel{
					{
						Keys:    bson.D{{Key: "run_id", Value: 1}, {Key: "step_name", Value: 1}},
						Options: options.Index().SetUnique(true),
					},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}
				return mexec.DropCollection(ctx, (*checkpointModel)(nil))
			},
		},
		&migrate.Migration{
			Name:    "create_dispatch_cron_entries",
			Version: "20240101000004",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*cronEntryModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colCronEntries, []mongo.IndexModel{
					{
						Keys:    bson.D{{Key: "name", Value: 1}},
						Options: options.Index().SetUnique(true),
					},
					{Keys: bson.D{{Key: "enabled", Value: 1}, {Key: "next_run_at", Value: 1}}},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}
				return mexec.DropCollection(ctx, (*cronEntryModel)(nil))
			},
		},
		&migrate.Migration{
			Name:    "create_dispatch_dlq",
			Version: "20240101000005",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*dlqEntryModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colDLQ, []mongo.IndexModel{
					{Keys: bson.D{{Key: "queue", Value: 1}, {Key: "failed_at", Value: -1}}},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}
				return mexec.DropCollection(ctx, (*dlqEntryModel)(nil))
			},
		},
		&migrate.Migration{
			Name:    "create_dispatch_events",
			Version: "20240101000006",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*eventModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colEvents, []mongo.IndexModel{
					{Keys: bson.D{{Key: "name", Value: 1}, {Key: "acked", Value: 1}, {Key: "created_at", Value: 1}}},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}
				return mexec.DropCollection(ctx, (*eventModel)(nil))
			},
		},
		&migrate.Migration{
			Name:    "create_dispatch_workers",
			Version: "20240101000007",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*workerModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colWorkers, []mongo.IndexModel{
					{Keys: bson.D{{Key: "state", Value: 1}}},
					{Keys: bson.D{{Key: "is_leader", Value: 1}}},
					{Keys: bson.D{{Key: "state", Value: 1}, {Key: "last_seen", Value: 1}}},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}
				return mexec.DropCollection(ctx, (*workerModel)(nil))
			},
		},
	)
}
