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
		&migrate.Migration{
			Name:    "create_dispatch_artifacts",
			Version: "20260811000001",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.CreateCollection(ctx, (*artifactModel)(nil)); err != nil {
					return err
				}

				// The key uniqueness index is partial: only live rows
				// collide, so a purged key becomes reusable.
				err := mexec.CreateIndexes(ctx, colArtifacts, []mongo.IndexModel{
					{
						Keys: bson.D{
							{Key: "backend", Value: 1},
							{Key: "bucket", Value: 1},
							{Key: "key", Value: 1},
						},
						Options: options.Index().
							SetUnique(true).
							SetPartialFilterExpression(bson.M{"deleted_at": bson.M{"$eq": nil}}),
					},
					{Keys: bson.D{{Key: "lifecycle", Value: 1}, {Key: "created_at", Value: 1}}},
					{Keys: bson.D{{Key: "deleted_at", Value: 1}}},
					{Keys: bson.D{{Key: "content_hash", Value: 1}}},
					{Keys: bson.D{{Key: "scope_app_id", Value: 1}, {Key: "scope_org_id", Value: 1}}},
				})
				if err != nil {
					return err
				}

				if err := mexec.CreateCollection(ctx, (*artifactLinkModel)(nil)); err != nil {
					return err
				}

				return mexec.CreateIndexes(ctx, colArtifactLinks, []mongo.IndexModel{
					{
						Keys: bson.D{
							{Key: "artifact_id", Value: 1},
							{Key: "owner_kind", Value: 1},
							{Key: "owner_id", Value: 1},
							{Key: "name", Value: 1},
							{Key: "attempt", Value: 1},
						},
						Options: options.Index().SetUnique(true),
					},
					{Keys: bson.D{{Key: "owner_kind", Value: 1}, {Key: "owner_id", Value: 1}}},
					{Keys: bson.D{{Key: "artifact_id", Value: 1}}},
				})
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				if err := mexec.DropCollection(ctx, (*artifactLinkModel)(nil)); err != nil {
					return err
				}

				return mexec.DropCollection(ctx, (*artifactModel)(nil))
			},
		},
		&migrate.Migration{
			Name:    "add_job_lease_index",
			Version: "20260812000001",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				// Mongo is schemaless, so the lease fields need no
				// migration — only the index the reclaim scan reads.
				return mexec.CreateIndexes(ctx, colJobs, []mongo.IndexModel{
					{Keys: bson.D{{Key: "state", Value: 1}, {Key: "lease_expires_at", Value: 1}}},
				})
			},
			Down: func(_ context.Context, _ migrate.Executor) error {
				// Dropping an index is not worth failing a rollback over.
				return nil
			},
		},
		&migrate.Migration{
			Name:    "add_job_resource_index",
			Version: "20260812140000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				mexec, ok := exec.(*mongomigrate.Executor)
				if !ok {
					return fmt.Errorf("expected mongomigrate executor, got %T", exec)
				}

				// Mongo is schemaless, so the new resource fields need no
				// migration -- only the compound index a future dequeue
				// predicate reads: the same (queue, priority DESC, run_at
				// ASC) ordering as the SQL backends' covering index, plus
				// req_memory_bytes so the predicate's numeric memory
				// comparison stays index-assisted too.
				return mexec.CreateIndexes(ctx, colJobs, []mongo.IndexModel{
					{Keys: bson.D{
						{Key: "queue", Value: 1},
						{Key: "priority", Value: -1},
						{Key: "run_at", Value: 1},
						{Key: "req_memory_bytes", Value: 1},
					}},
				})
			},
			Down: func(_ context.Context, _ migrate.Executor) error {
				// Dropping an index is not worth failing a rollback over.
				return nil
			},
		},
	)
}
