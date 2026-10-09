package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "create_durable_child_deliveries", Version: "20261019120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `CREATE TABLE IF NOT EXISTS dispatch_child_deliveries (
 namespace TEXT NOT NULL, source_workflow_id TEXT NOT NULL, source_run_id TEXT NOT NULL, delivery_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('result','close','cancel','cancel_ack')),
 target_workflow_id TEXT NOT NULL, target_run_id TEXT NOT NULL, target_build_id TEXT NOT NULL, target_queue TEXT NOT NULL,
 message JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL, available_at TIMESTAMPTZ NOT NULL,
 owner TEXT NOT NULL DEFAULT '', epoch BIGINT NOT NULL DEFAULT 0 CHECK(epoch>=0), attempt BIGINT NOT NULL DEFAULT 0 CHECK(attempt>=0),
 lease_until TIMESTAMPTZ, done BOOLEAN NOT NULL DEFAULT FALSE, disposition TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(namespace,source_workflow_id,source_run_id,delivery_id),
 FOREIGN KEY(namespace,source_workflow_id,source_run_id) REFERENCES dispatch_executions(namespace,workflow_id,run_id)
);
CREATE INDEX IF NOT EXISTS dispatch_child_deliveries_poll ON dispatch_child_deliveries(namespace,target_build_id,available_at) WHERE NOT done;
CREATE TABLE IF NOT EXISTS dispatch_child_delivery_receipts (
 namespace TEXT NOT NULL, source_workflow_id TEXT NOT NULL, source_run_id TEXT NOT NULL, delivery_id TEXT NOT NULL, request_id TEXT NOT NULL,
 digest TEXT NOT NULL, target_workflow_id TEXT NOT NULL, target_run_id TEXT NOT NULL,
 revision BIGINT NOT NULL, first_sequence BIGINT NOT NULL, last_sequence BIGINT NOT NULL, disposition TEXT NOT NULL,
 PRIMARY KEY(namespace,source_workflow_id,source_run_id,delivery_id,request_id),
 FOREIGN KEY(namespace,source_workflow_id,source_run_id,delivery_id) REFERENCES dispatch_child_deliveries(namespace,source_workflow_id,source_run_id,delivery_id)
)`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `DO $$ BEGIN
 IF to_regclass('dispatch_child_delivery_receipts') IS NOT NULL THEN
  LOCK TABLE dispatch_child_delivery_receipts IN ACCESS EXCLUSIVE MODE;
  IF EXISTS(SELECT 1 FROM dispatch_child_delivery_receipts) THEN RAISE EXCEPTION 'retained child delivery receipts prevent downgrade'; END IF;
 END IF;
 IF to_regclass('dispatch_child_deliveries') IS NOT NULL THEN
  LOCK TABLE dispatch_child_deliveries IN ACCESS EXCLUSIVE MODE;
  IF EXISTS(SELECT 1 FROM dispatch_child_deliveries) THEN RAISE EXCEPTION 'retained child deliveries prevent downgrade'; END IF;
 END IF;
 DROP TABLE IF EXISTS dispatch_child_delivery_receipts;
 DROP TABLE IF EXISTS dispatch_child_deliveries;
 END $$`)
			return err
		},
	})
}
