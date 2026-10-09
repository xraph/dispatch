package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "create_durable_cancellation_receipts", Version: "20261017120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `CREATE TABLE IF NOT EXISTS dispatch_cancellation_receipts (
 namespace TEXT NOT NULL, workflow_id TEXT NOT NULL, request_id TEXT NOT NULL,
 run_id TEXT NOT NULL, digest TEXT NOT NULL,
 revision BIGINT NOT NULL CHECK (revision > 0),
 first_sequence BIGINT NOT NULL CHECK (first_sequence > 0),
 last_sequence BIGINT NOT NULL CHECK (last_sequence >= first_sequence),
 PRIMARY KEY (namespace,workflow_id,request_id),
 FOREIGN KEY (namespace,workflow_id,run_id)
 REFERENCES dispatch_executions (namespace,workflow_id,run_id) ON DELETE CASCADE
)`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `DO $$ BEGIN
 IF to_regclass('dispatch_cancellation_receipts') IS NOT NULL THEN
   LOCK TABLE dispatch_cancellation_receipts IN ACCESS EXCLUSIVE MODE;
   IF EXISTS (SELECT 1 FROM dispatch_cancellation_receipts) THEN
     RAISE EXCEPTION 'retained cancellation receipts prevent downgrade';
   END IF;
   DROP TABLE dispatch_cancellation_receipts;
 END IF;
 END $$`)
			return err
		},
	})
}
