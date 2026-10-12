package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{
		Name: "add_durable_intent_receipts", Version: "20261015120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_receipts
                ADD COLUMN IF NOT EXISTS intent_digest TEXT NOT NULL DEFAULT ''`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			// Hold the table lock across the guard and removal so a concurrent
			// callback cannot save an intent after the guard has passed.
			_, err := exec.Exec(ctx, `DO $$ BEGIN
                LOCK TABLE dispatch_execution_receipts IN ACCESS EXCLUSIVE MODE;
                IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid='dispatch_execution_receipts'::regclass AND attname='intent_digest' AND NOT attisdropped) THEN
                    IF EXISTS (SELECT 1 FROM dispatch_execution_receipts WHERE intent_digest <> '') THEN
                        RAISE EXCEPTION 'retained intent receipts prevent downgrade';
                    END IF;
                    ALTER TABLE dispatch_execution_receipts DROP COLUMN intent_digest;
                END IF;
                END $$`)
			return err
		},
	})
}
