package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_outbox_conflict", Version: "20261029120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `
CREATE TABLE dispatch_delivery_compatibility (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
 minimum_protocol INTEGER NOT NULL CHECK(minimum_protocol=1)
);
INSERT INTO dispatch_delivery_compatibility VALUES(TRUE,1);
CREATE FUNCTION dispatch_delivery_publisher_check(protocol INTEGER) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE floor INTEGER;
BEGIN
 SELECT minimum_protocol INTO STRICT floor FROM dispatch_delivery_compatibility WHERE singleton;
 IF protocol IS DISTINCT FROM floor THEN
 RAISE EXCEPTION USING ERRCODE='DA003',MESSAGE='incompatible delivery publisher protocol';
 END IF;
 PERFORM set_config('dispatch.delivery_publisher_protocol',protocol::TEXT,TRUE);
END $$;
CREATE FUNCTION dispatch_outbox_conflict_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE floor INTEGER;
BEGIN
 SELECT minimum_protocol INTO STRICT floor FROM dispatch_delivery_compatibility WHERE singleton;
 IF current_setting('dispatch.delivery_publisher_protocol',TRUE) IS DISTINCT FROM floor::TEXT THEN
 RAISE EXCEPTION USING ERRCODE='DA003',MESSAGE='delivery publisher capability required';
 END IF;
 IF OLD.error_category='conflict' AND NEW IS DISTINCT FROM OLD THEN
 RAISE EXCEPTION 'blocked delivery requires protected repair; publisher artifact is incompatible';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_outbox_conflict BEFORE UPDATE ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_outbox_conflict_guard();
`)
		return err
	}, Down: func(_ context.Context, _ migrate.Executor) error {
		return fmt.Errorf("retained sink conflicts prohibit downgrade")
	}})
}
