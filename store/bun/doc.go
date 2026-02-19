// Package bunstore implements store.Store using the Bun ORM with PostgreSQL
// dialect. Suitable for teams already using Bun in their Forge/ControlPlane
// stack.
//
// The caller owns the *bun.DB lifecycle — bunstore never closes it. Pass the
// db handle through the constructor:
//
//	import (
//	    "github.com/uptrace/bun"
//	    "github.com/uptrace/bun/dialect/pgdialect"
//	    "github.com/uptrace/bun/driver/pgdriver"
//	    bunstore "github.com/xraph/dispatch/store/bun"
//	)
//
//	sqldb := sql.OpenDB(pgdriver.NewConnector(...))
//	db := bun.NewDB(sqldb, pgdialect.New())
//	store := bunstore.New(db)
//	store.Migrate(ctx)
package bunstore
