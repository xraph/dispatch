// Package postgres implements store.Store using the grove ORM with PostgreSQL
// dialect. Suitable for teams already using grove in their Forge/ControlPlane
// stack.
//
// The caller owns the *grove.DB lifecycle -- postgres never closes it. Pass the
// db handle through the constructor:
//
//	import (
//	    "github.com/xraph/grove"
//	    "github.com/xraph/dispatch/store/postgres"
//	)
//
//	db, _ := grove.Open(ctx, "pg", dsn)
//	store := postgres.New(db)
//	store.Migrate(ctx)
package postgres
