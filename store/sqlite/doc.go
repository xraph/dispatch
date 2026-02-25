// Package sqlite implements store.Store using the grove ORM with SQLite
// dialect. Suitable for embedded/edge deployments, CLI tools, and standalone
// applications.
//
// The caller owns the *grove.DB lifecycle -- sqlite never closes it. Pass the
// db handle through the constructor:
//
//	import (
//	    "github.com/xraph/grove"
//	    "github.com/xraph/dispatch/store/sqlite"
//	)
//
//	db, _ := grove.Open(ctx, "sqlite", dsn)
//	store := sqlite.New(db)
//	store.Migrate(ctx)
package sqlite
