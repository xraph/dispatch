// Package mongo implements store.Store using the grove ORM with MongoDB
// driver. Suitable for distributed deployments requiring horizontal scaling
// and flexible schema evolution.
//
// The caller owns the *grove.DB lifecycle -- mongo never closes it. Pass the
// db handle through the constructor:
//
//	import (
//	    "github.com/xraph/grove"
//	    "github.com/xraph/dispatch/store/mongo"
//	)
//
//	db, _ := grove.Open(ctx, "mongo", dsn)
//	store := mongo.New(db)
//	store.Migrate(ctx)
package mongo
