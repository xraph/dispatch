// Package postgres implements the store using pgx/v5 with raw SQL.
// Features: SKIP LOCKED dequeue, advisory lock leader election,
// LISTEN/NOTIFY for events, embedded SQL migrations.
package postgres
