package postgres

import (
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// errPrefix identifies this storage layer in wrapped errors and log messages.
// Concatenate it into format strings rather than passing it as an argument so
// the result stays a compile-time constant and go vet keeps checking the verbs:
//
//	fmt.Errorf(errPrefix+"get job: %w", err)
const errPrefix = "dispatch/postgres: "

// isNoRows returns true when err indicates no rows were found.
func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// isDuplicateKey checks if a PostgreSQL error is a unique_violation (23505).
func isDuplicateKey(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
