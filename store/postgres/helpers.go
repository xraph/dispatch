package postgres

import (
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

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
