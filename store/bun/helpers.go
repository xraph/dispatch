package bunstore

import (
	"database/sql"
	"errors"

	"github.com/uptrace/bun/driver/pgdriver"
)

// isNoRows returns true when err indicates no rows were found.
func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// isDuplicateKey checks if a PostgreSQL error is a unique_violation (23505).
func isDuplicateKey(err error) bool {
	var pgErr pgdriver.Error
	if errors.As(err, &pgErr) {
		return pgErr.Field('C') == "23505"
	}
	return false
}
