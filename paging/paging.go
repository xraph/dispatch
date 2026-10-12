// Package paging holds what every cursor-paged store list shares.
//
// Dispatch IDs are UUIDv7 TypeIDs minted with a monotonic counter, so a
// row's ID sorts by when it was created. Every paged list orders by ID,
// newest first, and its cursor is the ID of the last row a page returned:
// the next page is every matching row whose ID sorts strictly below it.
// A cursor is therefore stable across inserts and deletes, and a deleted
// cursor row still marks its place.
package paging

import (
	"errors"
	"fmt"

	"github.com/xraph/dispatch/id"
)

// DefaultLimit is the page size when a caller passes zero or less.
const DefaultLimit = 50

// ErrInvalidCursor is returned when a cursor is not an ID of the kind the
// list pages by. Stores never fall back to the first page on a bad cursor:
// a client that sent one would otherwise see page one again and believe
// it had reached the end.
var ErrInvalidCursor = errors.New("dispatch: invalid page cursor")

// Limit returns n, or DefaultLimit when n is zero or negative.
func Limit(n int) int {
	if n <= 0 {
		return DefaultLimit
	}

	return n
}

// Cursor validates raw as an ID with the given prefix. An empty raw is the
// first page and returns the nil ID.
func Cursor(raw string, prefix id.Prefix) (id.ID, error) {
	if raw == "" {
		return id.Nil, nil
	}

	parsed, err := id.ParseWithPrefix(raw, prefix)
	if err != nil {
		return id.Nil, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}

	return parsed, nil
}
