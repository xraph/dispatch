package contract

import (
	"bytes"
	"encoding/json"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
)

type Duration struct {
	Text string `json:"text"`
	MS   int64  `json:"ms"`
}

func duration(d time.Duration) Duration { return Duration{Text: d.String(), MS: d.Milliseconds()} }

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func timestamp(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func timestampPtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	return timestamp(*t)
}

// Payload exposes JSON as JSON and opaque content only as a size.
type Payload struct {
	Kind  string          `json:"kind"`
	JSON  json.RawMessage `json:"json,omitempty"`
	Bytes *int            `json:"bytes,omitempty"`
}

func projectPayload(data []byte, checkpoint bool) Payload {
	if json.Valid(data) {
		return Payload{Kind: "json", JSON: bytes.Clone(data)}
	}
	kind := "binary"
	if checkpoint {
		kind = "gob"
	}
	n := len(data)
	return Payload{Kind: kind, Bytes: &n}
}

type Page[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"nextCursor"`
	Complete   bool    `json:"complete"`
	AsOf       string  `json:"asOf"`
}

func newPage[T any](items []T, cursor string, complete bool, asOf time.Time) Page[T] {
	if items == nil {
		items = []T{}
	}
	return Page[T]{Items: items, NextCursor: nullable(cursor), Complete: complete, AsOf: asOf.UTC().Format(time.RFC3339Nano)}
}

func pageLimit(limit int) (int, error) {
	if limit < 0 {
		return 0, &fc.Error{Code: fc.CodeBadRequest, Message: "limit must not be negative"}
	}
	if limit == 0 {
		return 50, nil
	}
	return min(limit, 200), nil
}
