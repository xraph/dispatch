// Package wire carries execution requests and results across a process
// boundary.
//
// The envelope is a 4-byte big-endian length followed by a msgpack body.
// Framing rather than a bare value stream is what lets a parent tell a
// child that produced nothing (EOF before a header — it crashed) from one
// that produced a partial write (header then short body — corruption).
// Those are different failures and get different statuses.
package wire
