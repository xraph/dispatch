package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// ErrShortFrame marks a frame whose declared length exceeded the bytes
// actually available. The writer died mid-write.
var ErrShortFrame = errors.New("short frame")

// Encode writes one length-prefixed frame.
func Encode(w io.Writer, f *Frame) error {
	body, err := msgpack.Marshal(sanitize(f))
	if err != nil {
		return fmt.Errorf("dispatch/exec/wire: marshal frame: %w", err)
	}
	if len(body) > MaxFrameBytes {
		return fmt.Errorf("dispatch/exec/wire: frame of %d bytes exceeds the %d limit",
			len(body), MaxFrameBytes)
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body))) //nolint:gosec // guarded by the MaxFrameBytes check above
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("dispatch/exec/wire: write header: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("dispatch/exec/wire: write body: %w", err)
	}

	return nil
}

// sanitize returns a Frame safe to marshal. msgpack/v5 has built-in
// support for the error interface: it encodes a non-nil error as its
// Error() string and, on decode, reconstructs a new error from that
// string. Left alone, that would silently turn Result.Cause into a
// lookalike error on the far side of the wire instead of nil, which is
// exactly what exec.Result's own doc comment says a marshaling rung must
// not do: "A rung that marshals a Result leaves this nil and sets
// Permanent instead." Stripping it here, on a copy, keeps that contract
// without touching exec's types or mutating the caller's Frame.
func sanitize(f *Frame) *Frame {
	if f.Result == nil || f.Result.Cause == nil {
		return f
	}

	r := *f.Result
	r.Cause = nil
	out := *f
	out.Result = &r

	return &out
}

// Decode reads one length-prefixed frame.
//
// It returns io.EOF when the stream is empty, which means the writer
// produced nothing at all, and ErrShortFrame when a header was read but
// the body was incomplete.
func Decode(r io.Reader) (*Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("dispatch/exec/wire: header: %w", ErrShortFrame)
		}

		return nil, fmt.Errorf("dispatch/exec/wire: read header: %w", err)
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameBytes {
		return nil, fmt.Errorf("dispatch/exec/wire: declared frame of %d bytes exceeds the %d limit",
			n, MaxFrameBytes)
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("dispatch/exec/wire: body: %w", ErrShortFrame)
		}

		return nil, fmt.Errorf("dispatch/exec/wire: read body: %w", err)
	}

	var f Frame
	if err := msgpack.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("dispatch/exec/wire: unmarshal frame: %w", err)
	}

	return &f, nil
}
