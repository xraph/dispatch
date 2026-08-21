package wire_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/wire"
	"github.com/xraph/dispatch/id"
)

func TestRoundTripRequest(t *testing.T) {
	want := &exec.Request{
		JobID:       id.NewJobID(),
		Name:        "tessellate.model",
		Payload:     []byte(`{"detail":3}`),
		Attempt:     2,
		Deadline:    time.Now().Add(time.Hour).UTC().Truncate(time.Second),
		Fingerprint: "abc123",
		InputDir:    "/dispatch/in",
		OutputDir:   "/dispatch/out",
		Inputs:      []exec.InputSlot{{Name: "model", Path: "model/scene.ifc"}},
		Env:         map[string]string{"TMPDIR": "/tmp"},
	}

	var buf bytes.Buffer
	if err := wire.Encode(&buf, &wire.Frame{Kind: wire.KindRequest, Request: want}); err != nil {
		t.Fatalf("Encode() = %v", err)
	}

	got, err := wire.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	if got.Kind != wire.KindRequest {
		t.Fatalf("Kind = %v, want %v", got.Kind, wire.KindRequest)
	}
	if got.Request.Name != want.Name || got.Request.Attempt != want.Attempt {
		t.Errorf("Request = %+v, want name %q attempt %d", got.Request, want.Name, want.Attempt)
	}
	if string(got.Request.Payload) != string(want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Request.Payload, want.Payload)
	}
	if got.Request.JobID != want.JobID {
		t.Errorf("JobID = %v, want %v", got.Request.JobID, want.JobID)
	}
	if len(got.Request.Inputs) != 1 || got.Request.Inputs[0].Name != "model" {
		t.Errorf("Inputs = %+v, want one slot named model", got.Request.Inputs)
	}
}

func TestRoundTripResult(t *testing.T) {
	want := &exec.Result{
		Status:     exec.StatusHandlerError,
		HandlerErr: "bad IFC header",
		Permanent:  true,
		Usage:      exec.Usage{WallTime: 1500 * time.Millisecond},
		Outputs:    []exec.OutputFile{{Name: "mesh.glb", Size: 42, Hash: "blake3:9f"}},
	}

	var buf bytes.Buffer
	if err := wire.Encode(&buf, &wire.Frame{Kind: wire.KindResult, Result: want}); err != nil {
		t.Fatalf("Encode() = %v", err)
	}
	got, err := wire.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	if got.Result.Status != want.Status || got.Result.HandlerErr != want.HandlerErr {
		t.Errorf("Result = %+v, want %+v", got.Result, want)
	}
	if !got.Result.Permanent {
		t.Error("Permanent = false, want true — the wire permanence signal must survive")
	}
	if got.Result.Usage.WallTime != want.Usage.WallTime {
		t.Errorf("WallTime = %v, want %v", got.Result.Usage.WallTime, want.Usage.WallTime)
	}
	if len(got.Result.Outputs) != 1 || got.Result.Outputs[0].Name != "mesh.glb" {
		t.Errorf("Outputs = %+v, want one named mesh.glb", got.Result.Outputs)
	}
}

func TestResultCauseIsNotSerialised(t *testing.T) {
	// Cause is a live Go error and cannot cross a process boundary. It must
	// not break encoding, and it must come back nil.
	in := &exec.Result{Status: exec.StatusHandlerError, Cause: errors.New("boom")}

	var buf bytes.Buffer
	if err := wire.Encode(&buf, &wire.Frame{Kind: wire.KindResult, Result: in}); err != nil {
		t.Fatalf("Encode() = %v", err)
	}
	got, err := wire.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	if got.Result.Cause != nil {
		t.Errorf("Cause = %v, want nil after a round trip", got.Result.Cause)
	}
}

func TestDecodeEmptyStreamIsEOF(t *testing.T) {
	// The child wrote nothing at all — it crashed before producing a result.
	// This must be distinguishable from a truncated frame.
	_, err := wire.Decode(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Decode(empty) = %v, want io.EOF", err)
	}
}

func TestDecodeTruncatedFrame(t *testing.T) {
	var buf bytes.Buffer
	if err := wire.Encode(&buf, &wire.Frame{
		Kind:   wire.KindResult,
		Result: &exec.Result{Status: exec.StatusOK},
	}); err != nil {
		t.Fatalf("Encode() = %v", err)
	}

	full := buf.Bytes()
	_, err := wire.Decode(bytes.NewReader(full[:len(full)-2]))
	if !errors.Is(err, wire.ErrShortFrame) {
		t.Fatalf("Decode(truncated) = %v, want ErrShortFrame", err)
	}
}

func TestDecodeRejectsAbsurdLength(t *testing.T) {
	// A corrupt or hostile header must not make the parent allocate
	// gigabytes. Header is a 4-byte big-endian length.
	hdr := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := wire.Decode(bytes.NewReader(hdr)); err == nil {
		t.Fatal("Decode(absurd length) = nil, want an error")
	}
}
