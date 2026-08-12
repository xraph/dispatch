// Package trove adapts a *trove.Trove instance as an artifact.Backend.
//
// It is the reference backend for Dispatch's artifact plane, but not a
// required one: the core depends only on the artifact.Backend interface,
// so any object store can be plugged in instead.
//
// Two Trove features carry through without any code here. Its write-path
// middleware means compression, AES-256-GCM encryption, and virus
// scanning are configuration rather than Dispatch concerns — and scanning
// on write is what lets a malicious upload be rejected before any
// memory-unsafe parser opens it. Its multi-store routing means the
// backend name recorded on each artifact is simply the Trove store it
// lives in.
package trove
