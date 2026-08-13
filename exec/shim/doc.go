// Package shim is the child side of out-of-process execution.
//
// A sandboxed process re-execs the worker's own binary, which calls Main.
// Main builds a bare job.Registry and a credential-free artifact.Service
// over a local directory, reads a request, runs the handler, and writes a
// result. It never constructs an engine, a store, or a DI container, so
// the process that parses an untrusted file holds no database credential
// and no object-store client.
package shim
