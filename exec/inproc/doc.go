// Package inproc runs job handlers in the worker process.
//
// This is Dispatch's original behaviour and remains the default. It
// provides no isolation: the handler shares the worker's memory,
// credentials, file descriptors, and network. That is the right trade for
// handlers that do not touch untrusted bytes, where launching a process
// per job would be pure overhead, and the wrong one for anything parsing
// a customer upload with a memory-unsafe library.
package inproc
