// Package exec defines the execution boundary between the Dispatch worker
// and a job handler.
//
// Today a handler is an ordinary Go function called in-process, sharing the
// worker's memory, credentials, and network. Handlers that parse untrusted
// bytes with memory-unsafe native libraries need more than that, so exec
// generalises the call into an [Executor] with implementations forming an
// escalating ladder: in-process, subprocess, OCI container, and Kubernetes
// Job-per-task.
//
// exec is a leaf package. It imports only id, scope, and the root dispatch
// package — never job, worker, or engine — so that job.Options can carry an
// execution [Policy] without an import cycle. This mirrors how artifact is
// positioned for input declarations.
package exec
