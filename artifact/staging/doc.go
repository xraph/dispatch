// Package staging wires the artifact plane into job execution.
//
// It lives apart from the artifact package to break an import cycle: the
// middleware signature takes a *job.Job, and job imports artifact for its
// input declarations.
//
// The middleware stages a job's declared inputs before the handler runs,
// puts an artifact.Accessor in the context, and releases every cache
// lease afterwards — whether the handler returns, fails, or panics.
//
// Running staging as middleware rather than inside the executor is also
// the right seam for out-of-process execution: staging happens outside
// the boundary, so a sandboxed handler receives a directory of files
// rather than storage credentials.
package staging
