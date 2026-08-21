// Package cache is the worker-local staging cache for artifact inputs.
//
// Staging exists because the native libraries that read Dispatch's heavy
// inputs — CAD kernels, mesh importers, PDF engines — want a file path
// they can seek and memory-map, not a stream. The cache materialises an
// artifact to local disk and hands back that path.
//
// It is content-addressed. Entries live under their BLAKE3 hash, so two
// jobs consuming the same model share one copy, and re-running a job over
// an input it already staged costs nothing. The hash is computed during
// the download rather than by a separate pass: the bytes are already
// streaming to disk, so hashing them is free. That is what fills in the
// content_hash column an artifact carries as NULL after registration.
//
// Three mechanisms keep concurrent staging honest:
//
//   - Single-flight collapses concurrent stages of the same artifact into
//     one download.
//   - Leases pin an entry while a job is using it, so eviction cannot
//     pull a file out from under a running handler.
//   - A byte budget bounds total disk use, evicting unleased entries by
//     least-recent-use and making a job wait when nothing can be freed.
//
// That last mechanism is also the artifact plane's first piece of
// admission control: a job needing more staging space than is available
// waits instead of filling the disk.
//
// The budget is a resource.Manager, not a counter. Each cached object
// holds one lease for its bytes and eviction releases it, so with
// WithManager the staged bytes sit in the same ledger the worker admits
// jobs against: the cache is registered as that manager's disk
// reclaimer, and a job short on disk gets it by evicting rather than by
// waiting for a cache that has no reason to shrink. With no manager
// supplied the cache builds a private single-key one, which is the
// private disk budget it always had.
//
// The cache is a cache. Its index is an optimisation rebuilt from disk on
// startup, and a corrupt or missing index costs a re-download, never
// correctness.
package cache
