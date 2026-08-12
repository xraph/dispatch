// Package resource defines Dispatch's resource model: what a job needs,
// what a worker has, and the admission control between them.
//
// # Leaf package
//
// resource may import only the root dispatch package, dispatch/id, and
// the standard library. job imports resource for Options.Resources, so
// any edge back to job — or to artifact, worker, engine, or store — is
// an import cycle. This is why the estimator's input is plain data
// ([]InputSize) rather than []artifact.Ref: the caller translates.
//
// # Units
//
// Every quantity is an int64 in a canonical unit, so accounting that
// adds and subtracts the same values thousands of times cannot drift:
//
//	cpu     millicores      1 core   = 1000
//	memory  bytes
//	disk    bytes
//	gpu     milli-devices   1 device = 1000
//
// Any other key is a custom resource with user-defined semantics, in
// the style of Ray's resource dict. Custom quantities are integers.
package resource
