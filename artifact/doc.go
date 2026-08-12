// Package artifact defines Dispatch's data plane: tracked references to
// objects in external storage, the pluggable Backend interface those
// objects live behind, and the Store contract that persists their
// metadata and ownership links.
//
// This package is a leaf. It imports only the root dispatch package, the
// id package, and stdlib. The staging middleware, which needs job and
// middleware, lives in the artifact/staging sub-package so that job may
// import artifact without a cycle.
//
// Artifacts come in two lifecycles. Durable artifacts are written by the
// application and merely tracked here; Dispatch reads them and never
// deletes them. Ephemeral artifacts are created by Dispatch on a
// handler's behalf, refcounted through links, and swept once every owner
// is terminal and the retention window has passed.
package artifact
