// Package sweeper reclaims the storage Dispatch owns.
//
// It touches only ephemeral artifacts — the ones Dispatch itself created
// on a handler's behalf. Durable artifacts, which are the application's
// uploads merely tracked here, are unreachable from every code path in
// this package. That guarantee is enforced twice: the store's sweep
// queries constrain themselves to ephemeral with a literal, and the
// sweeper re-checks each artifact before acting on it.
//
// Deletion is two-phase. A sweep marks an artifact deleted and stops
// serving it; a later purge removes the bytes once a grace period has
// passed. A mistaken sweep is therefore observable and reversible for the
// length of that window rather than instantly destructive, and both
// phases are idempotent under retry.
//
// Sweeping runs on the elected leader only, so a fleet of workers does
// not race to delete the same objects.
package sweeper
