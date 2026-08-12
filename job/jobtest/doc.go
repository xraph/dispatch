// Package jobtest provides the shared conformance suite for the
// resource-aware dequeue contract.
//
// Every job.Store implementation runs RunDequeueSuite, so five backends
// written against five different query languages cannot quietly disagree
// about which jobs a worker is allowed to claim. Disagreement here is not
// cosmetic: the same job would become eligible on different workers
// depending only on which store the operator chose, and the dimension
// that silently drifts is the one that decides whether a 32 GB job lands
// on a 4 GB machine.
//
// The suite's two load-bearing cases are ZeroBudgetSelectsEverything,
// which is the backward-compatibility guarantee that an unconstrained
// caller still sees exactly what it saw before this option existed, and
// ClaimIsAtomicUnderConcurrency, which proves the fit predicate did not
// cost the claim its atomicity.
//
// The package deliberately depends only on job, resource, id, and the
// root package. It must never import a store backend: the backends
// import this, not the reverse.
package jobtest
