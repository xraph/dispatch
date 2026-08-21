// Package artifacttest provides a shared conformance suite and test
// doubles for artifact storage.
//
// Every artifact.Store implementation runs RunStoreSuite so all five
// backends are held to one contract. The suite's most important case is
// SweepNeverTouchesDurable: no sweep path, under any input, may mark a
// durable artifact.
package artifacttest
