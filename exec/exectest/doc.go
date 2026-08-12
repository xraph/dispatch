// Package exectest is the conformance suite every exec.Executor must pass.
//
// The rungs of the isolation ladder are meant to be interchangeable: the
// same handler, the same payload, and the same declared inputs must behave
// the same way whether the handler runs in-process or in a pod. One shared
// table-driven suite is how that stays true, and it is what lets a new rung
// land without redesigning the ones before it.
//
// Rungs differ in what they can enforce — in-process cannot kill a handler
// that ignores its deadline — so a rung declares its Capabilities and the
// suite asserts the enforcement cases only against rungs that claim them.
package exectest
