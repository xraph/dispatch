// Package resourcetest provides fakes for testing resource-aware code:
// a deterministic clock, a lease-based reclaimer, and a scripted
// estimator.
//
// It mirrors artifact/artifacttest, so a test that needs both staging
// and admission fakes reaches for the same shapes in both packages.
package resourcetest
