# Deployment and recovery qualification

On 14 October 2026, all fifteen assembled Linux drills passed with no skips.
Ten consecutive load repeats also passed: 1,280 accepted workflows completed,
with exact start-receipt replay and an authenticated survivor after every midpoint
drain. These runs used real Docker processes and PostgreSQL persistence.

## Tested composition

- Isolated Ubuntu 24.04.5 ARM64 VM, Linux 6.8, four CPUs and 6 GiB RAM.
- Rootless Docker provider, separate rootful launcher and private mTLS channels.
- PostgreSQL 17.11, capped at 384 MiB and one CPU, with durable writes enabled.
- Go 1.26.10 with workspace overrides disabled and no module replacements.
- Dispatch `de3621a604dd`, Ctrlplane `87291467f279`, and the published
  qualification module at `e43a3e4229e0`. Exact versions are retained in `go.mod`.
- Test executable SHA256:
  `23172fb9d98ae7bdf0c47ef3bb5a5b1093d991a460a7a530b1e2b5b2dd4d116b`.
- Worker executable SHA256:
  `166e55c38cdcd54ac84f77437880a06b6a439c166ea261c1ae953130459e8e81`.
- Worker image digest:
  `sha256:8d044785771666ec29664e198bd28e133f5cd962e408ca381685087673b55e1c`.

## Observed behavior

| Drill | Verified result |
| --- | --- |
| Admission authentication | Real Authsome/Warden and mTLS admission; unauthorized and restored authorities refused |
| Service enrollment | Exact signed creation custody, live worker channel, native reads and revocation |
| Worker death | SIGKILL followed by Docker restart cannot reuse the old boot enrollment; history stays unchanged |
| Operator API | Authenticated registration, drain and Begin; exact replay and denial paths |
| Original lifecycle | Ctrlplane original permission binds the real native runtime and Begin receipt |
| Quiescence | Fresh runtime proof admits the exact original operation |
| Controller restart | Two controller processes retain the same worker boot and drain receipt |
| Logical restore | Actual pg_dump/pg_restore preserves history and exact receipt replay across process reopens |
| Restore quarantine | A same-floor authority restore cannot reopen the independent admission ledger |
| Replica promotion | Actual physical streaming replica promotion preserves the accepted history |
| Load and drain | 128 workflows, eight submitters, two workers; one drains at 64 accepted requests and the survivor completes the rest |
| Safe Abort | Exact prepared native/core replay; no provider delegation; original process and network survive |
| Lost fresh grant | Original attempt closes permanently; a separately authorized retry completes |
| Lost Docker response | The real DELETE succeeds but its response is destroyed; custody stays unresolved and no second DELETE is sent |
| Positive recovery | Authenticated effect completion, cutoff, sealed coverage, native/core close, settlement, Finish and tombstone all complete |

The full campaign's positive recovery took 41.51 seconds. The load scenario
completed 128 workflows at 94.57 completions per second in that run, with a
112 ms drain. These are observations from this fixture, not production capacity
promises. The ten repeat runs exercised the same acceptance and history checks.

## Failures corrected during the drills

- Existing Warden namespaces now require an exact compatible definition when a
  host reopens them.
- Native proof comparisons use Dispatch's canonical timestamp precision.
- Safe Abort binds its exact prepared request. Its schema-2 command envelope
  retains the original combined 8192-byte budget.
- Default PostgreSQL pools exhausted the shared server under load and caused
  authentication to fail closed. Launcher and fixture pools now allow four
  connections each; worker pools allow eight. Ten repeated load runs passed
  after this correction.
- Fixed diagnostic categories identify authentication and channel failures
  without recording credentials or raw database errors.

## Checks

Formatting and Linux lint passed. Module verification, vet and builds passed on
macOS and Linux. The PostgreSQL-backed macOS race suite passed. The isolated
Linux run passed all 90 selected component tests (176 including subtests), with
zero skips and no race reports, including protected launcher ownership checks.
The fifteen assembled drills and ten load repeats also had zero skips.

## Verification boundary

The two-second admission deadline, 4096-byte evidence cap, 64-binding cap and
33-step cap remain unchanged. A restored deployment requires trusted
reconciliation before fresh admission. The pristine-opening fixture cannot
reopen a used deployment. An ambiguous Docker response remains unresolved until
new affirmative evidence exists; absence observed during cleanup is not evidence.

The assembled campaign used normal binaries. Component race checks run separately;
race instrumentation can exceed the admission deadline on this small VM. A
production-duration soak, whole-VM disk loss and a compromised host or ledger
administrator are outside this campaign. You still need to size connection and
resource budgets for your production topology.

The final Linux vulnerability scan found no reachable or imported-package
vulnerabilities. It reported one advisory in an unimported required module:
`GO-2026-5932`, the unmaintained `golang.org/x/crypto/openpgp` package.
The same-day GitHub check returned zero open Dependabot alerts for Dispatch,
Ctrlplane, Authsome, Forge, Warden, Chronicle, Relay and forge-dashboard.

Use [the build and run instructions](README.md#assembled-linux-deployment-and-recovery)
to repeat the campaign. Test resource identifiers and credentials are generated
for each invocation; the recorded binary and image digests identify this run.
