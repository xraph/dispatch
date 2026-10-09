# Workflow deadlines review record

The single-run deadline implementation was reviewed from `8f668b0` through
`c6d0217`. No actionable defects or deferred minor findings were found. You can
use run/execution deadlines, independent timeout processing and frozen timeout
queries within the limits recorded here. This is one checkpoint in the broader
[durable execution roadmap](durable-execution.md#required-work-and-evidence).

## Verification

All three implementation checkpoints passed their task completion gates. The
final code passed `make f`, `make l`, `go test ./...`, engine/durable/runtime/memory
race tests, and the full durable PostgreSQL integration race suite (256.490s).
The timeout example ran and printed the parent's timed-out state with its saved
child-timeout observation. The reviewer independently passed the focused
engine/durable/runtime/memory deadline and timeout tests and memory `TestDurable`.
The reviewer inspected the PostgreSQL evidence without repeating its full suite.

An earlier Task 1 PostgreSQL race run hit the existing activity-recovery test's
50 ms store timeout under concurrent toolchain load. That unchanged test then
passed three focused runs and the full suite passed on rerun. This timing failure
is retained in the record; it was not a passing gate.

## Decisions and costs

The rows preserve the implementation decisions in order; later checkpoints extend
the earlier guard decisions. All three checkpoints are complete. The final ten rows
resolve the behaviors the reviewer explicitly left outside this plan's judgment.
The full goal remains active for those unimplemented or unqualified requirements.

| # | Decision | Cost if wrong |
| --- | --- | --- |
| 1 | Continue inline on main and push verified commits under standing user authorization. | shared-branch changes require a follow-up commit. |
| 2 | Execution wins equal deadline ties. | future retry policy would require an explicit compatibility rule. |
| 3 | Existing runs have unlimited deadlines; zero timeout fields use omitempty to retain request fingerprints. | preexisting receipts would fail exact retry. |
| 4 | Persist both absolute deadlines now; chain inheritance remains required separate work. | callers could mistake one-run enforcement for complete chain semantics, so docs state the limit. |
| 5 | Use schema guards for older writer updates and immutable deadline metadata. | a rolling deployment can bypass timeout enforcement. |
| 6 | Expired child targets acknowledge ignored_expired; their timeout closure owns the terminal result. | child delivery could be lost if a future feature permits extending deadlines, which this API prohibits. |
| 7 | Use independent namespace timeout grants with no build requirement. | a retired build could leave deadline closure stranded. |
| 8 | Task 1 is a persistence/fencing checkpoint, not a usable timeout service until Tasks 2 and 3 pass. | a caller could enable deadlines without closure processing. |
| 9 | Preserve legacy child timed_out ApplicationError decoding when adding typed timeout events. | saved child histories become unreadable. |
| 10 | Signal creation resolves time after its workflow identity lock and writes timestamps/deadlines in its initial INSERT, matching ordinary and child starts. | a conflicting older writer outside the identity-lock protocol can consume part of the timeout during uniqueness waits; retries never extend deadlines. |
| 11 | Lock executions and tasks before schema alterations, so task guards can finish their reads before execution lock upgrades. | migration retries can deadlock with ordinary task writes. |
| 12 | Normalize dedicated SQLSTATE rejections at each public mutation boundary through deferred named error results. | coordinators could treat a definitive expiry as an ambiguous transport failure. |
| 13 | Execution-deadline schema guards currently reject all updates on expired running rows. Task 2 must extend the guard for independent timeout grants and validate migration retries do not restore the older restrictive definition. | valid timeout closure becomes stuck during upgrade retries. |
| 14 | Timeout grants live on execution rows and never increment execution revision or history. | operational ownership could corrupt deterministic replay or query snapshots. |
| 15 | The retry-safe deadline guard delegates to an optional timeout helper, and the timeout migration reinstalls that same guard. | retrying an older migration can disable expiry processing. |
| 16 | Timeout closure consumes ownership in its projection update while retaining epoch/attempt evidence. | a legacy worker can reuse an unrelated live timeout grant to publish an invalid terminal event. |
| 17 | Dedicated SQLSTATE DX003 maps timeout-grant expiry to ErrLeaseLost, including when history insertion crosses the grant deadline. | coordinators may misclassify stale timeout work as a transport failure or ordinary execution expiry. |
| 18 | Timeout and parent-close termination share forced-closure reconstruction, using the already validated terminal state. | a forced outcome could accidentally replay normal completion or launch cleanup; phase tests cover both closure types. |
| 19 | Typed child timeout metadata must match the captured ChildCommand durations and recorded child creation time. | a fabricated or stale deadline could be accepted as the child's outcome; strict malformed-history tests reject it. |
| 20 | Timeout polling does not load workflow code, while frozen queries still require the original compatible handler. | operators might expect queries after retiring every compatible build; docs keep that distinction explicit. |
| 21 | Engine and PostgreSQL integration tests extend the initial runtime RED rather than claiming a separate observed RED for each integration. | integration-specific missing behavior could have been hidden before implementation; live cross-layer assertions and the final independent review are retained. |
| 22 | Retry-chain and continue-as-new deadline inheritance remain open requirements, not part of the qualified single-run feature. Existing docs state this and the full goal stays active. | callers may assume a successor inherits the original absolute deadline before that behavior exists. |
| 23 | Deadline fencing neither physically interrupts nor reverses external effects; cooperative completion acknowledgment remains separate work. | an operator may mistake a timed-out state for proof that an external operation stopped or was undone. |
| 24 | Remote authorization, audit transport and production database-role separation remain unqualified. These APIs retain their documented trusted boundary. | a deployment could expose cross-namespace access or grant bypass through privileged callers. |
| 25 | Local races, transaction faults and connection-pool replacement establish only the tested storage/runtime behavior; process kills, failover, disaster recovery and production throughput still need deployment evidence. | operators could infer recovery or capacity guarantees that were never measured. |
| 26 | Mixed-version qualification covers database writer fencing, not every older runtime decoder. Deploy compatible readers before enabling deadline fields; full rollout compatibility remains open. | older parent/workflow readers can reject new timeout payloads and stall processing. |
| 27 | Keep a compatible handler available for historical queries after retiring ordinary workers; independent timeout closure itself requires none. | terminal runs can close successfully but become unqueryable through their old code. |
| 28 | Private query replay may observe accepted signals and ready selections under the existing query contract, without publishing consumption or winner events. | a caller may confuse an observation with durable workflow processing. |
| 29 | Expiry governs acceptance after locks and the guarded write; a transaction accepted before expiry can finish committing later. No commit/fsync-time deadline is promised. | a caller may expect terminal visibility or database durability to occur strictly before the deadline. |
| 30 | Arbitrary privileged SQL tampering and arbitrary Go side effects remain trusted-boundary limitations, not sandbox guarantees. | direct SQL or workflow code can change outside state without a valid deterministic history. |
| 31 | This backend deadline plan makes no Dashboard/operator UI claim. Durable transport and verified React operator flows remain required in the full goal. | operators may be given a backend capability without usable or authorized controls and visibility. |

## Deferred minors

None. No code fix pass or second review was needed.
