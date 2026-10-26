# Durable worker lifecycle

Use `Worker.Status`, `Engine.DurableStatus` or `Extension.DurableStatus` to inspect
polling readiness. Database health is separate. A worker reports `not_started`,
`running`, `draining`, `stopped` or `failed`, together with its immutable namespace,
queue, build, owner and process incarnation. A newly constructed worker gets a
fresh RuntimeID unless trusted host composition supplies one. A replacement must
never reuse the old incarnation's identity, even if its owner and instance names
stay the same.

`Ready` describes an active process poller. Direct `RunOnce` remains available
before `Run`, but doesn't advertise continuous polling. Each call registers before
it reaches the database, including activity timeouts, child deliveries and
namespace-wide execution timeouts. `Claims` separates calls waiting on a claim
response from calls processing their returned work. These counters belong to one
Worker; they are not persisted build-retirement facts.

## Claims the store doesn't answer

A claim can end without an answer: the store times out, the connection drops, or
the reply is lost after the grant committed. `Run` keeps polling through these.
Each poller backs off with full jitter, from `PollInterval` up to five seconds,
and starts over once a claim gets an answer. The compatibility read that gates
claims under retirement options works the same way. You'll see the error wrapped
in `ErrClaimUnknown` from `RunOnce`.

Nothing was processed, so retrying can't run a task twice. A grant that committed
while its reply was lost keeps its lease, and the next claim after expiry takes a
new epoch, the same recovery a replacement worker relies on. The lost claim still
counts in `UnknownClaims`, so a drain can't report success.

While a kind's claims keep failing, its `ClaimStatus` carries `Failures` and
`FailingSince`. `WorkerStatus.ClaimFailures` is the longest run among the kinds,
and `Engine.Health` fails for as long as one lasts. An error the store can't
classify, such as a missing table, is retried like a timeout, so watch health
for it. A claim the store refuses with a definitive error, such as `ErrInvalid` or
`ErrWriterCompatibility`, still stops `Run` unless it's ordinary lease contention.
So does any error while processing a claimed task.

## Begin and observe a drain

You supply a stable operation ID and explicit deadline:

```go
handle, err := worker.BeginDrain(ctx, runtime.DrainRequest{
    OperationID: "deployment-drain-42",
    Deadline: time.Now().Add(30 * time.Second),
})
if err != nil {
    return err
}
result, err := worker.WaitDrain(waitCtx, handle)
```

Capture the deadline once and preserve it when you retry. `BeginDrain` closes
claim admission permanently. A claim already sent to storage stays registered
while its result arrives and while the returned work finishes. The handler and
lease renewal keep their contexts during successful graceful drain. Every direct
`RunOnce` entrance uses the same admission gate.

The operation's deadline controls cancellation. `WaitDrain` only observes it, so a
short-lived request or disconnected observer cannot cancel another caller's drain.
The same operation ID and deadline recover the same handle; changed input conflicts.
A handle from another Worker incarnation is rejected.

When the operation expires, Dispatch cancels cooperative work and stops renewal.
An uncooperative Go handler stays in the in-flight count until it returns. The
result reports incomplete, and replacement workers recover through existing lease
expiry and epoch fencing. A lost claim response also prevents a successful drain:
the database may have committed a grant that the process never received. No local
counter establishes that such a grant was rolled back.

`DrainResult.Complete` preserves the operation outcome. After a timeout, a later
observation can report `Quiescent=true` while `Complete=false`. A pre-start drain
closes polling too. You need a replacement Worker to poll again.

## Engine and Forge shutdown

Engine and Extension expose `BeginDurableDrain` and `WaitDurableDrain`. Start the
explicit drain before terminal shutdown when you need graceful completion.
`Stop(ctx)` observes an accepted drain before beginning terminal quiescence. Its
context bounds that caller's wait. It does not supply a new operation deadline,
and `Stop` alone retains terminal cancellation semantics.

`StopWorkers` deliberately escalates to cancellation and waits for actual worker
quiescence. It may interrupt a graceful drain, which then reports incomplete.
Concurrent callers share the engine's existing terminal cleanup task. A timed-out
caller does not establish completion or close the store while handlers still run.

The extension keeps its authorized audit boundary and publisher available through
worker drain. Its existing delivery shutdown hook deactivates the boundary during
final teardown. A failed or timed-out Stop does not mark the extension stopped.

Queries still use the retained exact-build Worker after polling closes. They need
an available store and the original compatible workflow/query code. Full engine
Stop eventually closes storage, so retained query service must keep its own live
store/runtime or remain before that teardown.

## Evidence and remaining deployment work

Focused Memory tests exercise all six real claim paths with barriers before and
after the grant, renewal during drain, start/drain races, observer cancellation,
unknown claim outcomes, deadline expiry, replacement fencing and retained queries.
Engine and extension tests cover terminal-stop ordering, deliberate escalation,
continued authorized reads and publisher progress during drain.

Process drain does not close persisted build admission or prove historical-query
retention. Persisted retirement, writer floors, authorized lifecycle receipts and
host-verified query-removal guards require their separate deployment contracts and
qualification. This process API does not claim those capabilities.
