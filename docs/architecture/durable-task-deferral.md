# Workflow task deferral

A workflow decision can reference a build that is missing, retiring or retired.
The runtime preserves that decision's source history and schedules its workflow
task for another attempt. It does not turn deployment blockage into an application
failure or append the rejected child, continuation, output or consumed signals.

You can inspect the latest reason through `WorkflowTaskDeferralStore` or the
nullable `deferral` field on an authorized operator task response. The record
includes the exact target state and epoch, the source task epoch and revision,
its retry time and count. Numeric coordinates in the operator response are decimal
strings. `active` becomes false after the task is reclaimed or its run closes;
the last observation remains available.

The three refusal forms are explicit:

| State | Epoch | Reason |
| --- | --- | --- |
| Unregistered | 0 | `target_unregistered` |
| Retiring | Positive | `target_retiring` |
| Retired | Positive | `target_retired` |

The store checks these facts after locking the source execution and task under
namespace coordination. Registration or a changed state or epoch returns
`ErrAdmissionChanged` without releasing the task. This includes finalizing a
retiring build at the same epoch. The runtime then reevaluates the workflow with
its current token.

Scheduling policy 1 starts at one second, doubles and caps at thirty seconds.
The count resets when the target build, state or epoch changes. Reaching the cap
does not fail the workflow. Execution and task deadlines still apply.

The task release, schedule, immutable response, existing execution receipt and
required Chronicle intent commit together. The intent retains the trusted actor,
uses the deferral request ID and records the bounded target reason. New deferral
request IDs are limited to 256 bytes. No history sequence or execution
revision advances. The current record references the latest accepted response;
older responses retain their original retry time and count. Exact replay recovers
that response before current admission, lease, deadline or closure checks.

The runtime owns one request identity for the complete refusal. It holds the
existing renewal lock while retrying that exact request, for at most three bounded
store calls. A lost response cannot cause renewal of a token that the store may
have released. A confirmed deferral detaches renewal. An unresolved outcome stops
renewal, fails readiness and leaves drain incomplete. A lookup miss cannot prove
the write failed.

The operation stays in worker accounting until the actual call and reconciliation
return. A drain observer can time out without canceling it. The accepted drain's
own deadline still ends reconciliation; an uncooperative store call must return
before the process is quiescent. The saved receipt or original lease expiry then
governs replacement behavior.

A store wrapper must expose the optional capability to use this path. Missing
capability returns a compatibility error. Retiring a target never creates its
registration or fabricates an artifact mapping.
