# Dispatch dashboard migration

You can track the replacement of the templ dashboard here. The approved design is
`forge-dashboard/docs/superpowers/specs/2026-10-07-dispatch-dashboard-migration-design.md`.
The old source remains in `_dashboard/` until the React plugin passes its browser
checks against fixtures and a real SQLite engine.

## Status

| Area | Implementation | Verification |
|---|---|---|
| Worker heartbeat and five-backend cursor reads | Committed in Slice 1 | Store conformance and heartbeat tests passed |
| Operator actions and REST routes | Committed in Slice 2 through `c84a71e`, including replay generation fencing | Build, unit tests, focused race tests and ordinary lint passed; final review approved |
| Engine inspection | Committed through `6de5530` | Read-only snapshot and version tests pass under race; full build/unit and ordinary lint pass; final review approved with one comment correction |
| Five-backend operator persistence | Implemented | Memory, PostgreSQL, SQLite, MongoDB and Redis exercised under race; container startup failures required serial reruns |
| Contract foundation | Committed in `10f4f90` | Request bounds, actor propagation, wire primitives and error mapping pass; build/unit/lint pass; reviewed |
| Contract domain bindings and contributor | Pending | No registered intents yet |
| React plugin, ten navigation entries | Pending | No browser evidence |
| Stateful fixtures and host wiring | Pending | No browser evidence |
| Real SQLite browser flows | Pending | Not tested |
| Templ retirement | Pending | Sources retained |

Integration-tag lint currently reports a pre-existing `err` shadow in
`store/redis/store_test.go:54`. The first combined backend run had one Redis
container startup failure and one PostgreSQL container startup skip. All 204 Redis
tests and the omitted PostgreSQL case passed when rerun serially.

## Page and supporting template inventory

All 29 templ sources are accounted for below. Detail routes replace the old
`/detail?id=...` links with encoded path parameters. Every replacement is pending
implementation and browser verification unless the status table says otherwise.

| Source under `_dashboard/` | Replacement and behavior |
|---|---|
| `pages/overview.templ` | Overview: store counts, freshness and links to filtered lists; no inferred health score |
| `pages/jobs.templ` | Jobs: state, queue, name and scope filters; cursor pages; cancel and retry |
| `pages/job_detail.templ` | Job detail: lifecycle, ownership, payload, error, lease, resources, artifact links and actions |
| `pages/workflows.templ` | Workflows: state, name and scope filters; version and cursor pages |
| `pages/workflow_detail.templ` | Run detail: ordered checkpoints, recorded version, parent and children, input, error and replay preview |
| `pages/dlq.templ` | Dead letters: unreplayed default, filters, cursor pages, bounded replay and cutoff purge preview |
| `pages/dlq_detail.templ` | Entry detail: full error and payload, original/replayed job links, replay and delete |
| `pages/queues.templ` | Queues: real per-state counts plus explicitly local limits and active count |
| `pages/queue_detail.templ` | Queue detail: configuration and cursor-paged jobs for that queue |
| `pages/workers.templ` | Workers: heartbeat age, queues, concurrency, self and leader |
| `pages/worker_detail.templ` | Worker detail: capacity; resource leases for the serving process only |
| `pages/crons.templ` | Cron: enabled status, schedule, upcoming fires, toggles and run now |
| `pages/cron_detail.templ` | Cron detail: raw schedule, location, next five fires, lock, payload, toggle, run now and delete |
| `pages/handlers.templ` | Handlers: jobs and workflow definitions; new detail routes expose declarations and all versions |
| `pages/helpers.templ` | Shared timestamps, duration formatting, cron descriptions and cursor controls |
| `components/empty_state.templ` | Shared design-system empty state; distinct initial, filtered and incomplete-search results |
| `components/footer_links.templ` | Host navigation owns footer links; retain API documentation access where the host supports it |
| `components/json_viewer.templ` | Lazy read-only JSON editor; opaque binary and gob values get byte counts |
| `components/page_header.templ` | Shared compact page header |
| `components/path_rewriter.templ` | Scope-relative `PluginLink` and encoded path parameters |
| `components/stat_card.templ` | Shared compact stat tiles |
| `components/state_badge.templ` | Shared state badges |
| `settings/config.templ` | Read-only Engine page, with per-process settings labelled |
| `widgets/stats.templ` | Overview job/run counts; standalone host widget slot unavailable |
| `widgets/recent_jobs.templ` | Jobs list in newest-first order; standalone host widget slot unavailable |
| `widgets/dlq_count.templ` | Overview unreplayed count and DLQ list; standalone host widget slot unavailable |
| `widgets/queue_activity.templ` | Queues page; standalone host widget slot unavailable |
| `widgets/cluster_health.templ` | Workers page with heartbeat age instead of a health verdict; standalone host widget slot unavailable |
| `widgets/cron_schedule.templ` | Cron page with upcoming times; standalone host widget slot unavailable |

The eight old navigation entries become ten: Overview, Jobs, Workflows, Dead
letters, Artifacts, Queues, Workers, Cron, Handlers and Engine. The six widget IDs
(`dispatch-stats`, `dispatch-recent-jobs`, `dispatch-dlq`,
`dispatch-queue-activity`, `dispatch-cluster-health`, `dispatch-cron-schedule`)
and settings ID `dispatch-config` are recorded here so deleting their registration
cannot be mistaken for implementing new host extension points.

## Capabilities beyond the old pages

| Capability | Migration treatment |
|---|---|
| Operator-wide access | Explicit UI and manifest description; app/org filters, no principal-claim scoping |
| Store read failures | Return errors, retain stale displayed data and show its timestamp |
| Cursor coverage | `complete` describes search coverage; only absent `nextCursor` ends paging |
| Running cancellation | Persist cancellation; worker observes it during renewal; handlers must honor context |
| Retry and DLQ replay | Engine paths with claim protection, preserved payload/resources and operator events |
| DLQ delete and arbitrary cutoff purge | Confirm exact entry or count from preview; bulk replay reports partial results |
| Cron run now and timezone previews | Engine enqueue without moving the schedule; timezone-aware next fires |
| Workflow replay | Recorded version, checkpoint preview, conditional claim and engine-owned execution |
| Workflow checkpoint payloads | JSON rendered when valid; gob labelled without attempting browser decoding |
| Artifact plane | Optional: list/detail, lifecycle/scope filters, links and optional short-lived downloads |
| Execution and resource configuration | Actual registered executors, declarations, capacity, leases and local settings |
| Missing optional capabilities | `enabled: false`; never represent an absent capability as a healthy empty fleet |
| Notifications and live data | Visible polling through one hook; shared SDK subscriptions are a separate project |
| Job usage | Deferred until xraph/dispatch#33 merges; no fabricated usage panel |

## Known boundaries

`ResumeAll` still runs during startup without a cross-instance execution lock.
Ordinary workflow starts still execute in their caller. Neither is claimed as fixed
by the replay operation.

Replay claims now compare a stored generation as well as the state. An overlapping
request cannot reopen the run after its competitor has already finished. Store
updates also reject an older generation, and SQL migrations default existing runs
to generation zero. MongoDB documents and Redis records without the field use zero.
Every node serving replay actions must run this version; an older binary has no
generation check. Custom workflow stores must implement the new `ReopenRun`
argument and preserve the generation on reads and conditional updates.

Cron and cron-fired jobs do not acquire tenant scope. Empty scope filters mean
every scope. The dashboard is an operator tool, so these boundaries must remain
visible instead of being inferred from principal claims.

We will record further review findings here with their resolution before retiring
the old source. No passing unit suite establishes browser parity.

## Retirement gate

- [ ] All contract intents, capability responses, invalidations and error mappings verified.
- [ ] All ten pages and their detail routes tested at desktop and narrow widths.
- [ ] Every operator action verified through the HTTP contract with stateful fixtures.
- [ ] Real SQLite engine browser flows verify persisted reads and actions.
- [ ] Empty, filtered, incomplete, denied, stale and failed states checked.
- [ ] Old source removed in a focused commit after the checks above.
- [ ] Direct templ/forgeui dependencies and obsolete Makefile targets removed; build and tests pass.

Standalone host widgets remain a documented host limitation. Their data belongs on
the replacement pages; widget registration itself is not part of the retirement
claim.
