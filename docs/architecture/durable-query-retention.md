# Retain query service while retiring builds

You can stop polling and retire execution admission while keeping closed history queryable. Register a query runtime against the exact build artifact and configuration, then record evidence from your trusted verifier. A worker's ability to name a build is not evidence that it can query that build's retained history.

Historical retirement enrollment creates admission records only. It leaves artifact identity unknown. `RegisterBuild` can fill that identity once, using the current build version and trusted enrollment evidence. It preserves admission state and epoch. You cannot replace an established identity with a convenient mapping to another artifact.

`QueryRuntimeStore` is optional. Its bindings have an immutable runtime incarnation, host instance, artifact digest, configuration digest and probe policy. State changes advance a separate version. Store mutations, their accepted receipts and required Chronicle intents commit together. Both Memory and PostgreSQL recover exact accepted receipts before checking today's conditions.

A proof names the exact binding, configured verifier and policy version. It includes an evidence digest, verification time and bounded expiry. You choose the policy and maximum validity during trusted enrollment. Dispatch supplies no default compatibility inference. A local probe of one retained workflow demonstrates that case; it does not establish compatibility with arbitrary histories or query handlers.

## Finalization and removal

Finalizing a build with retained executions requires an active binding with current verification. PostgreSQL also enforces this condition for older protocol-one control callers. They may receive SQLSTATE `DL004` from their unchanged library. The current library maps it to `ErrQueryRetention`. Writer protocol one alone does not qualify a controller to manage query retention.

`InspectCompatibility.QueryRetentionSchemaVersion` reports store schema capability. Keep it separate from host artifact verification and controller qualification. An old controller may remain compatible for its existing operations while lacking the query retention contract. Query schema downgrade is refused because it would remove persisted safety conditions.

Begin removal reserves a binding and selects a verified survivor on a different instance under exclusive namespace coordination. A removing binding cannot serve as another reservation's survivor. Accepting or retiring builds require a survivor even without retained executions. A retired build with no retained executions can remove its last binding.

Check the accepted fence again before an external removal. The check compares current build state, epoch and version, candidate identity and removal epoch, and the exact survivor version and proof. Time is read after coordination and row waits. An accepted receipt proves the original reservation, not current permission to delete an instance. Finish records the external result against that original reservation; it does not renew an expired permission.

Before deleting an instance that serves several bindings, your controller must enumerate its bindings across every relevant namespace and build. A successful check for one binding does not authorize deletion of the whole instance.

## Abort requires settled deletion

You cannot abort an unknown deletion just because the instance still answers queries. The original deletion might complete later, after another removal has relied on the reactivated binding.

The trusted `QueryRemovalAbortVerifier` first settles the exact reservation's external operation, then captures fresh existence and query evidence. Its settlement names the accepted fence digest, stable host operation identity, configured verifier, evidence digest and settlement time. Allocate the host operation identity before dispatch. It need not be a provider acceptance ID.

The supported outcomes have specific meanings:

- `not_issued`: the controller atomically revoked future issuance. A stale worker cannot dispatch the operation afterward.
- `rejected`: the provider definitively rejected this operation without deleting the instance.
- `cancelled`: the operation definitively settled without deletion. Cancelling an HTTP context does not qualify.
- `fenced`: issuance was prevented, or the provider enforces a fence that prevents the outstanding deletion from completing.
- `settled_without_deletion`: the trusted controller has other definitive evidence that this exact operation settled without deletion.

A local ledger update, expired lease, transport timeout or fresh health probe cannot fence a deletion already sent to a provider. Unknown and in-flight outcomes remain removing. Dispatch does not claim that an arbitrary Docker DELETE can be cancelled after unknown acceptance.

Abort validates the current removing identity, state version and epoch under coordination. The fresh query proof must follow settlement and remain valid after lock waits. The immutable abort receipt retains the fence, settlement and accepted proof. Exact replay can recover that receipt after later state changes without reactivating the binding again.

Remote controls must resolve artifact mappings, capture verification and settle removal through trusted host implementations. Do not accept those facts from request JSON. Recover the stable command receipt before regenerating either proof. Store interfaces are trusted internal boundaries, not public proof submission endpoints.

## Qualification boundaries

The protocol-one qualification fixture uses an unchanged published Dispatch library in live processes across schema expansion. It covers refusal without a mapping, without proof, after expiry and while the only valid binding is removing. It also preserves old registration fingerprints, accepted receipt replay and pending Chronicle source verification.

Its owned controller can atomically prevent a never-issued removal and reject an issued operation with unknown outcome. That proves this fixture's issuance behavior. Persisted provider operation reconciliation and actual Docker removal outcomes require host qualification.

The PostgreSQL row fallback never acquires or upgrades exclusive coordination while holding rows. It fails with `DL002` when coordination is missing. `DL001` remains writer compatibility refusal, `DL003` target admission refusal and `DL004` query retention refusal. Keep these classifications separate when deciding whether to retry.
