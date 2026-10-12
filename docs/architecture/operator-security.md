# Dispatch operator security

Remote access requires a verified identity and an explicit installation grant.
Without security configuration, REST, DWP and dashboard intents deny access.
Trusted Go engine calls remain available to your application.

Existing job, checkpoint workflow, cron, DLQ, artifact and operational APIs are
installation-wide. Their scope and queue parameters are filters. They do not
isolate one tenant or namespace, and must never determine a policy tenant or
installation identifier. Namespace-scoped durable APIs follow in a separate slice.

## Configure your Forge host

Install Forge's authentication registry and Warden in the host container. Authsome
registers its session provider through that registry. Dispatch resolves both
services when a request arrives, so registering Dispatch before Authsome does not
introduce a startup dependency or permanently cache a missing provider.

Set the installation identifier and the tenant holding its Warden policies:

```go
package operatorhost

import (
    "github.com/xraph/dispatch"
    "github.com/xraph/dispatch/extension"
    "github.com/xraph/forge"
    "github.com/xraph/forge/extensions/auth"
    "github.com/xraph/vessel"
    "github.com/xraph/warden"
)

// Mount receives the host's configured provider registry, policy engine and store.
// The registry must contain a provider that publishes trusted credential provenance.
func Mount(app forge.App, registry auth.Registry, policies *warden.Engine, store dispatch.Storer) (*extension.Extension, error) {
    if err := vessel.Provide(app.Container(), func() auth.Registry { return registry }); err != nil {
        return nil, err
    }
    if err := vessel.Provide(app.Container(), func() *warden.Engine { return policies }); err != nil {
        return nil, err
    }
    e := extension.New(
        extension.WithStore(store),
        extension.WithOperatorSecurity(extension.SecurityConfig{
            InstallationID: "production-dispatch",
            PolicyTenant: "operations",
            Providers: []string{"session"},
        }),
        extension.WithDWP(),
    )
    if err := e.Register(app); err != nil {
        return nil, err
    }
    return e, nil
}
```

An assembled Forge host that already installed these services only needs the
extension options above. Do not provide duplicate container registrations. Forge
owns extension Start and Stop. WithOperatorSecurity defaults to the `session`
provider if you omit Providers. YAML provides the same host configuration:

```yaml
extensions:
  dispatch:
    security:
      installation_id: production-dispatch
      policy_tenant: operations
      providers: [session]
    enable_dwp: true
```

The Warden resource is `dispatch_installation:production-dispatch` in the configured
policy tenant, at that tenant's root policy namespace. Request context cannot
select a different tenant or descendant policy namespace. Subject kinds are
`user`, `service`, `api_key` and `service_acct`. Authsome's `service_account` kind
maps to `service_acct`; explicit unsupported or malformed kinds deny. A verified
legacy human identity may omit the kind. It still needs a nonempty subject.

## Grant the required actions

| Action | Access |
| --- | --- |
| `dispatch.installation.read` | Installation metadata and operational queries |
| `dispatch.installation.write` | Job, workflow, cron and DLQ commands |
| `dispatch.installation.payload.read` | Payloads, credentials, configuration, checkpoint data or signed artifact URLs |
| `dispatch.installation.subscribe` | DWP subscriptions and stream admission |
| `dispatch.installation.federation` | DWP federation commands |

Payload access is an additional check. REST job, run, DLQ and cron details/lists,
replay plans and commands returning those objects require it. Dashboard job,
workflow, DLQ and cron queries, engine.config and artifacts.presign require it,
as do commands returning their data. DWP job.get, workflow.get, workflow.timeline
and every subscription require it. Metadata and payload cannot be separated in
these legacy responses, so denying either denies the whole response.

The closed operation tables are `security.RESTOperation`, `ContractOperation` and
`DWPOperation`. REST uses the registered HTTP method and route template. Contracts
use the registered intent; every manifest intent has the named
`dispatchInstallationOperator` Warden delegate and the handler checks again.
DWP's common handler checks direct dispatch too. Unknown operations deny.

Use Warden policies or role permissions to grant these actions to the exact
operator or supported machine identity for this resource. For example, a policy
can grant metadata read to one subject:

```go
package operatorhost

import (
    "context"
    "github.com/xraph/warden/id"
    "github.com/xraph/warden/policy"
    "github.com/xraph/warden/store/memory"
)

func GrantMetadataRead(ctx context.Context, store *memory.Store, subject string) error {
    return store.CreatePolicy(ctx, &policy.Policy{
        ID: id.NewPolicyID(), TenantID: "operations", Name: "dispatch metadata operator",
        IsActive: true, Effect: policy.EffectAllow,
        Subjects: []policy.SubjectMatch{{Kind: "user", ID: subject}},
        Actions: []string{"dispatch.installation.read"},
        Resources: []string{"dispatch_installation:production-dispatch"},
    })
}
```

Warden errors deny access. Dispatch uses Check and rejects every returned policy
obligation because this slice has no obligation executor. An allow decision with
`require-mfa`, audit or another obligation is therefore denied, never silently
accepted. REST and RPC return 401 for missing identity, 403 for denial and a
sanitized 503 for unavailable policy evaluation. Authentication and policy calls
have a five-second deadline. Custom providers must honor that context.

## Credentials and browser requests

Stock REST/DWP authentication accepts an explicit Authorization Bearer/DPoP
credential or a DWP RPC/WebSocket frame token. SSE uses Authorization; tokens in
query parameters do not authenticate. Dispatch removes Cookie before provider
validation and requires the provider's trusted
`AuthContext.Metadata["credential_scheme"]` to be `bearer` or `dpop`, matching the
presentation. It preserves the real request context, method, URL and DPoP proof.
A cookie bridged upstream into Authorization remains a cookie in trusted provider
metadata and is rejected.

The Authsome compatibility floor is commit
`458d1ce55a70b094830dfdb6af4ad53aa1da7002`, which publishes provenance after successful
session validation for both human and machine callers. Until a release includes
that commit, older session providers fail closed on stock REST/DWP access. A
provider returning nil success, empty identity, malformed claims or absent
provenance also denies. A token-only DWP call cannot validate HTTP-bound proof and
the Forge DWP adapter rejects it.

Browser dashboard contracts retain Forge's verified browser identity and CSRF
transport. Cookie-only REST/DWP clients must migrate to that transport or present
an explicit credential. WithRemoteSecurity and api.WithSecurity allow an explicit
host-owned authenticator and authorizer. Those custom authenticators own their
cookie, origin, CSRF and credential provenance safeguards. DWP WithAuth alone does
not grant authority, even for NoopAuthenticator or wildcard legacy scope strings;
you must also configure its security boundary. NoopAuthenticator is an explicit
development opt-in.

WebSocket admission has a five-second authentication deadline. Passive WebSocket
and SSE subscriptions recheck operator subscription and payload grants every five
seconds and before each forwarded event. Incoming operations, ping and credit
frames check their grants again. Streams expire after five minutes and must
reauthenticate; idle streams close on expiry or policy revocation. Warden calls
can consume up to the five-second policy deadline, so an idle revocation closes
within one recheck interval plus that deadline.

## Configure durable workers

The durable runtime is opt-in and requires a store implementing durable.Store.
WithDurableWorkflows receives complete Go runtime options and takes precedence
over YAML worker routing/timing as one unit:

```go
package operatorhost

import (
    "github.com/xraph/dispatch/durable/runtime"
    "github.com/xraph/dispatch/extension"
)

func DurableOption() extension.ExtOption {
    return extension.WithDurableWorkflows(runtime.Options{
        Namespace: "operations", Queue: "durable", BuildID: "orders-v1", Owner: "worker-1",
        Workflows: map[string]runtime.WorkflowFunc{
            "order": func(*runtime.Workflow, []byte) ([]byte, error) { return []byte("completed"), nil },
        },
    })
}
```

For YAML routing, register Go functions with WithDurableHandlers instead:

```yaml
extensions:
  dispatch:
    durable:
      enabled: true
      namespace: operations
      queue: durable
      build_id: orders-v1
      owner: worker-1
      lease_duration: 30s
      poll_interval: 100ms
      store_timeout: 5s
      concurrency: 1
```

```go
package operatorhost

import (
    "github.com/xraph/dispatch/durable/runtime"
    "github.com/xraph/dispatch/extension"
)

func DurableHandlers() extension.ExtOption {
    return extension.WithDurableHandlers(map[string]runtime.WorkflowFunc{
        "order": func(*runtime.Workflow, []byte) ([]byte, error) { return []byte("completed"), nil },
    }, nil)
}
```

Register captures and validates the configured runtime exactly once during engine
construction. Registration maps are copied when options are created, when applied
to an extension and when the worker is built. Empty registrations, nil/invalid
handlers, missing namespace/queue/build/owner, invalid timing/concurrency and an
unsupported store fail registration. YAML values take precedence over the
corresponding Config fields; programmatic fields fill omissions, and Enabled is
an OR. Full WithDurableWorkflows options replace that merged configuration.

Extension Start runs migrations and starts the durable worker. Stop uses the
caller's deadline to bound draining. Health delegates to Engine.Health, including
worker processing failure even when storage Ping succeeds. Disabled durable
configuration retains the existing trusted checkpoint execution API.

Dispatch imports Forge authentication interfaces and Warden only. Authsome already
depends on Dispatch, so Dispatch must not import Authsome or Ctrlplane. This slice
does not establish namespace isolation, transactional Chronicle acceptance, outbox
delivery, assembled production deployment or full ecosystem qualification.
