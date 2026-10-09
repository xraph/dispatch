package ecosystem

import (
	"context"
	"encoding/json"

	"github.com/xraph/relay/catalog"
)

// RegisterRelaySchema installs the actual schema in the sink catalog. Hosts call
// this before admitting requests, using their trusted app identity.
func RegisterRelaySchema(ctx context.Context, c *catalog.Catalog, appID string) error {
	properties := map[string]any{}
	required := []string{"ID", "Destination", "SchemaVersion", "InstallationID", "Namespace", "AppID", "TenantID", "WorkflowID", "RunID", "SourceKind", "SourceID", "Sequence", "OccurredAt", "Action", "Outcome", "Target", "Metadata", "Fingerprint"}
	for _, key := range required {
		properties[key] = map[string]any{"type": "string"}
	}
	properties["Destination"] = map[string]any{"const": "relay"}
	properties["SchemaVersion"] = map[string]any{"const": MappingVersion}
	properties["SourceKind"] = map[string]any{"const": "event"}
	properties["Sequence"] = map[string]any{"type": "integer", "minimum": 0}
	metadata := map[string]any{}
	metadataKeys := []string{"ActorKind", "ActorID", "RequestID", "CorrelationID", "DecisionID", "PolicyVersion", "ReasonCode"}
	for _, key := range metadataKeys {
		metadata[key] = map[string]any{"type": "string"}
	}
	properties["Metadata"] = map[string]any{"type": "object", "required": metadataKeys, "properties": metadata, "additionalProperties": false}
	raw, err := json.Marshal(map[string]any{"type": "object", "required": required, "properties": properties, "additionalProperties": false})
	if err != nil {
		return err
	}
	_, err = c.RegisterType(ctx, catalog.WebhookDefinition{Name: RelayEventType, Group: "dispatch", Description: "Accepted durable Dispatch event", Schema: raw, SchemaVersion: "1", Version: "1"}, catalog.WithScopeAppID(appID))
	return err
}
