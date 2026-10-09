package security

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/id"
)

// ResourceTarget captures a typed identifier, never an arbitrary request value.
func ResourceTarget(prefix id.Prefix, raw string) (string, error) {
	value, err := id.ParseWithPrefix(raw, prefix)
	if err != nil {
		return "invalid-target", fmt.Errorf("dispatch: invalid audit resource identifier")
	}
	return string(prefix) + ":" + value.String(), nil
}

type auditSelector struct {
	Kind   string `json:"kind"`
	Name   string `json:"name,omitempty"`
	Queue  string `json:"queue,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Before string `json:"before,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
}

func selectorTarget(selector auditSelector) (string, error) {
	for _, value := range []string{selector.Name, selector.Queue} {
		if value != "" && (!durable.DeliveryIdentifier(value) || len(value) > 96) {
			return "invalid-target", fmt.Errorf("dispatch: audit selector exceeds identifier bounds")
		}
	}
	data, err := json.Marshal(selector)
	if err != nil || !durable.DeliveryIdentifier(string(data)) {
		return "invalid-target", fmt.Errorf("dispatch: invalid audit selector")
	}
	return string(data), nil
}

// CreationTarget identifies the input selector before an output ID exists.
func CreationTarget(kind, name, queue string) (string, error) {
	return selectorTarget(auditSelector{Kind: kind, Name: name, Queue: queue})
}

// BulkTarget describes the effective bounded replay or purge selection.
func BulkTarget(queue string, limit int, before time.Time, dryRun bool) (string, error) {
	if limit < 0 || limit > 1000 {
		return "invalid-target", fmt.Errorf("dispatch: invalid audit limit")
	}
	selector := auditSelector{Kind: "dlq-selection", Queue: queue, Limit: limit, DryRun: dryRun}
	if !before.IsZero() {
		selector.Before = before.UTC().Format(time.RFC3339Nano)
	}
	return selectorTarget(selector)
}
