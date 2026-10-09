package postgres

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/xraph/dispatch/durable"
)

func encodeWorkflowRetryPolicy(policy *durable.WorkflowRetryPolicy) (any, error) {
	if policy == nil {
		return nil, nil
	}
	return json.Marshal(policy)
}

func decodeWorkflowRetryPolicy(payload []byte, policy **durable.WorkflowRetryPolicy) error {
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(policy); err != nil {
		return err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return durable.ErrInvalid
	}
	normalized, err := durable.NormalizeWorkflowRetryPolicy(*policy)
	if err != nil || !durable.SameWorkflowRetryPolicy(normalized, *policy) {
		return durable.ErrInvalid
	}
	return nil
}
