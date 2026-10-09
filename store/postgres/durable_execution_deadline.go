package postgres

import (
	"errors"

	"github.com/xraph/dispatch/durable"
)

// Schema guards protect mixed-version deployments. Keep their rejection codes
// in the same error contract as the coordinator's checks after row locks.
func normalizeExecutionError(err error) error {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "DX001":
			return durable.ErrExecutionDeadline
		case "DX003":
			return durable.ErrLeaseLost
		case "DX002":
			return durable.ErrInvalid
		}
	}
	return err
}
