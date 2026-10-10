package postgres

import (
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xraph/dispatch/durable"
)

// Schema guards protect mixed-version deployments. Keep their rejection codes
// in the same error contract as the coordinator's checks after row locks.
func normalizeExecutionError(err error) error {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "DL001":
			return errors.Join(durable.ErrWriterCompatibility, err)
		case "DL002":
			return errors.Join(durable.ErrLifecycleBusy, err)
		case "DL003":
			var native *pgconn.PgError
			if errors.As(err, &native) {
				var refusal durable.BuildAdmissionError
				if json.Unmarshal([]byte(native.Detail), &refusal) == nil && durable.ValidateBuildID(refusal.BuildID) == nil {
					return errors.Join(&refusal, err)
				}
			}
			return errors.Join(durable.ErrBuildAdmission, err)
		case "DX001":
			return durable.ErrExecutionDeadline
		case "DX003":
			return durable.ErrLeaseLost
		case "DX002", "DX004":
			return durable.ErrInvalid
		}
	}
	return err
}
