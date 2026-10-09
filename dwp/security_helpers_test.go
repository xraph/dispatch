package dwp

import (
	"context"

	"github.com/xraph/dispatch/security"
)

func testBoundary() security.Boundary {
	return security.Boundary{Resource: security.Resource{InstallationID: "test", PolicyTenant: "test"}, Authorizer: security.AuthorizerFunc(func(_ context.Context, p security.Principal, action string, _ security.Resource) error {
		if p.Subject == "limited-user" && action != security.OperatorRead {
			return security.ErrForbidden
		}
		return nil
	})}
}
