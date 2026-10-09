package contract

import (
	"context"

	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/security"
)

func testPrincipal() fc.Principal { return fc.Principal{User: &dashauth.UserInfo{Subject: "operator"}} }
func testContext() context.Context {
	return dashauth.WithUser(context.Background(), testPrincipal().User)
}
func testBoundary() security.Boundary {
	return security.Boundary{Resource: security.Resource{InstallationID: "test", PolicyTenant: "test"}, Authorizer: security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return nil })}
}
