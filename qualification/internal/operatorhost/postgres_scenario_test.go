package operatorhost

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// isolatedPostgresScenario keeps lifecycle enrollment and executable identity
// separate from other scenarios while allowing one scenario to restart a host.
func isolatedPostgresScenario(t *testing.T) string {
	t.Helper()
	base := os.Getenv("DISPATCH_OPERATOR_DSN")
	if base == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("dedicated PostgreSQL required")
		}
		t.Skip("dedicated PostgreSQL required")
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" {
		t.Fatal("invalid PostgreSQL fixture configuration")
	}
	admin, err := pgx.Connect(t.Context(), base)
	if err != nil {
		t.Fatal("PostgreSQL fixture administration unavailable")
	}
	name := "dispatch_operator_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(t.Context(), "CREATE DATABASE "+identifier); err != nil {
		_ = admin.Close(context.Background())
		t.Fatal("could not create isolated PostgreSQL scenario")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, dropErr := admin.Exec(ctx, "DROP DATABASE "+identifier+" WITH (FORCE)"); dropErr != nil {
			t.Error("could not remove isolated PostgreSQL scenario")
		}
		if closeErr := admin.Close(ctx); closeErr != nil {
			t.Error("could not close PostgreSQL fixture administration")
		}
	})
	parsed.Path = "/" + name
	return parsed.String()
}
