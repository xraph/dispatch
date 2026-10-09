package contract

import (
	"os"
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	pgstore "github.com/xraph/dispatch/store/postgres"
)

func TestDurableNamespacePostgresHTTP(t *testing.T) {
	dsn := os.Getenv("DISPATCH_READ_TEST_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_READ_REQUIRED") == "1" {
			t.Fatal("dedicated PostgreSQL fixture required")
		}
		t.Skip("dedicated PostgreSQL fixture required")
	}
	driver := pgdriver.New()
	if err := driver.Open(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(driver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	runDurableContract(t, durableDeps(t, pgstore.New(db)))
}
