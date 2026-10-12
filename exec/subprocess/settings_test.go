package subprocess

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSettingsExcludeEnvironmentAndReportCoreLimit(t *testing.T) {
	e := New(WithEnv(map[string]string{"API_KEY": "private-value"}), WithArgs("secret-arg"),
		WithUser(123, 456), WithRlimits(Rlimits{AddressSpace: 1024, Core: 100}),
		WithStrictRlimits(), WithScratchDir("/scratch"))
	got := e.Settings()
	if !got.UserConfigured || got.UID != 123 || got.GID != 456 || !got.HasRlimits ||
		got.Rlimits.AddressSpace != 1024 || got.Rlimits.Core != 0 || !got.StrictRlimits || got.ScratchDir != "/scratch" {
		t.Fatalf("settings = %+v", got)
	}
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private-value") || strings.Contains(string(data), "secret-arg") {
		t.Fatal("settings exposed secrets")
	}
	got.Rlimits.AddressSpace = 999
	if e.Settings().Rlimits.AddressSpace != 1024 {
		t.Fatal("caller changed limits")
	}
}
