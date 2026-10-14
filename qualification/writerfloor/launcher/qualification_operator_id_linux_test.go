//go:build linux

package launcher

import (
	"strings"
	"testing"
)

func TestQualificationControllerID(t *testing.T) {
	for _, value := range []string{"", "operator", "recovery", strings.Repeat("a", 32)} {
		got, err := qualificationControllerID(value)
		expected := value
		if expected == "" {
			expected = "operator"
		}
		if err != nil || got != expected {
			t.Fatal("fixed controller identity", value, got, err)
		}
	}
	for _, value := range []string{"../operator", "operator/recovery", "operator.recovery", " recovery", strings.Repeat("a", 33)} {
		if _, err := qualificationControllerID(value); err == nil {
			t.Fatal("unsafe controller identity", value)
		}
	}
}
