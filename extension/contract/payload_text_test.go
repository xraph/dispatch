package contract

import (
	"encoding/json"
	"testing"
)

func TestPayloadTextPreservesJSONLexemes(t *testing.T) {
	source := []byte("{\n\"id\":9007199254740993,\"price\":1.2300,\"text\":\"<tag>\"\n}")
	want := string(source)
	projected := projectPayload(source, false)
	source[0] = '['
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	var browser map[string]any
	if decodeErr := json.Unmarshal(encoded, &browser); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if browser["jsonText"] != want {
		t.Fatalf("raw JSON text = %q, want %q", browser["jsonText"], want)
	}
	for _, raw := range []string{"null", "true", "123", "\"text\"", "[]", "{}"} {
		encoded, err = json.Marshal(projectPayload([]byte(raw), true))
		if err != nil {
			t.Fatal(err)
		}
		if decodeErr := json.Unmarshal(encoded, &browser); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if browser["jsonText"] != raw {
			t.Fatalf("scalar %q = %#v", raw, browser)
		}
	}
	for _, checkpoint := range []bool{false, true} {
		encoded, err = json.Marshal(projectPayload([]byte{0, 255}, checkpoint))
		if err != nil {
			t.Fatal(err)
		}
		var opaque map[string]any
		if decodeErr := json.Unmarshal(encoded, &opaque); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if _, exposed := opaque["jsonText"]; exposed {
			t.Fatalf("opaque text leaked: %s", encoded)
		}
	}
}
