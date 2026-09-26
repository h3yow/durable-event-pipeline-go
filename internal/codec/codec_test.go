package codec_test

import (
	"encoding/json"
	"testing"

	"github.com/h3yow/durable-event-pipeline-go/internal/codec"
)

// TestMissingVersionDefaultsToLegacy: a message without "v" must parse as
// version 0 rather than fail or be discarded.
func TestMissingVersionDefaultsToLegacy(t *testing.T) {
	raw := []byte(`{"id":"x","event":"thing","payload":"AA=="}`)
	e, err := codec.Unmarshal(raw)
	if err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if e.Version != codec.LegacyVersion {
		t.Fatalf("version=%d, want 0", e.Version)
	}
	if e.ID != "x" || e.Event != "thing" {
		t.Fatalf("known fields not parsed: %+v", e)
	}
	out, err := codec.Marshal(e)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(out, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["v"].(float64) != float64(codec.SchemaVersion) {
		t.Fatalf("legacy message must be upgraded to current v on rewrite, got %v", fields["v"])
	}
	t.Logf("missing v -> legacy v0 accepted and upgraded on write: %s", out)
}

// TestLegacyFreeFormJSONRetained: a legacy document that has no "event"
// field is retained wholesale as payload instead of being dropped.
func TestLegacyFreeFormJSONRetained(t *testing.T) {
	raw := []byte(`{"id":"old","anything":{"nested":[1,2,3]}}`)
	e, err := codec.Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if e.Version != 0 || len(e.Payload) == 0 {
		t.Fatalf("legacy free-form not retained: %+v", e)
	}
	if string(e.Payload) != string(raw) {
		t.Fatalf("payload must retain original bytes verbatim, got %s", e.Payload)
	}
	t.Logf("legacy free-form document retained in payload (%d bytes)", len(e.Payload))
}

// TestUnknownFieldsArePreserved: fields not known to this build must survive
// a decode -> encode round-trip byte-for-byte.
func TestUnknownFieldsArePreserved(t *testing.T) {
	raw := []byte(`{"v":1,"id":"x","event":"e","future_field":{"a":1},"xflag":42}`)
	e, err := codec.Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Unknown) != 2 {
		t.Fatalf("unknown fields=%v, want 2 preserved", e.Unknown)
	}
	out, err := codec.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var in, got map[string]json.RawMessage
	_ = json.Unmarshal(raw, &in)
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	for k, v := range in {
		if string(got[k]) != string(v) {
			t.Fatalf("unknown field %q changed on round-trip: %s -> %s", k, v, got[k])
		}
	}
	t.Logf("unknown fields preserved round-trip: %s", out)
}

// TestFutureVersionDeliveredNotRejected: unknown future version is parsed,
// flagged, and re-serialized keeping the original version number.
func TestFutureVersionDeliveredNotRejected(t *testing.T) {
	raw := []byte(`{"v":99,"id":"fut","event":"e","new_stuff":[true,false]}`)
	e, err := codec.Unmarshal(raw)
	if err != nil {
		t.Fatalf("future version must not be rejected: %v", err)
	}
	if !e.UnknownVersion || e.Version != 99 {
		t.Fatalf("future version not flagged: %+v", e)
	}
	if string(e.Unknown["new_stuff"]) != "[true,false]" {
		t.Fatalf("future-only field not preserved: %s", e.Unknown["new_stuff"])
	}
	out, err := codec.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(out, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["v"].(float64) != 99 {
		t.Fatalf("future version must be preserved on rewrite, got %v", fields["v"])
	}
	t.Logf("future v99 accepted, flagged and preserved: %s", out)
}

// TestInvalidJSONErrors: genuinely corrupt bytes are reported, not silently
// accepted.
func TestInvalidJSONErrors(t *testing.T) {
	if _, err := codec.Unmarshal([]byte(`{not json`)); err == nil {
		t.Fatal("expected error for malformed message")
	}
}
