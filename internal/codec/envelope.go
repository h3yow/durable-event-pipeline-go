// Package codec defines the on-disk message envelope and its
// forward/backward compatible (de)serialization rules.
package codec

import "encoding/json"

// SchemaVersion is the envelope version written by this implementation.
const SchemaVersion = 1

// LegacyVersion is assigned when a persisted message has no version field.
const LegacyVersion = 0

// Envelope wraps a business event with routing and version metadata.
//
// JSON field tags are part of the persisted format and must not be renamed.
type Envelope struct {
	Version int               `json:"v"`
	ID      string            `json:"id"`
	Key     string            `json:"key,omitempty"`
	Event   string            `json:"event"`
	Payload []byte            `json:"payload,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`

	// UnknownVersion is set when "v" is greater than any version this
	// build knows about. The message is still delivered; handlers may use
	// the flag to apply explicit downgrade logic.
	UnknownVersion bool `json:"-"`

	// Unknown preserves fields this build does not recognise so they are
	// never silently dropped on round-trip (forward compatibility).
	Unknown map[string]json.RawMessage `json:"-"`
}

var knownFields = map[string]struct{}{
	"v":       {},
	"id":      {},
	"key":     {},
	"event":   {},
	"payload": {},
	"headers": {},
}

// Marshal serializes an envelope using the current schema version.
// Unknown fields preserved from a previous Unmarshal are written back
// verbatim; known fields take precedence on name collisions.
func Marshal(e *Envelope) ([]byte, error) {
	out := make(map[string]json.RawMessage, len(e.Unknown)+6)
	for k, v := range e.Unknown {
		if _, ok := knownFields[k]; ok {
			continue
		}
		out[k] = v
	}

	version := e.Version
	if version <= 0 {
		version = SchemaVersion
	}
	put := func(key string, value any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		out[key] = raw
		return nil
	}
	if err := put("v", version); err != nil {
		return nil, err
	}
	if err := put("id", e.ID); err != nil {
		return nil, err
	}
	if e.Key != "" {
		if err := put("key", e.Key); err != nil {
			return nil, err
		}
	}
	if err := put("event", e.Event); err != nil {
		return nil, err
	}
	if len(e.Payload) > 0 {
		if err := put("payload", e.Payload); err != nil {
			return nil, err
		}
	}
	if len(e.Headers) > 0 {
		if err := put("headers", e.Headers); err != nil {
			return nil, err
		}
	}
	return json.Marshal(out)
}

// Unmarshal parses an envelope under deterministic compatibility rules:
//
//   - Missing "v" field: the message is treated as LegacyVersion (0).
//     Known fields are parsed as usual; if no "event" field is present the
//     complete raw bytes are retained in Payload so legacy free-form JSON
//     messages are never silently dropped.
//   - Unknown future "v": every known field is still parsed, unknown
//     fields/version are preserved and UnknownVersion is set to true.
//   - Unknown fields of any version: retained verbatim in Unknown and
//     written back unchanged by Marshal.
func Unmarshal(data []byte) (*Envelope, error) {
	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	e := &Envelope{Unknown: map[string]json.RawMessage{}}
	if vraw, ok := raw["v"]; ok {
		if err := json.Unmarshal(vraw, &e.Version); err != nil {
			return nil, err
		}
	} else {
		e.Version = LegacyVersion
	}
	if e.Version > SchemaVersion {
		e.UnknownVersion = true
	}

	if rawID, ok := raw["id"]; ok {
		if err := json.Unmarshal(rawID, &e.ID); err != nil {
			return nil, err
		}
	}
	if rawKey, ok := raw["key"]; ok {
		if err := json.Unmarshal(rawKey, &e.Key); err != nil {
			return nil, err
		}
	}
	if rawEvent, ok := raw["event"]; ok {
		if err := json.Unmarshal(rawEvent, &e.Event); err != nil {
			return nil, err
		}
	}
	if rawPayload, ok := raw["payload"]; ok {
		if err := json.Unmarshal(rawPayload, &e.Payload); err != nil {
			return nil, err
		}
	}
	if rawHeaders, ok := raw["headers"]; ok {
		if err := json.Unmarshal(rawHeaders, &e.Headers); err != nil {
			return nil, err
		}
	}

	_, hasEvent := raw["event"]
	if e.Version == LegacyVersion && !hasEvent {
		// Legacy free-form message: retain the whole document as payload.
		e.Payload = append(e.Payload[:0:0], data...)
	}

	for k, v := range raw {
		if _, known := knownFields[k]; known {
			continue
		}
		e.Unknown[k] = v
	}
	return e, nil
}
