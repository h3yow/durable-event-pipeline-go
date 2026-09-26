package broker_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/durable-event-pipeline-go/internal/broker"
	"github.com/h3yow/durable-event-pipeline-go/internal/codec"
)

// TestBrokerEndToEndVersionCompatibility publishes raw persisted bytes for:
//   - a legacy message lacking "v"
//   - a future-version message with an unknown field
//   - a current-version message
//
// and asserts every message reaches the handler, with deterministic
// compatibility flags and no silent field loss.
func TestBrokerEndToEndVersionCompatibility(t *testing.T) {
	var mu sync.Mutex
	got := map[string]*codec.Envelope{}
	h := func(_ context.Context, env *codec.Envelope) error {
		mu.Lock()
		got[env.ID] = env
		mu.Unlock()
		return nil
	}
	retry := broker.RetryPolicy{MaxAttempts: 1, Backoff: broker.ConstantBackoff(time.Millisecond)}
	b, cancel := newTestBroker(t, h, retry)
	defer func() { cancel(); _ = b.Close() }()
	ctx := context.Background()

	appendRaw := func(t *testing.T, raw []byte) {
		t.Helper()
		if err := b.AppendRawForTest(ctx, "p0", raw); err != nil {
			t.Fatal(err)
		}
	}

	appendRaw(t, []byte(`{"id":"legacy","event":"old-thing","payload":"AQID"}`))
	appendRaw(t, []byte(`{"v":7,"id":"future","event":"new-thing","hints":{"a":1}}`))
	appendRaw(t, []byte(`{"v":1,"id":"current","event":"thing"}`))

	waitFor(t, 5*time.Second, func() bool { return b.CommittedOffset("p0") == 3 })

	mu.Lock()
	defer mu.Unlock()
	leg, ok := got["legacy"]
	if !ok || leg.Version != codec.LegacyVersion {
		t.Fatalf("legacy message missing or wrong version: %+v", leg)
	}
	if string(leg.Payload) != "\x01\x02\x03" {
		t.Fatalf("legacy payload not delivered: % x", leg.Payload)
	}
	fut := got["future"]
	if fut == nil || !fut.UnknownVersion || fut.Version != 7 {
		t.Fatalf("future message not flagged/delivered: %+v", fut)
	}
	var hints map[string]any
	if err := json.Unmarshal(fut.Unknown["hints"], &hints); err != nil {
		t.Fatalf("future unknown field lost: %v (%v)", fut.Unknown, err)
	}
	if got["current"] == nil || got["current"].Version != codec.SchemaVersion {
		t.Fatalf("current message not delivered normally")
	}
	t.Logf("end-to-end compat OK: legacy v%d delivered, future v%d (unknownVersion=%v, unknownFields=%d), current v%d",
		leg.Version, fut.Version, fut.UnknownVersion, len(fut.Unknown), got["current"].Version)
}
