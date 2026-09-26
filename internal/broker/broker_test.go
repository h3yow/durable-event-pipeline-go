package broker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/durable-event-pipeline-go/internal/broker"
	"github.com/h3yow/durable-event-pipeline-go/internal/codec"
)

func newTestBroker(t *testing.T, h broker.Handler, retry broker.RetryPolicy) (*broker.Broker, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	var mu sync.Mutex
	logf := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		t.Logf("[broker] "+format, args...)
	}
	cfg := broker.Config{
		Dir:        dir,
		Group:      "test-group",
		Handler:    h,
		Retry:      retry,
		PollTick:   2 * time.Millisecond,
		Checkpoint: 10 * time.Millisecond,
		Logf:       logf,
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Start(ctx) }()
	return b, cancel
}

func newEnvelope(id, key, event string) *codec.Envelope {
	return &codec.Envelope{Version: codec.SchemaVersion, ID: id, Key: key, Event: event, Payload: []byte(`{}`)}
}

// waitFor polls cond until it returns true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// TestPartitionOrderAndConcurrency verifies per-partition strict offset
// order while independent partitions make concurrent progress.
func TestPartitionOrderAndConcurrency(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][]int64{}
	inHandler := map[string]bool{}
	maxConcurrent := 0

	h := func(_ context.Context, env *codec.Envelope) error {
		mu.Lock()
		p := env.Key
		inHandler[p] = true
		if len(inHandler) > maxConcurrent {
			maxConcurrent = len(inHandler)
		}
		// Encode offset in the event name for ordering assertions.
		var off int64
		fmt.Sscanf(env.Event, "e%d", &off)
		seen[p] = append(seen[p], off)
		mu.Unlock()

		time.Sleep(5 * time.Millisecond) // widen the concurrency window

		mu.Lock()
		delete(inHandler, p)
		mu.Unlock()
		return nil
	}

	retry := broker.RetryPolicy{MaxAttempts: 1, Backoff: broker.ConstantBackoff(time.Millisecond)}
	b, cancel := newTestBroker(t, h, retry)
	defer func() { cancel(); _ = b.Close() }()

	const parts = 3
	const perPart = 30
	ctx := context.Background()
	for i := 0; i < perPart; i++ {
		for p := 0; p < parts; p++ {
			part := fmt.Sprintf("p%d", p)
			env := newEnvelope(fmt.Sprintf("%s-%d", part, i), part, fmt.Sprintf("e%d", i))
			if _, err := b.PublishTo(ctx, part, env); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}

	waitFor(t, 30*time.Second, func() bool {
		for p := 0; p < parts; p++ {
			if b.CommittedOffset(fmt.Sprintf("p%d", p)) != int64(perPart) {
				return false
			}
		}
		return true
	})

	mu.Lock()
	defer mu.Unlock()
	for p := 0; p < parts; p++ {
		name := fmt.Sprintf("p%d", p)
		got := seen[name]
		if len(got) != perPart {
			t.Fatalf("%s: got %d deliveries, want %d", name, len(got), perPart)
		}
		for i, off := range got {
			if off != int64(i) {
				t.Fatalf("%s: out-of-order delivery at position %d: got offset %d, want %d",
					name, i, off, i)
			}
		}
	}
	if maxConcurrent < 2 {
		t.Fatalf("expected concurrent handlers across partitions, max observed %d", maxConcurrent)
	}
	t.Logf("partition order verified; max concurrent in-flight partitions=%d", maxConcurrent)
}
