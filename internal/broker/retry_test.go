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

// TestRetryBackoffAndDeadLetter covers:
//   - a retryable failure is redelivered up to MaxAttempts then dead-lettered
//     with reason "max-attempts-exceeded:..."
//   - a non-retryable failure bypasses retries with its explicit reason
//   - the dead-lettered offset still advances (later messages are consumed)
//   - a retrying partition never blocks an independent partition
func TestRetryBackoffAndDeadLetter(t *testing.T) {
	var mu sync.Mutex
	attempts := map[string]int{}

	h := func(_ context.Context, env *codec.Envelope) error {
		mu.Lock()
		attempts[env.ID]++
		n := attempts[env.ID]
		mu.Unlock()

		switch env.Event {
		case "always-fail":
			return broker.NewNackError(fmt.Errorf("boom"), true, "transient-flaky")
		case "fatal":
			return broker.NonRetryable(fmt.Errorf("bad message"), "invalid-payload")
		case "succeeds-after-retry":
			if n < 2 {
				return broker.NewNackError(fmt.Errorf("warmup"), true, "not-ready")
			}
			return nil
		default:
			return nil
		}
	}

	retry := broker.RetryPolicy{
		MaxAttempts: 3,
		Backoff:     broker.ConstantBackoff(2 * time.Millisecond),
	}
	b, cancel := newTestBroker(t, h, retry)
	defer func() { cancel(); _ = b.Close() }()
	ctx := context.Background()

	pub := func(id, part, event string) {
		t.Helper()
		if _, err := b.PublishTo(ctx, part, newEnvelope(id, part, event)); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
	// p0 order: fail-exhaustion, success-after-2-attempts, non-retryable, ok
	pub("m-fail", "p0", "always-fail")
	pub("m-then-ok", "p0", "succeeds-after-retry")
	pub("m-fatal", "p0", "fatal")
	pub("m-ok", "p0", "ok")
	// Independent partition keeps moving while p0 retries.
	for i := 0; i < 20; i++ {
		pub(fmt.Sprintf("q-%d", i), "p1", "ok")
	}

	waitFor(t, 10*time.Second, func() bool {
		return b.CommittedOffset("p0") == 4 && b.CommittedOffset("p1") == 20
	})

	mu.Lock()
	if attempts["m-fail"] != 3 {
		t.Fatalf("always-fail attempts=%d, want 3", attempts["m-fail"])
	}
	if attempts["m-then-ok"] != 2 {
		t.Fatalf("succeeds-after-retry attempts=%d, want 2", attempts["m-then-ok"])
	}
	if attempts["m-fatal"] != 1 {
		t.Fatalf("non-retryable attempts=%d, want 1", attempts["m-fatal"])
	}
	if attempts["m-ok"] != 1 {
		t.Fatalf("ok attempts=%d, want 1", attempts["m-ok"])
	}
	mu.Unlock()

	dead, err := b.DeadLetters(ctx)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	reasons := map[string]string{}
	for _, dl := range dead {
		env, perr := codec.Unmarshal(dl.Record)
		if perr != nil {
			t.Fatalf("dead record unmarshal: %v", perr)
		}
		reasons[env.ID] = dl.Reason
		if dl.Attempts < 1 || dl.MovedAt == "" {
			t.Fatalf("dead letter missing metadata: %+v", dl)
		}
	}
	if got := reasons["m-fail"]; got != "max-attempts-exceeded:transient-flaky" {
		t.Fatalf("m-fail reason=%q, want max-attempts-exceeded:transient-flaky", got)
	}
	if got := reasons["m-fatal"]; got != "invalid-payload" {
		t.Fatalf("m-fatal reason=%q, want invalid-payload", got)
	}
	if _, ok := reasons["m-then-ok"]; ok {
		t.Fatalf("message that recovered after retry must not be in dead log")
	}
	t.Logf("dead letters=%d reasons=%v", len(dead), reasons)
}

// TestRetryDoesNotBlockOtherPartitions publishes a stuck retrying message
// first and verifies another partition drains concurrently and completes.
func TestRetryDoesNotBlockOtherPartitions(t *testing.T) {
	release := make(chan struct{})
	h := func(_ context.Context, env *codec.Envelope) error {
		if env.Event == "slow-fail" {
			// Fail immediately with long backoff; partition head stays in flight.
			return broker.NewNackError(fmt.Errorf("x"), true, "slow")
		}
		return nil
	}
	retry := broker.RetryPolicy{
		MaxAttempts: 100,
		Backoff:     broker.ConstantBackoff(time.Hour),
	}
	b, cancel := newTestBroker(t, h, retry)
	defer func() { cancel(); close(release); _ = b.Close() }()
	ctx := context.Background()

	if _, err := b.PublishTo(ctx, "slow", newEnvelope("s1", "slow", "slow-fail")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := b.PublishTo(ctx, "fast", newEnvelope(fmt.Sprintf("f%d", i), "fast", "ok")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return b.CommittedOffset("fast") == 10 })
	if b.CommittedOffset("slow") != 0 {
		t.Fatalf("slow partition must not advance past unacked head, got %d", b.CommittedOffset("slow"))
	}
	t.Logf("fast partition drained to offset 10 while slow partition stuck at committed=0")
}
