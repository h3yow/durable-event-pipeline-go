package broker_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/durable-event-pipeline-go/internal/broker"
	"github.com/h3yow/durable-event-pipeline-go/internal/codec"
)

func openBroker(t *testing.T, dir string, h broker.Handler, retry broker.RetryPolicy) (*broker.Broker, context.CancelFunc) {
	t.Helper()
	logf := func(format string, args ...any) { t.Logf("[broker] "+format, args...) }
	b, err := broker.New(broker.Config{
		Dir:        dir,
		Group:      "test-group",
		Handler:    h,
		Retry:      retry,
		PollTick:   2 * time.Millisecond,
		Checkpoint: 10 * time.Millisecond,
		Logf:       logf,
	})
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Start(ctx) }()
	return b, cancel
}

// TestRestartRecoversInFlightAndOffsets:
//   - run 1: 2 successes, then a message that fails with a far-future backoff
//     (left in flight), then close the process
//   - run 2: reopen the SAME directory; the in-flight message is redelivered
//     immediately, its attempt count is preserved, and after MaxAttempts it
//     is dead-lettered; the trailing message then completes
//   - offsets and dead letters survive the restart and no already-acked
//     message is processed twice
func TestRestartRecoversInFlightAndOffsets(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	var mu sync.Mutex
	counts := map[string]int{}
	// Run 1: everything after m0/m1 fails with a 1h backoff so one record
	// remains in flight at shutdown.
	h1 := func(_ context.Context, env *codec.Envelope) error {
		mu.Lock()
		counts[env.ID]++
		mu.Unlock()
		if env.ID == "m0" || env.ID == "m1" {
			return nil
		}
		return broker.NewNackError(fmt.Errorf("temporary"), true, "transient")
	}
	retry1 := broker.RetryPolicy{MaxAttempts: 3, Backoff: broker.ConstantBackoff(time.Hour)}

	b1, c1 := openBroker(t, dir, h1, retry1)
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("m%d", i)
		if _, err := b1.PublishTo(ctx, "p0", newEnvelope(id, "p0", "e")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return b1.CommittedOffset("p0") == 2 })
	// Allow the in-flight attempt to register.
	time.Sleep(30 * time.Millisecond)
	c1()
	if err := b1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := counts["m0"]; got != 1 {
		t.Fatalf("m0 processed %d times in run1, want 1", got)
	}

	// Run 2: same failure shape but tiny backoff so retries exhaust quickly.
	retry2 := broker.RetryPolicy{MaxAttempts: 3, Backoff: broker.ConstantBackoff(time.Millisecond)}
	b2, c2 := openBroker(t, dir, h1, retry2)
	defer func() { c2(); _ = b2.Close() }()

	waitFor(t, 5*time.Second, func() bool { return b2.CommittedOffset("p0") == 4 })

	mu.Lock()
	defer mu.Unlock()
	if counts["m0"] != 1 || counts["m1"] != 1 {
		t.Fatalf("acked messages reprocessed after restart: m0=%d m1=%d", counts["m0"], counts["m1"])
	}
	// m2 had 1 attempt before shutdown; MaxAttempts=3 => exactly 2 more.
	if counts["m2"] != 3 {
		t.Fatalf("m2 total attempts across restart=%d, want 3", counts["m2"])
	}
	// m3 never started before shutdown, gets all 3 attempts fresh.
	if counts["m3"] != 3 {
		t.Fatalf("m3 total attempts=%d, want 3", counts["m3"])
	}

	dead, err := b2.DeadLetters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deadIDs := map[string]bool{}
	for _, dl := range dead {
		env, _ := codec.Unmarshal(dl.Record)
		deadIDs[env.ID] = true
	}
	if !deadIDs["m2"] || !deadIDs["m3"] {
		t.Fatalf("expected m2,m3 dead after restart, got %v", deadIDs)
	}
	t.Logf("restart recovery OK: committed=4 dead=%v counts=%v", deadIDs, counts)
}

// TestRestartDedupSkipsRecordedEffect models the crash window in which the
// business effect happened and its idempotency key/checkpoint were durable,
// but delivery was never acked in memory. After restart the record must be
// skipped (handler not re-invoked) and the offset advanced.
func TestRestartDedupSkipsRecordedEffect(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	var handlerCalls int32
	h := func(_ context.Context, env *codec.Envelope) error {
		atomic.AddInt32(&handlerCalls, 1)
		return nil
	}
	retry := broker.RetryPolicy{MaxAttempts: 1, Backoff: broker.ConstantBackoff(time.Millisecond)}

	// Run 1: publish and fully process one message, then publish a second
	// message whose offset reaches a crash-like state: log appended, offset
	// uncommitted, but dedup table already contains its id.
	b1, c1 := openBroker(t, dir, h, retry)
	if _, err := b1.PublishTo(ctx, "p0", newEnvelope("a", "p0", "e")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return b1.CommittedOffset("p0") == 1 })
	c1()
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate the crash window: append b to the durable log and record its
	// effect in the dedup table, but leave the committed offset at 1.
	raw, err := codec.Marshal(newEnvelope("b", "p0", "e"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.AppendRawForTest(ctx, "p0", raw); err != nil {
		t.Fatal(err)
	}
	if err := b1.SeedProcessedForTest("id:b"); err != nil {
		t.Fatal(err)
	}
	callsBefore := atomic.LoadInt32(&handlerCalls)
	if callsBefore != 1 {
		t.Fatalf("expected exactly 1 handler call before crash, got %d", callsBefore)
	}

	// Run 2: b must be dedup-skipped without invoking the handler.
	b2, c2 := openBroker(t, dir, h, retry)
	defer func() { c2(); _ = b2.Close() }()
	waitFor(t, 2*time.Second, func() bool { return b2.CommittedOffset("p0") == 2 })

	if got := atomic.LoadInt32(&handlerCalls); got != 1 {
		t.Fatalf("deduped record reprocessed after restart: calls=%d, want 1", got)
	}
	t.Logf("dedup-after-restart OK: handler calls stayed at 1, offset advanced to 2")
}
