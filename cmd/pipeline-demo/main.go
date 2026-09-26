// Command pipeline-demo runs the durable event pipeline against a local
// directory and prints delivery progress (partition / offset / attempt /
// failure reason). Run it twice against the same --dir to observe restart
// recovery: run 1 exits while failed messages are parked in backoff (still
// in flight); run 2 reopens the directory, redelivers them immediately with
// the preserved attempt count and finishes the dead-letter transfer.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/h3yow/durable-event-pipeline-go/internal/broker"
	"github.com/h3yow/durable-event-pipeline-go/internal/codec"
)

func retryPolicy(run string) broker.RetryPolicy {
	if run == "first" {
		// Long backoff: the process exits while failed messages are in flight.
		return broker.RetryPolicy{MaxAttempts: 3, Backoff: broker.ConstantBackoff(time.Hour)}
	}
	// Restarted process: short backoff so retries exhaust during the demo.
	return broker.RetryPolicy{
		MaxAttempts: 3,
		Backoff:     broker.ExponentialBackoff(20*time.Millisecond, 200*time.Millisecond),
	}
}

func main() {
	dir := flag.String("dir", "./data/pipeline", "local persistence directory")
	run := flag.String("run", "first", "run label: first=publish a batch; second=reuse the same directory")
	flag.Parse()

	publish := *run != "second"

	h := func(_ context.Context, env *codec.Envelope) error {
		switch env.Event {
		case "always-fail", "retry-forever-now":
			return broker.NewNackError(fmt.Errorf("dependency down"), true, "dependency-down")
		case "fatal":
			return broker.NonRetryable(fmt.Errorf("message rejected"), "invalid-payload")
		default:
			return nil
		}
	}

	cfg := broker.Config{
		Dir:        *dir,
		Group:      "demo-group",
		Handler:    h,
		Retry:      retryPolicy(*run),
		PollTick:   5 * time.Millisecond,
		Checkpoint: 50 * time.Millisecond,
		Logf:       func(f string, a ...any) { log.Printf(f, a...) },
	}
	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("open broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Start(ctx) }()

	if publish {
		events := []struct{ id, partition, event string }{
			{"e1", "orders", "ok"},
			{"e2", "orders", "fatal"},
			{"e3", "orders", "retry-forever-now"},
			{"e4", "billing", "always-fail"},
			{"e5", "billing", "ok"},
		}
		for _, e := range events {
			env := &codec.Envelope{
				Version: codec.SchemaVersion,
				ID:      e.id,
				Key:     e.partition,
				Event:   e.event,
				Payload: []byte(fmt.Sprintf(`{"order":"%s"}`, e.id)),
			}
			off, err := b.PublishTo(ctx, e.partition, env)
			if err != nil {
				log.Fatalf("publish %s: %v", e.id, err)
			}
			log.Printf("published id=%s partition=%s offset=%d", e.id, e.partition, off)
		}
	}

	// Run 1 exits while failed messages are parked in backoff (in flight);
	// run 2 finishes retries and dead-letter transfer after recovery.
	time.Sleep(300 * time.Millisecond)
	cancel()
	if err := b.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}

	dead, _ := b.DeadLetters(context.Background())
	log.Printf("---- summary ----")
	log.Printf("committed orders=%d billing=%d", b.CommittedOffset("orders"), b.CommittedOffset("billing"))
	log.Printf("dead letters=%d", len(dead))
	for _, dl := range dead {
		env, _ := codec.Unmarshal(dl.Record)
		log.Printf("  dead partition=%s offset=%d id=%s attempts=%d reason=%s",
			dl.Partition, dl.Offset, env.ID, dl.Attempts, dl.Reason)
	}
	log.Printf("state dir: %s (re-run with --dir=%s --run=second to recover in place)", *dir, *dir)
}
