// demo 演示本地持久化的分区投递与消费：重复运行可观察重启后的状态恢复。
//
// 用法：
//
//	go run ./cmd/demo          # 使用 ./demo-data 作为持久化目录
//	DEMO_DIR=/tmp/x go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/h3yow/durable-event-pipeline-go/internal/pipeline"
)

func main() {
	dir := os.Getenv("DEMO_DIR")
	if dir == "" {
		dir = "demo-data"
	}

	b, err := pipeline.OpenBroker(dir, 3)
	if err != nil {
		log.Fatalf("open broker: %v", err)
	}
	defer b.Close()

	// 打印本次启动的崩溃自愈结果（正常情况下全部无修复）。
	for _, r := range b.RecoveryReports() {
		if r.Repaired || r.NeedsAttention {
			log.Printf("[demo] recovery partition=%d repaired=%v needs_attention=%v valid=%d->%d bytes=%d->%d truncated=%d reason=%s",
				r.Partition, r.Repaired, r.NeedsAttention,
				r.ValidMessagesBefore, r.ValidMessagesAfter,
				r.BytesBefore, r.BytesAfter, r.TruncatedBytes, r.Reason)
		}
	}
	if attn := b.PartitionsNeedingAttention(); len(attn) > 0 {
		log.Printf("[demo] WARNING: partitions %v need manual intervention", attn)
	}

	s, err := pipeline.OpenStateStore(dir + "/consumer-state.json")
	if err != nil {
		log.Fatalf("open state: %v", err)
	}
	defer s.Close()

	// 每次运行追加一批消息；已提交位点保证重启后不会重复消费。
	runID := time.Now().Unix()
	for i := 0; i < 9; i++ {
		key := fmt.Sprintf("key-%d", i)
		payload := []byte(fmt.Sprintf(`{"run":%d,"seq":%d}`, runID, i))
		p, off, err := b.Produce(key, 1, payload)
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
		log.Printf("[demo] produced partition=%d offset=%d key=%s", p, off, key)
	}

	// 每轮的第一条消息模拟一次可重试失败，展示退避重投。
	var failedOnce bool
	handler := func(ctx context.Context, m *pipeline.Message) error {
		if !failedOnce {
			failedOnce = true
			return &pipeline.Failure{Reason: "simulated transient error", Retryable: true}
		}
		return nil
	}

	c := pipeline.NewConsumer(b, s, handler, pipeline.BackoffPolicy{
		Initial: 50 * time.Millisecond, Max: 500 * time.Millisecond, Multiplier: 2, MaxRetries: 3,
	}, log.Default())
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)

	// 等待所有分区追平日志末尾后退出。
	for {
		done := true
		for p := 0; p < b.NumPartitions(); p++ {
			if s.CommittedOffset(p) < b.PartitionLength(p) {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	c.Wait()

	for p := 0; p < b.NumPartitions(); p++ {
		log.Printf("[demo] partition=%d committed=%d length=%d", p, s.CommittedOffset(p), b.PartitionLength(p))
	}
	if dead := s.DeadLetters(); len(dead) > 0 {
		for _, d := range dead {
			log.Printf("[demo] dead letter id=%s reason=%s detail=%s", d.Message.ID, d.Reason, d.Detail)
		}
	}
	log.Printf("[demo] done; state persisted under %s (rerun to see recovery)", dir)
}
