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

	// 打印本次启动各分区的崩溃自愈结果（含修复前后规模 / 人工介入标记）。
	for _, r := range b.RecoveryStatus() {
		switch {
		case r.NeedsIntervention:
			log.Printf("[demo] partition=%d RECOVERY needs-intervention reason=%s damaged_offset=%d valid_records=%d detail=%q",
				r.Partition, r.Reason, r.DamagedOffset, r.AfterRecords, r.Detail)
		case r.Repaired:
			log.Printf("[demo] partition=%d RECOVERY repaired reason=%s records %d->%d removed=%d truncated_bytes=%d quarantine=%s",
				r.Partition, r.Reason, r.BeforeRecords, r.AfterRecords, r.RemovedRecords, r.TruncatedBytes, r.QuarantineFile)
		default:
			log.Printf("[demo] partition=%d recovery healthy records=%d", r.Partition, r.AfterRecords)
		}
	}
	if events, err := b.RecoveryEvents(); err == nil && len(events) > 0 {
		log.Printf("[demo] recovery events on record: %d (see %s/recovery.json)", len(events), dir)
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

	// 等待所有可推进分区追平日志末尾后退出；被标记人工介入的分区不参与等待。
	for {
		done := true
		for p := 0; p < b.NumPartitions(); p++ {
			if b.NeedsIntervention(p) {
				continue
			}
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
		flag := ""
		if b.NeedsIntervention(p) {
			flag = " HALTED(needs manual intervention)"
		}
		log.Printf("[demo] partition=%d committed=%d length=%d%s", p, s.CommittedOffset(p), b.PartitionLength(p), flag)
	}
	if dead := s.DeadLetters(); len(dead) > 0 {
		for _, d := range dead {
			log.Printf("[demo] dead letter id=%s reason=%s detail=%s", d.Message.ID, d.Reason, d.Detail)
		}
	}
	log.Printf("[demo] done; state persisted under %s (rerun to see recovery)", dir)
}
