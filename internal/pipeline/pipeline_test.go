package pipeline

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

// testWriter 把消费日志接入 testing.T，日志中带分区/位点/失败原因。
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func testBackoff() BackoffPolicy {
	return BackoffPolicy{Initial: 5 * time.Millisecond, Max: 30 * time.Millisecond, Multiplier: 2, MaxRetries: 3}
}

func newTestConsumer(t *testing.T, b *Broker, s *StateStore, h Handler) *Consumer {
	t.Helper()
	return NewConsumer(b, s, h, testBackoff(), log.New(testWriter{t}, "", 0))
}

// waitFor 轮询条件直到满足或超时。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

func setup(t *testing.T, partitions int) (*Broker, *StateStore) {
	t.Helper()
	dir := t.TempDir()
	b, err := OpenBroker(dir, partitions)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	s, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	return b, s
}

// TestPartitionOrder 验证同一分区内严格按写入顺序处理。
func TestPartitionOrder(t *testing.T) {
	b, s := setup(t, 1)
	defer b.Close()

	const n = 50
	for i := 0; i < n; i++ {
		if _, _, err := b.Produce("same-key", 1, []byte(fmt.Sprintf(`{"seq":%d}`, i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	var mu sync.Mutex
	var got []int64
	c := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		mu.Lock()
		got = append(got, m.Offset)
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	waitFor(t, "all messages consumed", func() bool { return s.CommittedOffset(0) == n })
	cancel()
	c.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("expected %d messages, got %d", n, len(got))
	}
	for i, off := range got {
		if off != int64(i) {
			t.Fatalf("order violated at index %d: offset=%d", i, off)
		}
	}
}

// TestCrossPartitionConcurrency 验证一个分区持续重试时不阻塞其他分区。
func TestCrossPartitionConcurrency(t *testing.T) {
	b, s := setup(t, 2)
	defer b.Close()

	// 找到映射到不同分区的两个 key。
	var key0, key1 string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if b.partitionFor(k) == 0 && key0 == "" {
			key0 = k
		}
		if b.partitionFor(k) == 1 && key1 == "" {
			key1 = k
		}
		if key0 != "" && key1 != "" {
			break
		}
	}

	// 分区 0：消息将一直可重试失败（退避期间阻塞本分区）。
	if _, _, err := b.Produce(key0, 1, []byte(`{"v":"failing"}`)); err != nil {
		t.Fatalf("produce: %v", err)
	}
	// 分区 1：多条正常消息。
	const n1 = 10
	for i := 0; i < n1; i++ {
		if _, _, err := b.Produce(key1, 1, []byte(`{"v":"ok"}`)); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	c := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		if m.Partition == 0 {
			return &Failure{Reason: "injected transient error", Retryable: true}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// 分区 0 还在重试时，分区 1 必须能推进到末尾。
	waitFor(t, "partition 1 fully consumed while partition 0 retries", func() bool {
		return s.CommittedOffset(1) == n1
	})
	// 分区 0 最终因超出重试上限进入死信并越过该消息。
	waitFor(t, "partition 0 message dead-lettered after max retries", func() bool {
		return s.CommittedOffset(0) == 1
	})
	cancel()
	c.Wait()

	dead := s.DeadLetters()
	if len(dead) != 1 || dead[0].Reason != "max_retries_exceeded" {
		t.Fatalf("expected 1 dead letter with reason max_retries_exceeded, got %+v", dead)
	}
	if dead[0].Message.Partition != 0 {
		t.Fatalf("dead letter from wrong partition: %d", dead[0].Message.Partition)
	}
}

// TestRetryThenSuccess 验证可重试失败按退避重投后成功，且不进入死信。
func TestRetryThenSuccess(t *testing.T) {
	b, s := setup(t, 1)
	defer b.Close()

	if _, _, err := b.Produce("k", 1, []byte(`{"v":1}`)); err != nil {
		t.Fatalf("produce: %v", err)
	}

	var mu sync.Mutex
	calls := 0
	var msgID string
	c := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		mu.Lock()
		calls++
		n := calls
		msgID = m.ID
		mu.Unlock()
		if n <= 2 {
			return &Failure{Reason: fmt.Sprintf("transient %d", n), Retryable: true}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	waitFor(t, "message consumed after retries", func() bool { return s.CommittedOffset(0) == 1 })
	cancel()
	c.Wait()

	mu.Lock()
	defer mu.Unlock()
	if calls != 3 {
		t.Fatalf("expected 3 handler calls, got %d", calls)
	}
	if len(s.DeadLetters()) != 0 {
		t.Fatalf("unexpected dead letters: %+v", s.DeadLetters())
	}
	if !s.IsApplied(msgID) {
		t.Fatalf("message %s not marked applied after success", msgID)
	}
}

// TestNonRetryableToDeadLetter 验证不可重试失败立即进入死信且原因可区分。
func TestNonRetryableToDeadLetter(t *testing.T) {
	b, s := setup(t, 1)
	defer b.Close()

	if _, _, err := b.Produce("k", 1, []byte(`{"v":1}`)); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if _, _, err := b.Produce("k", 1, []byte(`{"v":2}`)); err != nil {
		t.Fatalf("produce: %v", err)
	}

	c := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		if m.Offset == 0 {
			return &Failure{Reason: "schema violation", Retryable: false}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// 第一条进死信后位点必须越过它，第二条正常消费。
	waitFor(t, "offset advances past dead-lettered message", func() bool {
		return s.CommittedOffset(0) == 2
	})
	cancel()
	c.Wait()

	dead := s.DeadLetters()
	if len(dead) != 1 || dead[0].Reason != "non_retryable" || dead[0].Detail != "schema violation" {
		t.Fatalf("unexpected dead letters: %+v", dead)
	}
}

// TestRestartRecovery 验证进程重启后：位点不丢、消息不丢、业务效果不重复。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	const n = 20

	b, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, _, err := b.Produce("k", 1, []byte(fmt.Sprintf(`{"seq":%d}`, i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	s, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("open state: %v", err)
	}

	// 第一阶段：消费若干条后“崩溃”（取消 ctx，进程退出）。
	// 用 >= 5 是因为取消与位点提交之间存在竞态，崩溃点只需可观测、不必精确。
	var mu sync.Mutex
	applied := map[string]int{}
	c1 := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		mu.Lock()
		applied[m.ID]++
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	c1.Start(ctx)
	waitFor(t, "at least 5 messages consumed", func() bool { return s.CommittedOffset(0) >= 5 })
	cancel()
	c1.Wait()
	crashedAt := s.CommittedOffset(0)
	b.Close()
	s.Close()

	// 第二阶段：模拟进程重启，重新打开 broker 与 state。
	b2, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("reopen broker: %v", err)
	}
	defer b2.Close()
	s2, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	if got := s2.CommittedOffset(0); got != crashedAt {
		t.Fatalf("committed offset lost across restart: before=%d after=%d", crashedAt, got)
	}

	c2 := newTestConsumer(t, b2, s2, func(ctx context.Context, m *Message) error {
		mu.Lock()
		applied[m.ID]++
		mu.Unlock()
		return nil
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c2.Start(ctx2)
	waitFor(t, "all messages consumed after restart", func() bool { return s2.CommittedOffset(0) == n })
	cancel2()
	c2.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(applied) != n {
		t.Fatalf("expected %d distinct applied messages, got %d", n, len(applied))
	}
	for id, cnt := range applied {
		if cnt != 1 {
			t.Fatalf("message %s applied %d times, want exactly 1", id, cnt)
		}
	}
}

// TestVersionCompat 验证缺版本回落、未知字段保留、未知版本进死信。
func TestVersionCompat(t *testing.T) {
	b, s := setup(t, 1)
	defer b.Close()

	// 1) 缺少 version 字段 -> 回落到 DefaultVersion。
	if _, err := b.ProduceRaw(0, []byte(`{"id":"m-no-version","payload":{"a":1}}`)); err != nil {
		t.Fatalf("produce raw: %v", err)
	}
	// 2) 含未知顶层字段 -> 保留到 Extra。
	if _, err := b.ProduceRaw(0, []byte(`{"id":"m-extra","version":1,"payload":{"a":2},"future_field":{"x":9}}`)); err != nil {
		t.Fatalf("produce raw: %v", err)
	}
	// 3) 未知版本 -> 消费端转入死信。
	if _, err := b.ProduceRaw(0, []byte(`{"id":"m-v99","version":99,"payload":{"a":3}}`)); err != nil {
		t.Fatalf("produce raw: %v", err)
	}

	var mu sync.Mutex
	seen := map[string]*Message{}
	c := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		mu.Lock()
		cp := *m
		seen[m.ID] = &cp
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	waitFor(t, "all three messages settled", func() bool { return s.CommittedOffset(0) == 3 })
	cancel()
	c.Wait()

	mu.Lock()
	defer mu.Unlock()
	if seen["m-no-version"] == nil || seen["m-no-version"].Version != DefaultVersion {
		t.Fatalf("missing-version message not defaulted to %d: %+v", DefaultVersion, seen["m-no-version"])
	}
	if seen["m-extra"] == nil || seen["m-extra"].Extra["future_field"] == nil {
		t.Fatalf("unknown field not preserved: %+v", seen["m-extra"])
	}
	if seen["m-v99"] != nil {
		t.Fatalf("unsupported version must not reach handler: %+v", seen["m-v99"])
	}

	dead := s.DeadLetters()
	if len(dead) != 1 || dead[0].Reason != "unsupported_version" || dead[0].Message.ID != "m-v99" {
		t.Fatalf("unexpected dead letters: %+v", dead)
	}
	notes := s.CompatNotes()
	if !strings.Contains(notes["m-no-version"], "defaulted to 1") {
		t.Fatalf("missing compat note for m-no-version: %v", notes)
	}
	if !strings.Contains(notes["m-extra"], "future_field") {
		t.Fatalf("missing compat note for m-extra: %v", notes)
	}
}

// TestDuplicateDeliveryNoDuplicateEffect 验证重复投递不产生重复业务效果。
func TestDuplicateDeliveryNoDuplicateEffect(t *testing.T) {
	b, s := setup(t, 1)
	defer b.Close()

	if _, _, err := b.Produce("k", 1, []byte(`{"v":1}`)); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// 模拟“业务效果已生效但位点未来得及提交就崩溃”：
	// 先把该消息标记为已生效，位点仍停留在 0。
	m, err := b.Read(0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := s.MarkApplied(m.ID); err != nil {
		t.Fatalf("mark applied: %v", err)
	}

	var mu sync.Mutex
	calls := 0
	c := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	waitFor(t, "duplicate skipped and offset committed", func() bool { return s.CommittedOffset(0) == 1 })
	cancel()
	c.Wait()

	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Fatalf("handler must not run for already-applied message, ran %d times", calls)
	}
}
