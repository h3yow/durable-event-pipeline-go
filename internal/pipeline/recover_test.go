package pipeline

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// keyForPartition 返回一个映射到指定分区的测试 key。
func keyForPartition(t *testing.T, b *Broker, partition int) string {
	t.Helper()
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if b.partitionFor(k) == partition {
			return k
		}
	}
}

// produceN 向指定分区写入 n 条消息。
func produceN(t *testing.T, b *Broker, partition, n int) {
	t.Helper()
	key := keyForPartition(t, b, partition)
	for i := 0; i < n; i++ {
		p, _, err := b.Produce(key, 1, []byte(fmt.Sprintf(`{"seq":%d}`, i)))
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
		if p != partition {
			t.Fatalf("key mapped to partition %d, want %d", p, partition)
		}
	}
}

// appendTornTail 模拟进程写消息写到一半被强杀：向分区日志末尾追加半行残缺字节。
func appendTornTail(t *testing.T, dir string, partition int, partial []byte) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("partition-%d.log", partition))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.Write(partial); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	f.Close()
	t.Logf("partition=%d injected torn tail of %d bytes (no newline terminator)", partition, len(partial))
}

func findReport(t *testing.T, reps []PartitionRecovery, partition int) PartitionRecovery {
	t.Helper()
	for _, r := range reps {
		if r.Partition == partition {
			return r
		}
	}
	t.Fatalf("no recovery report for partition %d", partition)
	return PartitionRecovery{}
}

// TestTornTailRecovery 验证尾部写了一半的消息被识别并截断，
// 分区恢复消费，已写入的完整消息一条不丢，且恢复后新消息位点连续。
func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()

	b, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	produceN(t, b, 0, 10)
	produceN(t, b, 1, 5)
	if err := b.Close(); err != nil {
		t.Fatalf("close broker: %v", err)
	}

	// 模拟强杀：分区 0 留下半条消息。
	torn := []byte(`{"id":"torn-msg","partition":0,"offset":10,"vers`)
	appendTornTail(t, dir, 0, torn)

	// 重启：恢复应截断残缺尾部，健康分区不受影响。
	b2, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("reopen broker: %v", err)
	}
	defer b2.Close()

	rep0 := findReport(t, b2.RecoveryReports(), 0)
	t.Logf("partition=0 recovery report: %+v", rep0)
	if !rep0.Repaired {
		t.Fatalf("partition 0 should be repaired, report: %+v", rep0)
	}
	if rep0.ValidMessagesBefore != 10 || rep0.ValidMessagesAfter != 10 {
		t.Fatalf("valid messages before/after = %d/%d, want 10/10", rep0.ValidMessagesBefore, rep0.ValidMessagesAfter)
	}
	if rep0.TruncatedBytes != int64(len(torn)) {
		t.Fatalf("truncated bytes = %d, want %d", rep0.TruncatedBytes, len(torn))
	}
	if rep0.NeedsAttention {
		t.Fatalf("torn tail must not require manual intervention: %+v", rep0)
	}
	rep1 := findReport(t, b2.RecoveryReports(), 1)
	if rep1.Repaired || rep1.NeedsAttention {
		t.Fatalf("healthy partition 1 must be untouched, report: %+v", rep1)
	}
	if got := b2.PartitionLength(0); got != 10 {
		t.Fatalf("partition 0 length after recovery = %d, want 10", got)
	}

	// 恢复后继续写入：offset 必须接着 10 连续分配。
	key0 := keyForPartition(t, b2, 0)
	if _, off, err := b2.Produce(key0, 1, []byte(`{"seq":"after-recovery"}`)); err != nil {
		t.Fatalf("produce after recovery: %v", err)
	} else if off != 10 {
		t.Fatalf("offset after recovery = %d, want 10", off)
	}

	// 恢复后正常消费：11 条消息按序全部确认。
	s, err := OpenStateStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	var mu sync.Mutex
	var got []int64
	c := newTestConsumer(t, b2, s, func(ctx context.Context, m *Message) error {
		if m.Partition != 0 {
			return nil
		}
		mu.Lock()
		got = append(got, m.Offset)
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	waitFor(t, "partition 0 fully consumed after recovery", func() bool { return s.CommittedOffset(0) == 11 })
	waitFor(t, "partition 1 fully consumed after recovery", func() bool { return s.CommittedOffset(1) == 5 })
	cancel()
	c.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 11 {
		t.Fatalf("partition 0 consumed %d messages, want 11", len(got))
	}
	for i, off := range got {
		if off != int64(i) {
			t.Fatalf("partition 0 order violated at index %d: offset=%d", i, off)
		}
	}
}

// TestRecoveryIdempotent 验证恢复结果确定且可重复：
// 同一份损坏现场恢复多次结论一致，重复恢复不再改动文件。
func TestRecoveryIdempotent(t *testing.T) {
	dir := t.TempDir()

	b, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	produceN(t, b, 0, 8)
	if err := b.Close(); err != nil {
		t.Fatalf("close broker: %v", err)
	}
	torn := []byte(`{"id":"half-written","part`)
	appendTornTail(t, dir, 0, torn)

	logPath := filepath.Join(dir, "partition-0.log")
	corrupted, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read corrupted log: %v", err)
	}

	// 另复制一份完全相同的损坏现场，验证恢复结果与现场内容一一对应（确定性）。
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir2, "partition-0.log"), corrupted, 0o644); err != nil {
		t.Fatalf("clone corrupted log: %v", err)
	}

	// 第一次恢复。
	b1, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	rep1 := findReport(t, b1.RecoveryReports(), 0)
	if !rep1.Repaired || rep1.ValidMessagesAfter != 8 {
		t.Fatalf("first recovery report wrong: %+v", rep1)
	}
	recoveredOnce, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read recovered log: %v", err)
	}
	b1.Close()

	// 第二次打开同一份已恢复现场：不应再触发任何修复，文件内容保持不变。
	b2, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("reopen broker: %v", err)
	}
	defer b2.Close()
	rep2 := findReport(t, b2.RecoveryReports(), 0)
	t.Logf("partition=0 second-open report: %+v", rep2)
	if rep2.Repaired || rep2.NeedsAttention || rep2.TruncatedBytes != 0 {
		t.Fatalf("second recovery must be a no-op, report: %+v", rep2)
	}
	recoveredTwice, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read recovered log: %v", err)
	}
	if string(recoveredOnce) != string(recoveredTwice) {
		t.Fatalf("repeated recovery changed the file: %d -> %d bytes", len(recoveredOnce), len(recoveredTwice))
	}

	// 对相同损坏现场的独立副本做恢复，结果必须与第一次完全一致（确定性）。
	b3, err := OpenBroker(dir2, 1)
	if err != nil {
		t.Fatalf("open cloned broker: %v", err)
	}
	defer b3.Close()
	rep3 := findReport(t, b3.RecoveryReports(), 0)
	if !rep3.Repaired || rep3.TruncatedBytes != rep1.TruncatedBytes {
		t.Fatalf("cloned recovery mismatch: %+v vs %+v", rep3, rep1)
	}
	recoveredClone, err := os.ReadFile(filepath.Join(dir2, "partition-0.log"))
	if err != nil {
		t.Fatalf("read cloned log: %v", err)
	}
	if string(recoveredClone) != string(recoveredOnce) {
		t.Fatalf("identical corruption recovered differently: %d vs %d bytes", len(recoveredClone), len(recoveredOnce))
	}
	if got := b3.PartitionLength(0); got != 8 {
		t.Fatalf("cloned partition length = %d, want 8", got)
	}
}

// TestRecoveryPreservesCommittedProgress 验证尾部残缺恢复不会丢失已确认消息、
// 也不会让已推进位点倒退：恢复后从已提交位点继续，消息恰好生效一次。
func TestRecoveryPreservesCommittedProgress(t *testing.T) {
	dir := t.TempDir()
	const n = 10

	b, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	produceN(t, b, 0, n)
	s, err := OpenStateStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}

	// 第一阶段：正常消费一部分后“崩溃”。
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
	waitFor(t, "at least 4 messages consumed before crash", func() bool { return s.CommittedOffset(0) >= 4 })
	cancel()
	c1.Wait()
	crashedAt := s.CommittedOffset(0)
	t.Logf("partition=0 crashed at committed=%d", crashedAt)
	b.Close()
	s.Close()

	// 崩溃同时在日志末尾留下半条消息（写到一半被强杀）。
	appendTornTail(t, dir, 0, []byte(`{"id":"torn-after-crash","off`))

	// 第二阶段：重启恢复，位点不得倒退，剩余消息恰好消费一次。
	b2, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("reopen broker: %v", err)
	}
	defer b2.Close()
	rep := findReport(t, b2.RecoveryReports(), 0)
	t.Logf("partition=0 recovery report: %+v", rep)
	if !rep.Repaired || rep.ValidMessagesAfter != n {
		t.Fatalf("expected tail-only repair keeping %d messages, report: %+v", n, rep)
	}
	s2, err := OpenStateStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	if got := s2.CommittedOffset(0); got != crashedAt {
		t.Fatalf("committed offset regressed across recovery: before=%d after=%d", crashedAt, got)
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
	waitFor(t, "all messages consumed after recovery", func() bool { return s2.CommittedOffset(0) == n })
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

// TestMidLogCorruptionNeedsAttention 验证日志中部损坏（损坏点后仍有完整消息）时：
// 组件照常启动、不擅自删数据、标记分区需人工介入、拒绝该分区新写入，
// 且健康分区照常推进。
func TestMidLogCorruptionNeedsAttention(t *testing.T) {
	dir := t.TempDir()

	b, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	produceN(t, b, 0, 5)
	produceN(t, b, 1, 4)
	b.Close()

	// 把分区 0 的第 2 行（offset=2）替换成垃圾字节，保留其后的完整消息。
	logPath := filepath.Join(dir, "partition-0.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	lines[2] = `{"id":"corrupted","offset":2,"payload":{BROKEN`
	corrupted := strings.Join(lines, "\n")
	if err := os.WriteFile(logPath, []byte(corrupted), 0o644); err != nil {
		t.Fatalf("write corrupted log: %v", err)
	}
	t.Logf("partition=0 injected mid-log corruption at line=2")

	b2, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("reopen broker: %v", err)
	}
	defer b2.Close()

	rep := findReport(t, b2.RecoveryReports(), 0)
	t.Logf("partition=0 recovery report: %+v", rep)
	if !rep.NeedsAttention {
		t.Fatalf("mid-log corruption must be flagged for manual intervention: %+v", rep)
	}
	if rep.Repaired {
		t.Fatalf("mid-log corruption must not be auto-truncated (would lose good data): %+v", rep)
	}
	if got := b2.PartitionsNeedingAttention(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("PartitionsNeedingAttention = %v, want [0]", got)
	}
	// 文件内容保持原样，等待人工处理。
	after, _ := os.ReadFile(logPath)
	if string(after) != corrupted {
		t.Fatalf("mid-corrupt log must be left untouched")
	}
	// 该分区拒绝新写入，错误可区分。
	key0 := keyForPartition(t, b2, 0)
	if _, _, err := b2.Produce(key0, 1, []byte(`{"v":1}`)); !errors.Is(err, ErrPartitionNeedsAttention) {
		t.Fatalf("produce to attention partition: err=%v, want ErrPartitionNeedsAttention", err)
	}

	// 健康分区照常消费推进；受损分区消费到损坏点前停住，不拖垮组件。
	s, err := OpenStateStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	c := newTestConsumer(t, b2, s, func(ctx context.Context, m *Message) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	waitFor(t, "healthy partition 1 fully consumed", func() bool { return s.CommittedOffset(1) == 4 })
	// 受损分区只能推进到损坏点（offset=2）之前。
	waitFor(t, "damaged partition 0 stalls at corruption point", func() bool { return s.CommittedOffset(0) == 2 })
	cancel()
	c.Wait()
	if got := s.CommittedOffset(0); got != 2 {
		t.Fatalf("partition 0 committed = %d, want stalled at 2", got)
	}
}

// TestRecoveryScalesLinearly 验证单分区数据达到几十万条量级时，
// 恢复（打开时的扫描+截断）耗时随数据量近似线性增长，不出现明显退化。
func TestRecoveryScalesLinearly(t *testing.T) {
	// genCorruptLog 直接构造含 n 条完整消息 + 半行残缺尾部的分区日志，
	// 绕开 Produce 的逐条 fsync，避免准备数据拖慢测试。
	genCorruptLog := func(dir string, n int) {
		path := filepath.Join(dir, "partition-0.log")
		f, err := os.Create(path)
		if err != nil {
			t.Fatalf("create log: %v", err)
		}
		w := bufio.NewWriterSize(f, 1<<20)
		for i := 0; i < n; i++ {
			fmt.Fprintf(w, `{"id":"m-%d","partition":0,"offset":%d,"version":1,"payload":{"seq":%d}}`+"\n", i, i, i)
		}
		w.WriteString(`{"id":"torn","partition":0,"off`) // 写到一半的残缺尾部
		if err := w.Flush(); err != nil {
			t.Fatalf("flush log: %v", err)
		}
		f.Close()
	}

	recover := func(n int) (time.Duration, PartitionRecovery) {
		dir := t.TempDir()
		genCorruptLog(dir, n)
		start := time.Now()
		b, err := OpenBroker(dir, 1)
		if err != nil {
			t.Fatalf("open broker with %d messages: %v", n, err)
		}
		elapsed := time.Since(start)
		rep := findReport(t, b.RecoveryReports(), 0)
		if !rep.Repaired || rep.ValidMessagesAfter != int64(n) {
			t.Fatalf("recovery with %d messages wrong: %+v", n, rep)
		}
		b.Close()
		return elapsed, rep
	}

	const small, large = 50_000, 200_000
	d1, _ := recover(small)
	d2, _ := recover(large)
	t.Logf("recovery elapsed: messages=%d took %s; messages=%d took %s (ratio=%.2f, data ratio=%.1f)",
		small, d1, large, d2, float64(d2)/float64(d1), float64(large)/float64(small))
	// 数据量 4 倍，允许耗时比最高 8 倍（线性应为约 4 倍，留足机器抖动余量）；
	// 若实现退化为 O(n^2)（如逐行重扫文件），该比值会远超此上限。
	if d1 > 0 && d2 > 8*d1 {
		t.Fatalf("recovery time grew super-linearly: %d msgs in %s, %d msgs in %s", small, d1, large, d2)
	}
}
