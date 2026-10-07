package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// injectTornTail 模拟“消息写一半进程被强杀”：关闭 broker 后往分区日志
// 尾部直接追加一段不完整（无法解析为完整信封）的字节，且不带末尾换行。
func injectTornTail(t *testing.T, dir string, partition int, partial string) {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf("partition-%d.log", partition))
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open log for corruption: %v", err)
	}
	if _, err := f.WriteString(partial); err != nil {
		t.Fatalf("inject torn bytes: %v", err)
	}
	// 刻意不 Sync、不补换行：模拟断电瞬间页缓存/写入只落了一半。
	f.Close()
}

// statusFor 从恢复报告里取出指定分区的结果。
func statusFor(t *testing.T, b *Broker, partition int) PartitionRecovery {
	t.Helper()
	for _, r := range b.RecoveryStatus() {
		if r.Partition == partition {
			return r
		}
	}
	t.Fatalf("no recovery status for partition %d", partition)
	return PartitionRecovery{}
}

// TestTornTailRecovery 验证尾部半截记录被识别、截断，分区恢复正常。
func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	const n = 10

	b, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, _, err := b.Produce("k", 1, []byte(fmt.Sprintf(`{"seq":%d}`, i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	b.Close()

	// 第 11 条消息只写了一半（JSON 未闭合、无换行）。
	injectTornTail(t, dir, 0, `{"id":"aaaa1111","version":1,"partit`)

	b2, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("reopen broker on torn tail must not fail: %v", err)
	}
	defer b2.Close()

	if l := b2.PartitionLength(0); l != n {
		t.Fatalf("partition length after repair = %d, want %d (torn record must not count)", l, n)
	}
	st := statusFor(t, b2, 0)
	if !st.Repaired || st.Healthy || st.NeedsIntervention {
		t.Fatalf("unexpected recovery status: %+v", st)
	}
	if st.BeforeRecords != n+1 || st.AfterRecords != n || st.RemovedRecords != 1 {
		t.Fatalf("unexpected record counts: %+v", st)
	}
	if st.DamagedOffset != n {
		t.Fatalf("damaged offset = %d, want %d", st.DamagedOffset, n)
	}
	if st.TruncatedBytes <= 0 {
		t.Fatalf("truncated bytes should be positive: %+v", st)
	}
	if st.QuarantineFile == "" {
		t.Fatalf("torn bytes should be quarantined: %+v", st)
	}
	if _, err := os.Stat(st.QuarantineFile); err != nil {
		t.Fatalf("quarantine file missing: %v", err)
	}

	// 原来的好数据一条都不能坏。
	for off := int64(0); off < n; off++ {
		m, err := b2.Read(0, off)
		if err != nil {
			t.Fatalf("read valid offset %d after repair: %v", off, err)
		}
		if m.Offset != off {
			t.Fatalf("offset mismatch at %d: %d", off, m.Offset)
		}
	}
}

// TestRecoveryIdempotent 验证同一份损坏现场重启任意次，恢复结果一致，
// 不会误伤后续好数据，也不会多/少消息。
func TestRecoveryIdempotent(t *testing.T) {
	dir := t.TempDir()
	const n = 8

	b, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, _, err := b.Produce("k", 1, []byte(fmt.Sprintf(`{"seq":%d}`, i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	b.Close()
	injectTornTail(t, dir, 0, `{"id":"bbbb2222","ver`)

	var first PartitionRecovery
	for round := 0; round < 3; round++ {
		bk, err := OpenBroker(dir, 1)
		if err != nil {
			t.Fatalf("reopen round %d: %v", round, err)
		}
		st := statusFor(t, bk, 0)
		if l := bk.PartitionLength(0); l != n {
			t.Fatalf("round %d length = %d, want %d", round, l, n)
		}
		if round == 0 {
			if !st.Repaired || st.AfterRecords != n {
				t.Fatalf("round 0 unexpected status: %+v", st)
			}
			first = st
		} else {
			// 修复已落盘：再次打开应看到健康分区，规模与修复后严格一致，
			// 不能因重复扫描而继续截断好数据。
			if !st.Healthy || st.BeforeRecords != n || st.AfterRecords != n {
				t.Fatalf("round %d should see a healthy %d-record log, got %+v", round, n, st)
			}
		}
		bk.Close()
	}

	// 事件流水只记一次同一现场。
	bk, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bk.Close()
	events, err := bk.RecoveryEvents()
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 1 || !events[0].Repaired || events[0].DamagedOffset != n {
		t.Fatalf("expected exactly 1 recovery event for the same incident, got %+v", events)
	}
	if events[0].TruncatedBytes != first.TruncatedBytes {
		t.Fatalf("persisted event truncated bytes mismatch: event=%d first=%d", events[0].TruncatedBytes, first.TruncatedBytes)
	}
}

// TestConsumeAfterRecovery 验证修复后分区能恢复消费、追平并继续接收新消息，
// 位点不倒退、消息不丢不重。
func TestConsumeAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	const before = 6

	var err error
	b, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	s, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for i := 0; i < before; i++ {
		if _, _, err := b.Produce("k", 1, []byte(fmt.Sprintf(`{"seq":%d}`, i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	// 先消费掉前 3 条，推进位点，再“崩溃”。
	var mu sync.Mutex
	seen := map[string]int{}
	c0 := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error {
		mu.Lock()
		seen[m.ID]++
		mu.Unlock()
		return nil
	})
	ctx0, cancel0 := context.WithCancel(context.Background())
	c0.Start(ctx0)
	waitFor(t, "3 consumed before crash", func() bool { return s.CommittedOffset(0) >= 3 })
	cancel0()
	c0.Wait()
	// 循环退出后再读取权威位点（取消生效前可能又确认了若干条）。
	committed := s.CommittedOffset(0)
	b.Close()
	s.Close()

	injectTornTail(t, dir, 0, `{"id":"cccc3333","version":1,"paylo`)

	b2, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()
	s2, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	if got := s2.CommittedOffset(0); got != committed {
		t.Fatalf("committed offset regressed across crash recovery: %d -> %d", committed, got)
	}

	// 修复后再追加两条新消息。
	const after = 2
	for i := 0; i < after; i++ {
		if _, _, err := b2.Produce("k", 1, []byte(fmt.Sprintf(`{"seq":%d}`, before+i))); err != nil {
			t.Fatalf("produce after recovery: %v", err)
		}
	}

	c1 := newTestConsumer(t, b2, s2, func(ctx context.Context, m *Message) error {
		mu.Lock()
		seen[m.ID]++
		mu.Unlock()
		return nil
	})
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	c1.Start(ctx1)
	want := int64(before + after)
	waitFor(t, "partition fully consumed after recovery", func() bool { return s2.CommittedOffset(0) == want })
	cancel1()
	c1.Wait()

	if l := b2.PartitionLength(0); l != want {
		t.Fatalf("length = %d, want %d", l, want)
	}
	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s handled %d times, want exactly 1", id, n)
		}
	}
}

// corruptAt 在 offset 处把一条完整记录替换为残缺字节（保留该行结构：
// 残缺内容 + '\n'），用于制造“文件中部损坏”现场。
func corruptAt(t *testing.T, dir string, partition, offset int, partial string) {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf("partition-%d.log", partition))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	lines := bytes.Split(data, []byte("\n"))
	// lines[offset] 是第 offset 条记录；文件以 '\n' 结尾，末尾还有一个空串元素。
	if offset >= len(lines) {
		t.Fatalf("offset %d out of range %d", offset, len(lines))
	}
	lines[offset] = []byte(partial)
	if err := os.WriteFile(p, bytes.Join(lines, []byte("\n")), 0o644); err != nil {
		t.Fatalf("rewrite log: %v", err)
	}
}

// TestMidFileCorruptionIntervention 验证中部损坏不可自动修复：
// 损坏分区停摆并标记人工介入、日志原样保留；健康分区照常消费推进。
func TestMidFileCorruptionIntervention(t *testing.T) {
	dir := t.TempDir()
	const n = 10

	b, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	// 两个分区都写 n 条（按分区号直接落盘到指定分区）。
	for i := 0; i < n; i++ {
		raw := []byte(fmt.Sprintf(`{"id":"p0-%d","version":1,"payload":{"seq":%d}}`, i, i))
		if _, err := b.ProduceRaw(0, raw); err != nil {
			t.Fatalf("produce p0: %v", err)
		}
		raw1 := []byte(fmt.Sprintf(`{"id":"p1-%d","version":1,"payload":{"seq":%d}}`, i, i))
		if _, err := b.ProduceRaw(1, raw1); err != nil {
			t.Fatalf("produce p1: %v", err)
		}
	}
	b.Close()

	// 把分区 0 中部（offset 4）的一条好记录改成残缺行：其后仍有好数据。
	corruptAt(t, dir, 0, 4, `{"id":"broken-mid","ver`)
	before, err := os.Stat(filepath.Join(dir, "partition-0.log"))
	if err != nil {
		t.Fatalf("stat before reopen: %v", err)
	}

	b2, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("reopen must not fail: %v", err)
	}
	defer b2.Close()
	s, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("state: %v", err)
	}

	st := statusFor(t, b2, 0)
	if !st.NeedsIntervention || st.Repaired || st.Reason != "mid_file_corruption" {
		t.Fatalf("partition 0 should need intervention, got %+v", st)
	}
	if st.DamagedOffset != 4 || st.AfterRecords != 4 {
		t.Fatalf("unexpected damaged offset / surviving count: %+v", st)
	}
	if !b2.NeedsIntervention(0) {
		t.Fatalf("NeedsIntervention(0) must be true")
	}
	if b2.NeedsIntervention(1) {
		t.Fatalf("partition 1 must stay healthy")
	}
	// 损坏分区日志必须原样保留：字节数不变，也不生成取证文件。
	after, err := os.Stat(filepath.Join(dir, "partition-0.log"))
	if err != nil {
		t.Fatalf("stat after reopen: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("mid-corruption log must be untouched: size %d -> %d", before.Size(), after.Size())
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.torn")); len(matches) != 0 {
		t.Fatalf("no quarantine file expected for mid-corruption, got %v", matches)
	}
	st1 := statusFor(t, b2, 1)
	if !st1.Healthy || st1.AfterRecords != n {
		t.Fatalf("partition 1 unhealthy: %+v", st1)
	}

	// 向损坏分区继续写入必须被拒绝，防止在坏数据后追加造成更多混乱。
	if _, err := b2.ProduceRaw(0, []byte(`{"id":"new-after-corrupt","version":1,"payload":{}}`)); err == nil {
		t.Fatalf("append to intervened partition must fail")
	}

	// 消费器：分区 1 必须照常追平，分区 0 停在损坏点之前不推进。
	c := newTestConsumer(t, b2, s, func(ctx context.Context, m *Message) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	waitFor(t, "healthy partition 1 catches up", func() bool { return s.CommittedOffset(1) == n })
	// 给分区 0 一点时间，确认它始终停在初始位点（未消费任何消息），
	// 不越过损坏点、不反复报错崩溃。
	time.Sleep(150 * time.Millisecond)
	if got := s.CommittedOffset(0); got != 0 {
		t.Fatalf("partition 0 committed = %d, must stay at 0 (partition halted at damaged offset 4)", got)
	}
	cancel()
	c.Wait()
}

// TestRecoveryStatusVisible 验证恢复结果可在程序中查询：状态、规模与事件流水。
func TestRecoveryStatusVisible(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, _, err := b.Produce("k", 1, []byte(`{"v":1}`)); err != nil {
		t.Fatalf("produce: %v", err)
	}
	b.Close()
	injectTornTail(t, dir, 0, `{"id":"dddd4444","parti`)

	b2, err := OpenBroker(dir, 2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()

	all := b2.RecoveryStatus()
	if len(all) != 2 {
		t.Fatalf("expected status for 2 partitions, got %d", len(all))
	}
	var rep0, rep1 *PartitionRecovery
	for i := range all {
		switch all[i].Partition {
		case 0:
			rep0 = &all[i]
		case 1:
			rep1 = &all[i]
		}
	}
	if rep0 == nil || !rep0.Repaired || rep0.BeforeRecords != 2 || rep0.AfterRecords != 1 || rep0.RemovedRecords != 1 {
		t.Fatalf("partition 0 status wrong: %+v", rep0)
	}
	if rep1 == nil || !rep1.Healthy || rep1.AfterRecords != 0 {
		t.Fatalf("partition 1 status wrong: %+v", rep1)
	}
	events, err := b2.RecoveryEvents()
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 1 || events[0].Partition != 0 || events[0].Reason != "torn_tail_truncated" {
		t.Fatalf("unexpected events: %+v", events)
	}
}

// writeRawLines 不经 broker，直接以 JSONL 批量写 n 条信封到分区文件，
// 模拟磁盘上已有的大规模日志（单次 Sync），供恢复耗时测量使用。
func writeRawLines(t *testing.T, path string, n int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open bulk file: %v", err)
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20)
	for i := 0; i < n; i++ {
		line := fmt.Sprintf(`{"id":"bulk-%07d","partition":0,"offset":%d,"version":1,"key":"k","payload":{"seq":%d,"blob":"0123456789abcdef0123456789abcdef"}}`+"\n", i, i, i)
		if _, err := bw.WriteString(line); err != nil {
			t.Fatalf("bulk write: %v", err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

// TestRecoveryScalesLinearly 验证单分区数据到几十万条量级时，
// 恢复耗时随规模线性增长，不出现超线性退化。
func TestRecoveryScalesLinearly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in -short mode")
	}
	const smallN = 10_000
	const largeN = 200_000 // 20 倍
	const ratio = largeN / smallN

	measure := func(t *testing.T, n int) time.Duration {
		t.Helper()
		dir := t.TempDir()
		p := filepath.Join(dir, "partition-0.log")
		writeRawLines(t, p, n)
		start := time.Now()
		b, err := OpenBroker(dir, 1)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		d := time.Since(start)
		st := statusFor(t, b, 0)
		if !st.Healthy || st.AfterRecords != int64(n) {
			t.Fatalf("unexpected status: %+v", st)
		}
		b.Close()
		return d
	}

	dSmall := measure(t, smallN)
	dLarge := measure(t, largeN)
	t.Logf("recovery scan: %d records in %v; %d records in %v", smallN, dSmall, largeN, dLarge)

	// 线性预算：大规模耗时应接近 smallN 耗时的 ratio 倍；放宽到 4 倍线性预算，
	// 专门用于发现 O(n^2) 级退化（那种情况 ratio=20 时会放大到数百倍）。
	linearBudget := time.Duration(ratio) * dSmall * 4
	if dLarge > linearBudget {
		t.Fatalf("recovery scales worse than linear: %d records took %v, budget %v (small=%v)",
			largeN, dLarge, linearBudget, dSmall)
	}
	// 绝对值护栏：20 万条扫描不应到秒级。
	if dLarge > 5*time.Second {
		t.Fatalf("recovery too slow for %d records: %v", largeN, dLarge)
	}
}

// TestTornTailDoesNotRegressProgress 验证已确认消息与已推进位点不会因
// 尾部修复而回退：先把 5 条全部确认，再在尾部注入一条残缺记录，
// 重启修复后位点仍为 5，且第 6 条新消息接着 offset 5 写入。
func TestTornTailDoesNotRegressProgress(t *testing.T) {
	dir := t.TempDir()
	const n = 5

	b, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	s, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, _, err := b.Produce("k", 1, []byte(fmt.Sprintf(`{"seq":%d}`, i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	c := newTestConsumer(t, b, s, func(ctx context.Context, m *Message) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)
	waitFor(t, "all confirmed", func() bool { return s.CommittedOffset(0) == n })
	cancel()
	c.Wait()
	// 记录已生效消息 ID，修复后必须全部保留。
	b.Close()
	s.Close()

	// 崩溃现场：又一条消息写了一半。
	injectTornTail(t, dir, 0, `{"id":"eeee5555","version":1,"offset":5,"pay`)

	b2, err := OpenBroker(dir, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()
	s2, err := OpenStateStore(dir + "/state.json")
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	if got := s2.CommittedOffset(0); got != n {
		t.Fatalf("committed regressed: %d -> %d", n, got)
	}
	if l := b2.PartitionLength(0); l != n {
		t.Fatalf("length after repair = %d, want %d", l, n)
	}
	off, err := b2.ProduceRaw(0, []byte(`{"id":"ffff6666","version":1,"payload":{"seq":6}}`))
	if err != nil {
		t.Fatalf("append after repair: %v", err)
	}
	if off != n {
		t.Fatalf("new message offset = %d, must continue at %d", off, n)
	}

	c2 := newTestConsumer(t, b2, s2, func(ctx context.Context, m *Message) error { return nil })
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c2.Start(ctx2)
	waitFor(t, "new message consumed", func() bool { return s2.CommittedOffset(0) == n+1 })
	cancel2()
	c2.Wait()
}
