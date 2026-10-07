package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"sync"
)

// ErrOffsetBeyondEnd 表示请求的 offset 尚未写入（到达日志末尾）。
var ErrOffsetBeyondEnd = errors.New("offset beyond end of log")

// ErrPartitionNeedsAttention 表示分区日志中部损坏、等待人工介入，暂停写入。
var ErrPartitionNeedsAttention = errors.New("partition log corrupt mid-file, manual intervention required")

// partitionLog 是单分区的追加式持久化日志（JSON Lines，一条消息一行，
// 偏移量即行号，从 0 开始连续编号）。
type partitionLog struct {
	mu   sync.Mutex
	path string
	file *os.File // 追加写句柄
	next int64    // 下一个待分配 offset
	// attention 为 true 表示日志中部损坏、等待人工介入：
	// 该分区拒绝新写入，消费到位点处自然停住，不影响其他分区。
	attention bool
}

// openPartitionLog 打开（必要时创建）分区日志文件，执行崩溃自愈扫描，
// 恢复 next offset 并返回本分区的恢复报告。
//
// 自愈规则（结果只依赖文件内容，重复执行结论一致）：
//   - 尾部残缺（写到一半的行）：截断到最后一条完整消息之后；
//   - 中部损坏（损坏点后仍有完整消息）：不修改文件，标记 NeedsAttention；
//   - 截断只丢弃不完整尾部，已确认消息与位点不受影响、不会倒退。
func openPartitionLog(path string, partition int) (*partitionLog, *PartitionRecovery, error) {
	res, err := scanPartitionLog(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, nil, err
		}
		res = &scanResult{} // 文件尚不存在：视为空日志
	}

	report := &PartitionRecovery{
		Partition:           partition,
		ValidMessagesBefore: res.validLines,
		ValidMessagesAfter:  res.validLines,
		BytesBefore:         res.totalBytes,
		BytesAfter:          res.totalBytes,
		Reason:              res.reason,
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	l := &partitionLog{path: path, file: f, next: res.validLines}

	switch {
	case res.midCorrupt:
		// 中部损坏：截断会丢失损坏点之后的完整消息（可能含已确认消息），
		// 因此保持文件原样，标记需要人工介入，并拒绝后续写入。
		report.NeedsAttention = true
		l.attention = true
	case res.corruptTail && res.validBytes < res.totalBytes:
		// 尾部残缺：截断到最后一条完整消息末尾。截断点由扫描结果唯一确定，
		// 对已修复文件再次扫描不会再触发截断（幂等）。
		if err := f.Truncate(res.validBytes); err != nil {
			f.Close()
			return nil, nil, err
		}
		report.Repaired = true
		report.BytesAfter = res.validBytes
		report.TruncatedBytes = res.totalBytes - res.validBytes
	}
	return l, report, nil
}

// Append 追加一条消息并 fsync，返回分配的偏移量。
// 消息的 Partition/Offset 字段在此被覆写为权威值。
// 分区被标记为需要人工介入时返回 ErrPartitionNeedsAttention。
func (l *partitionLog) Append(m *Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.attention {
		return 0, ErrPartitionNeedsAttention
	}
	m.Offset = l.next
	data, err := json.Marshal(m)
	if err != nil {
		return 0, err
	}
	if _, err := l.file.Write(append(data, '\n')); err != nil {
		return 0, err
	}
	if err := l.file.Sync(); err != nil {
		return 0, err
	}
	l.next++
	return m.Offset, nil
}

// ReadFrom 读取 offset 处的消息；该 offset 尚未写入时返回 ErrOffsetBeyondEnd。
// 使用独立只读句柄扫描，避免与并发 Append 的写指针互相干扰。
func (l *partitionLog) ReadFrom(offset int64) (*Message, error) {
	l.mu.Lock()
	beyond := offset >= l.next
	l.mu.Unlock()
	if beyond {
		return nil, ErrOffsetBeyondEnd
	}
	rf, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer rf.Close()
	sc := bufio.NewScanner(rf)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var idx int64
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if idx == offset {
			var m Message
			if err := json.Unmarshal(line, &m); err != nil {
				return nil, err
			}
			return &m, nil
		}
		idx++
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nil, ErrOffsetBeyondEnd
}

// NextOffset 返回下一个待分配的 offset（即日志长度）。
func (l *partitionLog) NextOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

func (l *partitionLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
