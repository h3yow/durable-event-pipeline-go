package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ErrOffsetBeyondEnd 表示请求的 offset 尚未写入（到达日志末尾）。
var ErrOffsetBeyondEnd = errors.New("offset beyond end of log")

// ErrPartitionNeedsIntervention 表示分区日志出现无法安全自动修复的损坏。
var ErrPartitionNeedsIntervention = errors.New("partition log needs manual intervention")

// partitionLog 是单分区的追加式持久化日志（JSON Lines，一条消息一行，
// 偏移量即行号，从 0 开始连续编号）。
type partitionLog struct {
	mu   sync.Mutex
	path string
	file *os.File // 追加写句柄
	next int64    // 下一个待分配 offset

	// 崩溃现场恢复相关状态（openPartitionLog 扫描时确定）。
	recovery   PartitionRecovery
	intervened bool // 分区存在需人工介入的异常，消费循环不得继续
}

// openPartitionLog 打开（必要时创建）分区日志，并执行一次单遍扫描的崩溃自愈：
//   - 全部行都是完整 JSON 信封：健康，next = 有效行数；
//   - 仅最后一条非空记录残缺（写一半被强杀/断电的典型现场）：把该残缺尾部
//     原样留存到取证文件后截断，next = 之前的有效行数，分区恢复正常；
//   - 文件中部出现损坏：不做任何截断，标记需人工介入，该分区停止读写。
func openPartitionLog(path string, index int) (*partitionLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &partitionLog{path: path, file: f}

	rep, err := scanAndRepair(path, index)
	if err != nil {
		f.Close()
		return nil, err
	}
	l.recovery = rep
	l.next = rep.AfterRecords
	if rep.NeedsIntervention {
		l.intervened = true
	}
	return l, nil
}

// scanAndRepair 对日志做一次 O(文件长度) 的单遍扫描并按规则修复。
func scanAndRepair(path string, index int) (PartitionRecovery, error) {
	rep := PartitionRecovery{Partition: index, Healthy: true, At: time.Now().Unix()}

	f, err := os.Open(path)
	if err != nil {
		return rep, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return rep, err
	}
	fileSize := info.Size()

	br := bufio.NewReaderSize(f, 64*1024)
	var pos int64        // 当前行起点的字节偏移
	var validCount int64 // 已扫描到的有效记录数
	var totalCount int64 // 已扫描到的非空行总数
	var firstBadStart int64 = -1
	var firstBadRecord int64
	var lastStart int64 = -1 // 最后一条非空行的起点
	for {
		line, rerr := br.ReadBytes('\n')
		if len(line) > 0 {
			start := pos
			pos += int64(len(line))
			if len(bytes.TrimSpace(line)) > 0 {
				lastStart = start
				totalCount++
				if validateRecord(bytes.TrimSpace(line)) == nil {
					validCount++
				} else if firstBadStart < 0 {
					firstBadStart = start
					firstBadRecord = validCount
				}
			}
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return rep, rerr
			}
			break
		}
	}

	rep.BeforeRecords = totalCount
	if firstBadStart < 0 {
		// 健康：有效行数即下一个 offset。
		rep.AfterRecords = validCount
		return rep, nil
	}

	// 损坏只允许出现在“最后一条非空记录”：写一半崩溃只会留下残缺尾部。
	// 若损坏行之后还有任何记录，属于文件中部损坏，不能自动截断，交人工处理。
	if firstBadStart != lastStart {
		rep.Healthy = false
		rep.NeedsIntervention = true
		rep.DamagedOffset = firstBadRecord
		rep.AfterRecords = firstBadRecord
		rep.Reason = "mid_file_corruption"
		rep.Detail = fmt.Sprintf("invalid record at offset %d is followed by more data; log left untouched pending manual inspection", firstBadRecord)
		return rep, nil
	}

	// 尾部残缺：截断到损坏行起点之前，残缺字节留存取证。
	tail, err := os.ReadFile(path)
	if err != nil {
		return rep, err
	}
	torn := tail[firstBadStart:]
	qpath, err := quarantineTorn(path, index, firstBadRecord, torn)
	if err != nil {
		return rep, err
	}
	if err := os.Truncate(path, firstBadStart); err != nil {
		return rep, err
	}
	// 截断后 fsync，保证再次崩溃后修复结果依然在盘上。
	if tf, err := os.OpenFile(path, os.O_RDWR, 0o644); err == nil {
		tf.Sync()
		tf.Close()
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	rep.Healthy = false
	rep.Repaired = true
	rep.AfterRecords = firstBadRecord
	rep.RemovedRecords = totalCount - validCount
	rep.TruncatedBytes = fileSize - firstBadStart
	rep.DamagedOffset = firstBadRecord
	rep.QuarantineFile = qpath
	rep.Reason = "torn_tail_truncated"
	rep.Detail = fmt.Sprintf("incomplete trailing record at offset %d removed (quarantined); %d bytes truncated", firstBadRecord, rep.TruncatedBytes)
	return rep, nil
}

// validateRecord 校验一行是否为可解析且字段完整的消息信封。
// 未知版本不算损坏（消息合法，消费端会按规则转入死信）。
func validateRecord(line []byte) error {
	var m Message
	if err := json.Unmarshal(line, &m); err != nil {
		return err
	}
	if m.ID == "" {
		return errors.New("envelope missing id")
	}
	return nil
}

// quarantineTorn 把被截断的残缺字节原子写入取证文件：
// partition-N.offset-M.torn（先写 .tmp 再 rename）。
func quarantineTorn(path string, index int, offset int64, torn []byte) (string, error) {
	qpath := filepath.Join(filepath.Dir(path), fmt.Sprintf("partition-%d.offset-%d.torn", index, offset))
	tmp := qpath + ".tmp"
	if err := os.WriteFile(tmp, torn, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, qpath); err != nil {
		return "", err
	}
	if d, err := os.Open(filepath.Dir(qpath)); err == nil {
		d.Sync()
		d.Close()
	}
	return qpath, nil
}

// Append 追加一条消息并 fsync，返回分配的偏移量。
// 消息的 Partition/Offset 字段在此被覆写为权威值。
func (l *partitionLog) Append(m *Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.intervened {
		return 0, ErrPartitionNeedsIntervention
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

// recoveryStatus 返回该分区的恢复结果快照。
func (l *partitionLog) recoveryStatus() PartitionRecovery {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.recovery
}

// needsIntervention 报告该分区是否存在需人工介入的损坏。
func (l *partitionLog) needsIntervention() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.intervened
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
