package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

// ErrOffsetBeyondEnd 表示请求的 offset 尚未写入（到达日志末尾）。
var ErrOffsetBeyondEnd = errors.New("offset beyond end of log")

// partitionLog 是单分区的追加式持久化日志（JSON Lines，一条消息一行，
// 偏移量即行号，从 0 开始连续编号）。
type partitionLog struct {
	mu   sync.Mutex
	path string
	file *os.File // 追加写句柄
	next int64    // 下一个待分配 offset
}

// openPartitionLog 打开（必要时创建）分区日志文件，并扫描行数恢复 next offset。
func openPartitionLog(path string) (*partitionLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &partitionLog{path: path, file: f}
	// 扫描现有行数，恢复 next offset，保证重启后偏移连续。
	rf, err := os.Open(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	defer rf.Close()
	sc := bufio.NewScanner(rf)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			l.next++
		}
	}
	if err := sc.Err(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// Append 追加一条消息并 fsync，返回分配的偏移量。
// 消息的 Partition/Offset 字段在此被覆写为权威值。
func (l *partitionLog) Append(m *Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
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
