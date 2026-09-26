package pipeline

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// DeadLetter 记录进入死信的消息与可区分的原因。
type DeadLetter struct {
	Message Message `json:"message"`
	Reason  string  `json:"reason"` // "max_retries_exceeded" / "non_retryable" / "unsupported_version"
	Detail  string  `json:"detail"`
	At      int64   `json:"at"` // Unix 秒
}

// consumerState 是需持久化的消费者状态。
type consumerState struct {
	Committed map[int]int64     `json:"committed"` // 分区 -> 下一个待消费 offset
	Applied   map[string]bool   `json:"applied"`   // 已产生业务效果的消息 ID（幂等去重）
	Dead      []DeadLetter      `json:"dead"`
	Compat    map[string]string `json:"compat"`   // 消息 ID -> 兼容处理说明
	Attempts  map[string]int    `json:"attempts"` // 消息 ID -> 已重试次数（重启后不重置）
}

// StateStore 持久化消费位点、幂等记录与死信。
// 每次变更整体落盘：先写临时文件再 rename，保证崩溃时不会留下半截状态。
type StateStore struct {
	mu    sync.Mutex
	path  string
	state consumerState
}

// OpenStateStore 从 path 恢复状态；文件不存在则初始化为空状态。
func OpenStateStore(path string) (*StateStore, error) {
	s := &StateStore{path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		s.state = consumerState{
			Committed: map[int]int64{},
			Applied:   map[string]bool{},
			Compat:    map[string]string{},
			Attempts:  map[string]int{},
		}
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return nil, err
	}
	if s.state.Committed == nil {
		s.state.Committed = map[int]int64{}
	}
	if s.state.Applied == nil {
		s.state.Applied = map[string]bool{}
	}
	if s.state.Compat == nil {
		s.state.Compat = map[string]string{}
	}
	if s.state.Attempts == nil {
		s.state.Attempts = map[string]int{}
	}
	return s, nil
}

// flush 将当前状态原子写盘（调用方须持锁）。
func (s *StateStore) flush() error {
	data, err := json.MarshalIndent(&s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	// fsync 目录项，保证 rename 在崩溃后依然可见。
	if d, err := os.Open(filepath.Dir(s.path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// CommittedOffset 返回分区已提交位点（下一个待消费 offset）。
func (s *StateStore) CommittedOffset(partition int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Committed[partition]
}

// Commit 提交分区位点并持久化。只允许前进，不允许回退。
func (s *StateStore) Commit(partition int, nextOffset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nextOffset < s.state.Committed[partition] {
		return nil
	}
	s.state.Committed[partition] = nextOffset
	return s.flush()
}

// IsApplied 判断消息是否已产生业务效果。
func (s *StateStore) IsApplied(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Applied[id]
}

// MarkApplied 记录消息已产生业务效果并持久化。
func (s *StateStore) MarkApplied(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Applied[id] = true
	return s.flush()
}

// NoteCompat 记录一次兼容处理（如缺版本回落、未知字段保留）。
func (s *StateStore) NoteCompat(id, note string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Compat[id] = note
	return s.flush()
}

// IncrAttempt 将消息的重试计数加一并持久化，返回累计次数。
func (s *StateStore) IncrAttempt(id string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Attempts[id]++
	n := s.state.Attempts[id]
	return n, s.flush()
}

// Attempt 返回消息当前的重试计数。
func (s *StateStore) Attempt(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Attempts[id]
}

// AddDeadLetter 追加死信记录并持久化。
func (s *StateStore) AddDeadLetter(d DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Dead = append(s.state.Dead, d)
	return s.flush()
}

// DeadLetters 返回全部死信快照。
func (s *StateStore) DeadLetters() []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]DeadLetter, len(s.state.Dead))
	copy(out, s.state.Dead)
	return out
}

// CompatNotes 返回兼容处理记录快照。
func (s *StateStore) CompatNotes() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.state.Compat))
	for k, v := range s.state.Compat {
		out[k] = v
	}
	return out
}

func (s *StateStore) Close() error { return nil }
