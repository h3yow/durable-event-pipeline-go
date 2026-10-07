package pipeline

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// PartitionRecovery 是单个分区的崩溃现场恢复结果（分区级别可查询）。
type PartitionRecovery struct {
	// Partition 分区号。
	Partition int `json:"partition"`
	// Healthy 表示打开时未发现任何损坏。
	Healthy bool `json:"healthy"`
	// Repaired 表示检测到尾部残缺记录并已自动截断修复。
	Repaired bool `json:"repaired"`
	// NeedsIntervention 表示出现无法安全自动修复的异常（如文件中部损坏），
	// 该分区消费循环会停下等待人工介入，其余分区不受影响。
	NeedsIntervention bool `json:"needs_intervention"`
	// BeforeRecords 是修复前磁盘上非空记录行的总数（含损坏记录）。
	BeforeRecords int64 `json:"before_records"`
	// AfterRecords 是修复后保留下来的有效记录数。
	AfterRecords int64 `json:"after_records"`
	// RemovedRecords 是被移除的残缺记录数。
	RemovedRecords int64 `json:"removed_records"`
	// TruncatedBytes 是尾部修复截断的字节数。
	TruncatedBytes int64 `json:"truncated_bytes"`
	// DamagedOffset 是首个损坏记录在分区内的行号（offset）。
	DamagedOffset int64 `json:"damaged_offset"`
	// QuarantineFile 是被截下的残缺字节留存的取证文件路径（如有）。
	QuarantineFile string `json:"quarantine_file,omitempty"`
	// Reason 是机器可读的修复/异常原因：
	// "torn_tail_truncated" / "mid_file_corruption" / ""（健康）。
	Reason string `json:"reason,omitempty"`
	// Detail 是人工可读说明。
	Detail string `json:"detail,omitempty"`
	// At 是本次恢复发生的 Unix 时间戳（秒）。
	At int64 `json:"at"`
}

// recoveryBook 是 recovery.json 的持久化结构：每分区最新状态 + 异常/修复事件流水。
type recoveryBook struct {
	Last   map[int]PartitionRecovery `json:"last"`
	Events []PartitionRecovery       `json:"events"`
}

// recoveryStore 以 tmp+rename 原子写持久化分区恢复报告。
type recoveryStore struct {
	mu   sync.Mutex
	path string
}

func newRecoveryStore(path string) *recoveryStore { return &recoveryStore{path: path} }

// sameIncident 判断两次报告是否描述同一个损坏现场（忽略时间戳与取证文件名），
// 用于让“同一份现场重启多次”只产生一条事件，恢复记录确定且不膨胀。
func sameIncident(a, b PartitionRecovery) bool {
	return a.Partition == b.Partition &&
		a.Reason == b.Reason &&
		a.DamagedOffset == b.DamagedOffset &&
		a.TruncatedBytes == b.TruncatedBytes &&
		a.BeforeRecords == b.BeforeRecords &&
		a.AfterRecords == b.AfterRecords
}

// record 把一次分区打开时的恢复结果落盘：
//   - 健康报告只更新该分区最新状态，不写事件流水；
//   - 修复 / 需介入的报告，若与上次事件是同一现场则不重复记录，
//     保证同一损坏现场无论重启多少次结果一致。
func (rs *recoveryStore) record(rep PartitionRecovery) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	book, err := rs.loadLocked()
	if err != nil {
		return err
	}
	prev := book.Last[rep.Partition]
	book.Last[rep.Partition] = rep

	if rep.Repaired || rep.NeedsIntervention {
		if !sameIncident(prev, rep) {
			book.Events = append(book.Events, rep)
		}
	}

	data, err := json.MarshalIndent(book, "", "  ")
	if err != nil {
		return err
	}
	tmp := rs.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, rs.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(rs.path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// load 读取持久化的恢复记录；文件不存在时返回空记录。
func (rs *recoveryStore) load() (*recoveryBook, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.loadLocked()
}

func (rs *recoveryStore) loadLocked() (*recoveryBook, error) {
	book := &recoveryBook{Last: map[int]PartitionRecovery{}}
	data, err := os.ReadFile(rs.path)
	if errors.Is(err, fs.ErrNotExist) {
		return book, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, book); err != nil {
		// 状态文件自身只经原子写产生，损坏理论上来自外部改动；
		// 不拖垮组件，按空记录重新开始（损坏文件原样保留便于排查）。
		return &recoveryBook{Last: map[int]PartitionRecovery{}}, nil
	}
	if book.Last == nil {
		book.Last = map[int]PartitionRecovery{}
	}
	return book, nil
}
