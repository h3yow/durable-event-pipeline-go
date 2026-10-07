package pipeline

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
)

// Broker 用本地磁盘模拟消息代理：固定分区数，每分区一个追加式日志文件。
type Broker struct {
	dir        string
	partitions []*partitionLog
	rstore     *recoveryStore
}

// OpenBroker 打开 dir 下的 broker，分区数为 n；已有日志文件会被续用。
func OpenBroker(dir string, n int) (*Broker, error) {
	if n <= 0 {
		return nil, fmt.Errorf("partition count must be positive, got %d", n)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	b := &Broker{
		dir:    dir,
		rstore: newRecoveryStore(filepath.Join(dir, "recovery.json")),
	}
	for i := 0; i < n; i++ {
		l, err := openPartitionLog(filepath.Join(dir, fmt.Sprintf("partition-%d.log", i)), i)
		if err != nil {
			for _, pl := range b.partitions {
				pl.Close()
			}
			return nil, err
		}
		// 日志修复在打开时已经完成；报告持久化失败不影响已修复的分区，
		// 内存中的状态仍可通过 RecoveryStatus 查询。
		_ = b.rstore.record(l.recoveryStatus())
		b.partitions = append(b.partitions, l)
	}
	return b, nil
}

// NumPartitions 返回分区数。
func (b *Broker) NumPartitions() int { return len(b.partitions) }

// partitionFor 按 key 的 FNV 哈希确定分区；空 key 随机分配。
func (b *Broker) partitionFor(key string) int {
	if key == "" {
		var buf [8]byte
		rand.Read(buf[:])
		h := fnv.New32a()
		h.Write(buf[:])
		return int(h.Sum32() % uint32(len(b.partitions)))
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(b.partitions)))
}

// Produce 将 payload 写入 key 对应的分区，返回分区号与偏移量。
// version <= 0 时按 DefaultVersion 处理。
func (b *Broker) Produce(key string, version int, payload []byte) (partition int, offset int64, err error) {
	if version <= 0 {
		version = DefaultVersion
	}
	var idb [16]byte
	rand.Read(idb[:])
	m := &Message{
		ID:      hex.EncodeToString(idb[:]),
		Version: version,
		Key:     key,
		Payload: payload,
	}
	p := b.partitionFor(key)
	m.Partition = p
	off, err := b.partitions[p].Append(m)
	if err != nil {
		return 0, 0, err
	}
	return p, off, nil
}

// ProduceRaw 直接写入原始信封字节（用于注入缺版本/未知字段等兼容场景）。
// 信封按兼容规则解析后落盘；解析失败原样返回错误。
func (b *Broker) ProduceRaw(partition int, data []byte) (offset int64, err error) {
	if partition < 0 || partition >= len(b.partitions) {
		return 0, fmt.Errorf("partition %d out of range", partition)
	}
	m, err := UnmarshalEnvelope(data)
	if m == nil {
		return 0, err
	}
	// 版本不受支持的消息也照常落盘，由消费端按规则转入死信。
	m.Partition = partition
	return b.partitions[partition].Append(m)
}

// Read 读取指定分区 offset 处的消息。
func (b *Broker) Read(partition int, offset int64) (*Message, error) {
	if partition < 0 || partition >= len(b.partitions) {
		return nil, fmt.Errorf("partition %d out of range", partition)
	}
	return b.partitions[partition].ReadFrom(offset)
}

// PartitionLength 返回分区当前长度（下一个待分配 offset）。
func (b *Broker) PartitionLength(partition int) int64 {
	return b.partitions[partition].NextOffset()
}

// RecoveryStatus 返回每个分区最近一次打开时的恢复结果快照。
func (b *Broker) RecoveryStatus() []PartitionRecovery {
	out := make([]PartitionRecovery, len(b.partitions))
	for i, l := range b.partitions {
		out[i] = l.recoveryStatus()
	}
	return out
}

// RecoveryEvents 返回历史上发生过的修复 / 需介入异常事件（跨重启持久化）。
func (b *Broker) RecoveryEvents() ([]PartitionRecovery, error) {
	book, err := b.rstore.load()
	if err != nil {
		return nil, err
	}
	out := make([]PartitionRecovery, len(book.Events))
	copy(out, book.Events)
	return out, nil
}

// NeedsIntervention 报告分区是否存在需人工介入的异常。
func (b *Broker) NeedsIntervention(partition int) bool {
	if partition < 0 || partition >= len(b.partitions) {
		return false
	}
	return b.partitions[partition].needsIntervention()
}

func (b *Broker) Close() error {
	var first error
	for _, l := range b.partitions {
		if err := l.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
