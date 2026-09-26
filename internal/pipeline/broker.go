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
}

// OpenBroker 打开 dir 下的 broker，分区数为 n；已有日志文件会被续用。
func OpenBroker(dir string, n int) (*Broker, error) {
	if n <= 0 {
		return nil, fmt.Errorf("partition count must be positive, got %d", n)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	b := &Broker{dir: dir}
	for i := 0; i < n; i++ {
		l, err := openPartitionLog(filepath.Join(dir, fmt.Sprintf("partition-%d.log", i)))
		if err != nil {
			for _, pl := range b.partitions {
				pl.Close()
			}
			return nil, err
		}
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

func (b *Broker) Close() error {
	var first error
	for _, l := range b.partitions {
		if err := l.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
