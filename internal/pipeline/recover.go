package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// PartitionRecovery 记录单个分区在打开时执行的崩溃自愈结果，
// 供调用方查询哪些分区发生过修复、修复前后的数据规模、以及是否需要人工介入。
type PartitionRecovery struct {
	Partition int `json:"partition"` // 分区号

	// Repaired 为 true 表示本次打开截断了不完整尾部。
	Repaired bool `json:"repaired"`
	// NeedsAttention 为 true 表示发现日志中部损坏（损坏点之后仍存在完整消息），
	// 组件不做自动修改，等待人工介入；该分区暂停写入与消费推进。
	NeedsAttention bool `json:"needs_attention"`

	ValidMessagesBefore int64 `json:"valid_messages_before"` // 修复前可识别的完整消息数
	ValidMessagesAfter  int64 `json:"valid_messages_after"`  // 修复后保留的完整消息数
	BytesBefore         int64 `json:"bytes_before"`          // 修复前文件字节数
	BytesAfter          int64 `json:"bytes_after"`           // 修复后文件字节数
	TruncatedBytes      int64 `json:"truncated_bytes"`       // 本次截断丢弃的字节数

	Reason string `json:"reason,omitempty"` // 触发修复或人工介入的原因（含位点信息）
}

// scanResult 是对分区日志一次完整扫描的结论。
type scanResult struct {
	validLines  int64  // 有效前缀行数（= 恢复后的 next offset）
	validBytes  int64  // 有效前缀字节数（= 截断点）
	totalBytes  int64  // 文件总字节数
	corruptTail bool   // 是否存在损坏内容
	midCorrupt  bool   // 损坏点之后是否仍存在完整消息（需人工介入）
	reason      string // 第一处损坏的原因
}

// scanPartitionLog 顺序扫描分区日志（单遍 O(n)），定位有效数据前缀。
//
// 一行被视为有效，当且仅当：
//  1. 以 '\n' 结尾（最后一行缺少换行即视为写了一半的残缺数据）；
//  2. 去除空白后非空；
//  3. 是合法的 JSON 消息信封；
//  4. 信封内 offset 与行号一致（防止错位/重复行被当作好数据）。
//
// 扫描在第一处损坏后仍继续到文件末尾，以区分“尾部残缺”（可安全截断）
// 与“中部损坏”（后面还有好数据，必须留给人工处理）。
// 扫描结果只依赖文件内容，因此同一份损坏现场无论扫描多少次结论都一致。
func scanPartitionLog(path string) (*scanResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	res := &scanResult{}
	r := bufio.NewReaderSize(f, 256*1024)
	var pos int64 // 当前行起始字节偏移
	for {
		chunk, rerr := r.ReadBytes('\n')
		if len(chunk) > 0 {
			if !res.corruptTail {
				// 有效前缀内：严格校验（结尾换行、非空、合法 JSON、offset 连续）。
				if lineValid, lineReason := validateLogLine(chunk, res.validLines); lineValid {
					res.validLines++
					res.validBytes = pos + int64(len(chunk))
				} else {
					res.corruptTail = true
					res.reason = fmt.Sprintf("%s (at line=%d byte_offset=%d)", lineReason, res.validLines, pos)
				}
			} else if looksCompleteLine(chunk) {
				// 损坏点之后仍出现完整消息行：属于中部损坏，需人工介入。
				res.midCorrupt = true
			}
			pos += int64(len(chunk))
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
	}
	res.totalBytes = pos
	return res, nil
}

// looksCompleteLine 判断一行是否“看起来是一条完整消息”（宽松校验：
// 有换行结尾、非空、是合法 JSON 信封，不检查 offset 连续性）。
// 仅用于损坏点之后的扫描，以区分尾部残缺与中部损坏。
func looksCompleteLine(chunk []byte) bool {
	if chunk[len(chunk)-1] != '\n' {
		return false
	}
	line := bytes.TrimSpace(chunk)
	if len(line) == 0 {
		return false
	}
	var m Message
	return json.Unmarshal(line, &m) == nil
}

// validateLogLine 校验单行；chunk 为 ReadBytes('\n') 返回的原始字节（含换行符，若有）。
func validateLogLine(chunk []byte, expectOffset int64) (bool, string) {
	if chunk[len(chunk)-1] != '\n' {
		return false, "torn write: final line missing newline terminator"
	}
	line := bytes.TrimSpace(chunk)
	if len(line) == 0 {
		return false, "corrupt: blank line"
	}
	var m Message
	if err := json.Unmarshal(line, &m); err != nil {
		return false, fmt.Sprintf("corrupt: invalid JSON: %v", err)
	}
	if m.Offset != expectOffset {
		return false, fmt.Sprintf("corrupt: offset mismatch, envelope=%d expect=%d", m.Offset, expectOffset)
	}
	return true, ""
}
