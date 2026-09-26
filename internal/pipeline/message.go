package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"
)

func joinNotes(n []string) string { return strings.Join(n, "; ") }

// DefaultVersion 是消息缺少版本字段时按兼容规则回落到的版本。
const DefaultVersion = 1

// SupportedVersions 列出当前可解码的消息版本；其余版本视为不支持。
var SupportedVersions = map[int]bool{1: true, 2: true}

// Message 是写入分区的持久化消息。
type Message struct {
	ID        string          `json:"id"`
	Partition int             `json:"partition"`
	Offset    int64           `json:"offset"`
	Version   int             `json:"version"`
	Key       string          `json:"key,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	// Extra 保存信封中未识别的顶层字段：兼容规则要求保留而非静默丢弃。
	Extra map[string]json.RawMessage `json:"extra,omitempty"`
	// CompatNote 记录解析时触发的兼容规则（如缺版本回落），随消息落盘。
	CompatNote string `json:"compat_note,omitempty"`
}

// knownEnvelopeFields 是信封层已识别的字段名。
var knownEnvelopeFields = map[string]bool{
	"id": true, "partition": true, "offset": true,
	"version": true, "key": true, "payload": true, "extra": true,
}

// UnmarshalEnvelope 按确定的兼容规则解析原始字节为 Message：
//  1. 缺少 version 字段：回落到 DefaultVersion，不视为错误；
//  2. 未知顶层字段：收集进 Extra 保留，不静默丢弃；
//  3. version 不在 SupportedVersions 中：消息与不可重试的 *Failure 一并返回，
//     便于落盘后由消费端转入死信，原因 "unsupported_version"。
func UnmarshalEnvelope(data []byte) (*Message, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, &Failure{Reason: fmt.Sprintf("malformed envelope: %v", err), Retryable: false}
	}
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, &Failure{Reason: fmt.Sprintf("malformed envelope: %v", err), Retryable: false}
	}
	var notes []string
	if _, ok := raw["version"]; !ok {
		m.Version = DefaultVersion
		notes = append(notes, "missing version field, defaulted to 1")
	}
	for k, v := range raw {
		if knownEnvelopeFields[k] {
			continue
		}
		if m.Extra == nil {
			m.Extra = map[string]json.RawMessage{}
		}
		m.Extra[k] = v
		notes = append(notes, "unknown field preserved: "+k)
	}
	m.CompatNote = joinNotes(notes)
	if !SupportedVersions[m.Version] {
		return &m, &Failure{
			Reason:    fmt.Sprintf("unsupported_version: version=%d", m.Version),
			Retryable: false,
		}
	}
	return &m, nil
}

// DecodePayload 将消息体解码进 v。未知字段按 json 默认行为忽略，
// 但信封层未知字段已在 UnmarshalEnvelope 中保留到 Extra。
func DecodePayload(m *Message, v any) error {
	if err := json.Unmarshal(m.Payload, v); err != nil {
		return &Failure{Reason: fmt.Sprintf("decode payload: %v", err), Retryable: false}
	}
	return nil
}
