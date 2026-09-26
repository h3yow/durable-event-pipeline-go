# durable-event-pipeline-go

用本地磁盘持久化模拟消息代理与消费者的事件投递组件：分区有序、并发消费、
失败重试、死信转移、进程重启后状态可恢复。

## 结构

```
internal/pipeline/
  message.go   消息信封与版本兼容规则
  log.go       单分区追加式日志（JSON Lines，offset 即行号）
  broker.go    多分区代理，按 key 哈希分区，落盘即 fsync
  state.go     消费位点 / 幂等记录 / 重试计数 / 死信的持久化（tmp+rename 原子写）
  retry.go     指数退避策略与可区分失败类型
  consumer.go  每分区一个消费循环，确认后提交位点
cmd/demo/      可本地运行的演示程序
```

## 投递与确认语义

- **投递**：`Broker.Produce(key, version, payload)` 按 key 的 FNV 哈希选定分区，
  追加到该分区日志并 fsync 后返回 `(partition, offset)`；offset 在分区内从 0 连续编号。
- **分区内有序**：每个分区由独立循环按 offset 顺序逐条处理，前一条未确认
  （成功或进入死信）绝不处理下一条，位点也不会越过未确认消息。
- **跨分区并发**：各分区循环互不阻塞，一个分区退避重试期间其他分区照常推进。
- **确认**：位点（`committed = 下一个待消费 offset`）只在消息确认后提交并原子落盘。
- **幂等**：handler 成功后先记录 `applied[messageID]` 再提交位点；若崩溃发生在
  两者之间，重启后消息被重复投递，但会因已标记 applied 而跳过业务效果，
  保证“重复投递不产生重复效果”。

## 重试与死信规则

- handler 返回 `*pipeline.Failure{Retryable: true}`（或普通 error）视为可重试失败，
  按 `BackoffPolicy`（`Initial * Multiplier^(n-1)`，封顶 `Max`）退避后重投，
  退避只阻塞本分区。
- 重试计数按消息 ID 持久化，进程重启后继续累计，不会重置。
- 以下失败进入死信（`StateStore.DeadLetters()`），原因可区分：
  - `max_retries_exceeded`：可重试失败超出 `MaxRetries`；
  - `non_retryable`：`Failure{Retryable: false}`；
  - `unsupported_version`：消息版本不在支持列表。
- 进入死信即视为“已确认”，位点越过该消息继续推进，死信记录中含原始消息与原因。

## 版本兼容策略

`UnmarshalEnvelope` 按确定规则处理，绝不静默丢弃：

1. **缺少 `version` 字段**：回落到 `DefaultVersion = 1`，并在消息的
   `CompatNote` 中记录（随消息落盘，消费端持久化为 compat note）。
2. **未知顶层字段**：保留到 `Message.Extra`，handler 可读取，不丢弃。
3. **未知版本**：消息照常落盘，消费端识别后按不可重试失败转入死信，
  原因 `unsupported_version`。

## 状态恢复

- 分区日志：重启后扫描行数恢复下一个 offset，已写入消息不丢。
- 消费状态：`consumer-state.json` 保存位点、applied 集合、重试计数、死信、
  兼容记录；每次变更先写 `.tmp` 再 rename 并 fsync 目录，崩溃不会留下半截状态。
- 重启后从已提交位点继续，未确认消息重新投递，位点不会重复推进。

## 本地运行与验证

```bash
# 运行演示（重复执行可观察位点恢复，状态在 ./demo-data）
go run ./cmd/demo

# 运行全部测试（日志含分区标识、位点与失败原因）
go test -v ./internal/pipeline/

# 稳定性验证：竞态检测 + 反复运行
go test -race -count=5 ./...
```

测试覆盖：分区内顺序（`TestPartitionOrder`）、跨分区并发（`TestCrossPartitionConcurrency`）、
重试后成功（`TestRetryThenSuccess`）、重试上限与不可重试死信（`TestNonRetryableToDeadLetter`）、
重启恢复（`TestRestartRecovery`）、版本兼容（`TestVersionCompat`）、
重复投递幂等（`TestDuplicateDeliveryNoDuplicateEffect`）。
