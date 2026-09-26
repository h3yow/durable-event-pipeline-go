# durable-event-pipeline-go

一套**本地可运行**的事件投递与消费组件：用本地文件持久化模拟消息代理
（broker）与消费者，使分区有序、并发消费、失败重试、死信转移以及进程重启
后的状态恢复都可预期、可重复验证。

- 仅依赖 Go 标准库，无外部中间件。
- 所有写入 `fsync` 落盘；状态文件以 `tmp + fsync + rename` 原子替换。
- 每个分区一个独立 worker：分区内严格按 offset 顺序处理，分区间并发。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `internal/codec` | 消息信封（envelope）的版本化 JSON 编解码与兼容规则 |
| `internal/store` | 本地持久层：分区追加日志、死信日志、原子状态文件 |
| `internal/broker` | 投递/确认/退避重试/死信/恢复的核心逻辑与退避策略 |
| `cmd/pipeline-demo` | 可执行的本地演示（含“进程重启恢复”两次运行） |

## 本地存储布局

```
<data-dir>/
├── state.json                 # 消费组状态（原子替换）
├── dead.log                   # 死信记录（JSON Lines，追加、fsync）
└── partitions/
    └── <partition>.log        # 每个分区一条 JSON Lines 追加日志
```

- **分区日志**每行一条 `{"offset":n,"envelope":{...}}`，offset 从 0 单调递增。
- **state.json** 内容：
  - `committed`：每个分区**下一个待消费 offset**（= 已确认的最高 offset + 1）；
  - `in_flight`：已投递但未确认的记录（分区、offset、已尝试次数、最早可见时间）；
  - `processed`：幂等去重表（消息 ID -> offset）；
  - `dead`：死信记录的信息性镜像（`dead.log` 才是事实来源）。
- 崩溃时分区日志末尾若留下**写了一半的残行**，恢复时会确定性地截断该行，
  不会导致整个 broker 无法启动。

## 投递与确认语义

- `PublishTo(partition, env)`：消息追加到该分区日志并 `fsync` 后才返回
  offset；同分区严格按写入顺序读取。
- 每个分区由**唯一一个 worker** 串行处理，任意时刻该分区最多一条
  **在途（in-flight）**消息。因此：
  - 同一分区按 offset 顺序处理，下一条必须等上一条被确认或转入死信；
  - 未确认消息永远不会被位点越过（位点不能跳过失败中的队头）；
  - 不同分区各自独立 worker，一个分区的重试/阻塞不影响其他分区。
- 交付语义为**至少一次（at-least-once）**：
  - handler 返回 `nil` = ack：先把 `消息ID -> 去重表` 与
    `committed offset` 写入**同一份原子状态快照并同步落盘**，再处理下一条；
  - 若崩溃发生在“业务效果完成之后、状态落盘之前”，重启会重新投递。
    框架在确认时持久化了消息 ID 去重键；恢复期间若发现该消息已记录效果，
    会跳过 handler 直接推进位点（测试 `TestRestartDedupSkipsRecordedEffect`
    覆盖）。需要端到端业务幂等的场景，handler 应基于 `env.ID` 做幂等。

## 重试与死信规则

handler 返回的错误按以下规则分类（`broker.RetryPolicy`）：

| 错误类型 | 行为 | 死信原因 |
| --- | --- | --- |
| `nil` | ack，推进位点 | — |
| 普通 `error` | 视为可重试，原因 `handler_error` | 超过 `MaxAttempts` 后 `max-attempts-exceeded:handler_error` |
| `NewNackError(err, true, reason)` | 可重试，带稳定原因码 | 超过 `MaxAttempts` 后 `max-attempts-exceeded:<reason>` |
| `NonRetryable(err, reason)` | **不重试**，立即转死信 | `<reason>` |
| 信封 JSON 无法解析 | 不重试，立即转死信 | `unparseable-envelope` |

- `MaxAttempts` 是**总尝试次数**（含首次）；`<=1` 表示不重试。
- 退避由 `Backoff(n)` 决定（n 从 1 开始表示第 1 次重投前的等待），内置
  `ConstantBackoff` 与 `ExponentialBackoff(初始, 上限)`。
- 重试等待期间，该分区队头停留在 `in_flight`：**不阻塞其他分区**，
  本分区位点也**不会越过它**。
- 进入死信时，原始消息、分区、offset、尝试次数、原因、时间戳写入
  `dead.log`（fsync），随后位点才推进，因此失败消息不会丢失，后续消息可
  继续消费。

## 进程重启后的状态恢复

`broker.New` 打开目录时：

1. 回放所有分区日志，重建每条分区的高水位（next offset）；
2. 加载 `state.json`，恢复已提交位点、在途记录、去重表与死信；
3. 加载 `dead.log`，恢复全部死信记录；
4. 将**在途记录的退避截止时间清空**——重启后立即重投，但**已尝试次数保留**
   （重启不会“刷新”重试预算）。

崩溃/重启保证：已确认的位点不会回退、不会重复推进；未确认的消息一定被
再次投递；消息本体存于追加日志，未确认就不会丢失。

## 消息版本与字段兼容策略

信封当前版本 `v=1`。编解码遵循确定性兼容规则（见 `internal/codec`）：

- **缺少 `v` 字段**：按旧版 `v=0` 解析，已知字段照常读取，不报错、不丢弃；
  若旧文档连 `event` 字段都没有（自由格式 JSON），原始字节整体保留在
  `payload` 中；重写时升级为当前版本号。
- **更高的未知版本号**（前向兼容）：已知字段照常解析，
  `env.UnknownVersion = true` 标记给 handler；未知字段保留；重写时**保持原
  版本号不变**，不静默升级、不拒绝投递。
- **未知字段**：任何版本下都原样保留在 `env.Unknown`，经一次
  decode/encode 往返字节级不变（不会静默丢弃未来字段）。
- **确实损坏的 JSON**：返回错误，由 broker 以 `unparseable-envelope` 转入
  死信，而不是悄悄吞掉。

## 验证方法

```bash
# 全部自动化测试（顺序、并发、重试上限、死信原因、重启恢复、兼容、残行截断）
go test ./... -count=1

# 竞态检测
go test ./... -race -count=1

# 反复运行验证稳定性
go test ./... -count=10
```

测试日志统一打印**分区标识、offset、attempt 与失败原因**，例如：

```
deliver    partition=orders offset=2 id=e3 attempt=1
nack       partition=orders offset=2 id=e3 attempt=1 reason=dependency-down retry_in=40ms
dead-letter partition=orders offset=2 id=e3 attempt=3 reason=max-attempts-exceeded:dependency-down
ack        partition=orders offset=2 id=e3 attempt=3 committed=3
```

覆盖场景：

| 测试 | 覆盖点 |
| --- | --- |
| `TestPartitionOrderAndConcurrency` | 分区内严格有序 + 跨分区并发 |
| `TestRetryBackoffAndDeadLetter` | 退避重试、重试上限、非重试直转、可区分原因、位点继续推进 |
| `TestRetryDoesNotBlockOtherPartitions` | 重试分区不阻塞其他分区且位点不越过未完成消息 |
| `TestRestartRecoversInFlightAndOffsets` | 重启恢复在途（次数保留）、位点不丢不重、死信恢复 |
| `TestRestartDedupSkipsRecordedEffect` | 崩溃窗口后重启不产生重复业务效果 |
| `TestBrokerEndToEndVersionCompatibility` | 缺版本 / 未来版本 / 未知字段的端到端兼容投递 |
| `TestTornTailTruncation` | 崩溃残行的确定性恢复 |
| `codec_test.go` | 各类版本与字段兼容单元规则 |

### 本地演示（含重启）

```bash
# 第一次运行：发布混合批次，进程退出时让失败消息停留在“在途退避”中
go run ./cmd/pipeline-demo --dir /tmp/demo-data --run first

# 第二次运行：从同一目录恢复，立即重投在途消息并完成死信转移
go run ./cmd/pipeline-demo --dir /tmp/demo-data --run second
```

第一次运行可观察到 `committed orders=2`、两条消息 in-flight；第二次运行
日志出现 `recovery in_flight=2`、`redeliver ... attempt=2/3`，最终
`committed orders=3 billing=2` 且 `dead letters=3`。
