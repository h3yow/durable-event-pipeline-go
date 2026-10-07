# durable-event-pipeline-go

用本地磁盘持久化模拟消息代理与消费者的事件投递组件：分区有序、并发消费、
失败重试、死信转移、进程重启后状态可恢复，并能在“消息写一半进程被强杀/断电”
后自动识别并修复残缺数据（崩溃自愈）。

## 结构

```
internal/pipeline/
  message.go   消息信封与版本兼容规则
  log.go       单分区追加式日志（JSON Lines，offset 即行号）+ 打开时崩溃自愈
  broker.go    多分区代理，按 key 哈希分区，落盘即 fsync
  state.go     消费位点 / 幂等记录 / 重试计数 / 死信的持久化（tmp+rename 原子写）
  retry.go     指数退避策略与可区分失败类型
  recovery.go  分区恢复报告的持久化（recovery.json，tmp+rename 原子写）
  consumer.go  每分区一个消费循环，确认后提交位点
cmd/demo/      可本地运行的演示程序（启动时打印各分区恢复结果）
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

## 崩溃自愈（半截写入现场）

真实故障中进程可能在一次 `write` 落到一半时被 `SIGKILL` 或断电：分区日志
尾部留下一段不是完整 JSON 信封的残缺字节。旧版本会把残缺行计入长度，导致该
分区每次读取都反序列化失败、消费循环永久卡在原地。组件现在在**打开每个分区
日志时做一次单遍扫描**并自动处置：

- **尾部残缺（自动修复）**：若损坏只出现在最后一条非空记录（写一半崩溃只会
  造成这种形态），组件：
  1. 把残缺字节原样留存到取证文件 `partition-N.offset-M.torn`（原子 rename）；
  2. 把日志 `truncate` 到损坏行起点之前并 fsync（含目录项）；
  3. 该分区以修复后的长度继续接收写入与消费，前面的好数据一条不动。
- **中部损坏（人工介入）**：若损坏行之后还存在数据（不属于半截写入，可能是
  磁盘/人为改写），组件**绝不截断**，将该分区标记为需人工介入
  （`NeedsIntervention`），该分区的消费循环安静停摆、新写入被拒绝，
  其余健康分区照常推进。日志文件原样保留，等待人工检查。
- **校验口径**：一行必须是可解析且带 `id` 的信封才算有效记录；**未知版本不
  算损坏**，照常保留并由消费端按版本兼容规则转入死信。

确定性与幂等：

- 修复是纯函数式的“截断到最后一条有效记录之后”，同一份损坏现场无论重启
  多少次，磁盘结果与恢复指标完全一致；重复打开看到的是健康日志，不会再截断
  好数据，也不会凭空多/少消息。
- 修复事件持久化到 `recovery.json`（每分区最新状态 + 事件流水）；同一现场
  （分区/原因/损坏位点/截断字节数/前后规模相同）只记一条事件，重启不膨胀。
- 修复发生在任何新写入之前；修复若在执行中再次崩溃，重启后重新单遍扫描，
  仍然收敛到同一结果。

对既有语义的影响：

- **投递/确认**：已确认消息与已提交位点完全不受影响，修复只可能裁掉尾部
  “从未成功落盘”的不完整记录，位点只进不退；修复后新消息的 offset 紧接
  有效记录连续编号。那条没写完整的消息如同未投递成功，由生产者重发即可。
- **分区顺序 / 跨分区并发**：修复后分区内 offset 依旧连续、消费依旧严格
  有序；打开时各分区独立扫描，单个分区停摆不影响其他分区并发消费。
- **兼容性边界**：自愈只针对本组件自己的 JSON Lines 分区日志格式与
  `fsync`+`O_APPEND` 的写模型；`consumer-state.json` 与 `recovery.json`
  仍是原子整体写，本身不存在半截写问题。手工删改这些文件不属于支持场景。

可观测性（程序内查询）：

- `Broker.RecoveryStatus() []PartitionRecovery`：每个分区本次打开的恢复
  结果，含 `Healthy/Repaired/NeedsIntervention`、修复前后记录数
  （`BeforeRecords/AfterRecords`）、移除条数、截断字节数、损坏位点与
  取证文件路径；
- `Broker.RecoveryEvents()`：跨重启持久化的修复/介入事件流水；
- `Broker.NeedsIntervention(partition)`：是否存在需人工介入的异常分区。

性能：打开时的扫描是 O(文件字节数) 单遍读 + 尾部 O(1) `truncate`，不会
重读全文件，也不在消费热路径上。实测单分区 20 万条记录（约 30MB）恢复扫描
约 0.3s（见 `TestRecoveryScalesLinearly`，规模放大 20 倍耗时按线性预算
校验，专门用于发现 O(n²) 退化）。

## 本地运行与验证

```bash
# 运行演示（重复执行可观察位点恢复，状态在 ./demo-data）
go run ./cmd/demo

# 运行全部测试（日志含分区标识、位点与失败原因）
go test -v ./internal/pipeline/

# 稳定性验证：竞态检测 + 反复运行
go test -race -count=5 ./...

# 仅大规模线性恢复测试（-short 时跳过）
go test -v -run TestRecoveryScalesLinearly ./internal/pipeline/
```

手动复现半截写入自愈：

```bash
# 1) 先跑一次 demo，落一批消息
DEMO_DIR=/tmp/demo-rec go run ./cmd/demo
# 2) 往某个分区日志尾部直接追加一段不完整 JSON（无闭合、无换行），模拟断电
printf '%s' '{"id":"manual-torn","version":1,"partit' \
  >> /tmp/demo-rec/partition-1.log
# 3) 再次运行：启动日志会打印
#    partition=1 RECOVERY repaired reason=torn_tail_truncated records N->N-1 ...
#    并生成取证文件 /tmp/demo-rec/partition-1.offset-*.torn
DEMO_DIR=/tmp/demo-rec go run ./cmd/demo
cat /tmp/demo-rec/recovery.json   # 查看分区级恢复报告与事件流水
```

测试覆盖：分区内顺序（`TestPartitionOrder`）、跨分区并发（`TestCrossPartitionConcurrency`）、
重试后成功（`TestRetryThenSuccess`）、重试上限与不可重试死信（`TestNonRetryableToDeadLetter`）、
重启恢复（`TestRestartRecovery`）、版本兼容（`TestVersionCompat`）、
重复投递幂等（`TestDuplicateDeliveryNoDuplicateEffect`）、
尾部半截写入自愈（`TestTornTailRecovery`）、恢复幂等与跨重启一致性
（`TestRecoveryIdempotent`）、修复后继续消费且位点不回退
（`TestConsumeAfterRecovery` / `TestTornTailDoesNotRegressProgress`）、
中部损坏人工介入且健康分区不被拖垮（`TestMidFileCorruptionIntervention`）、
恢复报告可查询（`TestRecoveryStatusVisible`）、大规模线性恢复
（`TestRecoveryScalesLinearly`）。
