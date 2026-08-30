# RocketMQ 发送重试、重复消息与幂等

> 本文回答两件事：发送失败后重试会不会写出重复消息、该如何规避影响；以及
> Apache RocketMQ 有没有 Kafka 那种幂等 Producer。
> 结论以当前工作区源码与仓库内文档为准。
> 发送链路细节见
> [`rocketmq_producer_send_source_flow.md`](rocketmq_producer_send_source_flow.md)
> 第 12 节；队列选择与换 Broker 见
> [`producer-message-queue-selection.md`](producer-message-queue-selection.md)。

## 1. 发送失败重试会不会产生重复消息？

**会。** Producer 发送失败后的自动重试按 **at-least-once** 设计：消息可能已经在
Broker 落盘，只是客户端没拿到成功确认，再发一次就会多出一条内容相同的消息。
RocketMQ **不会**靠这次重试自动做成 exactly-once。

官方文档同样写明：同步/异步发送失败会重发，尽量避免丢失，但会导致重复；
FAQ 只承诺 **at least once**。见 `docs/en/Feature.md`（Message Resend）和
`docs/en/FAQ.md`（Are messages delivered exactly once?）。

### 1.1 失败分类

| 情况 | 消息是否已写入 | 重试是否重复 |
|------|----------------|--------------|
| 请求根本没到 Broker（连不上、写 socket 失败且未发出） | 通常否 | 一般不重复 |
| 已 append，响应超时 / 网络中断 | **是** | **会重复** |
| `FLUSH_DISK_TIMEOUT` / `FLUSH_SLAVE_TIMEOUT` | **可能已写入** | 默认不换 Broker 再试；若打开 `retryAnotherBrokerWhenNotStoreOK` 则更容易重复 |
| Broker 返回可重试错误码（如 `SYSTEM_BUSY`） | 取决于拦截发生在写入前还是后 | 可能重复 |
| `InterruptedException` | 不确定 | 立即抛出，不自动重试 |

默认同步发送：`retryTimesWhenSendFailed=2`，一共最多 3 次。重试会重新选队列，并尽量
避开上次 Broker。异步路径用 `retryTimesWhenSendAsyncFailed`（默认同样是 2）。
ONEWAY 无重试、无结果。

### 1.2 最典型的窗口：等响应超时

```text
Producer                Broker
   | ---- request ------> |
   |                      | append 成功
   | <X-- response -------| 响应丢失或回来太晚
   | timeout              |
   | ---- retry --------> | 第二次 append
```

客户端无法从超时本身判断第一次请求停在网络、Broker 队列、CommitLog append、刷盘还是
响应返回阶段。

### 1.3 `UNIQ_KEY` 不会在存储层去重

同一条 `Message` 的自动重试会沿用同一个 `UNIQ_KEY`（`MessageClientIDSetter.setUniqID`，
属性名 `UNIQ_KEY`）。它用于追踪和 IndexFile 按 Key 查询，**普通消息存储不会按
`UNIQ_KEY` 去重**。两次 append 就是两条独立消息（offset 不同）。

消费侧还有另一类重复：位点大约 5 秒批量提交，重启/重平衡时可能再拉已消费过的消息。
这和发送重试是两件独立的事，最终都要业务幂等兜底。

## 2. 怎样避免影响

1. **消费端幂等（主路径）**  
   用稳定业务主键（订单号、事件 ID、幂等键），不要用 Broker `msgId`/`offset` 当唯一
   业务键。落库前查重、主键冲突即跳过、或 Redis `SETNX` + 业务事务。
   `docs/cn/best_practice.md` 明确：`msgId` 全局唯一，但「同一业务内容、两个不同
   msgId」仍然可能（客户端重投、业务主动重发）。

2. **Producer 不要在框架已重试之外再盲目重试**  
   `send()` 抛异常后再由业务循环重发，会叠加重试次数。若必须自己重试，要和客户端
   超时、`retryTimesWhenSendFailed` 对齐，并仍用同一业务幂等键。

3. **顺序消息：失败不要换队列**  
   用 `MessageQueueSelector` 把同一 sharding key 钉在同一队列。默认同步发送的
   「换 Broker 重试」不会套在这条路径上；业务自行重试时必须仍用同一 selector。

4. **能区分的失败就不要重试**  
   消息体非法、无权限等确定性错误，重试只会重复失败或放大重复。客户端只对
   `retryResponseCodes` 等可恢复错误自动重试。

5. **不要把 `send()` 无异常当成「只写成功一次」**  
   一次调用内部可能已经多次写入。业务成功应以消费幂等 + 必要时查询下游状态为准。

`retryAnotherBrokerWhenNotStoreOK` 默认 `false`：刷盘或副本超时通常直接返回非
`SEND_OK` 的 `SendResult`，不会再换 Broker。打开后第一次消息已经 append，更容易重复。

## 3. RocketMQ 有没有 Kafka 那种幂等 Producer？

**没有。** Apache RocketMQ 没有 Kafka 那种「Broker 按 PID + 序号拦截重复写入」的
幂等 Producer。不要在源码或配置里找 `enable.idempotence` 的等价开关。

发送头上也没有 PID / sequence / producer epoch 那套字段。

### 3.1 Kafka 幂等 Producer 在做什么

`enable.idempotence=true` 之后：

1. Broker 给 Producer 实例分配 **PID**（会话级）。
2. 每个 **topic-partition** 上消息带单调递增 **sequence**。
3. Broker 只接受 `lastSeq + 1`；同一 `(PID, partition, seq)` 再来一次就当重复，
   **不再 append**。

因此：**同一次 Producer 会话里、发到同一分区的自动重试**，不会在日志里多出一条。
它不覆盖：

- Producer 进程重启（新 PID，序号从 0 开始）
- 业务自己再 `send` 一次（新序号，Broker 当新消息）
- 换分区重试（状态按分区记）
- 消费侧未提交位点导致的重复消费（跨消费-生产要靠事务 EOS）

### 3.2 RocketMQ 里容易被误认成「幂等」的机制

| 机制 | 实际作用 | 是不是 Kafka 幂等 Producer |
|------|----------|---------------------------|
| `UNIQ_KEY` | 客户端唯一 ID，重试沿用；IndexFile 可按它查询 | **否**。存储仍会再 append |
| 同步/异步发送重试 | 失败后换队列、尽量换 Broker | **否**。为可用性，更容易写出第二条 |
| 事务消息 | 本地事务与半消息提交/回查 | **否**。解决的是「业务事务是否提交」，不是重试去重 |
| `duplicationEnable` | 存储侧复制相关开关 | **否**。和 Producer 幂等无关 |
| 消费过程幂等（最佳实践） | 用业务主键去重 | 应用层，不是 Broker 协议 |

Static Topic 设计文档（`docs/cn/statictopic/`）提过「将来实现的幂等数据」迁移，
那是预留口吻，当前源码没有 Kafka 式的 per-producer sequence 校验。

### 3.3 为什么不好直接抄 Kafka

Kafka 幂等依赖：**重试必须打回同一分区**，Broker 只记该分区上的 last seq。
RocketMQ 默认同步失败会 **换 Broker/换队列**，就是为了躲开故障节点。若按 Kafka
方式去重，重试必须钉死队列，和现在的故障规避是相反方向。

Kafka 幂等本身也只保证「日志里不因 **协议层重试** 重复」，不是端到端业务
Exactly-Once。消费重复、应用重发、重启后重投，两边都还要业务幂等。

## 4. 结论

- 发送失败重试 **可以** 产生重复消息，这是 at-least-once 的代价。
- Apache RocketMQ **没有** Kafka 式 Broker 侧幂等 Producer。
- 规避影响的正确做法是 **消费（及下游）按业务主键幂等**，而不是指望 Broker
  按 `UNIQ_KEY` 去重，或关掉所有重试。
- 若必须在存储层消灭 Producer 重试重复，需要自己做（或等产品层能力），
  开源协议目前没有这一层。
