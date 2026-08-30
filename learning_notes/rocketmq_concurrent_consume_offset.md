# Push 并发消费：位点提交、重试与死信

以 `MessageListenerConcurrently` 消费时，线程池会并发处理同一个队列上的多条消息，
但 Broker 上每个队列只有一个消费位点。本文用「一次拉到位点 1～5，其中位点 3 消费
失败、其余成功」这个场景，把位点怎么提交、失败消息去哪了、什么时候会重复消费整条
链路讲清楚。

范围：`DefaultMQPushConsumer` 的 Pull 消费路径。POP 消费用 ACK / 不可见时间机制，
位点语义不同，不在本文范围。

相关笔记：

- [RocketMQ DefaultMQPushConsumer Pull 消费流程分析](consumer_flow_analysis.md)
  —— Rebalance、Pull、并发/顺序消费总览
- [RocketMQ 顺序消息](rocketmq_ordered_message.md) —— 顺序消费失败会卡住整条队列
- [RocketMQ 发送重试、重复消息与幂等](rocketmq_send_retry_and_idempotency.md)

## 1. 一句话结论

**并发消费提交的位点，永远是 ProcessQueue 里剩余消息的最小 offset——一个随各
消费线程完成顺序而滑动的「最小未处理位点」。** 失败的消息先被发回 Broker（进入
重试主题），然后从 ProcessQueue 移除，位点不会停在失败处，最终会提交到 6。
at-least-once 语义由 `%RETRY%` 主题的重试消息保证，不靠位点回退。唯一让位点卡在
3 的情况：发回 Broker 失败（见 5.3）。

## 2. 先分清三套「进度」

同一个 `MessageQueue` 上同时存在三个数字，不要混成一个 offset：

| 名字 | 存在哪 | 含义 | 谁在改 |
|------|--------|------|--------|
| 拉取位点 `PullRequest.nextOffset` | 本机 `PullRequest` | 下次向 Broker **拉**从哪开始 | 每次 Pull 成功后设为 `pullResult.nextBeginOffset` |
| 消费位点 `OffsetStore.offsetTable` | 本机内存，定时持久化到 Broker | 消费组认为「下次从哪开始消费」 | `processConsumeResult` 里 `updateOffset(mq, offset, true)` |
| 未结账窗口 `ProcessQueue.msgTreeMap` | 本机内存 | 已拉下、尚未从窗口移除的消息 | `putMessage` / `removeMessage` |

Broker 持久化的是第二套。重启或重平衡后，新的 `PullRequest.nextOffset` 从它重算，
**不会**接着崩溃前的内存值往下拉。

消费位点更新走 `ControllableOffset.update(target, increaseOnly=true)`
（`consumer/store/ControllableOffset.java`）：只允许位点变大。并发批次乱序完成
（先完成 5、后完成 3）时，后写入的小值不会把进度拉回去。

关键默认参数（均可配置）：

| 参数 | 默认值 | 作用 |
|------|--------|------|
| `consumeMessageBatchMaxSize` | 1 | 一次用户回调通常只收到一条消息 |
| `pullBatchSize` | 32 | 一次 Pull 最多拉多少条 |
| `persistConsumerOffsetInterval` | 5s（首次约 10s 后） | 消费位点持久化到 Broker 的间隔 |
| `maxReconsumeTimes` | 16 | 重试次数上限，超过进死信 |
| `consumeTimeout` | 15 分钟 | 窗口内消息的消费超时阈值 |
| `consumeConcurrentlyMaxSpan` | 2000 | 窗口内最大位点差，超过则暂停拉取 |

## 3. 拉下来之后：ProcessQueue 窗口

Rebalance 给本实例分到队列后，用 `OffsetStore.readOffset(READ_FROM_STORE)` 算出
起始位点，创建携带空 `ProcessQueue` 的 `PullRequest`。`PullMessageService` 拉到
位点 1～5 后（`DefaultMQPushConsumerImpl.pullMessage` 的 `PullCallback.onSuccess`）：

```text
pullRequest.nextOffset = nextBeginOffset      // 例如 6
processQueue.putMessage(msgs)                 // TreeMap 放入 1～5
consumeMessageService.submitConsumeRequest    // 提交线程池
```

`ProcessQueue` 是这条队列在本机的消费快照，核心就一个按 offset 排序的
TreeMap（`ProcessQueue.java`）：

```java
private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<>();
```

`putMessage` 放入 1～5 后：`msgTreeMap = {1,2,3,4,5}`，`queueOffsetMax = 5`。
`firstKey()` 永远是窗口里最小的未移除位点，这是本文的主角。

`consumeMessageBatchMaxSize = 1` 时，5 条消息拆成 5 个 `ConsumeRequest` 丢进
`consumeExecutor`，五个线程并行执行用户 `consumeMessage`，完成顺序任意。

## 4. 单条消息结账：processConsumeResult

每个 `ConsumeRequest.run()` 调完监听器后走
`ConsumeMessageConcurrentlyService.processConsumeResult`
（约 242～310 行）。监听器返回 `RECONSUME_LATER`、返回 `null`、或抛异常，都按
失败处理；batch=1 时成败就是这一条消息的成败。

集群模式下的失败处理顺序：

1. 对失败的消息调用 `sendMessageBack` 发回 Broker（见第 6 节）
2. 发回失败的从本次 `msgs` 列表拿掉，5 秒后重新 `submitConsumeRequest`
3. 然后统一调 `processQueue.removeMessage(msgs)`：把列表里还在的消息从 TreeMap
   删掉，**返回值就是新的消费位点**
4. `offsetStore.updateOffset(mq, offset, true)` 更新内存位点

`removeMessage`（`ProcessQueue.java` 187～223 行）的返回值规则：

```java
result = queueOffsetMax + 1;          // 先假定窗口会被清空
// ...按 queueOffset 逐条删除...
if (!msgTreeMap.isEmpty()) {
    result = msgTreeMap.firstKey();   // 窗口非空：取剩余最小位点
}
// 窗口空了：保持 queueOffsetMax + 1，即最后拉取位点 + 1
```

## 5. 场景走读：1～5 已拉下，3 失败

### 5.1 时间线：位点被窗口最小值拖着走

约定 batch=1，五个 `ConsumeRequest` 并行，完成顺序如下：

```text
窗口初始 {1,2,3,4,5}

1 成功 remove → {2,3,4,5}   提交 2
2 成功 remove → {3,4,5}     提交 3
4 成功 remove → {3,5}       提交 3   ← 4 完成了，但位点被 3 拖住
5 成功 remove → {3}         提交 3
3 失败 + sendBack 成功 remove → {}  提交 queueOffsetMax+1 = 6
```

只要 3 还在 TreeMap 里，提交值就是 3；4、5 先完成也不会越过它。3 发回成功被移除
后，位点一步跳到 6。这就是「滑动的最小未处理位点」：**位点只由窗口里最小的未
移除 offset 决定，与完成顺序无关。**

拉取和消费是脱钩的：`nextOffset` 已经是 6，本实例会继续拉 7、8… 放进窗口，但
消费位点永远过不了窗口里的最小 key。若窗口 `lastKey - firstKey` 超过
`consumeConcurrentlyMaxSpan`（默认 2000），`pullMessage` 会暂停拉取，防止 3 卡住
时窗口无限膨胀。

### 5.2 失败 + sendBack 成功：位点越过失败消息

`sendMessageBack` 成功后，消息 3 和成功的消息一样参加 `removeMessage`，从原队列
窗口移除。原队列位点最终提交到 **6**，失败的 3 此时活在 `%RETRY%<consumerGroup>`
的另一条队列上（另一套位点），延迟后重新投递给这个消费组。

要点：原队列位点前进 ≠ 丢弃失败消息。at-least-once 由重试主题保证，业务必须
幂等。

### 5.3 失败 + sendBack 失败：位点钉在 3

不能假装 3 已交给 Broker，否则消息会丢。此时（`processConsumeResult`
约 290～300 行）：

```text
sendMessageBack 失败
  → msgs.remove(3)，3 不参加 removeMessage
  → TreeMap 仍含 3
  → 提交位点 = 3
  → 5 秒后只把 3 重新 submitConsumeRequest
```

同一进程内：

- 只反复回调 **3**，不会循环重放 4、5
- `nextOffset` 仍在 6 之后，不回头拉
- 定时任务把 **3** 持久化到 Broker

注意：sendBack 一直失败时，重试消息进不了 Broker，**不会**出现「重试 16 次后进
DLQ」。`reconsumeTimes` 只在本机递增，3 只在本机打转。

### 5.4 何时会再消费 4 和 5

只有按**消费位点 3** 重新拉才会：进程重启，或队列被重平衡走再分配回来。此时
`RebalancePushImpl.computePullFromWhereWithException` 从 Broker 读到 3，新的
`PullRequest.nextOffset = 3`，会再拉到 3、4、5——4、5 已经成功过，这是重复消费。
不是「sendBack 失败持续多久就循环消费多久 4、5」。

### 5.5 兜底：过期消息清理

`ConsumeMessageConcurrentlyService.start()` 按 `consumeTimeout`（15 分钟）周期调
`cleanExpireMsg` → `ProcessQueue.cleanExpiredMsg`（75～127 行）：扫窗口头部最多
16 条，消费开始时间超时的再试一次 `sendMessageBack(msg, 3)`，**成功才从 TreeMap
删掉**；仍失败则继续钉在窗口里。

## 6. sendMessageBack：失败消息的下一站

`sendMessageBack` 不是「把原队列位点停住、下次从原 offset 重拉同一条」，而是把
失败消息在 Broker 上存一份**延迟消息**到 `%RETRY%<consumerGroup>` 主题。原 Topic、
原队列上这条消息视为已处理完。

`DefaultMQPushConsumerImpl.sendMessageBack`（约 751～785 行）两级降级：

1. 优先 RPC `consumerSendMessageBack`：Broker 根据 commitLog offset 找到原消息，
   写入重试主题
2. RPC 失败则退化为普通 `send`（`sendMessageBackAsNormalMessage`）：构造新消息发
   到 `%RETRY%<group>`，延迟级别 `3 + reconsumeTimes`

客户端返回 false（两级都失败）才会走 5.3 的「留在本机」分支。

Broker 侧（`AbstractSendMessageProcessor`，约 176～184 行）判断：
`reconsumeTimes >= maxReconsumeTimes` 或 `delayLevel < 0` 时写入
`%DLQ%<consumerGroup>`，否则按延迟级别写入 `SCHEDULE_TOPIC_XXXX`，到期后转投重试
主题。

重试消息是**一条新消息**：重投时 `resetRetryAndNamespace` 会把 topic 还原成原
Topic，`reconsumeTimes` 加一。超过次数上限进 DLQ，不再自动投递。

## 7. 位点何时落到 Broker

`processConsumeResult` 只改内存 `offsetTable`。`MQClientInstance` 的定时任务
（首次约 10s 后，每 5s）调 `persistAllConsumerOffset` →
`RemoteBrokerOffsetStore.persistAll` → `updateConsumeOffsetToBroker` 写到 Broker。

所以存在窗口：内存已是 6，Broker 上可能还是更早的值。进程崩溃落在这个窗口里，
会按 Broker 上更旧的位点重拉，产生重复消费——这也是「消费必须幂等」的另一个
来源。

## 8. 三条出路对照

```text
拉 1～5 放入 ProcessQueue
        │
        ├── 成功
        │     从 TreeMap 删除 → 位点 = 剩余最小 offset（或 max+1）
        │
        ├── 失败 + sendBack 成功
        │     从 TreeMap 删除（原队列视为已处理）
        │     消息出现在 %RETRY% → 延迟再投 → 超限进 %DLQ%
        │     位点继续前进，最终到 6
        │
        └── 失败 + sendBack 失败
              留在 TreeMap，位点钉在 3
              5s 后本地再消费 3（本实例不重放 4、5）
              重启/重平衡按位点 3 重拉 → 3、4、5 都可能再来（重复）
              sendBack 从未成功 → 永远不进 DLQ
```

## 9. 和顺序消费、batch>1、广播模式的差别

**顺序消费**：失败消息放回同一 `ProcessQueue`（`makeMessageToConsumeAgain`），
暂停该队列，位点不能越过失败那条。位点提交走 `ProcessQueue.commit()`
（267～294 行）：取 `consumingMsgOrderlyTreeMap.lastKey() + 1`——即本轮
`takeMessages` 取出那批的最大位点，失败 rollback 后这批不产生位点推进。并发消费
没有这个「取出窗口」，只看 `msgTreeMap.firstKey()`。语义区别：顺序消费停在失败
处，并发消费是滑动的最小未处理位点 + 重试主题再投。

**`consumeMessageBatchMaxSize > 1`**：一次回调多条消息只能返回一个状态：

- 默认 `ackIndex = Integer.MAX_VALUE`，`CONSUME_SUCCESS` 视为整批成功
- `RECONSUME_LATER` 把 `ackIndex` 打成 -1，整批发回重试
- 想表达「前 k 条成功」必须 `context.setAckIndex(k)`，没法只单独失败中间那条

**广播模式**：失败消息直接丢弃（只打 warn 日志），不 `sendMessageBack`，位点仍
按 `removeMessage` 前进。

## 10. 源码入口

| 步骤 | 位置 |
|------|------|
| 拉到后放入窗口并提交消费 | `DefaultMQPushConsumerImpl.pullMessage` → `PullCallback.onSuccess` |
| 拆批进线程池 | `ConsumeMessageConcurrentlyService.submitConsumeRequest` |
| 用户回调 + 异常兜底 | `ConsumeRequest.run` |
| 成败、sendBack、删窗口、改位点 | `ConsumeMessageConcurrentlyService.processConsumeResult` |
| 位点 = 剩余最小 offset | `ProcessQueue.removeMessage` |
| sendBack 两级降级 | `DefaultMQPushConsumerImpl.sendMessageBack` / `sendMessageBackAsNormalMessage` |
| 内存位点只增不减 | `RemoteBrokerOffsetStore.updateOffset` / `ControllableOffset.update` |
| 定时持久化到 Broker | `MQClientInstance.startScheduledTask`（间隔 `persistConsumerOffsetInterval`） |
| 重启从哪拉 | `RebalancePushImpl.computePullFromWhereWithException` |
| 过期消息兜底 sendBack | `ProcessQueue.cleanExpiredMsg` |
| Broker 侧延迟/死信判定 | `AbstractSendMessageProcessor`（consumer send back 处理） |
| 顺序消费位点提交 | `ProcessQueue.takeMessages` / `commit` / `makeMessageToConsumeAgain` |
