# RocketMQ DefaultMQPushConsumer Pull 消费流程分析

> 本文以当前代码中 `DefaultMQPushConsumer` 的经典 `MessageRequestMode.PULL` 路径为准。POP 模式使用 `PopRequest`、`PopProcessQueue` 和独立的 POP 消费服务，并以 ACK/ChangeInvisibleTime 管理确认，不在本文范围内。

## 1. 核心类关系总览

```
MQPushConsumer (接口)
  └── DefaultMQPushConsumer (外观类, 用户操作入口)
        └── DefaultMQPushConsumerImpl (内部实现, 核心逻辑)
              ├── RebalanceImpl (队列分配)
              │     └── RebalancePushImpl (Push模式实现)
              ├── ConsumeMessageService (消费服务接口)
              │     ├── ConsumeMessageConcurrentlyService (并发消费)
              │     └── ConsumeMessageOrderlyService (顺序消费)
              ├── ProcessQueue (处理队列快照)
              ├── PullAPIWrapper (拉取API封装)
              └── OffsetStore (消费进度存储)
                    ├── RemoteBrokerOffsetStore (集群模式: 远程存储)
                    └── LocalFileOffsetStore (广播模式: 本地文件)

MQClientInstance (客户端共享组件)
  ├── PullMessageService (拉取线程，处理 PullRequest / PopRequest)
  └── RebalanceService (重平衡线程)
```

**核心流程**: `Rebalance(分配队列) → Pull(拉取消息) → Consume(消费消息) → 更新并持久化 offset`

---

## 2. 启动流程 (DefaultMQPushConsumerImpl.start)

### 2.1 启动时序

```
DefaultMQPushConsumer.start()
  └── DefaultMQPushConsumerImpl.start()          [line 923]
        ├── checkConfig()                        [line 1025] 校验配置
        ├── copySubscription()                   复制订阅信息
        ├── 创建 MQClientInstance                [line 938] 客户端实例
        ├── 初始化 RebalanceImpl                  [line 940-943]
        ├── 创建 PullAPIWrapper                   [line 946-949]
        ├── 创建 OffsetStore                     [line 952-966]
        │     ├── BROADCASTING → LocalFileOffsetStore
        │     └── CLUSTERING → RemoteBrokerOffsetStore
        ├── offsetStore.load()                   [line 967] 加载偏移量
        ├── 创建 Pull / POP ConsumeMessageService [line 969-982]
        │     ├── MessageListenerOrderly    → ConsumeMessageOrderlyService
        │     └── MessageListenerConcurrently → ConsumeMessageConcurrentlyService
        ├── 启动 consumeMessageService 和 consumeMessagePopService [line 984-986]
        ├── mQClientFactory.registerConsumer()   [line 988] 注册消费者
        ├── mQClientFactory.start()              [line 997] 启动客户端
        │     ├── 启动 PullMessageService         [ServiceThread, 独立线程]
        │     ├── 启动 RebalanceService           [ServiceThread, 独立线程]
        │     └── 启动各种定时任务(心跳、offset持久化等)
        └── 成功发送首次心跳后 rebalanceImmediately() [line 1015-1016]
```

### 2.2 关键初始化点

**ConsumeMessageService 创建** (line 969-982):
```java
if (messageListener instanceof MessageListenerOrderly) {
    this.consumeOrderly = true;
    this.consumeMessageService = new ConsumeMessageOrderlyService(this, ...);
    this.consumeMessagePopService = new ConsumeMessagePopOrderlyService(this, ...);
} else if (messageListener instanceof MessageListenerConcurrently) {
    this.consumeOrderly = false;
    this.consumeMessageService = new ConsumeMessageConcurrentlyService(this, ...);
    this.consumeMessagePopService = new ConsumeMessagePopConcurrentlyService(this, ...);
}
```

**OffsetStore 选择** (line 952-966):
- `BROADCASTING` → `LocalFileOffsetStore`: 本地文件存储 (每个消费者独立)
- `CLUSTERING` → `RemoteBrokerOffsetStore`: 远程Broker存储 (消费者组共享进度)

---

## 3. Rebalance 队列分配机制

### 3.1 RebalanceService 线程

`RebalanceService` 是一个 `ServiceThread`，运行在独立线程中。平衡时的默认间隔为 **20 秒**，不平衡时降为默认 **1 秒**；两者都可通过系统属性配置。

```java
// RebalanceService.java line 40-58
public void run() {
    long realWaitInterval = waitInterval;
    while (!this.isStopped()) {
        this.waitForRunning(realWaitInterval);
        long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
        if (interval < minInterval) {
            realWaitInterval = minInterval - interval;
        } else {
            boolean balanced = this.mqClientFactory.doRebalance();
            realWaitInterval = balanced ? waitInterval : minInterval;
            lastRebalanceTimestamp = System.currentTimeMillis();
        }
    }
}
```

### 3.2 Rebalance 流程

```
RebalanceService.run()
  └── MQClientInstance.doRebalance()
        └── DefaultMQPushConsumerImpl.doRebalance()
              └── RebalanceImpl.doRebalance(isOrder)       [line 232]
                    ├── 遍历所有订阅的 topic
                    │     ├── clientRebalance(topic) 客户端Rebalance?
                    │     │     ├── true → rebalanceByTopic(topic)  [line 268]
                    │     │     └── false → getRebalanceResultFromBroker(topic)  [line 345]
                    │     └── 更新 ProcessQueueTable
                    └── truncateMessageQueueNotMyTopic()  [line 259] 清理不再归属的队列
```

### 3.3 客户端 Rebalance 策略 (rebalanceByTopic)

**广播模式 (BROADCASTING)** (line 271-285):
- 直接获取 topic 下所有队列，所有消费者消费全部队列

**集群模式 (CLUSTERING)** (line 287-342):
```java
// 1. 获取所有队列
Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
// 2. 获取同组所有消费者ID
List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
// 3. 排序后使用分配策略分配
Collections.sort(mqAll);
Collections.sort(cidAll);
List<MessageQueue> allocateResult = strategy.allocate(
    this.consumerGroup, this.mQClientFactory.getClientId(), mqAll, cidAll);
// 4. 更新 ProcessQueueTable
boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
```

### 3.4 队列分配策略更新 (updateProcessQueueTableInRebalance) [line 426]

```java
// 1. 标记当前 topic 中不再归属或拉取已过期的队列为 dropped
for (Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
    MessageQueue mq = entry.getKey();
    ProcessQueue pq = entry.getValue();
    if (mq.getTopic().equals(topic)
        && (!mqSet.contains(mq) || (pq.isPullExpired() && consumeType() == CONSUME_PASSIVELY))) {
        pq.setDropped(true);  // 标记丢弃
        removeQueueMap.put(mq, pq);
    }
}
// 2. 清理成功后才移除；顺序消费可能等待本地消费锁
for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
    if (removeUnnecessaryMessageQueue(entry.getKey(), entry.getValue())) {
        processQueueTable.remove(entry.getKey());
    }
}
// 3. 为新分配的队列创建 PullRequest
for (MessageQueue mq : mqSet) {
    if (!processQueueTable.containsKey(mq)) {
        if (needLockMq && !lock(mq)) continue;
        removeDirtyOffset(mq);
        ProcessQueue pq = createProcessQueue();
        long nextOffset = this.computePullFromWhere(mq);
        if (nextOffset >= 0 && processQueueTable.putIfAbsent(mq, pq) == null) {
            PullRequest pullRequest = new PullRequest();
            pullRequest.setConsumerGroup(consumerGroup);
            pullRequest.setNextOffset(nextOffset);
            pullRequest.setMessageQueue(mq);
            pullRequest.setProcessQueue(pq);
            pullRequestList.add(pullRequest);
        }
    }
}
// 4. 将 PullRequest 分发给 PullMessageService
this.dispatchPullRequest(pullRequestList, 500);
```

### 3.5 起始消费位置计算 (computePullFromWhereWithException) [line 166]

```java
long offset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
if (offset >= 0) return offset;  // 所有策略优先使用已持久化的 offset
if (offset != -1) throw new MQClientException(...);  // 查询存储失败

switch (consumeFromWhere) {
    case CONSUME_FROM_LAST_OFFSET:
    case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
    case CONSUME_FROM_MIN_OFFSET:
    case CONSUME_FROM_MAX_OFFSET:
        return retryTopic ? 0L : maxOffset(mq);
    case CONSUME_FROM_FIRST_OFFSET:
        return 0L;
    case CONSUME_FROM_TIMESTAMP:
        return retryTopic ? maxOffset(mq) : searchOffset(mq, consumeTimestamp);
}
```

---

## 4. Pull 消息拉取流程

### 4.1 PullMessageService 线程

`PullMessageService` 是 `MQClientInstance` 持有的 `ServiceThread`，内部维护 `LinkedBlockingQueue<MessageRequest>`。它可处理 `PullRequest` 和 `PopRequest`；本文只展开 `PullRequest`。

```java
// PullMessageService.java line 126-144
public void run() {
    while (!this.isStopped()) {
        MessageRequest request = this.messageRequestQueue.take();
        if (request.getMessageRequestMode() == MessageRequestMode.POP) {
            this.popMessage((PopRequest) request);
        } else {
            this.pullMessage((PullRequest) request);  // 默认拉取模式
        }
    }
}
```

### 4.2 完整 Pull 流程

```
PullMessageService 线程
  └── pullMessage(pullRequest)                       [line 105]
        └── DefaultMQPushConsumerImpl.pullMessage()  [line 246]
              ├── 1. 前置检查
              │     ├── processQueue.isDropped()? → 丢弃
              │     ├── makeSureStateOK() → 状态检查
              │     └── isPause()? → 延迟重试
              ├── 2. 流控检查
              │     ├── cachedMessageCount > pullThresholdForQueue? → 延迟
              │     ├── cachedMessageSize > pullThresholdSizeForQueue? → 延迟
              │     └── (并发) maxSpan > consumeConcurrentlyMaxSpan? → 延迟
              ├── 3. 顺序消费特殊处理
              │     ├── processQueue.isLocked()? 检查队列锁
              │     └── 首次拉取时修正 offset  (computePullFromWhereWithException)
              ├── 4. 构建 PullCallback 回调
              │     ├── onSuccess → 处理拉取结果
              │     └── onException → 异常处理，延迟重试
              ├── 5. 集群模式仅读取内存 offset；值大于 0 时随 Pull 请求携带
              ├── 6. 构建 sysFlag (PullSysFlag.buildSysFlag)
              └── 7. 调用 pullAPIWrapper.pullKernelImpl() 发送请求到 Broker
                    └── CommunicationMode.ASYNC (异步拉取)
```

### 4.3 拉取结果处理 (PullCallback.onSuccess) [line 345]

```java
switch (pullResult.getPullStatus()) {
    case FOUND:                    // 找到消息
        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
        if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
            executePullRequestImmediately(pullRequest);
        } else {
            boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
            consumeMessageService.submitConsumeRequest(
                pullResult.getMsgFoundList(), processQueue, messageQueue, dispatchToConsume);
            if (defaultMQPushConsumer.getPullInterval() > 0) {
                executePullRequestLater(pullRequest, defaultMQPushConsumer.getPullInterval());
            } else {
                executePullRequestImmediately(pullRequest);
            }
        }
        break;
    case NO_NEW_MSG:               // 没有新消息
    case NO_MATCHED_MSG:           // 无匹配消息
        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
        executePullRequestImmediately(pullRequest);  // 立即发起下一次拉取
        break;
    case OFFSET_ILLEGAL:           // 偏移量非法
        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
        processQueue.setDropped(true);
        executeTask(() -> {
            offsetStore.updateAndFreezeOffset(mq, pullRequest.getNextOffset());
            offsetStore.persist(mq);
            rebalanceImpl.removeProcessQueue(mq);
            mQClientFactory.rebalanceImmediately();
        });
        break;
}
```

### 4.4 ProcessQueue 消息缓存

`ProcessQueue` 是队列的消息快照，内部使用 `TreeMap<Long, MessageExt>` 按 offset 排序存储消息。

```java
// ProcessQueue.java（主要字段，节选）
public class ProcessQueue {
    private final TreeMap<Long, MessageExt> msgTreeMap;      // 通用消息缓存
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap; // msgTreeMap 的顺序消费子集
    private final AtomicLong msgCount;                        // 消息计数
    private final AtomicLong msgSize;                         // 消息大小
    private volatile long queueOffsetMax = 0L;                // 已见最大 offset
    private volatile boolean dropped = false;                 // 是否被丢弃
    private volatile boolean locked = false;                  // 是否锁定(顺序消费)
    private volatile boolean consuming = false;               // 是否正在消费(并发)
    private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock(); // 保护两个 TreeMap
    private final ReadWriteLock consumeLock = new ReentrantReadWriteLock(); // 顺序消费锁
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastLockTimestamp = System.currentTimeMillis();  // 锁续期判断依据
}
```

**putMessage** (line 129): 将拉取的消息放入 TreeMap；当此前未标记 `consuming` 时返回 `true` 并设置该标记。顺序消费使用这个返回值决定是否提交一个消费任务；并发消费每次都会提交批次。

**removeMessage** (line 187): 并发消费完成后从 TreeMap 移除已处理消息；仍有缓存时返回最小缓存 offset，缓存清空时返回已见最大 offset 加一。

---

## 5. Consume 消息消费流程

### 5.1 并发消费 (ConsumeMessageConcurrentlyService)

#### 5.1.1 提交消费请求 (submitConsumeRequest) [line 187]

```java
public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue,
    MessageQueue messageQueue, boolean dispatchToConsume) {
    final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
    if (msgs.size() <= consumeBatchSize) {
        ConsumeRequest request = new ConsumeRequest(msgs, processQueue, messageQueue);
        try {
            this.consumeExecutor.submit(request);
        } catch (RejectedExecutionException e) {
            this.submitConsumeRequestLater(request);
        }
    } else {
        // 分批提交；最后一批可以小于 consumeBatchSize
        for (int total = 0; total < msgs.size(); ) {
            List<MessageExt> batch = new ArrayList<>(consumeBatchSize);
            for (int i = 0; i < consumeBatchSize && total < msgs.size(); i++, total++) {
                batch.add(msgs.get(total));
            }
            ConsumeRequest request = new ConsumeRequest(batch, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(request);
            } catch (RejectedExecutionException e) {
                for (; total < msgs.size(); total++) {
                    batch.add(msgs.get(total));
                }
                this.submitConsumeRequestLater(request);
            }
        }
    }
}
```

#### 5.1.2 ConsumeRequest.run() 消费核心逻辑 [line 377]

```java
public void run() {
    // 1. 检查队列是否被丢弃
    if (this.processQueue.isDropped()) return;

    // 2. 重置重试主题和命名空间
    defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, consumerGroup);

    // 3. 有 Hook 时执行 before
    if (defaultMQPushConsumerImpl.hasHook()) {
        defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
    }

    // 4. 调用用户注册的 MessageListener；异常或 null 会按 RECONSUME_LATER 处理
    status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);

    // 5. 有 Hook 时执行 after
    if (defaultMQPushConsumerImpl.hasHook()) {
        defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
    }

    // 6. 处理消费结果
    processConsumeResult(status, context, this);
}
```

#### 5.1.3 消费结果处理 (processConsumeResult) [line 242]

```java
public void processConsumeResult(ConsumeConcurrentlyStatus status, ...) {
    switch (status) {
        case CONSUME_SUCCESS:
            // context.getAckIndex() 指定成功前缀；默认值为最后一条
            break;
        case RECONSUME_LATER:
            ackIndex = -1;  // 所有消息消费失败
            break;
    }

    switch (messageModel) {
        case BROADCASTING:
            // 广播模式：失败的消息直接丢弃
            break;
        case CLUSTERING:
            // 集群模式：失败的消息发送回 Broker 重试
            for (int i = ackIndex + 1; i < msgs.size(); i++) {
                boolean result = this.sendMessageBack(msg, context);
                if (!result) {
                    msgBackFailed.add(msg);  // 发送失败，稍后重试
                }
            }
            // 只有发送回 Broker 失败的消息在本地重新提交
            if (!msgBackFailed.isEmpty()) {
                consumeRequest.getMsgs().removeAll(msgBackFailed);
                this.submitConsumeRequestLater(msgBackFailed, ...);
            }
            break;
    }

    // 从 ProcessQueue 移除已消费消息
    // 已成功消费或已成功发回 Broker 的消息才从缓存移除
    long offset = processQueue.removeMessage(consumeRequest.getMsgs());
    // 更新消费进度
    if (offset >= 0 && !processQueue.isDropped()) {
        offsetStore.updateOffset(messageQueue, offset, true);
    }
}
```

### 5.2 顺序消费 (ConsumeMessageOrderlyService)

#### 5.2.1 核心差异

与并发消费的关键区别：

| 特性 | 并发消费 | 顺序消费 |
|------|---------|---------|
| 消息提交 | 按消息列表提交 | 先检查 `dispatchToConsume`，为 true 才提交 |
| 队列锁 | 不需要 | 集群模式需要 Broker 级别的队列锁 (`lockBatchMQ`)；广播模式只使用本地对象锁 |
| 消费粒度 | 按消息列表 | 按队列粒度，**一个队列同一时间只有一个线程消费** |
| 重试策略 | 发回 Broker 的重试 Topic | 未到上限时本地延迟重试；到上限时发送到重试 Topic，由 Broker 路由死信 |
| 失败处理 | 失败消息继续后续消费 | 消费失败暂停整个队列 |

#### 5.2.2 顺序消费核心机制

**队列锁** (MessageQueueLock):
```java
// 每个队列分配一个对象锁，确保同一队列串行消费
final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
synchronized (objLock) {
    // 循环消费直到队列为空或超时
    for (boolean continueConsume = true; continueConsume; ) {
        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
        status = messageListener.consumeMessage(msgs, context);
        continueConsume = processConsumeResult(msgs, status, context, this);
    }
}
```

**定期锁续期** (lockMQPeriodically):
```java
// 仅集群顺序消费：默认每20秒向 Broker 续期队列锁
this.scheduledExecutorService.scheduleAtFixedRate(
    () -> this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll(),
    1000, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
```

**消费结果处理** (processConsumeResult) [line 274]:
```java
switch (status) {
    case SUCCESS:
        commitOffset = processQueue.commit();  // 提交偏移量
        break;
    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
        if (checkReconsumeTimes(msgs)) {  // 检查重试次数
            processQueue.makeMessageToConsumeAgain(msgs);  // 放回队列
            submitConsumeRequestLater(processQueue, mq, suspendTimeMillis);  // 延迟重试
            continueConsume = false;  // 停止消费循环
        } else {
            // 已成功发送到重试 Topic；Broker 将按重试次数决定是否改投 DLQ
            commitOffset = processQueue.commit();
        }
        break;
}
```

---

## 6. Offset 消费进度管理

### 6.1 OffsetStore 架构

```
OffsetStore (接口)
  ├── RemoteBrokerOffsetStore (集群模式)
  │     └── offsetTable: ConcurrentMap<MessageQueue, ControllableOffset>
  └── LocalFileOffsetStore (广播模式)
        └── offsetTable: ConcurrentMap<MessageQueue, ControllableOffset>
```

### 6.2 进度更新时机

**消费成功后** (在 ConsumeMessageService 的 processConsumeResult 中):
```java
long offset = processQueue.removeMessage(msgs);  // 缓存非空时为最小缓存 offset，否则为最大已见 offset + 1
offsetStore.updateOffset(messageQueue, offset, true);  // 更新内存中的offset
```

### 6.3 进度持久化

**集群模式** (RemoteBrokerOffsetStore):
- `updateOffset()`: 更新内存中的 `offsetTable` (ConcurrentMap)
- `persistAll()`: 定时将内存中的 offset 批量提交到 Broker (MQClientInstance 定时任务)
- `readOffset()`: 按 `ReadOffsetType` 读取；`MEMORY_FIRST_THEN_STORE` 先读内存再查询 Broker，`READ_FROM_STORE` 直接查询 Broker
- `updateConsumeOffsetToBroker()`: 通过 RPC 发送 `UpdateConsumerOffsetRequestHeader` 到 Broker

**广播模式** (LocalFileOffsetStore):
- 默认持久化到 `<HOME>/.rocketmq_offsets/<clientId>/<group>/offsets.json`（JSON 格式）；可由 `rocketmq.client.localOffsetStoreDir` 覆盖。

### 6.4 进度读取策略 (ReadOffsetType)

```java
enum ReadOffsetType {
    READ_FROM_MEMORY,           // 只从内存读取
    MEMORY_FIRST_THEN_STORE,   // 优先内存，没有再读存储
    READ_FROM_STORE            // 从存储读取 (Broker/文件)
}
```

---

## 7. 消息重试与死信机制

### 7.1 并发消费重试

消费失败 (`RECONSUME_LATER`) 时，消息被发回 Broker 的 `%RETRY%<consumerGroup>` 重试主题：
```java
// sendMessageBack 发送到 Broker
this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, messageQueue);
```

重试间隔由 Broker 的 `messageDelayLevel` 配置决定：
```
1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
```

### 7.2 顺序消费重试

顺序消费在未达到最大重试次数时使用**本地延迟重试**：
```java
// 暂停当前队列一段时间后重新消费
processQueue.makeMessageToConsumeAgain(msgs);  // 将消息放回msgTreeMap
this.submitConsumeRequestLater(processQueue, mq,
    context.getSuspendCurrentQueueTimeMillis());  // 默认suspend时间
```

### 7.3 死信队列

并发消费由 Broker 在处理发回请求时判断重试次数；顺序消费达到上限时，客户端会把消息作为普通消息发往 `%RETRY%<consumerGroup>`（携带 `PROPERTY_MAX_RECONSUME_TIMES`）。Broker 的 `handleRetryAndDLQ` 再将超过上限的消息改投 `%DLQ%<consumerGroup>`，因此客户端并不直接写 DLQ。另外 Broker 对顺序消息还有一条直达 DLQ 的分支：发回请求到达时若该消费组的重平衡锁未过期（判定为顺序消息，`!rebalanceLockManager.isLockAllExpired(group)`），消息会被**直接**改投 `%DLQ%<group>` 而不再走 `%RETRY%`：

```java
// ConsumeMessageOrderlyService.checkReconsumeTimes  [line 360]
if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
    // 发送到重试 Topic，并携带 reconsumeTimes / maxReconsumeTimes
    Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
    this.defaultMQPushConsumerImpl.getmQClientFactory().getDefaultMQProducer().send(newMsg);
}
```

---

## 8. 流控机制

### 8.1 客户端流控 (在 pullMessage 中)

| 检查项 | 条件 | 行为 |
|-------|------|------|
| 消息数量 | `cachedMsgCount > pullThresholdForQueue` (默认1000) | 延迟50ms再拉取 |
| 消息大小 | `cachedMsgSize > pullThresholdSizeForQueue` (默认100 MiB，仅计消息体) | 延迟50ms再拉取 |
| 消息跨度 | `maxSpan > consumeConcurrentlyMaxSpan` (默认2000) | 延迟50ms再拉取 |
| 暂停状态 | `isPause()` | 延迟1000ms再拉取 |

### 8.2 Broker 流控

当 Broker 返回 `FLOW_CONTROL` 响应码时，客户端延迟 20ms 重试。

---

## 9. 完整消费流程时序图

```
Consumer 启动
    │
    ├──→ DefaultMQPushConsumerImpl.start()
    │       ├── 初始化: OffsetStore, ConsumeMessageService, PullAPIWrapper
    │       └── MQClientInstance.start()
    │             ├── RebalanceService 启动 (独立线程，每20秒执行)
    │             └── PullMessageService 启动 (独立线程，阻塞等待PullRequest)
    │
    ├──→ RebalanceService.doRebalance()
    │       ├── 从本地 topicSubscribeInfoTable 读取队列集合
    │       ├── 向 Broker 查询同组消费者列表
    │       ├── 分配队列 (AllocateMessageQueueStrategy)
    │       └── 创建 PullRequest → dispatchPullRequest → PullMessageService
    │
    ├──→ PullMessageService 线程
    │       └── DefaultMQPushConsumerImpl.pullMessage()
    │             ├── 流控检查
    │             ├── 构建 PullCallback (异步回调)
    │             └── pullAPIWrapper.pullKernelImpl() 发送到 Broker
    │                   │
    │                   └── Broker 返回消息
    │                         │
    │                         └── PullCallback.onSuccess()
    │                               ├── processQueue.putMessage() 缓存消息
    │                               └── consumeMessageService.submitConsumeRequest()
    │                                     └── 提交到消费线程池
    │
    └──→ 消费线程池
            └── ConsumeRequest.run()
                  ├── 调用 MessageListener.consumeMessage()
                  ├── processConsumeResult()
                  ├── processQueue.removeMessage() 移除已消费消息
                  └── offsetStore.updateOffset() 更新消费进度
                        └── (定时) persistAll → 持久化到 Broker / 本地文件
```

---

## 10. 关键设计要点

1. **本文的 Push 模式本质是 Pull**: 在 `MessageRequestMode.PULL` 配置下，`DefaultMQPushConsumer` 由客户端通过 `PullMessageService` 持续拉取消息，实现"推"的使用体验。

2. **异步拉取**: 使用 `CommunicationMode.ASYNC` 异步拉取，通过 `PullCallback` 回调处理结果，避免阻塞拉取线程。

3. **ProcessQueue 的双重角色**: 既是消息缓存（暂存拉取到的消息），又维护消费进度。`TreeMap` 保证缓存按 offset 排列；只有顺序监听器据此串行调用用户监听器。

4. **消费进度两段式更新**: 先更新内存 (updateOffset)，再定时持久化 (persistAll)，平衡性能与可靠性。

5. **顺序消费的锁机制**: 集群顺序消费通过 Broker `lockBatchMQ` 维持队列归属，并通过本地 `MessageQueueLock` 串行调用监听器；它不提供端到端 exactly-once 语义，业务仍需处理重复消费。

6. **Rebalance 的队列撤销**: 先把不再归属的 `ProcessQueue` 标为 `dropped`，再尝试清理。顺序消费会争取本地消费锁后再解锁并移除；并发消费中的任务可能完成用户回调，但 dropped 后不会再提交其消费结果。

生产侧如何把同一订单固定到同一队列、以及发送因果顺序，见
[RocketMQ 顺序消息：生产投递与因果顺序](rocketmq_ordered_message.md)。
