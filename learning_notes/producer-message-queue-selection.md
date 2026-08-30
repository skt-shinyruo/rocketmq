# Producer MessageQueue 选择逻辑

在默认发送链路中，`DefaultMQProducerImpl.sendDefaultImpl` 获取 Topic 路由后，会通过
`MQFaultStrategy.selectOneMessageQueue` 选择本次发送使用的 `MessageQueue`：

```text
DefaultMQProducer.send(Message)
  -> DefaultMQProducerImpl.sendDefaultImpl(SYNC)
     -> tryToFindTopicPublishInfo
     -> MQFaultStrategy.selectOneMessageQueue
     -> sendKernelImpl
```

它的核心逻辑是：

> 从 Topic 的可写队列中做本地轮询；重试时尽量避开上一次发送失败的 Broker。开启延迟故障规避后，再过滤暂时不可用的 Broker。

它不会查询队列积压量，也不会比较所有 Broker 后选择延迟最低的一个。

## Topic 路由如何进入本地缓存

发送前，`DefaultMQProducerImpl.tryToFindTopicPublishInfo` 先从
`topicPublishInfoTable` 查询 Topic 对应的 `TopicPublishInfo`。缓存不存在或队列列表为空时，
客户端会向 NameServer 拉取路由，并通过
`MQClientInstance.topicRouteData2TopicPublishInfo` 转换成可供 Producer 使用的队列列表。

因此，选择 Queue 的输入来自客户端本地缓存；不是每发送一条消息都查询 NameServer。

## 候选队列从哪里来

NameServer 返回 Topic 路由后，客户端在
`MQClientInstance.topicRouteData2TopicPublishInfo` 中为每个可写 Broker 构造队列列表。

例如，Broker A 和 Broker B 各有 4 个写队列：

```text
[A:0, A:1, A:2, A:3, B:0, B:1, B:2, B:3]
```

普通 Topic 的 Broker 按名称排序，每个 Broker 的队列按 `queueId` 依次加入。一个 Broker
拥有的写队列越多，它在轮询中被选中的次数也越多。

相关源码：

- `client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java`
- `client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java`

## 默认策略：轮询并在重试时换 Broker

默认配置 `sendLatencyEnable=false`。选择逻辑等价于：

```java
MessageQueue mq = tpInfo.selectOneMessageQueue(brokerFilter);
if (mq != null) {
    return mq;
}
return tpInfo.selectOneMessageQueue();
```

`TopicPublishInfo` 使用线程本地索引选择队列：

```java
index = threadLocalIndex.incrementAndGet() % messageQueueList.size();
```

索引具有以下特点：

- 每个业务线程独立；
- 初始位置随机；
- 每选择一次递增；
- 有过滤器时最多扫描整个队列列表一轮。

`ThreadLocalIndex` 本质上是线程私有的轮询游标。它通过 `ThreadLocal<Integer>` 保存
当前线程的索引，因此多个发送线程共享同一个 `ThreadLocalIndex` 对象时，仍然各自递增，
互不影响，也不需要使用 `AtomicInteger` 竞争同一个计数值。

线程首次调用 `incrementAndGet()` 时会从随机值开始，避免多个发送线程总是同时从队列
列表的第一个位置起步。每次递增后，结果会与 `0x7FFFFFFF` 做按位与，从而在 `int`
溢出后仍返回非负数，能够继续安全地用于取模。`reset()` 也只会把当前线程的游标重置到
一个新的随机位置。

因此，它适合队列轮询，但不是全局计数器：它不保证跨线程单调递增，也不能用于生成
唯一 ID。

第一次发送时，`lastBrokerName` 为 `null`，所有队列都符合条件，因此实际效果是从一个
随机起点开始轮询。

同步发送失败后，下一次重试会把刚才使用的 Broker 作为 `lastBrokerName`。
`BrokerFilter` 过滤的是该 Broker 的全部队列，而不只是刚才失败的那个队列。

例如第一次选择 `A:1` 后发送失败：

```text
扫描 A:2 -> 仍是 Broker A，跳过
扫描 A:3 -> 仍是 Broker A，跳过
扫描 B:0 -> Broker B，选择
```

如果 Topic 只有 Broker A，扫描一轮无法找到其他 Broker，策略会退化成不带过滤器的
普通轮询，仍然选择 A 上的某个队列：优先换 Broker，但不会因无法切换而直接放弃发送。

同步发送默认配置为失败重试 2 次，因此最多选择并发送 3 次。

相关源码：

- `client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java`
- `client/src/main/java/org/apache/rocketmq/client/common/ThreadLocalIndex.java`
- `client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java`

## 延迟故障规避策略

开启 `sendLatencyEnable` 后，选择优先级变成：

```text
1. 不在隔离期，并且不是上次发送使用的 Broker
2. 网络可达，并且不是上次发送使用的 Broker
3. 任意队列
```

对应实现为：

```java
tpInfo.selectOneMessageQueue(availableFilter, brokerFilter);
tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter);
tpInfo.selectOneMessageQueue();
```

其中：

- `available`：当前时间已经超过 Broker 的临时隔离截止时间；
- `reachable`：Broker 当前被认为网络可达。

如果所有 Broker 都不满足前两层条件，最后仍会选择任意队列，保证选择逻辑有兜底。

## Broker 如何进入隔离期

发送完成或发生异常后，客户端按 `brokerName` 更新故障信息：

```java
updateFaultItem(brokerName, latency, isolation, reachable);
```

默认情况下，发送耗时对应的隔离时间如下：

| 发送耗时 | 隔离时间 |
| ---: | ---: |
| `< 550ms` | 0 |
| `550ms - 1799ms` | 2s |
| `1800ms - 2999ms` | 5s |
| `3000ms - 4999ms` | 6s |
| `5000ms - 14999ms` | 10s |
| `>= 15000ms` | 30s |

发生需要强制隔离的异常时，`MQFaultStrategy` 使用 `10000ms` 作为计算延迟，因此默认隔离
10 秒。

故障记录的粒度是 `brokerName`，所以一个 Broker 出现故障后，策略会暂时规避该 Broker
上的全部 MessageQueue。

延迟数据只用于计算 Broker 应该被临时避让多久；正常队列选择仍然是轮询，而不是
“挑选当前延迟最低的 Broker”。

## 选中 Queue 后如何发送

一个 `MessageQueue` 由以下三个字段确定：

```text
topic + brokerName + queueId
```

`sendKernelImpl` 根据 `brokerName` 从本地路由中找到 Broker 地址，并把 `queueId` 写入
`SendMessageRequestHeader`。Broker 收到请求后，便知道消息应该写入该 Topic 的哪一个队列。

相关源码：

- `DefaultMQProducerImpl.sendKernelImpl`
- `SendMessageRequestHeader.setQueueId`

## 重试时会切换 Broker 吗？

**结论：不是固定的。同步与异步发送重试时都会优先切换到其他 Broker**
（若路由为空，或过滤后没有其他 Broker 的候选队列，会回退到完整队列列表，仍可能选回原 Broker）。

### 同步发送（SYNC）— 会切换 Broker

`DefaultMQProducerImpl.sendDefaultImpl()` 中
（`client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java:756-766`）：

```java
int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + retryTimesWhenSendFailed : 1;
for (; times < timesTotal; times++) {
    String lastBrokerName = null == mq ? null : mq.getBrokerName();
    ...
    MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName, resetIndex);
```

每次重试都会把**上一次失败的 Broker 名**（`lastBrokerName`）传给队列选择器。
在 `TopicPublishInfo.selectOneMessageQueue(lastBrokerName)`（第 109 行）中：

```java
for (int i = 0; i < this.messageQueueList.size(); i++) {
    MessageQueue mq = selectOneMessageQueue();
    if (!mq.getBrokerName().equals(lastBrokerName)) {
        return mq;   // 优先选一个不同 Broker 的队列
    }
}
return selectOneMessageQueue(); // 实在找不到才退回原逻辑
```

即：**优先选择与上次失败不同的 Broker**；只有当该 Topic 只部署在一个 Broker 上时才会退回
同一个 Broker。此外 `MQFaultStrategy` 还会结合延迟故障规避（Broker 隔离），进一步避开有问题的
Broker。

### 异步发送（ASYNC）— 同样优先换 Broker

异步重试走的是 `MQClientAPIImpl.onExceptionImpl()`
（`client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java:719-730`）：

```java
String retryBrokerName = brokerName;//by default, it will send to the same broker
if (topicPublishInfo != null) {
    MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName, false);
    retryBrokerName = instance.getBrokerNameFromMessageQueue(mqChosen);
}
```

注意：这里调用 `selectOneMessageQueue(topicPublishInfo, brokerName, false)` 时传入了
当前失败的 `brokerName` 作为 `lastBrokerName`。`brokerFilter` 的生效条件只看
`lastBrokerName` 是否为 null，与 `resetIndex` 无关（`resetIndex` 只控制是否重置轮询
游标）。因此只要 `topicPublishInfo` 可用，异步重试就会**优先选一个其他 Broker 的
队列**；如果过滤后没有候选队列，选择器会回退到完整队列列表。代码注释里的
"by default, it will send to the same broker" 只说明 `topicPublishInfo == null` 时的初始兜底，
并不排除“路由存在但只有原 Broker”时的回退。

### 汇总

| 发送方式 | 重试是否换 Broker | 控制参数 |
| --- | --- | --- |
| 同步 SYNC | ✅ 优先换到其他 Broker | `retryTimesWhenSendFailed`（默认 2） |
| 异步 ASYNC | ✅ 优先换到其他 Broker（路由为空或无其他候选时可能回到原 Broker） | `retryTimesWhenSendAsyncFailed`（默认 2） |
| ONEWAY | 无重试 | — |

另外补充一点：即使换了 Broker，如果消息发送成功但返回的是 `FLUSH_DISK_TIMEOUT` /
`SLAVE_NOT_AVAILABLE` 等状态，同步模式下还需 `retryAnotherBrokerWhenNotStoreOK=true`
才会继续重试其他 Broker。

相关源码：

- `client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java`
- `client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java`
- `client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java`
- `client/src/test/java/org/apache/rocketmq/client/producer/selector/SelectMessageQueueRetryTest.java`

## 自定义 Queue 选择

默认发送不会根据消息 Key 或消息体做哈希。若业务要求相同业务键始终进入同一 Queue，
可以调用带 `MessageQueueSelector` 的 `send` 重载：

```java
producer.send(message, (queues, msg, arg) -> {
    int index = (arg.hashCode() & Integer.MAX_VALUE) % queues.size();
    return queues.get(index);
}, orderId);
```

此时客户端仍负责获取 Topic 的可写队列列表，但最终选择哪个 Queue 由调用方的
`MessageQueueSelector` 决定，而不再走默认轮询策略。
