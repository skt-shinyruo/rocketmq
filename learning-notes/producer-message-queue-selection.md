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
