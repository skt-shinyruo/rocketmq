# RocketMQ Producer 发送消息源码全链路

本文基于当前仓库的 RocketMQ `5.5.0` 源码，主线是 Java 客户端的普通消息：

```java
SendResult result = producer.send(message);
```

重点不是记住一串方法名，而是回答下面几个问题：

1. Producer 如何从 Topic 找到真正要连接的 Broker？
2. 同步、异步、单向发送的线程与重试边界有什么不同？
3. Broker 返回成功前，消息究竟写到了哪里，是否已经刷盘、复制并可消费？
4. `msgId`、`offsetMsgId` 和 `queueOffset` 分别是谁生成的？
5. 发送超时后为什么仍可能已经落盘，重试为什么会产生重复消息？

范围说明：普通消息走经典 `DefaultMessageStore -> CommitLog` 主线。自动批量、事务消息、
延时消息和静态 Topic 会单独说明分叉点，但不展开它们各自的完整生命周期。

## 1. 先看全链路

一次默认同步发送的主调用链如下：

```text
业务线程
  DefaultMQProducer.send(Message)
    -> namespace / autoBatch 判断
    -> DefaultMQProducerImpl.sendDefaultImpl(SYNC)
       -> makeSureStateOK + Validators.checkMessage
       -> tryToFindTopicPublishInfo
          -> 本地路由缓存
          -> 必要时查询 NameServer
       -> MQFaultStrategy.selectOneMessageQueue
       -> sendKernelImpl
          -> 查 Broker 地址 / VIP 地址
          -> UNIQ_KEY / 压缩 / sysFlag / Hook
          -> SendMessageRequestHeader
          -> MQClientAPIImpl.sendMessage
             -> RemotingCommand
             -> NettyRemotingClient.invokeSync
                -> 建连或复用 Channel
                -> opaque -> ResponseFuture
                -> writeAndFlush

Broker Netty 线程
  NettyDecoder
    -> NettyRemotingAbstract.processRequestCommand
       -> rejectRequest / 提交 RequestTask 到 SendMessageThread_ 线程池

Broker 发送线程
  RPCHook.before / 认证 / 授权
    -> SendMessageProcessor.processRequest
    -> 解析请求头 / 静态 Topic 重写 / Broker Hook
    -> preSend + msgCheck
    -> 构造 MessageExtBrokerInner
    -> DefaultMessageStore.asyncPutMessage
       -> PutMessageHook
       -> CommitLog.asyncPutMessage
          -> 分配 queueOffset
          -> 编码 CommitLog 记录
          -> append 到 MappedFile
          -> 等待或唤醒刷盘
          -> 按配置等待 HA ACK
    -> handlePutMessageResult（默认异步发送时在 Future continuation）
       -> SendMessageResponseHeader
       -> 写回响应

客户端响应路径
  SYNC:
    Netty 线程 processResponseCommand(opaque) -> putResponse / 唤醒业务发送线程
    业务发送线程 invokeSync 返回 -> processSendResponse -> SendResult
  ASYNC:
    callback executor（资源不足时可能是 Netty 线程）
      -> processSendResponse -> SendCallback

Broker 后台线程，独立于发送响应
  ReputMessageService
    -> 扫描 CommitLog
    -> 构建 ConsumeQueue
    -> 构建 IndexFile / RocksDB Index
    -> 通知长轮询 Consumer
```

这里最重要的边界是：

- NameServer 只提供路由，消息正文不经过 NameServer。
- `CommitLog.asyncPutMessage` 名字里有 `async`，但编码和内存 append 仍在当前 Broker
  发送线程同步完成；异步的是刷盘、复制结果的 future 以及响应续接。
- Broker 的发送响应不等待 `ConsumeQueue` 和索引构建，更不等待 Consumer 消费。

主入口源码：

- [`DefaultMQProducer.java`](../client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java)
- [`DefaultMQProducerImpl.java`](../client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java)
- [`MQClientAPIImpl.java`](../client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java)
- [`SendMessageProcessor.java`](../broker/src/main/java/org/apache/rocketmq/broker/processor/SendMessageProcessor.java)
- [`DefaultMessageStore.java`](../store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java)
- [`CommitLog.java`](../store/src/main/java/org/apache/rocketmq/store/CommitLog.java)

## 2. 不同发送 API 不是同一条路径

现有源码不能简单概括成“所有入口最后都查路由、选队列、失败重试”。先区分三类队列
入口。

| 队列入口 | 如何得到队列 | 同步自动重试 | 异步 Remoting 失败重试 |
| --- | --- | --- | --- |
| 默认 `send(msg)` | 查发布路由，由 `MQFaultStrategy` 选择 | 有，默认总共最多 3 次 | 有，可根据路由换 Broker |
| 固定 `send(msg, mq)` | 调用方直接给定 | 无外层重试 | 有，但因没有 `TopicPublishInfo`，仍发原 Broker |
| `send(msg, selector, arg)` | 查路由，把队列列表交给业务 selector | 无外层重试 | 有，但选定后没有 `TopicPublishInfo`，仍发原 Broker |

再区分通信模式：

| 模式 | API 何时结束 | 是否等 Broker 响应 | Broker 错误如何到业务侧 |
| --- | --- | --- | --- |
| `SYNC` | 收到响应或抛异常 | 是 | 返回 `SendResult` 或抛异常 |
| `ASYNC` | 异步任务提交后返回 | 回调链等待 | `SendCallback.onSuccess/onException` |
| `ONEWAY` | 提交异步连接/写入后返回 | 否 | 没有 Broker 存储结果 |

两个容易忽略的细节：

1. 固定队列和 selector 的“同步发送”没有 `sendDefaultImpl` 外层循环；不能把默认同步
   发送的三次尝试套到它们身上。
2. 固定队列和 selector 的“异步发送”仍有 `MQClientAPIImpl` 内部重试，但由于传入的
   `topicPublishInfo` 为 `null`，它只能重试当前 Broker，不能重新做默认负载均衡。

## 3. API 层：namespace 与自动批量

### 3.1 namespace 会先改写 Topic

`DefaultMQProducer.send` 首先执行：

```java
msg.setTopic(withNamespace(msg.getTopic()));
```

因此客户端内部的路由缓存、请求头和 Broker 看到的是带 namespace 的资源名。响应转成
`SendResult` 时会去掉 namespace，使业务侧仍看到原 Topic。

`sendKernelImpl` 只有成功解析 `brokerAddr` 并进入内部 `try` 后，才会在
`finally` 中恢复压缩前的 body，并去掉 Topic 的 namespace。如果 API 层的
校验、查路由或 selector 失败，或者已进入 kernel 但刷新路由后仍找不到 Broker
地址，这个 `finally` 都不会执行，调用方 Message 的 Topic 可能仍带 namespace。
批量路径还有一层例外：显式 Batch 和 autoBatch 都可能先给原始单条 Message 写入
namespace，kernel 的 `finally` 恢复的是新建的 `MessageBatch` wrapper，不会逐条恢复
原始 Message 的 Topic。因此实践中不要并发复用并修改同一个 `Message` 实例。

### 3.2 autoBatch 只覆盖部分重载

`autoBatch=true` 时，并非所有 `send` 都进入 `ProduceAccumulator`：

- 不显式传 `timeout` 的普通同步、异步、固定队列和 selector 重载可以尝试自动聚合。
- 所有显式传 `timeout` 的重载直接进入 `DefaultMQProducerImpl`，绕过 autoBatch。
- `sendOneway` 不走 autoBatch。
- 已经是 `MessageBatch`、延时消息、Retry Topic 消息、带 Producer Group 属性的消息，
  以及准入检查判定 accumulator 已满时，都会直接发送。

自动聚合的精确 key 是：

```text
(topic, 指定的 MessageQueue 或 null, waitStoreMsgOK, tag)
```

同步与异步各自使用独立的聚合 Map，不会互相凑批。默认阈值是：

- 单批 body 累计超过 `32 KiB`；或
- 第一条消息进入后达到 `10 ms`；
- 全局 `32 MiB` body 准入阈值。

`32 MiB` 不是严格上限：实现先判断当前值是否已达阈值，再把整条 body
加入计数，因此可以被最后一条消息越过。更细的实现边界是，容量记账早于延时、
Retry Topic 和 Producer Group 等排除检查，当这些检查使消息改为直发时，当前代码没有
回滚这次计数。

同步调用方会等待这一批实际发出，再取得拆分后的 `SendResult`；异步调用方加入批次后
返回，最终批量请求完成时再逐条触发原 callback。批次编码成 `MessageBatch` 后仍调用
`sendDirect`，所以路由、网络和 Broker 存储主线并没有另一套实现。

这也意味着 autoBatch 的等待时间不属于随后新开始的默认发送超时预算。

自动批量源码：
[`ProduceAccumulator.java`](../client/src/main/java/org/apache/rocketmq/client/producer/ProduceAccumulator.java)。

### 3.3 RocketMQ 与 Kafka 的批量发送对比

RocketMQ 有批量发送，但经典 Java 客户端长期以调用方显式组批为主，因此默认使用时看起来
更像逐条发送；当前 `5.5.0` 源码还提供了可选的自动攒批。

显式批量直接把一个消息集合编码成一次 `MessageBatch` 请求：

```java
List<Message> messages = new ArrayList<>();
messages.add(message1);
messages.add(message2);
messages.add(message3);
SendResult result = producer.send(messages);
```

同一批消息必须具有相同的 Topic 和 `waitStoreMsgOK`，不支持延时消息和 Retry Topic
消息；编码后的整个批次还受 Producer `maxMessageSize` 限制，默认是 `4 MiB`。入口是
[`MQProducer.send(Collection<Message>)`](../client/src/main/java/org/apache/rocketmq/client/producer/MQProducer.java)。
约束检查分两处：同 Topic/同 `waitStoreMsgOK`/禁延时与 Retry Topic 由
[`MessageBatch.generateFromList`](../common/src/main/java/org/apache/rocketmq/common/message/MessageBatch.java)
检查，而逐条与整批的大小限制由 `DefaultMQProducer.batch()` 及 `sendDefaultImpl`
里的 [`Validators.checkMessage`](../client/src/main/java/org/apache/rocketmq/client/Validators.java)
检查。

需要类似 Kafka Producer 的透明攒批时，可以在 `start()` 前开启 autoBatch：

```java
producer.setAutoBatch(true);
producer.batchMaxDelayMs(10);
producer.batchMaxBytes(32 * 1024);
producer.start();

producer.send(message, callback);
```

异步发送更接近 Kafka 的使用方式：调用线程把消息加入 accumulator 后立即返回，等待达到
时间或大小阈值后统一发送。同步发送也能进入 accumulator，但当前调用会等待所属批次发出，
单线程逐条同步调用通常无法互相凑批。

| 对比项 | Kafka Producer | RocketMQ 经典 Java Producer |
| --- | --- | --- |
| 默认模型 | 单条 `send` 透明进入按 partition 组织的 accumulator | `autoBatch` 默认关闭，传统方式由调用方显式传 `Collection<Message>` |
| 时间阈值 | `linger.ms` | `batchMaxDelayMs`，启用后默认 `10 ms` |
| 单批大小 | `batch.size` | `batchMaxBytes`，启用后默认 `32 KiB` |
| 总缓存 | `buffer.memory` | `totalBatchMaxBytes`，启用后默认 `32 MiB` |
| 聚合维度 | Topic partition | Topic、指定 Queue、`waitStoreMsgOK`、Tag |

这些参数只是用途近似，不是完全相同的协议语义。RocketMQ 的 autoBatch 仍会生成
`MessageBatch` 并接回普通发送主线；显式传 `timeout` 的重载和 `sendOneway` 会绕过它，
延时消息、Retry Topic 消息等也会退回单条直发。旧版客户端如果没有 `setAutoBatch`，仍可
使用 `send(Collection<Message>)` 显式批量发送。

## 4. 默认同步主线：`sendDefaultImpl`

下面先只看最常见的默认同步发送。它包含路由、默认队列选择和外层重试，是理解其他
入口的基准。

### 4.1 状态与消息校验

`sendDefaultImpl` 首先执行：

```java
makeSureStateOK();
Validators.checkMessage(msg, defaultMQProducer);
```

`makeSureStateOK` 要求 Producer 已经成功 `start()`，状态为 `RUNNING`。启动时 Producer
会注册到共享的 `MQClientInstance`，启动 Remoting Client、路由刷新、心跳、故障探测
等后台任务。

客户端消息校验包括：

- `Validators.checkMessage` 本身要求 `Message` 不为 `null`。
- Topic 非空、长度和字符合法，并且不是禁止直接发送的系统 Topic。
- body 不为 `null` 且长度不为 0。
- body 不超过 Producer 的 `maxMessageSize`，默认 `4 MiB`。
- 特殊的 LMQ multi-dispatch 属性不包含非法路径分隔符。

进入 `Validators` 后的同步校验失败会在当前线程抛 `MQClientException`，还没有网络
请求。但公共 `send` 入口在调用 Validator 前先执行 `msg.setTopic(...)`，所以
`send(null)` 实际会更早抛 `NullPointerException`。默认异步路径通常在异步发送线程里
执行这些检查，错误通过 `SendCallback.onException` 返回。

校验源码：
[`Validators.java`](../client/src/main/java/org/apache/rocketmq/client/Validators.java)。

### 4.2 获取 `TopicPublishInfo`

`tryToFindTopicPublishInfo(topic)` 的过程是：

```text
topicPublishInfoTable[topic]
  -> 有非空 messageQueueList：直接使用
  -> 没有：查询真实 Topic 的 NameServer 路由并更新缓存
  -> 仍没有且从未取得真实路由：查询默认 Topic TBW102 作为建 Topic 兜底
```

NameServer 返回 `TopicRouteData`，客户端主要生成三类本地数据：

1. `brokerAddrTable`：`brokerName -> brokerId -> address`。
2. Producer 的 `TopicPublishInfo.messageQueueList`。
3. 静态 Topic 的逻辑队列到当前物理 Broker endpoint 映射。

普通 Topic 的路由转换会：

- 只处理有写权限的 `QueueData`。
- 确认同名 `BrokerData` 中存在 Master 地址。
- 按 `writeQueueNums` 展开成多个
  `MessageQueue(topic, brokerName, queueId)`。

例如两个 Broker 各有 4 个写队列，最终列表有 8 个 `MessageQueue`。这里的队列只是
逻辑坐标，不是客户端创建了 8 个内存队列。

客户端默认每 30 秒后台刷新一次已使用 Topic 的路由，发送时缓存缺失也会主动刷新。
因此路由是一个会更新但仍可能短暂过期的本地快照。

默认 Topic 兜底只让客户端获得“可以尝试发往哪些 Broker”的路由。Broker 是否真的
创建业务 Topic，还取决于 Broker 端自动创建 Topic 的配置；生产环境不应依赖它。

更细的路由对象拆解见
[`rocketmq_message_send_and_consume_flow.md`](./rocketmq_message_send_and_consume_flow.md)。

### 4.3 默认队列选择

`MQFaultStrategy.selectOneMessageQueue` 并不是创建队列，而是从路由列表中选一个现有的
逻辑队列。

默认未开启延迟故障规避时：

1. `TopicPublishInfo` 使用线程本地递增索引做轮询。
2. 重试时先过滤上一次失败的 `brokerName`。
3. 如果过滤后没有候选，例如 Topic 只有一个 Broker，退化为从完整列表选择，所以
   “避开上次 Broker”只是尽力而为。

`sendLatencyFaultEnable` 默认是 `false`。关闭时 `updateFaultItem` 是 no-op。开启后会
优先选择故障表中“available”的 Broker，再退到“reachable”的 Broker，最后才不带
过滤选择。客户端按耗时、隔离标记和可达性更新故障项。

源码：

- [`TopicPublishInfo.java`](../client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java)
- [`MQFaultStrategy.java`](../client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java)

### 4.4 同步重试循环

默认配置 `retryTimesWhenSendFailed=2`，所以：

```java
timesTotal = 1 + retryTimesWhenSendFailed; // 默认 3 次尝试
```

每一轮会记录上次 Broker、重新选队列、计算剩余超时，再调用一次 `sendKernelImpl`。
异常处理不是“一律重试”：

| 结果 | 默认同步路径的处理 |
| --- | --- |
| `MQClientException` | 记录故障后继续下一轮 |
| `RemotingException` | 继续下一轮；开启故障规避时以 `isolation=true` 更新，只有启用探测器时才置为不可达 |
| `MQBrokerException` | 仅响应码在 `retryResponseCodes` 中才继续 |
| `InterruptedException` | 立即向上抛，不重试 |
| `SendResult.sendStatus != SEND_OK` | 默认直接返回；只有配置 `retryAnotherBrokerWhenNotStoreOK=true` 才继续 |

默认可重试 Broker 响应码包括：

```text
TOPIC_NOT_EXIST, SERVICE_NOT_AVAILABLE, SYSTEM_ERROR, SYSTEM_BUSY,
NO_PERMISSION, NO_BUYER_ID, NOT_IN_CURRENT_UNIT, GO_AWAY
```

如果某次已得到非 `SEND_OK` 的 `SendResult`，且配置了
`retryAnotherBrokerWhenNotStoreOK=true` 继续重试后仍未取得更好结果，循环结束时
仍可能返回这个 `SendResult`，而不是抛异常（默认该开关为 false：同步发送收到
非 `SEND_OK` 结果会立即返回，不再重试）。业务代码不能只判断 `send()` 是否正常
返回，还要检查 `sendStatus`。

## 5. 共享内核：`sendKernelImpl`

默认、固定队列和 selector 路径最终都调用 `sendKernelImpl`。它负责把“已选中的逻辑
队列”变成一条可发送的 Broker 请求。

### 5.1 从逻辑 Broker 找到地址

`MessageQueue` 只保存 `topic + brokerName + queueId`。kernel 通过
`findBrokerAddressInPublish(brokerName)` 从路由缓存取 Master 地址；如果缺失，会刷新
一次 Topic 路由后重查。

静态 Topic 是例外：逻辑 `MessageQueue` 先通过 endpoint 映射解析为当前承载它的物理
Broker 名称。Broker 会把响应的 queueId 和 queueOffset 转回逻辑值；但客户端构造
`SendResult.messageQueue` 时使用实际物理 `brokerName`，不是静态 Topic 的 mock brokerName。

取得地址后，客户端还会按 `sendMessageWithVIPChannel` 配置决定是否转换为 VIP 通道
地址。最后复用或新建到该地址的 Netty Channel。

### 5.2 为消息补齐发送元数据

kernel 在网络请求前依次处理：

1. **唯一 ID**：非 `MessageBatch` 调用 `MessageClientIDSetter.setUniqID`。只有消息尚无
   `UNIQ_KEY` 时才生成，因此同一个 Message 的自动重试会沿用同一个 `UNIQ_KEY`。
2. **namespace 标记**：存在 namespace 时写入 instance ID 属性。
3. **压缩**：非批量消息 body 大于等于默认 `4 KiB` 时尝试压缩，默认算法是 ZLIB，
   并在 `sysFlag` 写入压缩位和算法位。
4. **事务标志**：`PROPERTY_TRANSACTION_PREPARED=true` 时写入事务 prepared sysFlag。
5. **客户端 Hook**：先执行禁止发送检查 Hook，再执行发送前 Hook。
6. **请求头**：把 Producer Group、Topic、默认 Topic、默认队列数、queueId、sysFlag、
   born timestamp、flag、properties、重试次数、批量标记和 brokerName 写入
   `SendMessageRequestHeader`。

压缩后的 body 会作为 Remoting body 发送并以压缩形态进入 CommitLog，Consumer 解码时
再根据 sysFlag 解压。调用方原 body 在 kernel 的 `finally` 中恢复；异步发送还会在
必要时 clone Message，避免网络尚未完成时就把待发送的压缩 body 改回去。

客户端 Hook 常用于 Trace、监控和自定义拦截。它和 Broker 端的 Remoting Hook、认证
授权 Pipeline、Broker SendMessageHook、Store PutMessageHook 是不同层次，不能混为
同一个 Hook。

### 5.3 两个 Producer 客户端 Hook 的区别

`sendKernelImpl` 在构造请求头并向 Broker 发送前，会依次处理两类 Hook：

```text
CheckForbiddenHook（发送许可校验）
  -> SendMessageHook.sendMessageBefore（发送前追踪）
  -> 向 Broker 发送
  -> SendMessageHook.sendMessageAfter（记录结果或异常）
```

#### `CheckForbiddenHook`

它先将 NameServer 地址、Producer Group、通信模式、Broker 地址、消息、
目标队列和 Unit 模式封装到 `CheckForbiddenContext`，再依次调用已注册的
`CheckForbiddenHook.checkForbidden`。

这是一个强制校验扩展点，可用于权限、黑白名单或其他发送限制。Hook 抛出
`MQClientException` 时，异常会继续向上抛出，本次消息不会发送。这段代码
自身不包含具体禁止规则，只负责执行已注册的 Hook。

#### `SendMessageHook`

它将 Producer、Producer Group、通信模式、客户端地址、Broker 地址、消息、
目标队列和 namespace 封装到 `SendMessageContext`。上下文默认把消息标记为
普通消息，并根据消息属性进一步识别：

- `PROPERTY_TRANSACTION_PREPARED=true`：事务半消息。
- 存在延时级别或定时投递属性：延时消息。
- 其他情况：普通消息。

然后执行 `sendMessageBefore`；发送完成后，发送结果或异常会放入同一个
上下文，再执行 `sendMessageAfter`。RocketMQ 内置的消息轨迹和 OpenTracing 实现就使用
了这个扩展点。`SendMessageHook` 抛出的异常会被 Producer 捕获并记录告警日志，
不会阻止正常发送。

因此，两者的核心区别是：`CheckForbiddenHook` 可以拒绝发送；`SendMessageHook`
主要观察和记录发送过程，其自身失败不影响消息发送。

## 6. 从请求对象到网络字节

### 6.1 Request Code 与请求头版本

`MQClientAPIImpl.sendMessage` 根据消息类型创建 `RemotingCommand`：

| 消息类型 | Request Code |
| --- | --- |
| 普通消息，默认 smart header | `SEND_MESSAGE_V2` |
| 关闭 smart header | `SEND_MESSAGE` |
| `MessageBatch` | `SEND_BATCH_MESSAGE` |
| Reply 消息 | `SEND_REPLY_MESSAGE(_V2)` |

V2 并不是另一套发送语义。`SendMessageRequestHeaderV2` 只是把字段名压缩成 `a`、`b`、
`c` 等短名称以减少协议头开销，Broker 收到后会还原成统一的
`SendMessageRequestHeader`。

### 6.2 Remoting 帧结构

Netty Encoder 写出的主体结构是：

```text
+------------------+---------------------------+----------------+-----------+
| totalLength 4B   | headerLength+type 4B      | header bytes   | body      |
+------------------+---------------------------+----------------+-----------+
```

- `totalLength` 是后续帧长度。
- 第二个整数的高位标记序列化类型，低位表示 header 长度。
- header 中有 request code、opaque、flag 和业务请求头字段。
- body 就是消息 body，可能已压缩或是批量编码后的多条消息。

源码：

- [`RemotingCommand.java`](../remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingCommand.java)
- [`NettyEncoder.java`](../remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyEncoder.java)
- [`NettyDecoder.java`](../remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyDecoder.java)

### 6.3 `opaque` 如何关联请求和响应

每个 `RemotingCommand` 有一个 `opaque` 请求 ID。发送前，客户端创建
`ResponseFuture` 并放入：

```text
responseTable[opaque] = ResponseFuture
```

Broker 响应会复制同一个 `opaque`。客户端 Netty 线程收到响应后从 `responseTable`
删除对应 future：同步调用唤醒等待线程，异步调用执行 callback。异步重试复用请求内容
时会分配新的 `opaque`，避免旧响应误配到新尝试。

### 6.4 三种通信模式的真实边界

`CommunicationMode` 表示客户端与 Broker 之间处理一次请求的方式：

| 模式 | 调用线程是否等待 Broker 响应 | 获取结果的方式 | 典型用途 |
| --- | --- | --- | --- |
| `SYNC` | 是 | 方法直接返回 `SendResult` | 需要立即确认发送结果的业务消息 |
| `ASYNC` | 否 | `SendCallback` 回调 | 高并发、希望减少业务线程等待的发送场景 |
| `ONEWAY` | 否，Broker 也不返回响应 | 没有发送结果 | 日志、监控等允许少量消息丢失的场景 |

对应的 Producer API 用法：

```java
// SYNC：当前线程等待发送结果
SendResult result = producer.send(message);

// ASYNC：发送结果通过回调返回
producer.send(message, new SendCallback() {
    @Override
    public void onSuccess(SendResult result) {
    }

    @Override
    public void onException(Throwable e) {
    }
});

// ONEWAY：不等待 Broker 响应，也没有回调
producer.sendOneway(message);
```

异步并不等于不可靠，调用方仍应处理失败回调。单向发送则无法确认 Broker 是否已成功
处理和保存消息，只适合能够接受消息丢失的场景。

#### SYNC 与 ASYNC 的共同点

两者都是请求-响应通信，Broker 都会返回处理结果。它们也共用消息检查、Topic 路由、
队列选择、消息压缩、请求构造、发送 Hook 和 Broker 存储等主流程。区别主要发生在请求
发出之后：谁等待响应，以及结果如何交给业务代码。

以默认发送 API 为例：

```text
SYNC
业务线程 -> 路由、选队列、构造请求 -> invokeSync -> 等待并解析响应
        <- 返回 SendResult 或抛出异常

ASYNC
业务线程 -> 提交发送任务 -> 返回
异步发送线程 -> 路由、选队列、构造请求 -> invokeAsync -> 返回
回调线程     <- Broker 响应 -> 解析响应 -> onSuccess / onException
```

| 对比项 | `SYNC` | `ASYNC` |
| --- | --- | --- |
| Producer API | `SendResult send(...)` | `void send(..., SendCallback)` |
| 正常结果 | 方法返回值 | `onSuccess` |
| 发送失败 | 向调用方抛出异常 | `onException` |
| 业务线程 | 等待 Broker 响应 | 通常只提交任务，不等待响应 |
| 默认外层发送次数 | `1 + retryTimesWhenSendFailed` | 1 |
| 后续重试 | 在 `sendDefaultImpl` 循环中完成 | 在异步回调的 `onExceptionImpl` 中完成 |

同步重试会重新选择队列，并优先避开上一次失败的 Broker。异步重试由
`retryTimesWhenSendAsyncFailed` 控制；默认发送带有 Topic 路由时，当前源码也会调用
`selectOneMessageQueue` 重新选队列，因此目标是否变化取决于路由和故障策略。

`ASYNC` 的“不阻塞”特指不等待 Broker 响应，并不保证调用瞬间返回。开启异步背压后，
业务线程可能等待在途消息数或消息字节数的 Semaphore；排队和等待时间也会消耗本次
发送的 timeout 预算。参数检查、任务提交等调用阶段的错误仍可能直接抛出，网络发送和
Broker 处理阶段的结果则通过 callback 返回。

#### SYNC

`invokeSync` 的预算同时覆盖建连/取 Channel、获取 Remoting 并发信号量、写请求和等待
响应。响应到达后才进入 `processSendResponse`。

#### ASYNC

默认异步发送先把完整发送逻辑提交到异步发送 Executor。若开启异步背压，业务线程还
可能在提交前等待“在途消息数”和“在途 body 字节数”两个 Semaphore。网络完成后，
Remoting callback 解析响应、更新 Broker 故障项，并调用业务 `SendCallback`。

异步重试位于 `MQClientAPIImpl.onExceptionImpl`，主要处理连接、写请求、等待响应等
Remoting 失败。Broker 已正常返回但响应码不是四种可转成 `SendStatus` 的状态时，当前
代码不会按同步路径的 `retryResponseCodes` 再试；`RemotingTooMuchRequestException`
也不会继续重试。

#### ONEWAY

`invokeOneway` 给请求打上 one-way 标记，不在 `responseTable` 登记等待项。客户端提交
异步建连/写入后即可返回，甚至不能保证返回时 socket write 已经成功完成。请求若
成功到达 Broker，Broker 仍会正常执行校验与存储，但 Remoting 层看到 one-way 标志后
不会把响应写回客户端。

因此 ONEWAY 只能表达“尽力提交”，不能表达“Broker 已保存”。

Remoting 核心实现：

- [`NettyRemotingAbstract.java`](../remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java)
- [`NettyRemotingClient.java`](../remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingClient.java)

## 7. Broker 如何接住发送请求

### 7.1 从 Netty 线程到发送线程池

Broker 启动时把以下 Request Code 都注册到同一个 `SendMessageProcessor`：

```text
SEND_MESSAGE
SEND_MESSAGE_V2
SEND_BATCH_MESSAGE
CONSUMER_SEND_MSG_BACK
```

并绑定 `sendMessageExecutor`。默认发送线程数是 `min(CPU, 4)`，队列容量是 10000。

请求还在 Netty 线程时会先做快速拒绝：

- 当前是普通 Slave 且未启用 Slave Acting Master。
- OS PageCache 被判定繁忙。
- TransientStorePool 已耗尽。
- 发送线程池队列已满。

这些情况直接返回 `SYSTEM_BUSY`。Broker Fast Failure 还会清理在发送队列里等待过久的
请求，避免旧请求继续占用资源。

注册与分发源码：

- [`BrokerController.java`](../broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java)
- [`NettyRemotingAbstract.java`](../remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java)
- [`BrokerFastFailure.java`](../broker/src/main/java/org/apache/rocketmq/broker/latency/BrokerFastFailure.java)

### 7.2 Processor 之前还有安全与 RPC 管线

发送线程实际执行顺序可以概括为：

```text
RPCHook.doBeforeRequest
  -> AuthenticationPipeline
  -> AuthorizationPipeline
  -> SendMessageProcessor.processRequest
  -> RPCHook.doAfterResponse
```

这条管线在 `sendMessageExecutor` 中执行，不在 Netty I/O 线程执行。认证、授权或
RPC Hook 可以在消息 Processor 之前拒绝请求；RPC Hook 的普通异常会让 Remoting 返回
`SYSTEM_ERROR`。进入 Processor 后的 Broker `SendMessageHook.sendMessageBefore` 行为不同：
`AbortProcessException` 主动中止，其他异常被静默吞掉。

默认 `asyncSendEnable=true` 时，Processor 挂上存储 future 的 continuation 后返回
`null`。此时 Remoting 管线的 `RPCHook.doAfterResponse` 看到的也是 `null`；最终响应由
continuation 写回。单条消息 continuation 使用 `putMessageFutureExecutor`，批量路径使用
`sendMessageExecutor`。

### 7.3 `processRequest` 的业务分流

`SendMessageProcessor.processRequest` 依次完成：

1. 把 V1/V2 请求头解析成统一对象。
2. 构造静态 Topic 映射上下文，必要时把逻辑队列改写到当前物理队列。
3. 构造 Broker 发送 Hook 上下文，并给属性补 Broker region、trace switch。
4. 执行发送前 Hook。
5. 删除不允许客户端保留的内部属性。
6. 按 `batch` 标志进入普通消息或批量消息处理。

### 7.4 `preSend` 与 Broker 侧校验

普通消息先调用 `preSend`：

- 创建响应对象并复制 request `opaque`。
- Broker 尚未到允许接收消息的时间时拒绝。
- 校验 Topic 名称和禁止发送的系统 Topic。
- 查找 `TopicConfig`；不存在时按默认 Topic 尝试自动创建。
- 校验 queueId 没有超出 Topic 队列范围。

这层校验和客户端校验不是重复浪费。客户端输入不可信，路由也可能过期，Broker 必须
以自己的当前配置做最终裁决。

校验实现位于
[`AbstractSendMessageProcessor.java`](../broker/src/main/java/org/apache/rocketmq/broker/processor/AbstractSendMessageProcessor.java)。

### 7.5 构造 `MessageExtBrokerInner`

Broker 不会直接把 Remoting body 原样当作完整存储记录，而是创建
`MessageExtBrokerInner`：

| 字段 | 来源 |
| --- | --- |
| `topic`, `queueId` | 请求头；负 queueId 才由 Broker 随机选 |
| `body`, `flag`, properties | Remoting 请求 |
| `bornTimestamp` | Producer 请求头 |
| `bornHost` | 当前 Netty Channel 的远端地址 |
| `storeHost` | 当前 Broker 存储地址 |
| `reconsumeTimes` | 请求头 |
| `tagsCode` | 按 Topic 过滤类型和 Tag 计算 |
| `sysFlag` | 压缩、事务、地址类型等标志 |

如果客户端没有提供 `UNIQ_KEY`，Broker 会兜底生成一个。随后还有几条分支：

- Retry Topic 可能按重试次数改写到 DLQ。
- Priority Topic 可以按优先级改写 queueId；普通 Topic 会清除无效 priority 属性。
- Compaction Topic 强制要求 message key。
- 事务 prepared 消息进入 `TransactionalMessageService`。
- 普通消息进入 `MessageStore.asyncPutMessage`。

## 8. Store 层：从对象到 CommitLog 字节

### 8.1 四个 `PutMessageHook`

`DefaultMessageStore.asyncPutMessage` 在 CommitLog 前按顺序执行：

```text
checkBeforePutMessage
  -> innerBatchChecker
  -> handleScheduleMessage
  -> handleLmqQuota
```

其中 `checkBeforePutMessage` 会检查：

- Store 是否关闭。
- 当前角色是否允许写入。
- `RunningFlags` 是否可写，例如磁盘满时会关闭写权限。
- Topic 的 UTF-8 字节长度和 body 是否合法。
- OS PageCache 是否繁忙。

延时消息也在这里被改写到内部调度 Topic 或 Timer Topic，因此它后面仍复用 CommitLog
主线。

源码：
[`HookUtils.java`](../broker/src/main/java/org/apache/rocketmq/broker/util/HookUtils.java)。

### 8.2 `asyncPutMessage` 哪部分是同步的

调用：

```java
CompletableFuture<PutMessageResult> future = commitLog.asyncPutMessage(msg);
```

并不代表 CommitLog append 被扔到另一个线程。下面这些工作在调用线程内完成：

```text
设置 storeTimestamp 和 bodyCRC
  -> 确定消息存储版本和 IPv4/IPv6 标记
  -> 计算需要的副本 ACK 数
  -> topicQueueLock(topic + queueId)
     -> 分配逻辑 queueOffset
     -> MessageExtEncoder.encode
     -> putMessageLock
        -> 选择/创建 MappedFile
        -> appendMessage
     -> append 成功后递增内存 queueOffset
  -> 创建刷盘 future 和 HA future
```

只有最后两个 future 可能尚未完成。默认 Broker `asyncSendEnable=true` 时，Processor 把
组装响应的 continuation 挂到 future 后就释放发送线程；但调用
`commitLog.asyncPutMessage` 本身已经完成了本次 append。

### 8.3 两把锁分别保护什么

`topicQueueLock(topic-queue)` 与 `putMessageLock` 解决两个不同的顺序问题：

- `topicQueueLock`：保证同一 Topic/Queue 的逻辑 offset 分配、物理 append 和 offset
  递增不交叉。
- `putMessageLock`：保证所有 Topic、所有 Queue 写入全局 CommitLog 时形成唯一的物理
  顺序。

所以 RocketMQ 同时存在两种顺序：

```text
queueOffset     某个 Topic/Queue 内的逻辑顺序
physicalOffset  所有消息在 CommitLog 中的全局物理顺序
```

### 8.4 CommitLog 记录如何形成

`MessageExtEncoder` 先编码大部分字段，`DefaultAppendMessageCallback` 在持有 append 锁时
回填必须到最后一刻才能确定的 CommitLog 字段：

- queueOffset。
- physicalOffset。
- 最终 storeTimestamp。

基于 `storeHost + physicalOffset` 的物理消息 ID 不是 CommitLog 记录的回填字段。
`AppendMessageResult.getMsgId()` 在需要时由这两个值计算并缓存。

一条记录的核心布局是：

```text
totalSize | magicCode | bodyCRC | queueId | flag
queueOffset | physicalOffset | sysFlag
bornTimestamp | bornHost | storeTimestamp | storeHost
reconsumeTimes | preparedTransactionOffset
bodyLength | body | topicLength | topic | propertiesLength | properties
```

append 目标通常是 `MappedFile` 的内存映射区；启用 TransientStorePool 时可能先写堆外
writeBuffer。这里的 `append` 只表示内存写位置前移，不天然等于数据已经 `force` 到
物理磁盘。

当前文件剩余空间不足时，CommitLog 写入 `BLANK_MAGIC_CODE` 封住文件尾，创建下一个
MappedFile，再重新 append 当前消息。

## 9. 刷盘与 HA：`SEND_OK` 的真正含义

append 成功后，CommitLog 并行得到两个 future：

```java
flushFuture = handleDiskFlush(...);
replicaFuture = needHandleHA ? handleHA(...) : completedFuture(PUT_OK);

return flushFuture.thenCombine(replicaFuture, ...);
```

两个 future 都完成后 Broker 才形成最终 `PutMessageStatus`。

### 9.1 刷盘确认

| 配置 | `waitStoreMsgOK` | Producer 响应前的保证 |
| --- | --- | --- |
| `ASYNC_FLUSH` | 任意 | flush future 立即完成；按配置唤醒或等待后台周期刷盘 |
| `SYNC_FLUSH` | `true`，默认 | 等待 `flushedWhere` 越过本消息末尾 |
| `SYNC_FLUSH` | `false` | 只唤醒刷盘线程，不等待 |

同步刷盘等待超过 `syncFlushTimeout` 时返回 `FLUSH_DISK_TIMEOUT`。这个状态说明 append 已
完成，只是规定时间内没有得到刷盘确认，不等于“消息一定不存在”。

### 9.2 副本确认

当前经典 CommitLog 只有同时满足以下条件才等待 HA ACK：

- 消息 `waitStoreMsgOK=true`，缺省即为 true。
- 未启用 duplication 模式。
- Broker 角色是 `SYNC_MASTER`。

`ASYNC_MASTER` 不等待副本 ACK。即使配置为 `SYNC_MASTER`，当前版本的
`inSyncReplicas` 也包含 Master 自身，默认值为 1；`needAckNums <= 1` 会立即成功。要把
一个 Slave 的 ACK 纳入成功边界，需要相应配置大于 1 的 ISR ACK 数。

需要等待时，Master 以消息末尾 `physicalOffset + wroteBytes` 创建
`GroupCommitRequest`。足够多副本的复制 offset 越过该位置才完成；超过 `slaveTimeout`
返回 `FLUSH_SLAVE_TIMEOUT`。

Controller 或 Slave Acting Master 模式还可能在 append 前检查 ISR 数量，不足时返回
`IN_SYNC_REPLICAS_NOT_ENOUGH`，这与“已经 append、等待 ACK 超时”是不同阶段。

`SLAVE_NOT_AVAILABLE` 仍保留在响应映射中，但不要把它简单理解成当前普通
`SYNC_MASTER` 没有 Slave 时的唯一结果；当前 `CommitLog.handleHA` 主线主要产生
`FLUSH_SLAVE_TIMEOUT` 或 append 前的 ISR 不足。

默认 Store 配置是 `ASYNC_FLUSH + ASYNC_MASTER`。因此默认配置下的 `SEND_OK` 主要表示
消息已成功 append 到 Broker 的 CommitLog 内存写路径，并不包含物理刷盘确认和副本
ACK。

## 10. 为什么返回成功时 Consumer 仍可能暂时看不到

CommitLog append 前，`assignQueueOffset` 只读取内存表的当前值并写入消息；只有 append
成功后，`increaseQueueOffset` 才递增内存 offset 表。这两步都没有当场写出对应的
ConsumeQueue 条目。

后台 `ReputMessageService` 随后执行：

```text
从 reputFromOffset 扫描 CommitLog
  -> checkMessageAndReturnSize 解析 DispatchRequest
  -> CommitLogDispatcherBuildConsumeQueue
     -> 写 Topic/Queue 对应的 ConsumeQueue 条目
  -> CommitLogDispatcherBuildIndex
     -> 按 Key/UNIQ_KEY 构建索引
  -> notifyMessageArriveIfNecessary
```

因此三个时刻必须分开：

```text
T1: CommitLog append 完成，可形成 SendResult.queueOffset
T2: ConsumeQueue / Index 构建完成，Consumer 和 Key 查询可定位消息
T3: Consumer 拉取并完成业务消费
```

Broker 发送响应最多等待 T1 加上配置要求的刷盘/复制确认，不等待 T2，更不等待 T3。
`SEND_OK` 从来不表示“Consumer 已经消费成功”。

Reput 默认扫描到 `confirmOffset`：普通非 duplication 模式下，`SYNC_FLUSH` 以
`flushedWhere` 为上界，`ASYNC_FLUSH` 以 CommitLog `maxOffset` 为上界。因此异步
刷盘时 ConsumeQueue 可能先于 CommitLog force 构建；同步刷盘时不会扫过已刷盘位置。
ConsumeQueue 本身还由独立服务刷盘，发送 future 既不等待 CQ 构建，也不等待 CQ 刷盘。

分发实现位于 `DefaultMessageStore.ReputMessageService` 和
`CommitLogDispatcherBuildConsumeQueue`。

## 11. Broker 响应如何变成 `SendResult`

### 11.1 Store 状态到响应码

`SendMessageProcessor.handlePutMessageResult` 把下面四种状态都视为“消息已 append，可以
返回带 offset 的发送结果”：

| `PutMessageStatus` | Broker `ResponseCode` | 客户端 `SendStatus` |
| --- | --- | --- |
| `PUT_OK` | `SUCCESS` | `SEND_OK` |
| `FLUSH_DISK_TIMEOUT` | 同名 | 同名 |
| `FLUSH_SLAVE_TIMEOUT` | 同名 | 同名 |
| `SLAVE_NOT_AVAILABLE` | 同名 | 同名 |

它们都会填充：

- CommitLog 物理 `msgId`。
- 实际 queueId。
- queueOffset。
- transactionId。
- 精确延时消息可能还有 recall handle。

其他状态，如 `MESSAGE_ILLEGAL`、`SERVICE_NOT_AVAILABLE`、`SYSTEM_BUSY`、MappedFile 创建
失败或 ISR 不足，会转成错误响应；客户端收到后抛 `MQBrokerException`。

### 11.2 三个 ID/Offset 不要混淆

| `SendResult` 字段 | 生成方 | 含义 |
| --- | --- | --- |
| `msgId` | Producer | 消息属性 `UNIQ_KEY`，用于业务追踪和唯一 Key 查询 |
| `offsetMsgId` | Broker CommitLog | `storeHost(IP+port) + physicalOffset` 的编码，可定位物理记录 |
| `queueOffset` | Broker Store | 该 Topic/Queue 内的逻辑序号 |

Broker 响应头里的字段名 `msgId` 实际是物理 ID；客户端在构造 `SendResult` 时把它放进
`offsetMsgId`，再把原消息 `UNIQ_KEY` 放进 `msgId`。这正是最容易被字段名误导的地方。

普通外部 Batch 的 `msgId` 和 `offsetMsgId` 可能是逗号分隔的多条 ID，autoBatch
会按原消息顺序拆成逐条 `SendResult`。BatchCQ inner-batch 返回批次级 ID，autoBatch
不拆 ID 和 offset，而是让每条原消息的回调共享同一个批次级 `SendResult`。

### 11.3 正常回调不等于 `SEND_OK`

同步 `send()` 可以正常返回一个 `sendStatus != SEND_OK` 的 `SendResult`。异步路径也会
对这四种状态调用 `SendCallback.onSuccess`，业务仍要检查其中的 `sendStatus`。

默认 `retryAnotherBrokerWhenNotStoreOK=false`，因此刷盘或副本超时通常直接返回业务，
不会自动换 Broker。开启这个配置会重试，但第一次消息已经 append，更容易产生重复。

## 12. 失败、重试与重复消息

### 12.1 默认同步路径的失败矩阵

| 失败点 | Broker 是否可能已存储 | 默认行为 |
| --- | --- | --- |
| 进入 `Validators` 后的校验失败 | 否 | 直接抛 `MQClientException` |
| 无 Topic 路由 | 否 | 直接抛 `MQClientException` |
| 建连失败 | 通常否 | 换队列/Broker 重试 |
| socket 写失败 | 不确定 | 换队列/Broker 重试 |
| 等待响应超时 | 是 | 换队列/Broker 重试 |
| Broker 返回可重试错误码 | 取决于错误阶段 | 按 `retryResponseCodes` 重试 |
| Broker 返回刷盘/复制超时 | 是，已经 append | 默认返回非 OK `SendResult` |
| 线程被中断 | 不确定 | 立即抛出，不自动重试 |

“等待响应超时”是重复消息最典型的窗口：

```text
Producer                Broker
   | ---- request ------> |
   |                      | append 成功
   | <X-- response -------| 响应丢失或回来太晚
   | timeout              |
   | ---- retry --------> | 第二次 append
```

客户端无法从超时本身判断第一次请求停在网络、Broker 队列、CommitLog append、刷盘还是
响应返回阶段。自动重试沿用同一个 `UNIQ_KEY`，但普通消息存储不会因此自动去重，两次
请求仍可形成两条 CommitLog 记录。

所以 RocketMQ 普通发送的重试机制体现的是 at-least-once 取向，也为业务实现至少
一次投递提供基础；有限重试本身不是交付保证，更不是天然 exactly-once。业务应以
订单号、事件 ID 等稳定业务主键做消费幂等，不能把客户端 `send()` 的一次调用等同于
Broker 里恰好一条记录。与 Kafka 幂等 Producer 的对比、以及业务侧规避清单见
[`rocketmq_send_retry_and_idempotency.md`](rocketmq_send_retry_and_idempotency.md)。

### 12.2 默认异步路径

默认异步发送只有一次 `sendDefaultImpl` 外层尝试，重试在 Remoting callback 内部：

- `retryTimesWhenSendAsyncFailed` 默认 2。
- 默认路径持有 `TopicPublishInfo`，Remoting 失败时可以选择其他 Broker。
- 固定队列和 selector 路径没有该路由对象，只会重试原 Broker。
- 重试复用请求内容并换新 `opaque`。
- 预算耗尽或不允许重试时调用 `SendCallback.onException`。

### 12.3 顺序消息为什么要单独看

默认重试可能换 Broker 和 Queue，因此不承诺跨队列全局顺序。业务用 selector 把相同
Sharding Key 固定到同一队列时，同步路径没有默认发送的外层重试，结果是客户端
不会在这一层悄悄换掉已经选定的队列。若业务自行重试，仍要同时处理固定队列、
重复投递和失败时序。

## 13. `timeout` 是逐层扣减的预算，不是绝对 deadline

默认同步路径在查路由前记录起点，每轮尝试传入剩余时间：

```text
send timeout
  - 查路由耗时
  - 选队列与准备消息耗时
  - 前面尝试耗时
  - 建连/等信号量/网络往返耗时
  = 当前层可用预算
```

`sendMsgMaxTimeoutPerRequest` 默认 `-1`，表示不额外限制。设置为非负值后，只要后面仍有
重试机会，当前单次请求最多使用这个值，给后续尝试留出机会；最后一次仍可使用全部
剩余预算。

但当前实现有几处无法统一成严格的端到端 deadline：

1. NameServer 路由 RPC 使用独立的 `mqClientApiTimeout`，默认也是 3000 ms，而不是
   调用方传入的 send timeout。路由调用返回后，发送链才检查总耗时是否已超。
2. 固定队列同步路径先检查前置耗时，却把原始 timeout 传给 kernel，而不是严格传
   `timeout - cost`。
3. 不带 timeout 的 selector 重载可能先用一份默认超时完成选择，再进入固定队列发送，
   后者重新取得一份默认发送超时。
4. autoBatch 的聚合等待发生在随后 `sendDirect` 的计时之前。
5. 异步 Executor 排队、callback 调度和系统停顿会让墙钟时间越过名义预算；相关 async
   timeout 重载在源码中也标记了 timeout 语义问题。

因此更准确的理解是：RocketMQ 在主要阻塞点传递并扣减“剩余预算”，用于停止继续
尝试；它不是实时系统意义上的硬 deadline，也不能证明超时瞬间 Broker 已停止处理。

## 14. 重要分支如何接回主线

### 14.1 显式 Batch 与 autoBatch

`send(Collection<Message>)` 显式创建 `MessageBatch`；autoBatch 则由 accumulator 动态
创建。两者都会给批次对象设置 `UNIQ_KEY`，并共享编码和请求入口：

```text
MessageDecoder.encodeMessages
  -> SEND_BATCH_MESSAGE
  -> SendMessageProcessor.sendBatchMessage
     -> 普通 ConsumeQueue: MessageStore.asyncPutMessages -> CommitLog.asyncPutMessages
     -> BatchCQ inner-batch: MessageStore.asyncPutMessage -> CommitLog.asyncPutMessage
```

当前客户端不会再压缩 `MessageBatch`。普通 Batch 中每条消息编码为独立 CommitLog
记录并占用连续 queueOffset；BatchCQ 把整批作为一条 inner-batch CommitLog 记录，
Consumer 再按 `NEED_UNWRAP_FLAG` 解包。

### 14.2 事务消息

事务 prepared 消息在客户端设置属性与 sysFlag。Broker 检测到后不直接走普通
`MessageStore`，而是进入 `TransactionalMessageService.asyncPrepareMessage`：

- `parseHalfMessageInner` 保存真实 Topic 和 queueId，把事务 sysFlag 重置为 NOT，
  再改写到 half Topic 的 queue 0。
- prepared 阶段只进入 half Topic 的 ConsumeQueue，不进入业务 Topic。
- commit 不是让原 half 记录原地可见，而是另写一条恢复真实 Topic 的消息，
  再向 op Topic 写标记以移除 half 待处理状态。
- rollback 只处理 half/op 状态，不写业务消息。

所以第一次 `sendMessageInTransaction` 的发送结果只表示 half message 的保存结果，不是
业务消息已经对 Consumer 可见。

### 14.3 延时消息

Store 的 `handleScheduleMessage` Hook 在 CommitLog 前处理延时语义：

- delay level 消息改写到内部 Schedule Topic。
- 精确定时功能未启用时直接拒绝；启用后，只有合法且位于未来的投递时间才改写到
  Timer Topic，并保存真实 Topic、queueId 和投递时间属性。
- 已过期的普通投递时间保留真实 Topic 并立即投递；超过最大延时或删除请求时间非法
  会返回对应错误。

到期服务之后再把消息恢复到真实 Topic。第一次发送的 `SEND_OK` 表示延时载体已经按
当前刷盘/复制配置保存，不表示已到投递时间。

### 14.4 静态 Topic

客户端路由包含逻辑队列到 endpoint 的映射，发送时解析当前物理 Broker；Broker 的
`TopicQueueMappingManager` 校验当前 Broker 是否是该逻辑队列的映射 leader，改写物理
queueId，并在响应时把 queueId/queueOffset 转回逻辑值。但客户端构造返回的
`MessageQueue` 时使用实际物理 `brokerName`，所以不能把整个返回对象称为稳定的逻辑队列。

## 15. 建议断点与观察变量

按下面顺序单步，能看到一条消息从业务对象变成 CommitLog 记录，再变回 `SendResult`：

| 断点 | 重点观察 |
| --- | --- |
| `DefaultMQProducer.send` | namespace、是否进入 autoBatch |
| `DefaultMQProducerImpl.sendDefaultImpl` | `timesTotal`、`times`、`curTimeout`、`brokersSent` |
| `tryToFindTopicPublishInfo` | 缓存命中、NameServer 查询、默认 Topic 兜底 |
| `MQFaultStrategy.selectOneMessageQueue` | `lastBrokerName`、过滤与降级选择 |
| `sendKernelImpl` | brokerAddr、UNIQ_KEY、压缩前后 body、sysFlag、requestHeader |
| `MQClientAPIImpl.sendMessage` | requestCode、opaque、通信模式 |
| `NettyRemotingAbstract.invoke0` | responseTable、Semaphore、write future |
| `SendMessageProcessor.processRequest` | V2 头还原、静态 Topic、Hook、single/batch 分流 |
| `SendMessageProcessor.sendMessage` | `MessageExtBrokerInner`、事务/延时/重试分支 |
| `DefaultMessageStore.asyncPutMessage` | PutMessageHook 的返回值 |
| `CommitLog.asyncPutMessage` | queueOffset、两把锁、MappedFile、AppendMessageResult |
| `DefaultAppendMessageCallback.doAppend` | physicalOffset、storeTimestamp、物理 msgId |
| `CommitLog.handleDiskFlushAndHA` | flushFuture、replicaFuture、最终 PutMessageStatus |
| `handlePutMessageResult` | ResponseCode、响应头 msgId/queueOffset |
| `MQClientAPIImpl.processSendResponse` | `msgId` 与 `offsetMsgId` 的重新组装 |
| `ReputMessageService.doReput` | CommitLog 到 ConsumeQueue/Index 的异步距离 |

建议至少跑三次：

1. 路由已缓存的正常 `SEND_OK`。
2. 清空路由缓存后观察 NameServer 查询。
3. 在 Broker append 后、写响应前暂停，制造客户端超时，再观察相同 `UNIQ_KEY` 的重试
   是否形成不同 `offsetMsgId`。

## 16. 最终心智模型

Producer 发送不是一次简单的 socket write，而是三段协议：

```text
客户端寻址协议
  Topic -> TopicPublishInfo -> MessageQueue -> brokerName -> brokerAddr

Broker 存储协议
  Remoting request -> MessageExtBrokerInner -> queueOffset + physicalOffset
  -> CommitLog append -> 可选刷盘确认 + 可选副本 ACK

异步可见性协议
  CommitLog -> Reput -> ConsumeQueue / Index -> Consumer 拉取与消费
```

判断一次发送是否满足业务要求，至少要分别回答：

1. 客户端是否收到了 Broker 响应？
2. `sendStatus` 是否为 `SEND_OK`，还是刷盘/副本确认超时？
3. 当前 Broker 配置下 `SEND_OK` 包含刷盘或副本 ACK 吗？
4. 超时重试是否可能写入重复消息？
5. Consumer 是否按业务主键实现了幂等？

只有把这五个问题分开，才能准确理解 RocketMQ 的发送结果，而不是把“API 正常返回”、
“消息已落 CommitLog”、“消息已持久化”、“消息已复制”和“消息已消费”误认为同一件事。
