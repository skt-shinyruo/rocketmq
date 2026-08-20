# `tryToFindTopicPublishInfo` 在发送过程中做什么

发送消息时会执行：

```java
TopicPublishInfo topicPublishInfo =
    this.tryToFindTopicPublishInfo(msg.getTopic());
```

这一步的核心作用是：**取得当前 Topic 的可写队列和 Broker 路由，供后续选择一个
`MessageQueue` 发送消息**。它还没有真正向 Broker 发送消息。

源码入口：
[DefaultMQProducerImpl.java](../client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java)。

## 1. `TopicPublishInfo` 中有什么

`TopicPublishInfo` 是 Producer 侧的 Topic 发布路由信息，主要包含：

- `messageQueueList`：该 Topic 当前可用于发送的队列，每项由 Topic、Broker 名称和
  Queue ID 组成。
- `sendWhichQueue`：轮询选择队列时使用的线程本地索引。
- `orderTopic`：是否为顺序 Topic。
- `haveTopicRouterInfo`：是否已经从 NameServer 取得过该 Topic 的路由。
- `topicRouteData`：NameServer 返回的原始路由数据。

类定义：
[TopicPublishInfo.java](../client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java)。

`ok()` 的判断很直接：`messageQueueList` 不为 `null` 且不为空。也就是说，只有存在
可发送队列时，这份发布信息才可用于正常发送。

## 2. 查找过程

`tryToFindTopicPublishInfo(topic)` 分三步处理。

### 第一步：先查本地缓存

Producer 使用 `topicPublishInfoTable` 缓存每个 Topic 的 `TopicPublishInfo`：

```text
topic -> TopicPublishInfo
```

如果缓存存在且 `ok()` 为 `true`，通常可以直接返回，发送路径不必每次访问
NameServer。

### 第二步：缓存缺失或没有可写队列时，查询真实 Topic

如果缓存中没有这个 Topic，或者已有对象的队列列表为空，方法会：

1. 先用 `putIfAbsent` 放入一个空的 `TopicPublishInfo`，作为该 Topic 的缓存占位。
2. 调用 `MQClientInstance.updateTopicRouteInfoFromNameServer(topic)` 查询 NameServer。
3. 从缓存中重新读取更新后的 `TopicPublishInfo`。

NameServer 返回的是 `TopicRouteData`。客户端会把其中具有写权限、存在 Master 的
Broker 队列转换成 `MessageQueue` 列表，然后调用各 Producer 的
`updateTopicPublishInfo` 更新 `topicPublishInfoTable`。

转换和更新逻辑位于：
[MQClientInstance.java](../client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java)。

### 第三步：真实 Topic 没有路由时，尝试默认 Topic

如果查询后仍然既没有有效队列，也没有取得真实 Topic 的路由，方法会再次调用：

```java
updateTopicRouteInfoFromNameServer(topic, true, defaultMQProducer);
```

`isDefault = true` 表示查询默认自动建 Topic `TBW102` 的路由，并把它转换成当前
Topic 的发布队列。发送到 Broker 后，只有 Broker 开启自动创建 Topic 时，Broker
才可能据此创建真实 Topic；生产环境通常应提前显式创建 Topic，不能依赖这个兜底。

## 3. `TopicRouteData` 示例解读

Example 中查询 NameServer 得到：

```text
TopicRouteData [
  orderTopicConf=null,
  queueDatas=[
    QueueData [
      brokerName=broker-a,
      readQueueNums=4,
      writeQueueNums=4,
      perm=6,
      topicSysFlag=0
    ]
  ],
  brokerDatas=[
    BrokerData [
      brokerName=broker-a,
      brokerAddrs={0=10.255.255.254:10911},
      enableActingMaster=false
    ]
  ],
  filterServerTable={},
  topicQueueMappingInfoTable=null
]
```

它表达的是：**当前 Topic 位于 `broker-a`，有 4 个可写队列和 4 个可读队列，
客户端应连接 `10.255.255.254:10911` 这个 Master Broker。**

`TopicRouteData` 本身没有打印 Topic 名称，因为 Topic 是查询路由时单独传入的参数。
它保存的是这个 Topic 对应的 Queue 和 Broker 拓扑，而不是消息正文或消费数据。

### 3.1 `orderTopicConf=null`

没有 NameServer 维护的全局顺序 Topic 配置，因此客户端按普通 Topic 路由生成队列。

这不表示该 Topic 完全不能发送顺序消息。业务仍可通过选择同一个 `MessageQueue`，
例如按业务 Key 或 Sharding Key 固定选队列，保证同一队列内的发送顺序。它只表示
本次路由没有使用 `orderTopicConf` 这种专门的顺序 Topic 配置。

### 3.2 `queueDatas`

`QueueData` 描述某个 Broker 上这个 Topic 的队列数量和权限。这里仅有一项：

| 字段 | 值 | 含义 |
| --- | --- | --- |
| `brokerName` | `broker-a` | 这些队列属于名为 `broker-a` 的 Broker 组 |
| `readQueueNums` | `4` | Consumer 可读取 4 个逻辑队列 |
| `writeQueueNums` | `4` | Producer 可向 4 个逻辑队列写消息 |
| `perm` | `6` | `PERM_READ(4) | PERM_WRITE(2)`，即 `RW-`，可读可写 |
| `topicSysFlag` | `0` | 未设置 Unit、UnitSub 等 Topic 系统标志，是普通配置 |

`QueueData` 是聚合描述，并没有逐项打印队列。客户端会根据数量把它展开成：

```text
MessageQueue(topic, broker-a, 0)
MessageQueue(topic, broker-a, 1)
MessageQueue(topic, broker-a, 2)
MessageQueue(topic, broker-a, 3)
```

读写队列数通常相同，但它们是两个独立配置。如果某个 Topic 有 4 个写队列、8 个读
队列，Producer 只会生成 4 个发布队列，Consumer 则会看到 8 个订阅队列。

权限常量定义在：
[PermName.java](../common/src/main/java/org/apache/rocketmq/common/constant/PermName.java)。

### 3.3 `brokerDatas`

`BrokerData` 负责把逻辑 Broker 名称映射到真实网络地址：

```text
broker-a -> {0=10.255.255.254:10911}
```

Map 的 Key 是 `brokerId`，Value 是 Broker 地址。`brokerId=0` 是
`MixAll.MASTER_ID`，所以当前只有一个 Master，没有显示 Slave。若存在传统主从
Broker，可能还会看到其他非 0 ID，例如：

```text
brokerAddrs={0=master:10911, 1=slave:10911}
```

`enableActingMaster=false` 表示未启用 Slave Acting Master 能力。该能力开启后，
Master 缺失时 NameServer 可以把存活且 Broker ID 最小的 Slave 作为只读的 Acting
Master 返回，并清除 Topic 写权限；它不是让 Slave 接收写入。当前示例本身也没有
Slave 地址，所以不会涉及这条兜底路径。

Producer 从可写 `QueueData` 生成发布队列时，还会确认同名 `BrokerData` 中存在
`brokerId=0` 的 Master。发送时先选择一个 `MessageQueue`，再通过 `broker-a` 找到
`10.255.255.254:10911`，最终向这个地址发送请求。

如果 Producer 日志出现连接 `10.255.255.254:10911` 失败，说明客户端无法访问
Broker 注册到 NameServer、并对客户端公布的这个地址。此时应检查 Broker 所在网络
以及 `brokerIP1` 配置；不能只检查 NameServer 地址，因为 NameServer 返回的 Broker
地址才是消息实际发送和拉取时连接的地址。

Broker 路由模型定义在：
[BrokerData.java](../remoting/src/main/java/org/apache/rocketmq/remoting/protocol/route/BrokerData.java)。

### 3.4 `filterServerTable={}`

没有为 Broker 注册额外的 Filter Server。这个字段主要服务旧式 ClassFilter 过滤
路径；普通 Tag 或属性过滤不要求它必须有值，因此本地示例中为空是正常的。

### 3.5 `topicQueueMappingInfoTable=null`

没有静态 Topic 的逻辑队列映射信息，所以这是普通 Topic 路由。源码中的实际字段名
是 `topicQueueMappingByBroker`，这里只是 `toString()` 使用了
`topicQueueMappingInfoTable` 这个显示名称。

静态 Topic 可以把逻辑队列映射到不同 Broker，并在迁移时维护 epoch 和当前物理
队列位置；本例不需要走这条转换分支。

路由对象定义在：
[TopicRouteData.java](../remoting/src/main/java/org/apache/rocketmq/remoting/protocol/route/TopicRouteData.java)。

### 3.6 `brokerName` 如何解析为 Broker 地址

这里不是先根据 Topic 查询 `brokerName`，再拿 `brokerName` 向 NameServer 发起第二次
地址查询。Topic 路由响应已经同时包含 `QueueData` 和 `BrokerData`，客户端只需要在
本地把两者关联起来。

#### 第一步：Broker 注册路由

Broker 向 NameServer 注册时会上报 `brokerName`、`brokerId`、`brokerAddr` 和 Topic
配置。NameServer 主要维护两类索引：

```text
topicQueueTable:
  topic -> brokerName -> QueueData

brokerAddrTable:
  brokerName -> brokerId -> brokerAddr
```

例如一个主从组会登记为：

```text
broker-a -> {
  0 -> 10.0.0.1:10911,  // Master
  1 -> 10.0.0.2:10911   // Slave
}
```

注册代码位于
[RouteInfoManager.java](../namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java)。

#### 第二步：客户端按 Topic 查询完整路由

首次使用某个 Topic 或刷新路由时，客户端向 NameServer 发送
`GET_ROUTEINFO_BY_TOPIC`。NameServer 先从 `topicQueueTable` 找出承载该 Topic 的
`brokerName`，再从 `brokerAddrTable` 复制相应地址，组装成一个 `TopicRouteData`
返回。例如：

```text
queueDatas:
  broker-a -> writeQueueNums=4
  broker-b -> writeQueueNums=4

brokerDatas:
  broker-a -> {0=10.0.0.1:10911, 1=10.0.0.2:10911}
  broker-b -> {0=10.0.0.3:10911}
```

查询入口位于
[MQClientAPIImpl.java](../client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java)，
NameServer 的响应组装位于
[RouteInfoManager.java](../namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java)。

#### 第三步：客户端拆成几个本地缓存

`MQClientInstance.updateTopicRouteInfoFromNameServer` 收到路由后，主要更新：

1. **Broker 地址缓存**：
   `brokerAddrTable[brokerName][brokerId] = brokerAddr`。
2. **Producer 发布路由**：`topicRouteData2TopicPublishInfo` 检查写权限和 Master，
   按 `writeQueueNums` 展开为 `MessageQueue(topic, brokerName, queueId)`，写入
   `topicPublishInfoTable`。
3. **Consumer 订阅路由**：`topicRouteData2TopicSubscribeInfo` 检查读权限，按
   `readQueueNums` 生成订阅队列，供 Rebalance 分配。

以上示例会为 Producer 生成：

```text
MessageQueue(topic, broker-a, 0..3)
MessageQueue(topic, broker-b, 0..3)
```

转换和缓存更新代码位于
[MQClientInstance.java](../client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java)。

#### 第四步：Producer 本地解析 Master 地址

Producer 先从发布路由中选出一个 `MessageQueue`，例如：

```text
MessageQueue(OrderTopic, broker-a, queueId=2)
```

随后 `sendKernelImpl` 取出 `brokerName=broker-a`，调用
`findBrokerAddressInPublish(brokerName)` 查询客户端本地 `brokerAddrTable`。发送路径
固定取 `brokerId=0`，因此得到 Master 地址：

```text
brokerAddrTable["broker-a"][0] -> 10.0.0.1:10911
```

客户端再向该地址发送请求，并把 `queueId=2` 写入请求头。普通 Topic 直接使用
`MessageQueue` 中的 `brokerName`；静态 Topic 会先把逻辑队列映射到当前实际承载它的
物理 Broker。

发送寻址代码位于
[DefaultMQProducerImpl.java](../client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java)
和
[MQClientInstance.java](../client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java)。

#### Consumer 的区别

Consumer 同样从 `MessageQueue` 取得 `brokerName`，但会调用
`findBrokerAddressInSubscribe(brokerName, brokerId, ...)`。Producer 写入固定找 Master，
Consumer 拉取则可以根据 Broker 的建议和配置选择 Master 或 Slave；目标地址不在缓存
时，拉取路径会先刷新该 Topic 的路由再重试。

消费寻址代码位于
[PullAPIWrapper.java](../client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullAPIWrapper.java)。

客户端默认每 30 秒刷新一次已经使用的 Topic 路由；首次发送发现发布路由不存在时也会
立即查询 NameServer。因此正常发送过程中使用的是本地路由快照，并不会每发送一条消息
都访问 NameServer。

整个寻址过程可以概括为：

```text
Broker 注册 brokerName / brokerId / brokerAddr
  -> 客户端按 Topic 查询一次 TopicRouteData
  -> 缓存 MessageQueue 列表和 brokerAddrTable
  -> 选择 MessageQueue
  -> 取得 brokerName
  -> 本地查询 brokerId 对应的地址
  -> 连接 Broker 发送或拉取
```

## 4. 返回后怎么使用

调用方先检查：

```java
topicPublishInfo != null && topicPublishInfo.ok()
```

检查通过后，`selectOneMessageQueue` 会从 `messageQueueList` 中选择一个队列，随后
`sendKernelImpl` 根据队列中的 Broker 名称找到 Broker 地址，最终发起网络请求。

发送循环中的具体代码是：

```java
MessageQueue mqSelected = this.selectOneMessageQueue(
    topicPublishInfo,
    lastBrokerName,
    resetIndex
);
```

它的作用是：**从当前 Topic 的所有可写 `MessageQueue` 中，为本次发送挑选一个目标
队列，并在重试时尽量避开上一次使用的 Broker。**

### 4.1 三个参数分别是什么

| 参数 | 含义 |
| --- | --- |
| `topicPublishInfo` | 当前 Topic 的发布路由，包含所有候选 `MessageQueue` 和轮询索引 |
| `lastBrokerName` | 上一次发送所选队列的 Broker 名称；首次发送时为 `null` |
| `resetIndex` | 是否重置队列选择索引；在同步发送进入重试后变为 `true` |

在 `sendDefaultImpl` 的第一次循环中，`mq` 还没有赋值，因此：

```text
lastBrokerName = null
resetIndex = false
```

如果同步发送失败并进入下一轮，`mq` 保存着上次选中的队列，因此：

```text
lastBrokerName = 上次发送的 Broker
resetIndex = true
```

该方法本身只是选择队列，不执行网络发送。返回的 `mqSelected` 随后才传给
`sendKernelImpl`。

### 4.2 默认情况下怎么选择

`DefaultMQProducerImpl.selectOneMessageQueue` 自身没有选择算法，而是委托给：
[MQFaultStrategy.java](../client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java)。

默认 `sendLatencyEnable=false`，选择过程为：

1. 使用 `BrokerFilter` 排除 `brokerName == lastBrokerName` 的队列。
2. 使用 `TopicPublishInfo.sendWhichQueue` 保存的线程本地索引进行轮询。
3. 每次把索引加一，再对队列总数取模，得到一个候选队列。
4. 如果候选属于上次的 Broker，就继续向后扫描，最多检查整个队列列表一次。
5. 如果所有队列都属于上次的 Broker，无法避开它，就退化为不带过滤条件再选一次。

这里的轮询索引是线程本地的，并以随机值初始化，所以不同发送线程不会都固定从
Queue 0 起步。索引实现位于：
[ThreadLocalIndex.java](../client/src/main/java/org/apache/rocketmq/client/common/ThreadLocalIndex.java)。

### 4.3 结合当前路由理解

前面的 `TopicRouteData` 最终生成四个发布队列：

```text
(topic, broker-a, queueId=0)
(topic, broker-a, queueId=1)
(topic, broker-a, queueId=2)
(topic, broker-a, queueId=3)
```

首次发送时 `lastBrokerName=null`，过滤器不会排除任何队列，因此会按当前线程的轮询
索引选出其中一个，例如：

```text
mqSelected = MessageQueue(topic, broker-a, queueId=2)
```

同步发送失败后，`lastBrokerName=broker-a`。算法会尝试排除 `broker-a`，但当前四个
队列全部属于它，所以找不到其他 Broker，最终只能退化为再次从 `broker-a` 选择一个
队列。它可能换 Queue，但无法完成跨 Broker 故障转移。

如果路由中还有 `broker-b`，重试时就会优先从 `broker-b` 的队列中选择，从而避免
立即再次请求刚刚失败的 `broker-a`。

### 4.4 开启延迟故障规避后

当 `sendLatencyEnable=true` 时，`MQFaultStrategy` 会结合之前发送记录的耗时和异常
状态选择，优先级如下：

1. 选择“当前可用”并且不属于 `lastBrokerName` 的队列。
2. 如果没有，选择“网络可达”并且不属于 `lastBrokerName` 的队列。
3. 如果仍然没有，退化为从全部队列中轮询一个。

`available` 表示 Broker 的临时隔离时间已经结束；`reachable` 表示 Broker 没有被
连接检测或发送异常标记为不可达。

`resetIndex=true` 只在开启延迟故障规避时生效，它会把当前线程的轮询索引重置为新
的随机值，让重试换一个选择起点。默认未开启延迟故障规避时，这个参数不会参与
选择。

顺序消息依赖固定队列来保证顺序，不应随意开启会干预队列选择的延迟故障规避。

### 4.5 选择之后做什么

得到 `mqSelected` 后，代码会：

```text
MessageQueue(topic, brokerName, queueId)
  -> 用 brokerName 查询 Master 地址
  -> 把 queueId 写入 SendMessageRequestHeader
  -> sendKernelImpl 向目标 Broker 发送消息
```

所以这行代码决定了两个关键结果：**消息发给哪个 Broker，以及写入这个 Topic 的哪
个逻辑 Queue。**

如果最终仍没有有效发布路由，则抛出 `No route info of this topic`，消息不会发送。

整体链路可以概括为：

```text
发送消息
  -> 查询 Producer 本地发布路由缓存
  -> 缓存不可用时向 NameServer 查询 Topic 路由
  -> 必要时尝试 TBW102 默认路由
  -> 得到可写 MessageQueue 列表
  -> 选择一个队列
  -> 查找 Broker 地址并发送
```

因此，这行代码可以理解为：**在真正发送之前，确保 Producer 知道这个 Topic 可以
发往哪些 Broker 的哪些队列。**

## 5. 消息发送到哪里处理

需要区分两种“处理”：

- **Broker 处理发送请求**：接收、校验并持久化消息。
- **Consumer 处理业务消息**：读取消息后执行用户注册的消息监听器。

`tryToFindTopicPublishInfo` 只负责发送前的路由准备。普通单条消息在 Producer、
Broker 和存储层的主要调用链如下：

```text
DefaultMQProducerImpl.sendDefaultImpl
  -> selectOneMessageQueue
  -> DefaultMQProducerImpl.sendKernelImpl
  -> MQClientAPIImpl.sendMessage
  -> NettyRemotingClient
  -> BrokerController 注册的 SendMessageProcessor
  -> SendMessageProcessor.processRequest / sendMessage
  -> DefaultMessageStore.asyncPutMessage
  -> CommitLog.asyncPutMessage
```

### 5.1 Producer 组装并发送请求

`sendKernelImpl` 根据选中的 `MessageQueue` 找到 Broker 地址，并准备
`SendMessageRequestHeader`。Header 中包含 Topic、Queue ID、Producer Group、消息
属性和出生时间等信息，消息体则放在 `RemotingCommand.body` 中。

`MQClientAPIImpl.sendMessage` 会把它封装为 `SEND_MESSAGE` 或
`SEND_MESSAGE_V2` 请求；批量消息使用 `SEND_BATCH_MESSAGE`。随后根据同步、异步或
单向发送模式，通过 Netty 客户端把请求发到目标 Broker。

请求组装位置：
[MQClientAPIImpl.java](../client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java)。

### 5.2 Broker 把请求分发给 `SendMessageProcessor`

Broker 启动时，`BrokerController.registerProcessor()` 建立请求码和处理器的映射：

```text
SEND_MESSAGE       -> SendMessageProcessor
SEND_MESSAGE_V2    -> SendMessageProcessor
SEND_BATCH_MESSAGE -> SendMessageProcessor
```

这些请求由 `sendMessageExecutor` 线程池执行。注册位置：
[BrokerController.java](../broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java)。

因此，普通发送请求到达 Broker 后，核心入口是：

```java
SendMessageProcessor.processRequest(...)
```

处理器源码：
[SendMessageProcessor.java](../broker/src/main/java/org/apache/rocketmq/broker/processor/SendMessageProcessor.java)。

### 5.3 `SendMessageProcessor` 怎么处理

以普通单条消息为例，主要步骤是：

1. 解析 `SendMessageRequestHeader`，处理静态 Topic 映射并执行发送前 Hook。
2. `preSend` 和 `msgCheck` 检查 Broker 是否可写、Topic 名称和权限、Topic 配置以及
   Queue ID 是否合法。
3. Topic 不存在时，尝试根据请求中的默认 Topic 创建它。这正是前文 `TBW102`
   兜底路由在 Broker 端对应的逻辑；自动创建未开启或创建失败时返回
   `TOPIC_NOT_EXIST`。
4. 把请求转换为 Broker 内部对象 `MessageExtBrokerInner`，设置消息体、属性、
   Queue ID、Tag 哈希、客户端地址、存储地址、重试次数和唯一 ID 等字段。
5. 普通消息调用 `MessageStore.asyncPutMessage`；批量、事务、延迟和重试消息会进入
   各自的分支，但最终仍要进入相应的存储路径。
6. 根据存储结果生成响应，成功响应中包含 `msgId`、`queueId` 和 `queueOffset`，再
   返回 Producer。

### 5.4 消息怎么存下来

默认存储实现是：
[DefaultMessageStore.java](../store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java)。

它把普通消息继续交给：
[CommitLog.java](../store/src/main/java/org/apache/rocketmq/store/CommitLog.java)。

`CommitLog.asyncPutMessage` 会为消息分配队列偏移量、计算 CRC、编码消息，然后在写锁
内把消息顺序追加到当前 `MappedFile`。追加完成后，再按照 Broker 配置处理刷盘和
主从复制确认：

- 异步刷盘不要求数据已经同步落到磁盘才返回。
- 同步刷盘需要等待刷盘结果。
- `SYNC_MASTER` 等需要复制确认的模式还会等待相应 Slave ACK。

Broker 根据这些结果返回 `PUT_OK`、`FLUSH_DISK_TIMEOUT`、
`FLUSH_SLAVE_TIMEOUT` 等状态。Producer 收到成功响应，只表示 Broker 已按当前刷盘和
复制配置完成了发送确认，**不表示 Consumer 已经执行完业务逻辑**。

消息正文保存在 CommitLog。后台 `ReputMessageService` 会继续扫描 CommitLog，通过
Dispatcher 为消息建立 ConsumeQueue 和 IndexFile 等索引。ConsumeQueue 主要保存
物理偏移、消息大小和 Tag 等信息，Consumer 拉取时先查它，再根据物理位置读取
CommitLog 中的消息正文。

## 6. Consumer 在哪里处理业务消息

Broker 不会直接调用业务代码。以常见的 `DefaultMQPushConsumer` 并发消费为例，
“Push” 的实现仍然是客户端长轮询拉取：

```text
Consumer 发起 PULL_MESSAGE
  -> Broker 的 PullMessageProcessor
  -> DefaultMessageStore.getMessageAsync
  -> 从 ConsumeQueue 定位 CommitLog 消息
  -> 消息返回 Consumer
  -> DefaultMQPushConsumerImpl.pullMessage
  -> ConsumeMessageConcurrentlyService.submitConsumeRequest
  -> 用户的 MessageListenerConcurrently.consumeMessage
```

Broker 的拉取入口是：
[PullMessageProcessor.java](../broker/src/main/java/org/apache/rocketmq/broker/processor/PullMessageProcessor.java)。

客户端收到消息后，`DefaultMQPushConsumerImpl` 先把消息放入本地 `ProcessQueue`，再
提交给消费线程池：
[DefaultMQPushConsumerImpl.java](../client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java)。

并发消费线程最终执行用户注册的：

```java
listener.consumeMessage(messages, context);
```

具体位置：
[ConsumeMessageConcurrentlyService.java](../client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageConcurrentlyService.java)。

如果监听器返回 `CONSUME_SUCCESS`，客户端推进消费位点；返回
`RECONSUME_LATER` 或抛出异常时，集群消费模式下会把消息发回 Broker 进入重试流程，
超过最大重试次数后进入死信 Topic。顺序消费使用
`ConsumeMessageOrderlyService`，但“Broker 提供消息、Consumer 客户端执行监听器”
这个边界不变。

## 7. 完整链路

```text
Producer
  -> 查询 Topic 路由
  -> 选择 MessageQueue
  -> 向对应 Broker 发送 SEND_MESSAGE 请求

Broker
  -> SendMessageProcessor 校验并构造内部消息
  -> DefaultMessageStore / CommitLog 持久化
  -> 按刷盘和复制策略返回发送结果
  -> 后台构建 ConsumeQueue 等索引

Consumer
  -> 从 Broker 拉取消息
  -> 提交到客户端消费线程池
  -> 调用用户 MessageListener
  -> 成功则推进位点，失败则进入重试流程
```
