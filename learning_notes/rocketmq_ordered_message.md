# RocketMQ 顺序消息：生产投递与因果顺序

顺序消息要求生产和消费两边同时满足约束。消费侧使用 `MessageListenerOrderly` 对
单个队列串行回调；生产侧必须把同一业务键投递到同一个 `MessageQueue`，并且同一
键上的多条消息按业务因果顺序、成功后再发下一条。

本文说明分区有序如何落地，以及订单「创建 → 支付 → 发货」应怎样发送。

相关笔记：

- [Producer MessageQueue 选择逻辑](producer-message-queue-selection.md) —— 默认轮询与
  `MessageQueueSelector`
- [RocketMQ DefaultMQPushConsumer Pull 消费流程分析](consumer_flow_analysis.md) ——
  顺序消费锁与本地串行回调
- [RocketMQ 静态主题（Static Topic / Logic Queue）](rocketmq_static_topic_logic_queue.md)
  —— `hash(key) mod N` 在扩容时为何会打乱顺序

官方样例：`example/src/main/java/org/apache/rocketmq/example/ordermessage/`，
文档说明见 `docs/cn/RocketMQ_Example.md` 第 2 节。

## 1. RocketMQ 保证什么、不保证什么

默认 `producer.send(msg)` 按 Round Robin 把消息打到不同队列；消费端从多个队列
并行拉取。这种路径**不保证**同一业务实体的多条消息有序，也不保证全局 FIFO。

若控制发送，使同一 Sharding Key 的消息依次进入**同一个**队列，消费时只对该队列
按 `queueOffset` 串行处理，则该队列内有序。这是**分区有序（相对有序）**。

当 Topic 实际只使用一个队列，且 Producer 对消息按同一条业务序列串行发送、Consumer
也只由一个实例串行处理时，才可以讨论**全局有序**。仅把队列数设为 1 并不能约束多个
Producer 的并发提交顺序；Broker 只能保证最终到达该队列的 `queueOffset` 顺序。全局有序
吞吐差，订单场景几乎都用分区有序。

对应关系：

```text
同一 orderId
  → MessageQueueSelector 固定同一个 MessageQueue
  → Broker 在该队列上递增 queueOffset
  → MessageListenerOrderly 按 offset 串行回调
```

## 2. 生产侧：如何投递到同一队列

不要用普通 `send(msg)`。使用带 `MessageQueueSelector` 的重载，第三个参数是分片键
（如 `orderId`）。选择器接口只有一个方法，定义在
`client/src/main/java/org/apache/rocketmq/client/producer/MessageQueueSelector.java:23`：

```java
MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
```

```java
MessageQueueSelector selector = (mqs, msg, arg) -> {
    String orderId = (String) arg;
    return mqs.get(Math.floorMod(orderId.hashCode(), mqs.size()));
};

String orderId = "ORDER-1001";
for (String event : new String[] {"CREATE", "PAY", "SHIP"}) {
    Message msg = new Message(
        "OrderTopic",
        event, // Tag 只用于过滤，不负责路由
        orderId,
        event.getBytes(StandardCharsets.UTF_8)
    );
    SendResult result = producer.send(msg, selector, orderId);
    if (result.getSendStatus() != SendStatus.SEND_OK) {
        throw new IllegalStateException("send failed: " + result.getSendStatus());
    }
}
```

也可以使用内置 `SelectMessageQueueByHash`，算法是 `arg.hashCode() % mqs.size()`
（负值取绝对值）。源码：
`client/src/main/java/org/apache/rocketmq/client/producer/selector/SelectMessageQueueByHash.java`。

客户端仍通过 `tryToFindTopicPublishInfo` 取得可写队列列表，但最终选哪个队列由
selector 决定，不再走默认轮询。实现入口：
`DefaultMQProducerImpl.sendSelectImpl`
（`client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java:1329`）。

分片键必须是订单号这类稳定业务键，不能用「创建 / 支付 / 发货」的 Tag 或消息类型。
Tag 只用于过滤，不决定队列。

selector 本身要写成**无状态、确定性**的函数：同一 `arg` 在同一份 `mqs` 上必须算出
同一个结果，不能引入随机数、时间、计数器等可变状态，否则同一订单会被打散到不同
队列。

## 3. 订单三步怎样投递

假设 Topic 有 4 个写队列，三个订单的事件在时间上交错（真实系统也是如此）：

| 发送顺序 | orderId | 事件 | 落到的队列（示意） |
| --- | --- | --- | --- |
| 1 | A | 创建 | `hash(A) % 4` → 例如 Q2 |
| 2 | B | 创建 | `hash(B) % 4` → 例如 Q0 |
| 3 | A | 支付 | 仍是 Q2 |
| 4 | A | 发货 | 仍是 Q2 |
| 5 | B | 支付 | 仍是 Q0 |

要点：

1. 同一 `orderId` 的三条消息必须映射到同一队列。
2. 同一 `orderId` 必须按创建 → 支付 → 发货的**因果顺序**发送；前一条至少拿到
   `SEND_OK` 再发下一条。
3. 不同订单可以并行发送。它们进入不同队列时互不影响；进入同一队列时只保证各自
   内部有序，队列里会交错。
4. 消费端必须注册 `MessageListenerOrderly`。只固定生产队列、仍用并发监听器，同一
   队列仍可能被本地线程池并行处理。

官方示例用 `orderId % mqs.size()` 选队列，模拟数据里同一订单的「创建、付款、推送、
完成」交错出现在列表中，但相同 `orderId` 仍进入同一队列。

## 4. 同一订单的因果顺序怎么保证

「不要对同一订单异步乱序并发发送」指的是：**同一 `orderId` 上的发送必须串行**。
后一条只能在前一条已经成功进入选定队列之后再发。

`SendStatus` 定义在
`client/src/main/java/org/apache/rocketmq/client/producer/SendStatus.java`：
`SEND_OK`、`FLUSH_DISK_TIMEOUT`、`FLUSH_SLAVE_TIMEOUT`、`SLAVE_NOT_AVAILABLE`。
顺序场景建议只有 `SEND_OK` 才视为成功并继续发下一条；刷盘/副本超时按失败处理，
重试时仍用同一 selector 和同一 `orderId`，不要换队列。

### 4.1 业务事件本身就是串行的（最常见）

创建、支付、发货通常发生在不同时刻，甚至不同服务：

```text
下单成功 → 同步 send(创建) → 接口才返回
用户支付成功 → 同步 send(支付)
仓库发货成功 → 同步 send(发货)
```

支付流程不会在「创建消息尚未发出」时被调用。需要保证的是：当前步骤的消息没发成功，
就不要提交本步骤、更不要进入下一步。

```java
SendResult r = producer.send(msg, selector, orderId);
if (r.getSendStatus() != SendStatus.SEND_OK) {
    throw new RuntimeException("send failed: " + r.getSendStatus());
}
```

同步 `send` 阻塞到 Broker 返回。`SEND_OK` 表示消息已进入 selector 选出的那个队列。

多服务时，顺序仍靠**业务状态机**：只有订单已是「已创建」才允许发支付消息。不需要
三个服务再加一把分布式锁来排队发送，除非它们会并发写出同一订单的多个事件。

### 4.2 同一处连发三条：同步循环

内存里已经备好创建、付款、发货时，对同一个 `orderId` 用同步循环，不要三次
`send` 后不管结果：

```java
String[] steps = {"CREATE", "PAY", "SHIP"};
for (String step : steps) {
    Message msg = new Message("OrderTopic", step, orderId, body(step));
    SendResult r = producer.send(msg, selector, orderId);
    if (r.getSendStatus() != SendStatus.SEND_OK) {
        throw new IllegalStateException("stop, do not send later steps");
    }
}
```

循环本身就是「前一条成功才进入下一次」。不同 `orderId` 可以并行。

### 4.3 必须异步发送时：按 orderId 串行

错误做法：同一订单三次 `send(..., callback)` 同时发出。网络先到后发，队列顺序会反。

正确做法：按 `orderId` 排队，上一条 callback 成功后再发下一条。不同订单仍可并行。

```java
ConcurrentHashMap<String, CompletableFuture<Void>> tails = new ConcurrentHashMap<>();

void sendOrderly(String orderId, Message msg) {
    tails.compute(orderId, (id, prev) -> {
        CompletableFuture<Void> predecessor = prev == null
            ? CompletableFuture.completedFuture(null) : prev;
        return predecessor.thenCompose(v -> {
            CompletableFuture<Void> done = new CompletableFuture<>();
            try {
                producer.send(msg, selector, orderId, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult r) {
                        if (r.getSendStatus() == SendStatus.SEND_OK) {
                            done.complete(null);
                        } else {
                            done.completeExceptionally(new IllegalStateException(r.toString()));
                        }
                    }
                    @Override
                    public void onException(Throwable e) {
                        done.completeExceptionally(e);
                    }
                });
            } catch (Exception e) {
                done.completeExceptionally(e);
            }
            return done;
        });
    });
}
```

要点：

- 锁或队列的粒度是 `orderId`，不要全局一把锁。
- 支付消息必须排在该订单创建消息之后，不能另起线程直接 `send`。
- 异步失败要打断后续步骤。

也可以按 `hash(orderId) % N` 把同一订单固定到同一条单线程 worker。

### 4.4 不要依赖的做法

| 做法 | 结果 |
| --- | --- |
| 普通异步连发三次 | 到达顺序无保证 |
| 发完创建不等结果就发支付 | 可能支付先入队 |
| 发送失败换队列重试 | 顺序断在两个队列上 |
| 三个服务并发发，只靠「业务上应该先创建」 | 重试和重复请求会乱序 |

## 5. 生产与消费的其它约束

- **同步 selector 发送更稳妥。** `sendSelectImpl` 选定队列后走 `sendKernelImpl`，
  不会像默认发送那样失败后换 Broker / 换队列。业务自行重试时必须仍用同一个
  selector 和同一个 `orderId`。
- **队列数不要随便改。** 映射是 `hash(key) % N`，N 变了同一订单会换队列，新旧消息
  可能分到两个队列。扩容仍要保序时，需要 Static Topic 这类逻辑队列数不变的方案。
- **不要把延时 / 定时消息和顺序因果链混在一起。** 延时消息会先入定时存储再投递，
  到达真实队列的时刻与发送时刻不一致。
- 消费侧集群顺序消费通过 Broker `lockBatchMQ` 维持队列归属，本地用
  `MessageQueueLock` 串行调用监听器。它不提供 exactly-once，业务仍需处理重复消费。
  发送重试导致的重复、以及与 Kafka 幂等 Producer 的差异见
  [`rocketmq_send_retry_and_idempotency.md`](rocketmq_send_retry_and_idempotency.md)。

落地建议：订单事件用同步 `producer.send(msg, selector, orderId)`，在业务状态提交
成功的那一步发送，失败则本步骤失败。只有同一进程要对同一订单连续发多条、又要打满
吞吐时，才使用按 `orderId` 串行的异步链。
