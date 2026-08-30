# RocketMQ Read Queue 与 Write Queue

## 一句话结论

read queue / write queue 不是两套物理队列，而是同一个 Topic 在路由元数据里的两个独立"计数器"——**write 队列数决定生产者能往哪些队列发，read 队列数决定消费者能从哪些队列消费**。拆开之后，就可以独立地调整"收消息的能力"和"发消息的能力"。

## 代码中的定义

源码位置：`common/src/main/java/org/apache/rocketmq/common/TopicConfig.java`

```java
public class TopicConfig {
    public static int defaultReadQueueNums = 16;
    public static int defaultWriteQueueNums = 16;

    private int readQueueNums;   // 消费者可见的队列数
    private int writeQueueNums;  // 生产者可见的队列数
}
```

NameServer 下发路由时，每个 Broker 对应一条 `QueueData`，里面分别携带 read / write 两个队列数。客户端拿到路由后的用法（`client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java`）：

```java
// 生产者路由：TopicPublishInfo 按 writeQueueNums 构建可发送队列
for (int i = 0; i < qd.getWriteQueueNums(); i++) {
    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
    info.getMessageQueueList().add(mq);
}

// 消费者订阅信息：按 readQueueNums 构建，且要求 perm 可读
for (int i = 0; i < qd.getReadQueueNums(); i++) {
    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
    mqList.add(mq);
}
```

- **发送端**：Producer 做负载均衡、选队列发消息，只在 write 队列范围内选。
- **消费端**：Consumer 做 Rebalance 分配队列，只在 read 队列范围内分。

## 本质：同一套存储的两个视角

底层存储（CommitLog / ConsumeQueue）只有一套，queueId 0~N 是共享的。read/write queue nums 只是客户端视角的"可见范围"，不影响 Broker 实际建多少个 ConsumeQueue 文件：

```mermaid
graph LR
    subgraph 路由元数据 QueueData
        W["writeQueueNums = 8"]
        R["readQueueNums = 8"]
    end
    W -->|"Producer 可见 queueId 0~7"| P[Producer 路由]
    R -->|"Consumer 可见 queueId 0~7"| C[Consumer Rebalance]
    P --> Q["Broker 物理队列 queueId 0~7（一套）"]
    C --> Q
```

## 为什么要区分

核心诉求：**生产能力和消费能力的变化频率、原因完全不同，希望能独立调整而互不影响**。典型场景：

### 1. 优雅下线 Broker（最常用）

要把某台 Broker 摘掉时，先把它的 write 队列数改成 0（或把 perm 改成只读）：

- Producer 立刻不再往它发新消息；
- Consumer 还能继续把它上面积压的消息消费完；
- 等积压清零，再把 read 队列数改成 0，最后下线。

如果读写不分离，这一步就没法平滑做——要么丢消息，要么消息卡死。

### 2. 独立扩缩容

- 想提升消费吞吐：加消费者、加 read 队列数让 Rebalance 分得更开，不影响生产链路；
- 想临时限制生产端：缩小 write 队列数即可。

两个方向的操作互不干扰。

### 3. 配合 perm 使用

`perm`（`PermName.PERM_READ = 4`，`PERM_WRITE = 2`）是开关，read/write queue nums 是刻度：

- perm 决定"能不能"；
- queue nums 决定"能多少"。

例如 perm = 只读时，write 队列数再多 Producer 也不可见。

## 一个坑

两个数指同一个 queueId 空间，**正常情况必须相等**。如果把 readQueueNums 改小而 writeQueueNums 不动：

- Producer 仍然会往 queueId ≥ readQueueNums 的队列写消息；
- Consumer 永远看不到这些队列；
- 消息会无限积压且无法消费。

所以运维上改这两个数要成对操作，只在上面说的过渡场景（下线 Broker、灰度迁移）里才故意让它们短暂不一致。

## 相关笔记

- [MessageQueue 概念详解](messagequeue-concept.md) —— 队列的逻辑标识本身
- [RocketMQ Broker 路由注册机制](rocketmq_broker_route_registration.md) —— QueueData / TopicConfig 如何注册到 NameServer
- [Producer MessageQueue 选择逻辑](producer-message-queue-selection.md) —— Producer 如何在 write 队列中做选择
- [RocketMQ DefaultMQPushConsumer Pull 消费流程分析](consumer_flow_analysis.md) —— Consumer 如何在 read 队列上消费
