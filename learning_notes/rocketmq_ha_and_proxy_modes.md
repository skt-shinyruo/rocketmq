# RocketMQ DLedger、Controller 与 Proxy 模式

这三种“模式”不在同一个层面：

| 模式 | 解决的问题 | 所在层次 |
| --- | --- | --- |
| DLedger | Broker 数据如何复制、Master 故障后如何选新 Leader | 存储与复制 |
| Controller | 谁来决定哪个 Broker 是 Master，并自动切换角色 | Broker 高可用控制 |
| Proxy | 客户端如何访问 RocketMQ | 接入层 |

## 一、DLedger 模式

DLedger 相当于把一个 Broker 主从组改造成 Raft 复制组：

```text
Broker A (Leader)
Broker B (Follower)
Broker C (Follower)
```

消息写入 Leader，并复制到多数节点后才算成功。Leader 故障后，剩余节点通过 Raft 自动选举新 Leader。

特点：

- Broker 没有永久固定的 Master/Slave 身份。
- 通常需要至少 3 个节点，以保证多数派。
- Raft 同时负责数据复制和 Leader 选举。
- 一致性较强，但多数派写入会增加延迟。
- 它改变了 CommitLog 的实现和复制链路，侵入存储层较深。

可以理解为：**让 Broker 数据本身成为一个 Raft 日志。**

## 二、Controller 模式

Controller 不接管 CommitLog，而是管理传统 Broker 副本组的角色：

```text
Controller 集群
      |
      | 选举/切换 Master
      v
Broker A (Master)  <->  Broker B/C (Slave)
```

当 Master 故障时，Controller 根据副本状态选择合适的 Slave，将其原地提升为 Master，不需要重启 Broker。

特点：

- Broker 仍使用 RocketMQ 自身的 CommitLog 和主从复制机制。
- Controller 负责故障检测、Master 选举和元数据管理。
- Controller 自身通过共识协议保证高可用。
- Broker 的 Master/Slave 角色可以动态变化。
- 相比 DLedger，存储机制变化较小，也更适合 RocketMQ 5.x 的副本治理。

可以理解为：**数据仍由 Broker 复制，Controller 只负责决定谁当 Master。**

DLedger 与 Controller 是两套不同的 Broker 高可用方案，**且互斥、不能叠加使用**：
`BrokerStartup` 启动时检查到 `enableControllerMode` 与 `enableDLegerCommitLog`
同时为 true 会直接退出（`System.exit(-4)`）：

```text
DLedger：    Raft 直接管理数据复制和 Leader
Controller：Controller 管理 Master，Broker 自己复制数据
```

## 三、Proxy 模式

Proxy 与前两者正交。它不决定 Broker 如何选主，也不负责消息持久化，而是客户端和 Broker 之间的接入层：

```text
gRPC / Remoting 客户端
          |
        Proxy
          |
    NameServer + Broker
```

它主要提供：

- RocketMQ 5.x gRPC API。
- 传统 Remoting 协议接入。
- 路由、认证、协议转换和请求转发。
- 隐藏内部 Broker 地址，更适合 Kubernetes、负载均衡和公网接入。

两种部署方式：

- `LOCAL`：Proxy 和 Broker 在同一 JVM，部署简单、少一次网络转发，但生命周期与 Broker 绑定。
- `CLUSTER`：Proxy 独立部署，可以无状态水平扩容，更适合生产和云原生环境。

## 四、组合关系

典型结构是：

```text
客户端
  |
Proxy（可选：LOCAL 或 CLUSTER）
  |
Broker 高可用方案
  +-- DLedger
  或
  +-- Controller
```

因此：

- **DLedger 和 Controller 是 Broker 高可用方案。**
- **Proxy 是客户端接入方案。**
- 可以使用“Proxy + DLedger”或“Proxy + Controller”。
- 不能把三者看成三个相互替代的 RocketMQ 运行模式。
