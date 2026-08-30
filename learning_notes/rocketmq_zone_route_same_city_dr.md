# RocketMQ 同城灾备：机房闭环路由（Zone Mode）

同城灾备模式下，Name Server 和 Broker 都是跨机房部署的，对生产者和消费者而言在逻辑上
是同一套大集群。由于客户端选择路由时是"无差别随机"的，大约 50% 的读写请求会跨机房访问。
问题：能否在**维持相同可用性**的前提下，让正常情况下读写请求尽可能闭环在本机房？

答案是可以。RocketMQ 5.x 内置了 **Zone 感知路由（机房模式）**：Broker 注册时上报所在
机房，客户端声明自己所在的机房，NameServer 在下发 Topic 路由时把远端机房的 Broker 过滤
掉，读写自然闭环在本机房。可用性则靠"数据副本跨机房 + 故障时路由规则自动破坏"兜底。

本文基于当前仓库 `learning` 分支源码整理。

## 一、问题本质

路由信息本身是无差别的：客户端从 NameServer 拿到的 `TopicRouteData` 包含集群里所有
Broker 的所有队列，Producer 随机选队列发送，Consumer Rebalance 时把所有队列（含远端
机房）都纳入分配。Name Server 侧客户端也是随机挑一台连接。因此"逻辑上同一套大集群"
必然带来约一半的跨机房流量。

要闭环，就需要让"机房"成为路由可见性的维度，这正是 Zone Mode 做的事。

## 二、Zone Mode 的三个环节（源码对应）

### 1. Broker 上报机房

Broker 配置 `MessageStoreConfig` 的 `zoneName` 后，注册时把机房名带给 NameServer，
`RouteInfoManager#registerBroker` 会将其写入 `BrokerData.zoneName`：

- `store/src/main/java/org/apache/rocketmq/store/config/MessageStoreConfig.java`（`zoneName` 字段）
- `namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java`（注册时 `brokerData.setZoneName(zoneName)`）
- `remoting/src/main/java/org/apache/rocketmq/remoting/protocol/route/BrokerData.java:40`（路由协议中新增的 `zoneName` 字段）

### 2. 客户端声明机房

客户端通过系统属性或环境变量声明：

```text
-Drocketmq.zone.mode=true
-Drocketmq.zone.name=机房A
```

对应常量定义在 `common/src/main/java/org/apache/rocketmq/common/MixAll.java`：

- `ROCKETMQ_ZONE_MODE_ENV` / `ROCKETMQ_ZONE_MODE_PROPERTY`（`rocketmq.zone.mode`）
- `ROCKETMQ_ZONE_ENV` / `ROCKETMQ_ZONE_PROPERTY`（`rocketmq.zone.name`）
- 请求扩展字段 `__ZONE_MODE` / `__ZONE_NAME`

`remoting/src/main/java/org/apache/rocketmq/remoting/rpchook/DynamicalExtFieldRPCHook.java:28`
会给客户端发出的**每一个请求**附加这两个扩展字段。

### 3. NameServer 过滤路由

`namesrv/src/main/java/org/apache/rocketmq/namesrv/route/ZoneRouteRPCHook.java` 拦截
`GET_ROUTEINFO_BY_TOPIC` 的响应：若请求带 zone mode，则从 `TopicRouteData` 中剔除
`zoneName` 不等于客户端机房的 Broker（对应的 `BrokerData` 和 `QueueData` 一并移除，
同时清理 `FilterServerTable`）。

过滤后的效果：

- **写闭环**：Producer 拿到的路由只剩本机房 Master，消息全部落本机房；对端机房通过
  Slave 复制同步数据。
- **读闭环**：Consumer 的 Rebalance 与拉取都基于过滤后的路由，只消费本机房 Master 上的
  队列（远端机房的同名 Topic 队列由对端消费者负责）。同一个队列只出现在一个机房的视图里，
  两个机房视图天然互斥，不会重复消费；Offset 也都上报给本机房 Master。
- **NameServer 闭环**：客户端固定连本机房 NameServer（通过 VIP/DNS 指向本机房）。

## 三、部署前提：对称 + 副本跨机房

Zone 过滤只是"路由可见性"层面的闭环，要闭环后依然"够用"、且故障时不丢可用性，部署上
必须满足：

1. **两个机房各自部署完整 Name Server**。NameServer 无状态，Broker 向所有 NameServer
   注册，所以客户端只连本机房 NameServer 不影响任何可用性。
2. **Broker 分组对称部署**：机房 A、B 各有一批 Broker 组的 Master（如 broker-a 组
   Master 在 A、broker-b 组 Master 在 B），Topic 的读写队列分散在两个机房的 Master 上。
   这样每个机房都有全量 Topic 队列，过滤后依然可用。
3. **每个组的 Slave/副本放在对端机房**（主从交叉，或 DLedger 三副本 1+1+1），保证任一
   机房挂掉后另一机房仍有全量数据。

## 四、为什么可用性不降级

关键在 `ZoneRouteRPCHook#filterByZoneName` 里的一个"例外"（源码注释：
`master down, consume from slave. break nearby route rule.`）：

- **某组 Master 挂了**：`BrokerAddrs` 里没有 MASTER_ID 的组**不做剔除**，整组（含对端
  机房 Slave 地址）保留在路由里。本机房消费者可以直接从对端机房的 Slave 拉数据（故障
  时刻暂时跨机房，可用性优先于闭环）；Producer 自动切到本机房其他存活的组。
- **整个机房挂了**：幸存机房的客户端本来就看自己机房的路由，照常读写；原机房的应用随
  双活切换迁到幸存机房后，`rocketmq.zone.name` 跟随部署单元变化，自动拿到新机房的路由。
  数据不丢的前提就是"副本跨机房"。
- **NameServer 挂了**：机房内部多副本即可，与普通集群无异。

一句话总结：**NameServer 用"本地接入"闭环，读写路由用 Zone 过滤闭环，可用性用
"跨机房副本 + Master 故障时路由规则自动破坏"保底。**

## 五、需要注意的边界

- Zone 过滤是**硬过滤**，没有"本机房无可用 Broker 就回退全量路由"的逻辑。机房级故障时
  必须配合应用层切流（应用实例连同 `rocketmq.zone.name` 一起迁到幸存机房），而不是指望
  RocketMQ 单方面回退。
- 主从自动切换（Controller 模式）触发后，新 Master 可能落在对端机房，该组的 `zoneName`
  会随注册更新，闭环暂时被打破——这是预期行为：故障期间优先可用性，恢复后闭环重建。
- 顺序消息、事务消息等绑定单组 Broker 的特性，在机房级故障期间会受影响，需要业务侧评估。

## 相关笔记

- [RocketMQ Broker 路由注册机制](rocketmq_broker_route_registration.md)
- [RocketMQ DLedger、Controller 与 Proxy 模式](rocketmq_ha_and_proxy_modes.md)
- [RocketMQ Read Queue 与 Write Queue](rocketmq_read_write_queue.md)
