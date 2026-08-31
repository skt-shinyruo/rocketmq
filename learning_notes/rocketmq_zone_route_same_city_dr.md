# RocketMQ 同城灾备：机房闭环路由（Zone Mode）

同城多机房部署中，Producer 和 Consumer 默认能看到所有 Broker，正常流量可能跨机房。
RocketMQ 5.x 的 Zone Route 可以在 NameServer 返回路由时过滤其他 Zone，但它只解决
**路由可见性**，不会自动完成副本选主、Consumer 跨机房分配或机房故障切流。因此它能减少
跨机房访问，不能单独保证“可用性完全不变”。

本文基于当前仓库 `learning` 分支源码整理。

## 一、Zone Mode 的三个环节

### 1. Broker 上报复制组所属 Zone

Broker 的 `BrokerOuterAPI` 使用 `DynamicalExtFieldRPCHook`，从 JVM 系统属性或环境变量读取
Zone，并在注册请求中加入 `__ZONE_NAME`：

```text
# JVM 参数
-Drocketmq.zone=zone-a

# 或环境变量
ROCKETMQ_ZONE=zone-a
```

对应常量是 `MixAll.ROCKETMQ_ZONE_PROPERTY`（`rocketmq.zone`）和
`MixAll.ROCKETMQ_ZONE_ENV`（`ROCKETMQ_ZONE`）。`MessageStoreConfig` 中没有 `zoneName`
配置项。

NameServer 收到注册请求后，把 Zone 写入 `BrokerData.zoneName`。需要特别注意：

- `BrokerData` 对应整个 `brokerName` 复制组，不对应组内单个 Master/Slave；
- 一个复制组只有一个 `zoneName`；
- 同组任一实例注册都会覆盖这个字段。

因此同一个 `brokerName` 下的所有实例必须上报**相同的逻辑 Zone**。即使 Slave 物理部署
在另一机房，也不能让它上报不同 Zone，否则该组的 Zone 会随 Master/Slave 注册反复变化。
当前实现不能表达“同一复制组内每个副本各自所在的物理机房”。

相关实现：

- `remoting/src/main/java/org/apache/rocketmq/remoting/rpchook/DynamicalExtFieldRPCHook.java`
- `namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java`
- `remoting/src/main/java/org/apache/rocketmq/remoting/protocol/route/BrokerData.java`

### 2. 客户端声明 Zone 并开启过滤

需要过滤路由的客户端同时配置：

```text
-Drocketmq.zone.mode=true
-Drocketmq.zone=zone-a
```

或者：

```text
ROCKETMQ_ZONE_MODE=true
ROCKETMQ_ZONE=zone-a
```

Hook 会把 `__ZONE_MODE` 和 `__ZONE_NAME` 附加到请求；NameServer 只在处理
`GET_ROUTEINFO_BY_TOPIC` 的成功响应时应用 Zone 过滤。

### 3. NameServer 过滤复制组

`ZoneRouteRPCHook#filterByZoneName` 按 `BrokerData` 过滤整个复制组：

- 有 Master 且组 Zone 不匹配：删除该组的 `BrokerData`、`QueueData` 和 FilterServer 路由；
- 组 Zone 匹配：保留；
- 组内已没有 `brokerId=0`：无论 Zone 是否匹配都保留，允许故障时从 Slave 读取。

所以过滤后的本机房只包含属于本 Zone 的 Broker 组及其队列子集，并不是 Topic 的全部逻辑
队列。Producer 可以据此只向本 Zone 的 Master 发送。

## 二、推荐部署边界

### 1. Producer 就近写

可以在两个机房分别部署不同 `brokerName` 的复制组，并给每个组设置固定逻辑 Zone：

```text
broker-a 组（所有成员均上报 zone-a）
broker-b 组（所有成员均上报 zone-b）
```

机房 A 的 Producer 配置 `zone-a`，机房 B 的 Producer 配置 `zone-b`。每个客户端只会看到
本 Zone 的可写队列。Topic 必须在两个 Zone 都有可写 Broker 组，否则某个 Zone 可能得到空的
发布路由。

副本可以跨机房部署以提高数据冗余，但该副本仍应上报复制组的固定逻辑 Zone。副本跨机房
只提供数据副本，不代表 Master 故障后一定能继续写；经典 Master-Slave 没有 Master 时只能
读，恢复写入还需要 Controller/DLedger 选主或应用切流到其他可写组。

### 2. Consumer 就近读

不要让同一个 Consumer Group 中位于不同机房的客户端分别开启不同 Zone 过滤。默认 Rebalance
假设组内所有客户端看到相同的 `mqAll` 和 `cidAll`；如果两边拿到互斥的队列集合，部分队列
可能被计算给另一机房的 clientId，最终无人消费。

跨机房部署同一个 Consumer Group 时，更稳妥的做法是：

- Consumer 不开启 Zone Route，保证组内所有实例看到完整且一致的 Topic 路由；
- 使用 `AllocateMachineRoomNearby`，通过 `MachineRoomResolver` 同时识别 Broker 和 Consumer
  的机房，在同一份完整路由上优先分配本机房队列；
- 某机房没有存活 Consumer 时，该策略会把对应队列分给其他机房实例。

如果所有 Consumer 实例都固定在同一 Zone，才可以直接用 Zone Route 限制它们看到的队列，
但必须另行保证被过滤的其他队列有消费者处理。

## 三、故障语义

- **Master 消失**：Zone Hook 会保留这个无 Master 的复制组，Consumer 可能跨 Zone 从 Slave
  读取；Producer 不能因此向 Slave 写入。
- **Controller/DLedger 完成选主**：新 Master 注册后，路由按该复制组固定的逻辑 Zone 再次
  过滤。Zone 本身不负责选主。
- **整个机房故障**：Zone Route 没有“本 Zone 无 Broker 时自动回退全量路由”的逻辑。
  应用必须切换到幸存机房，并同步修改或移除 Zone 配置。
- **NameServer 故障**：客户端需要配置多个可用 NameServer 地址；Zone Mode 不提供额外的
  NameServer 高可用机制。

因此可用性来自副本策略、自动选主和应用切流，Zone Route 只负责正常情况下的路由过滤。

## 相关笔记

- [RocketMQ Broker 路由注册机制](rocketmq_broker_route_registration.md)
- [RocketMQ DLedger、Controller 与 Proxy 模式](rocketmq_ha_and_proxy_modes.md)
- [RocketMQ Read Queue 与 Write Queue](rocketmq_read_write_queue.md)
