# RocketMQ 消息存储模型详解

## 目录

- [一、存储架构概述](#一存储架构概述)
  - [1.1 整体架构](#11-整体架构)
  - [1.2 核心组件职责](#12-核心组件职责)
- [二、核心存储文件详解](#二核心存储文件详解)
  - [2.1 CommitLog](#21-commitlog)
  - [2.2 ConsumeQueue](#22-consumequeue)
  - [2.3 IndexFile](#23-indexfile)
  - [2.4 TimerLog（延时消息存储）](#24-timerlog延时消息存储)
- [三、关键存储服务](#三关键存储服务)
  - [3.1 ReputMessageService](#31-reputmessageservice)
  - [3.2 FlushManager](#32-flushmanager)
  - [3.3 AllocateMappedFileService](#33-allocatemappedfileservice)
  - [3.4 TransientStorePool](#34-transientstorepool)
  - [3.5 CleanCommitLogService](#35-cleancommitlogservice)
  - [3.6 StoreCheckpoint](#36-storecheckpoint)
- [四、消息写入流程](#四消息写入流程)
  - [4.1 完整写入流程](#41-完整写入流程)
  - [4.2 写入核心步骤](#42-写入核心步骤)
- [五、消息读取流程](#五消息读取流程)
  - [5.1 消费读取流程](#51-消费读取流程)
  - [5.2 Key 查询流程](#52-key-查询流程)
  - [5.3 MessageId 查询流程](#53-messageid-查询流程)
- [六、高可用与数据可靠性](#六高可用与数据可靠性)
  - [6.1 Master/Slave 架构](#61-masterslave-架构)
  - [6.2 HA 同步机制](#62-ha-同步机制)
  - [6.3 数据完整性保障](#63-数据完整性保障)
- [七、存储优化机制](#七存储优化机制)
  - [7.1 PageCache 与 Mmap 优化](#71-pagecache-与-mmap-优化)
  - [7.2 冷热数据分离](#72-冷热数据分离)
  - [7.3 多路径存储](#73-多路径存储)
  - [7.4 RocksDB 存储引擎](#74-rocksdb-存储引擎)
- [八、存储恢复流程](#八存储恢复流程)
  - [8.1 启动恢复流程](#81-启动恢复流程)
  - [8.2 恢复核心逻辑](#82-恢复核心逻辑)
- [九、典型应用场景与配置建议](#九典型应用场景与配置建议)
  - [9.1 场景配置建议](#91-场景配置建议)
  - [9.2 性能优化建议](#92-性能优化建议)
  - [9.3 容量规划](#93-容量规划)
- [十、消息压缩机制](#十消息压缩机制)
- [十一、事务消息存储](#十一事务消息存储)
- [十二、消息过滤机制](#十二消息过滤机制)
- [十三、版本差异说明](#十三版本差异说明)
- [十四、监控指标与调优](#十四监控指标与调优)
- [十五、总结](#十五总结)

## 一、存储架构概述

RocketMQ 的存储模型采用**混合型存储架构**，核心设计理念是将消息主体与索引分离，通过顺序写优化和零拷贝技术实现高性能的消息存储与检索。

### 1.1 整体架构

RocketMQ 存储架构分为四大子系统，各组件间的数据流关系如下：

**数据流向**：
```
Producer ──Send Message──→ CommitLog ──异步分发──→ ReputMessageService
                                                    │
                              ┌─────────────────────┴─────────────────────┐
                              ↓                                           ↓
                        ConsumeQueue                                IndexFile
                              │                                           │
                              ↓                                           ↓
                        Consumer                                    Query API
                                                              (按Key查询)

延时消息链路（TimerLog 写入独立于 reput 分发，但依赖其构建 Timer Topic 的 ConsumeQueue）：
Producer ──Send Message──→ Broker HookUtils.transformTimerMessage
                              └──→ CommitLog(rmq_sys_wheel_timer)
                                     └──→ Reput 构建 Timer Topic ConsumeQueue
                                            └──→ TimerMessageStore.TimerEnqueueGetService
                                                   └──→ TimerLog/TimerWheel ──到期改写──→ Delay Consumer
```

**组件依赖关系**：
```
CommitLog ──依赖──→ MappedFile ──借用──→ TransientStorePool (堆外内存池)
  │                      │
  │                      ↓
  ├──触发──→ FlushManager (刷盘管理)
  │
  ├──预分配──→ AllocateMappedFileService
  │
  ├──同步──→ HAService (高可用)
  │
  ├──持久化──→ StoreCheckpoint (检查点)
  │
  └──清理──→ CleanCommitLogService（只清 CommitLog）
                ConsumeQueueStore.CleanConsumeQueueService
                （按 CommitLog 最小物理位点清理 ConsumeQueue，并调用 IndexService
                 删除过期 IndexFile）
                TimerMessageStore 内部定时任务（按 CommitLog 最小物理位点清理 TimerLog）
```

**子系统划分**：
- **存储核心**：CommitLog（消息主体）、ConsumeQueue（消费索引）、IndexFile（Key索引）、TimerLog（延时消息）
- **服务线程**：ReputMessageService（异步分发）、FlushManager（刷盘）、AllocateMappedFileService（预分配）、CleanCommitLogService、ConsumeQueueStore.CleanConsumeQueueService、TimerMessageStore 定时清理任务（清理）
- **存储优化**：TransientStorePool（堆外内存）、MappedFile（内存映射）
- **高可用与恢复**：HAService（主从同步）、StoreCheckpoint（故障恢复）

### 1.2 核心组件职责

| 组件 | 职责 | 文件路径 |
|-----|------|---------|
| **CommitLog** | 集中存储所有 Topic 的消息主体 | `$storePath/commitlog/` |
| **ConsumeQueue** | 按 Topic+QueueId 组织的索引文件 | `$storePath/consumequeue/{topic}/{queueId}/` |
| **IndexFile** | 按 Key 建立的哈希索引 | `$storePath/index/` |
| **TimerLog** | 延时消息的时间轮存储 | `$storePath/timerlog/` |
| **ReputMessageService** | 异步构建 ConsumeQueue 和 IndexFile | 后台线程 |
| **FlushManager** | 管理刷盘策略（同步/异步） | 后台线程 |
| **AllocateMappedFileService** | 预分配 MappedFile，避免写入阻塞 | 后台线程 |
| **TransientStorePool** | 堆外内存池，优化写性能 | 内存缓冲区 |
| **StoreCheckpoint** | 存储恢复检查点信息 | `$storePath/checkpoint` |

---

## 二、核心存储文件详解

### 2.1 CommitLog

**设计原理**：CommitLog 是 RocketMQ 的核心存储文件，所有 Topic 的消息按写入顺序集中存储在同一组文件中。这种设计充分利用了磁盘顺序写的特性。

**文件命名规则**：文件名是 20 位数字，表示文件起始偏移量（左补零）

```
第一个文件：00000000000000000000，起始偏移量 0，大小 1GB
第二个文件：00000000001073741824，起始偏移量 1073741824（1GB）
第三个文件：00000000002147483648，起始偏移量 2147483648（2GB）
```

**文件组织**：

```
commitlog/
├── 00000000000000000000  (起始偏移量 0，大小 1GB)
├── 00000000001073741824  (起始偏移量 1073741824，大小 1GB)
├── 00000000002147483648  (起始偏移量 2147483648，大小 1GB)
└── ...
```

**消息格式**（完整结构）：

| 字段 | 大小 | 说明 |
|-----|------|-----|
| **Total Size** | 4 bytes | 消息总长度 |
| **Magic Code** | 4 bytes | 魔数，`MESSAGE_MAGIC_CODE = -626843481` (0xdaa320a7) |
| **Body CRC** | 4 bytes | 消息体 CRC 校验值 |
| **Queue ID** | 4 bytes | 消息所在队列 ID |
| **Flag** | 4 bytes | 消息标志位 |
| **Queue Offset** | 8 bytes | 在 ConsumeQueue 中的逻辑偏移量 |
| **Physical Offset** | 8 bytes | 在 CommitLog 中的物理偏移量 |
| **Sys Flag** | 4 bytes | 系统标志位（事务、延迟、BornHost/StoreHost 类型等） |
| **Born Timestamp** | 8 bytes | 消息生成时间戳 |
| **Born Host** | 8/20 bytes | 消息生成地址（IPv4 为 8B，IPv6 为 20B） |
| **Store Timestamp** | 8 bytes | 消息存储时间戳 |
| **Store Host** | 8/20 bytes | 消息存储地址（IPv4 为 8B，IPv6 为 20B） |
| **Reconsume Times** | 4 bytes | 重试次数 |
| **Prepared Offset** | 8 bytes | 事务消息的 Prepared 偏移量 |
| **Body Length** | 4 bytes | 消息体长度 |
| **Body** | N bytes | 消息体内容 |
| **Topic Length** | 1/2 bytes | Topic 名称长度（V1 为 1B，V2 为 2B） |
| **Topic** | X bytes | Topic 名称 |
| **Props Length** | 2 bytes | 属性长度 |
| **Properties** | Y bytes | 消息属性（Key、Tag、延迟级别等） |

**核心代码**（[CommitLog.java](../store/src/main/java/org/apache/rocketmq/store/CommitLog.java)）：

```java
public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
    // 1. 获取写入锁（支持自适应自旋锁/重入锁/自旋锁）
    // 2. 查找或创建 MappedFile
    // 3. 使用 MessageExtEncoder 编码消息
    // 4. 追加消息到 MappedFile（优先使用 TransientStorePool 的 writeBuffer）
    // 5. 唤醒 FlushManager 和 HA 复制线程
}

public SelectMappedBufferResult getData(final long offset) {
    int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
    if (mappedFile != null) {
        int pos = (int) (offset % mappedFileSize);
        return mappedFile.selectMappedBuffer(pos);
    }
    return null;
}
```

**存储配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `storePathCommitLog` | `~/store/commitlog` | CommitLog 存储路径 |
| `mappedFileSizeCommitLog` | 1GB | 单个 CommitLog 文件大小 |
| `flushIntervalCommitLog` | 500ms | 异步刷盘间隔 |
| `flushCommitLogTimed` | true | 是否定时刷盘 |

### 2.2 ConsumeQueue

**设计原理**：ConsumeQueue 是 CommitLog 的索引文件，按 Topic 和 QueueId 分层组织，实现"按 Topic 快速定位消息"的需求。

**存储路径**：`$storePath/consumequeue/{topic}/{queueId}/{fileName}`

**文件组织结构**：

```
consumequeue/
├── TopicA/
│   ├── 0/                    (QueueId = 0)
│   │   ├── 00000000000000000000   (起始偏移量 0)
│   │   ├── 0000000000006000000    (起始偏移量 6,000,000)
│   │   └── ...
│   └── 1/                    (QueueId = 1)
│       ├── 00000000000000000000
│       ├── 0000000000006000000
│       └── ...
└── TopicB/
    └── 0/
        └── ...
```

**文件命名规则**：文件名表示文件起始偏移量（字节），每个文件包含 30 万条记录（`300000 * 20 = 6,000,000 bytes`），第二个文件名为 `0000000000006000000`。

**条目结构**（每条 20 bytes，定长设计）：

```
┌─────────────────────────────────────────────────────────────────┐
│              ConsumeQueue Entry (20 bytes)                      │
├─────────────────────────┬───────────────────────────────────────┤
│ CommitLog Offset        │ 8 bytes  │ 消息在 CommitLog 中的偏移量 │
│ Body Size               │ 4 bytes  │ 消息体大小                 │
│ Tag HashCode            │ 8 bytes  │ Tag 的哈希值（用于过滤）   │
└─────────────────────────┴──────────┴────────────────────────────┘
```

**特点**：

- **定长设计**：每条 20 bytes，支持数组式随机访问，`offset = queueOffset * 20`
- **文件大小**：每个文件包含 30 万条记录，约 5.72MB（`300000 * 20 = 6,000,000 bytes`）
- **顺序读取**：消费时顺序遍历，配合 PageCache 预读，性能接近内存
- **Tag 过滤**：Broker 在 `getMessage` 时先按索引条目中的 Tag Hash（tagsCode）预过滤，不匹配的消息不会返回给 Consumer，避免无效的 CommitLog 读取；Consumer 收到消息后再做 Tag 字符串的最终精确校验

**ConsumeQueueExt**（扩展索引）：

当启用 `enableConsumeQueueExt` 时，ConsumeQueue 还会维护扩展索引文件，存储额外的过滤信息（如 Tag 位图），支持更精确的消息过滤。

**核心代码**（[ConsumeQueue.java](../store/src/main/java/org/apache/rocketmq/store/ConsumeQueue.java)）：

```java
public static final int CQ_STORE_UNIT_SIZE = 20;

public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
    int mappedFileSize = this.mappedFileSize;
    long offset = startIndex * CQ_STORE_UNIT_SIZE;
    if (offset >= this.getMinLogicOffset()) {
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, CQ_STORE_UNIT_SIZE);
        }
    }
    return null;
}
```

### 2.3 IndexFile

**设计原理**：IndexFile 提供按 Key 或时间区间查询消息的能力，底层实现类似 HashMap 的哈希表结构。

**存储路径**：`$storePath/index/{fileName}`（文件名为创建时间戳）

**文件结构**（固定大小，420,000,040 bytes）：

```
IndexFile
├── Header (40 bytes)
│   ├── beginTimestamp  │ 8 bytes  │ 起始时间戳
│   ├── endTimestamp    │ 8 bytes  │ 结束时间戳
│   ├── beginPhyOffset  │ 8 bytes  │ 起始物理偏移量
│   ├── endPhyOffset    │ 8 bytes  │ 结束物理偏移量
│   ├── hashSlotCount   │ 4 bytes  │ 哈希槽数量
│   └── indexCount      │ 4 bytes  │ 索引条目数量
│
├── Hash Slot Table (20,000,000 bytes = 500万 × 4 bytes)
│   └── 每个槽存储链表头指针
│
└── Index Data Area (400,000,000 bytes = 2000万 × 20 bytes)
    └── 每个索引条目：
        ├── KeyHash        │ 4 bytes  │ 键哈希值
        ├── PhyOffset      │ 8 bytes  │ CommitLog 物理偏移量
        ├── TimeDiff       │ 4 bytes  │ 与起始时间的差值（秒）
        └── NextIndexOffset│ 4 bytes  │ 下一个索引的偏移量
```

**哈希索引结构**（链表法解决哈希冲突）：

```
Hash Slot Table
├── Slot[0]  ──→  Index Entry 1  ──→  Index Entry 3  ──→  NULL
├── Slot[1]  ──→  Index Entry 2  ──→  Index Entry 4  ──→  NULL
├── Slot[2]  ──→  NULL
└── ...

查询流程：
1. 计算 Key 的哈希值：keyHash = hash(key)
2. 定位哈希槽：slotIndex = keyHash % hashSlotNum
3. 获取链表头指针：headOffset = HashSlot[slotIndex]
4. 遍历链表：依次读取 Index Entry，比较 KeyHash，找到匹配项
5. 根据匹配项的 PhyOffset 读取 CommitLog 中的消息
```

**索引构建规则**：

- 若消息设置了 `UNIQ_KEY`，则用 `topic + "#" + UNIQ_KEY` 作为索引键
- 若消息设置了 `KEYS`（多个用空格分隔），则对每个 KEY 创建索引 `topic + "#" + KEY`

**核心代码**（[IndexFile.java](../store/src/main/java/org/apache/rocketmq/store/index/IndexFile.java)）：

```java
public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
    if (this.indexHeader.getIndexCount() < this.indexNum) {
        int keyHash = indexKeyHashMethod(key);
        int slotPos = keyHash % this.hashSlotNum;
        int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;
        
        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
        if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
            slotValue = invalidIndex;
        }
        
        long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
        timeDiff = timeDiff / 1000;
        
        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
            + this.indexHeader.getIndexCount() * indexSize;
        
        this.mappedByteBuffer.putInt(absIndexPos, keyHash);
        this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
        this.mappedByteBuffer.putInt(absIndexPos + 12, (int) timeDiff);
        this.mappedByteBuffer.putInt(absIndexPos + 16, slotValue);
        
        this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
        this.indexHeader.incrIndexCount();
        return true;
    }
    return false;
}
```

### 2.4 TimerLog（延时消息存储）

**设计原理**：TimerLog 是 RocketMQ 延时消息的存储（`timerWheelEnable` 默认 true），与时间轮（TimerWheel）配合工作：消息先写入 TimerLog 文件，TimerWheel 按时间槽管理各消息的触发时机。

**存储路径**：`$storePath/timerlog/`

**TimerLog 单元结构**（每条 52 bytes）：

| 字段 | 大小 | 说明 |
|-----|------|-----|
| **Size** | 4 bytes | 单元大小 |
| **Prev Position** | 8 bytes | 前一个单元位置（链表结构） |
| **Magic Value** | 4 bytes | 魔数，用于校验 |
| **Write Time** | 8 bytes | 写入时间戳，用于追踪 |
| **Delayed Time** | 4 bytes | 延迟时间（与写入时间的差值，单位毫秒，见 `TimerMessageStore` 写入 `(int)(delayedTime - tmpWriteTimeMs)`） |
| **CommitLog Offset** | 8 bytes | 对应的 CommitLog 物理偏移量 |
| **Message Size** | 4 bytes | 消息大小 |
| **Topic Hash** | 4 bytes | 真实 Topic 的哈希值 |
| **Reserved** | 8 bytes | 预留字段 |

**TimerWheel 结构**：

```
TimerWheel (时间轮存储文件)
├── Slot[0]  ──→  TimerLog Entry1  ──→  TimerLog Entry2  ──→  NULL
├── Slot[1]  ──→  TimerLog Entry3  ──→  NULL
└── Slot[N]  ──→  NULL

时间轮工作原理：
1. 每个 Slot 对应一个时间槽，时间跨度由 timerPrecisionMs 决定（默认 1000ms）
2. 延时消息根据其延迟时间计算对应的 Slot 索引
3. 消息索引存储在 TimerLog 中，Slot 通过 firstPos/lastPos 维护链表头和尾
4. 轮询线程定期扫描到达时间的 Slot，触发消息投递
```

**Slot 结构**（每条 32 bytes）：

| 字段 | 大小 | 说明 |
|-----|------|-----|
| **delayTime** | 8 bytes | 延迟时间点 |
| **firstPos** | 8 bytes | 该槽第一条消息在 TimerLog 中的位置 |
| **lastPos** | 8 bytes | 该槽最后一条消息在 TimerLog 中的位置 |
| **num** | 4 bytes | 该槽消息数量 |
| **magic** | 4 bytes | 魔数 |

**核心组件**：

| 组件 | 职责 |
|-----|------|
| **TimerWheel** | 时间轮调度器，按时间槽管理延时消息，每个槽指向 TimerLog 中的消息链表 |
| **TimerLog** | 延时消息的物理存储文件，采用链表结构存储消息索引 |
| **TimerCheckpoint** | 时间轮状态检查点，支持故障恢复 |
| **TimerMessageStore** | 延时消息存储管理器，协调 TimerWheel 和 TimerLog |

**配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `mappedFileSizeTimerLog` | 100MB | 单个 TimerLog 文件大小 |
| `timerPrecisionMs` | 1000ms | 时间精度（每个槽的时间跨度） |
| `timerRollWindowSlot` | 172800 | 滚动窗口槽数（2天） |
| `timerMaxDelaySec` | 259200 | 最大延迟时间（3天） |

**核心代码**（[TimerLog.java](../store/src/main/java/org/apache/rocketmq/store/timer/TimerLog.java) / [Slot.java](../store/src/main/java/org/apache/rocketmq/store/timer/Slot.java)）：

```java
public final static int UNIT_SIZE = 4  //size
        + 8 //prev pos
        + 4 //magic value
        + 8 //curr write time
        + 4 //delayed time
        + 8 //offsetPy
        + 4 //sizePy
        + 4 //hash code of real topic
        + 8; //reserved value

public static final short SIZE = 32; // Slot 大小
```

---

## 三、关键存储服务

### 3.1 ReputMessageService

**核心职责**：Broker 后台服务线程，异步构建 ConsumeQueue 和 IndexFile（以及 Compaction 分发）。注意：**TimerLog 索引不由 reput 分发器直接构建**——Broker 的 `HookUtils.transformTimerMessage` 先把定时消息改写为系统 Topic `rmq_sys_wheel_timer`；reput 负责构建该 Topic 的 ConsumeQueue，随后由 `TimerMessageStore` 的 `TimerEnqueueGetService` 从 ConsumeQueue 读取，再经 `enqueue()` 写入 TimerLog/TimerWheel。

**工作流程**：

```
CommitLog ──新消息写入──→ ReputService ──触发 reput──→ ReputService 读取新消息
                                                              │
                                                              ↓
                                                    解析消息元数据(Topic/QueueId/Tag/Key)
                                                              │
                              ┌───────────────────────────────┼───────────────────────────────┐
                              ↓                               ↓
                        ConsumeQueue                    IndexService
                        (写入索引条目)                   (写入 Key 索引)

（TimerLog 不走此分发器：rmq_sys_wheel_timer → Reput 构建 ConsumeQueue → TimerEnqueueGetService → TimerLog/TimerWheel）
```

**处理流程**：

1. 从 CommitLog 的 `reputOffset` 位置读取新写入的消息
2. 解析消息的 Topic、QueueId、Tag、Key 等元数据
3. 将消息索引写入对应的 ConsumeQueue 文件
4. 若消息有 Key，则构建 IndexFile 索引
5. 若启用了 Compaction（默认 `enableCompaction=true`），分发对应消息
6. 延时消息（定时消息）不在此链路构建 TimerLog 索引：其索引由 `TimerMessageStore.TimerEnqueueGetService` 独立构建

**并发构建**：

RocketMQ 支持并发构建 ConsumeQueue，通过配置 `enableBuildConsumeQueueConcurrently` 启用 `ConcurrentReputMessageService`，提升索引构建吞吐量。

**CommitLogDispatcher 链模式**：

ReputMessageService 通过 `dispatcherList` 链式调用多个 `CommitLogDispatcher` 实现，每个 Dispatcher 负责构建不同类型的索引。

```
dispatcherList 链式调用顺序：
ReputService ──→ CommitLogDispatcherBuildConsumeQueue ──→ CommitLogDispatcherBuildIndex
                                                              │
                                                              ↓
                                                    CommitLogDispatcherBuildTransIndex
                                                              │
                                                              ↓（仅 enableCompaction=true 时注册，默认 true）
                                                    CommitLogDispatcherCompaction
```

**Dispatcher 职责**：

| Dispatcher | 职责 | 是否默认启用 |
|-----------|------|------------|
| **CommitLogDispatcherBuildConsumeQueue** | 构建 ConsumeQueue 索引 | 是 |
| **CommitLogDispatcherBuildIndex** | 构建 IndexFile 索引 | 是 |
| **CommitLogDispatcherBuildTransIndex** | 构建事务消息索引 | 默认注册，但仅 `transRocksDBEnable=true`（默认 false）时实际生效 |
| **CommitLogDispatcherCompaction** | Compaction 分发 | `enableCompaction=true`（默认 true）时注册 |

**核心代码**（[DefaultMessageStore.java](../store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java)）：

```java
private final LinkedList<CommitLogDispatcher> dispatcherList = new LinkedList<>();

// 初始化时注册 Dispatcher
this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());
this.dispatcherList.addLast(new CommitLogDispatcherBuildTransIndex());

if (messageStoreConfig.isEnableCompaction()) {
    this.dispatcherList.addLast(new CommitLogDispatcherCompaction(compactionService));
}

// ReputMessageService 遍历 dispatcherList 分发消息
for (CommitLogDispatcher dispatcher : this.dispatcherList) {
    dispatcher.dispatch(request);
}
```

### 3.2 FlushManager

**核心职责**：管理刷盘策略，确保消息从 PageCache 持久化到磁盘。

**刷盘策略对比**：

| 策略 | 优点 | 缺点 | 适用场景 |
|-----|------|------|---------|
| **SYNC_FLUSH** | 成功响应前完成本机刷盘 | 延迟高、吞吐量低 | 需要降低进程/断电丢失窗口的业务 |
| **ASYNC_FLUSH** | 延迟低、吞吐量高 | 断电可能丢失数据 | 日志收集、实时计算 |

**同步刷盘流程**（SYNC_FLUSH）：

```
步骤1: Producer ──Send Message──→ Broker
步骤2: Broker ──写入 MappedByteBuffer──→ PageCache
步骤3: Broker ──flush() 同步刷盘──→ Disk
步骤4: Disk ──刷盘完成──→ Broker
步骤5: Broker ──Return ACK──→ Producer

特点：成功响应前等待本机 `flush()` 完成，降低进程崩溃或断电时的丢失窗口，但不能覆盖
磁盘损坏、控制器缓存未持久化等所有故障，因此不能单独承诺“零丢失”。
```

**异步刷盘流程**（ASYNC_FLUSH）：

```
步骤1: Producer ──Send Message──→ Broker
步骤2: Broker ──写入 MappedByteBuffer──→ PageCache
步骤3: Broker ──Return ACK (立即)──→ Producer

步骤4: FlushThread 定时轮询/阈值触发
步骤5: FlushThread ──flush() 异步刷盘──→ Disk
步骤6: Disk ──刷盘完成──→ FlushThread

特点：延迟低、吞吐量高，但断电可能丢失数据
```

**核心代码**（[FlushManager.java](../store/src/main/java/org/apache/rocketmq/store/FlushManager.java)）：

```java
public interface FlushManager {
    void start();
    void shutdown();
    void wakeUpFlush();
    void wakeUpCommit();
    void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt);
    CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt);
}
```

### 3.3 AllocateMappedFileService

**核心职责**：预分配 MappedFile，避免消息写入时同步分配文件导致的性能抖动。

**预分配策略**：

```
步骤1: CommitLog ──putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath)──→ AllocateService
步骤2: AllocateService ──放入 PriorityBlockingQueue──→ 等待队列

步骤3: 后台线程循环处理：
       ├─ AllocateService ──创建 MappedFile（mmap / FileChannel）──→ MappedFile
       └─ AllocateService ──warmMappedFile（预热）──→ PageCache

步骤4: AllocateService ──返回 MappedFile──→ CommitLog
```

**预热机制**：

当配置 `warmMapedFileEnable` 为 true 时，新分配的 MappedFile 会进行预热，预先将文件内容加载到 PageCache，避免首次访问时的缺页中断。

**核心代码**（[AllocateMappedFileService.java](../store/src/main/java/org/apache/rocketmq/store/AllocateMappedFileService.java)）：

```java
public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
    AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
    boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;
    
    if (nextPutOK) {
        boolean offerOK = this.requestQueue.offer(nextReq);
    }
    
    AllocateRequest result = this.requestTable.get(nextFilePath);
    if (result != null) {
        boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
        if (waitOK) {
            this.requestTable.remove(nextFilePath);
            return result.getMappedFile();
        }
    }
    return null;
}
```

### 3.4 TransientStorePool

**核心职责**：提供堆外内存缓冲区，优化消息写入性能，减少 GC 压力。

**设计原理**：

```
内存布局：
┌──────────────────────────────────────────────────────────────────────┐
│ 堆内存 (Heap)                                                        │
│ ┌─────────────────────────────────────────────────────────────────┐  │
│ │ 应用程序对象 (App Objects)                                      │  │
│ └─────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────────────┐
│ 堆外内存 (Off-Heap) - TransientStorePool (mlock 锁定)                 │
│ ┌─────────────────────────────────────────────────────────────────┐  │
│ │ ByteBuffer[0]  ByteBuffer[1]  ByteBuffer[2]  ...               │  │
│ │   (1GB)          (1GB)          (1GB)                          │  │
│ └─────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘

数据流向：
应用程序 ──borrowBuffer()──→ TransientStorePool ──写入数据──→ ByteBuffer
                                                              │
                                                              ↓
                                                    commit() ──→ PageCache
                                                              │
                                                              ↓
                                                    returnBuffer() ──→ TransientStorePool (复用)
```

**核心特性**：

- 使用 `ByteBuffer.allocateDirect()` 分配堆外内存
- 使用 `mlock()` 锁定内存，防止被操作系统换出到磁盘
- 内存池复用，减少内存分配/释放开销
- 消息先写入堆外缓冲区，再异步提交到 PageCache

**核心代码**（[TransientStorePool.java](../store/src/main/java/org/apache/rocketmq/store/TransientStorePool.java)）：

```java
public void init() {
    for (int i = 0; i < poolSize; i++) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
        final long address = PlatformDependent.directBufferAddress(byteBuffer);
        Pointer pointer = new Pointer(address);
        LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));
        availableBuffers.offer(byteBuffer);
    }
}

public ByteBuffer borrowBuffer() {
    ByteBuffer buffer = availableBuffers.pollFirst();
    if (availableBuffers.size() < poolSize * 0.4) {
        log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
    }
    return buffer;
}
```

**配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `transientStorePoolSize` | 5 | 缓冲区数量 |
| `fastFailIfNoBufferInStorePool` | false | 缓冲区不足时是否快速失败 |

### 3.5 CleanCommitLogService

**核心职责**：定期清理过期的 CommitLog。ConsumeQueue 和 IndexFile 由
`ConsumeQueueStore.CleanConsumeQueueService` 根据 CommitLog 最小有效偏移另行清理。

**清理策略**：

```
定时触发清理
    │
    ↓
检查磁盘使用率
    │
    ├──> diskSpaceCleanForciblyRatio (85%) ──→ 立即清理并允许更激进的删除
    │                                           │
    └──< diskSpaceCleanForciblyRatio ──→ 由 diskMaxUsedSpaceRatio、deleteWhen 或手工触发清理
                                              │
                                              ├──> fileReservedTime (72小时) ──→ 清理过期文件
                                              │                                           │
                                              └──< fileReservedTime ──→ 跳过            │
                                                                                        ↓
                                                                              检查是否被引用
                                                                                        │
                                                                                        ├── 未被引用 ──→ 删除文件
                                                                                        └── 被引用 ──→ 保留文件
```

**配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `fileReservedTime` | 72 hours | 文件保留时间 |
| `deleteWhen` | "04" | 清理时间点（凌晨4点） |
| `deleteFileBatchMax` | 10 | 单次删除文件数量上限 |
| `diskMaxUsedSpaceRatio` | 75% | 普通单路径空间达到该比例时进入清理判断；多路径时也参与逻辑容量判断 |
| `diskSpaceCleanForciblyRatio` | 85% | 强制清理阈值 |

### 3.6 StoreCheckpoint

**核心职责**：存储恢复检查点信息，支持 Broker 故障重启后的状态恢复。

**检查点文件结构**（6 个字段共 48 bytes 有效数据；文件本体按 4KB 页大小映射创建，实际占用为一个页）：

| 偏移 | 大小 | 字段 | 说明 |
|-----|------|-----|------|
| 0 | 8 bytes | physicMsgTimestamp | CommitLog 最大消息时间戳 |
| 8 | 8 bytes | logicsMsgTimestamp | ConsumeQueue 最大消息时间戳 |
| 16 | 8 bytes | indexMsgTimestamp | IndexFile 最大消息时间戳 |
| 24 | 8 bytes | masterFlushedOffset | Master 已刷盘偏移量 |
| 32 | 8 bytes | confirmPhyOffset | 已确认偏移量 |
| 40 | 8 bytes | logicsPhysicalOffset | ConsumeQueue 对应的物理偏移量 |

**核心代码**（[StoreCheckpoint.java](../store/src/main/java/org/apache/rocketmq/store/StoreCheckpoint.java)）：

```java
public void flush() {
    this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
    this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
    this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);
    this.mappedByteBuffer.putLong(24, this.masterFlushedOffset);
    this.mappedByteBuffer.putLong(32, this.confirmPhyOffset);
    this.mappedByteBuffer.putLong(40, this.logicsPhysicalOffset);
    this.mappedByteBuffer.force();
}
```

---

## 四、消息写入流程

### 4.1 完整写入流程

```
步骤1: Producer ──Send Message──→ Broker
步骤2: Broker ──获取写入锁──→ PutMessageLock
步骤3: Broker ──获取/预分配 MappedFile──→ AllocateService

步骤4-7: 写入消息（两种模式）
    ┌─ 模式A: 使用 TransientStorePool
    │   Broker ──borrowBuffer()──→ TransientPool
    │   TransientPool ──返回 ByteBuffer──→ Broker
    │   Broker ──写入消息──→ ByteBuffer
    │   Broker ──commit() -> FileChannel──→ MappedFile
    │   TransientPool ──returnBuffer()──→ 复用缓冲区
    │
    └─ 模式B: 直接写入 MappedByteBuffer
        Broker ──写入 MappedByteBuffer──→ MappedFile

步骤8: Broker ──唤醒刷盘线程──→ FlushManager
步骤9: Broker ──唤醒 HA 复制线程──→ HAService
步骤10: Broker ──Return ACK──→ Producer

步骤11: ReputService 异步构建索引（后台）
        ├─ ConsumeQueue 索引
        └─ IndexFile 索引（有 Key 时）
        （延时消息的 TimerLog 索引由 TimerMessageStore 独立构建，不走 reput 链路）
```

### 4.2 写入核心步骤

**Step 1：获取写入锁**

```java
PutMessageLock adaptiveBackOffSpinLock = new AdaptiveBackOffSpinLockImpl();
this.putMessageLock = messageStore.getMessageStoreConfig().getUseABSLock() 
    ? adaptiveBackOffSpinLock 
    : messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() 
        ? new PutMessageReentrantLock() 
        : new PutMessageSpinLock();
```

**锁策略对比**：

| 锁类型 | 适用场景 | 特点 |
|-------|---------|------|
| **AdaptiveBackOffSpinLock** | 高并发写入 | 自适应退避自旋，减少锁竞争 |
| **PutMessageReentrantLock** | 通用场景 | 可重入锁，稳定性好 |
| **PutMessageSpinLock** | 低延迟场景 | 纯自旋，无上下文切换 |

**Step 2：消息编码**

使用 `MessageExtEncoder` 按固定格式编码消息：

```java
public static int calMsgLength(MessageVersion messageVersion,
    int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
    int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
    
    return 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength 
        + 8 + storehostAddressLength + 4 + 8 + 4 + bodyLength 
        + messageVersion.getTopicLengthSize() + topicLength 
        + 2 + propertiesLength;
}
```

**Step 3：追加消息**

```java
AppendMessageResult result = mappedFile.appendMessage(msg, appendMessageCallback);
```

**Step 4：刷盘策略**

- **同步刷盘**：调用 `mappedFile.flush(0)` 等待刷盘完成
- **异步刷盘**：唤醒 `FlushCommitLogService`，后台线程定时刷盘

---

## 五、消息读取流程

### 5.1 消费读取流程

```
步骤1: Consumer ──Pull Message(topic, queueId, offset)──→ Broker
步骤2: Broker ──读取索引条目(CommitLog Offset + Size + Tag Hash)──→ ConsumeQueue
步骤3: ConsumeQueue ──返回索引数据──→ Broker
步骤3.5: Broker ──按 Tag Hash 预过滤（MessageFilter 比较索引条目 tagsCode，不匹配则跳过）──→ CommitLog
步骤4: Broker ──根据物理偏移量读取消息──→ CommitLog
步骤5: CommitLog ──返回完整消息──→ Broker
步骤6: Broker ──Return Message──→ Consumer
步骤7: Consumer ──按 Tag 字符串精确校验（不匹配则丢弃）──→ 业务处理
```

### 5.2 Key 查询流程

```
步骤1: Client ──Query by Key──→ Broker
步骤2: Broker ──查询索引──→ IndexService
步骤3: IndexService ──计算 Key Hash，查找 Hash Slot──→ IndexFile
步骤4: IndexFile ──返回索引条目链表──→ IndexService
步骤5: IndexService ──遍历链表，读取消息──→ CommitLog
步骤6: CommitLog ──返回消息──→ IndexService
步骤7: IndexService ──返回消息列表──→ Broker
步骤8: Broker ──Return Message(s)──→ Client
```

### 5.3 MessageId 查询流程

**MessageId 结构**：

MessageId 的长度取决于 Broker 地址类型：

| 地址类型 | MessageId 长度 | 结构 |
|---------|--------------|------|
| **IPv4** | 16 bytes | 4 bytes IP + 4 bytes 端口 + 8 bytes 偏移量 |
| **IPv6** | 28 bytes | 16 bytes IP + 4 bytes 端口 + 8 bytes 偏移量 |

**IPv4 MessageId 结构**（16 bytes）：

| 字段 | 大小 | 说明 |
|-----|------|-----|
| IP 地址 | 4 bytes | Broker 地址 |
| 端口号 | 4 bytes | Broker 端口（Int） |
| CommitLog 偏移量 | 8 bytes | 物理偏移量 |

**核心代码**（[MessageDecoder.java](../common/src/main/java/org/apache/rocketmq/common/message/MessageDecoder.java)）：

```java
public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
    input.flip();
    int msgIDLength = addr.limit() == 8 ? 16 : 28;  // IPv4: 16 bytes, IPv6: 28 bytes
    input.limit(msgIDLength);
    input.put(addr);      // IP + Port (8 bytes for IPv4, 20 bytes for IPv6)
    input.putLong(offset); // CommitLog offset (8 bytes)
    return UtilAll.bytes2string(input.array());
}

public static MessageId decodeMessageId(final String msgId) {
    byte[] bytes = UtilAll.string2bytes(msgId);
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byte[] ip = new byte[msgId.length() == 32 ? 4 : 16];  // 32 hex chars = 16 bytes = IPv4
    byteBuffer.get(ip);
    int port = byteBuffer.getInt();
    long offset = byteBuffer.getLong();
    return new MessageId(new InetSocketAddress(InetAddress.getByAddress(ip), port), offset);
}
```

**查询方式**：这里的 MessageId 指物理 `offsetMsgId`。解析它可以直接获得 Broker 地址和
CommitLog 物理偏移量，无需 IndexFile。Producer 返回的 `SendResult.msgId` 通常是客户端
生成的 `UNIQ_KEY`；按这个值或按业务 Key 查询时，仍需走 `IndexFile`（或对应的 RocksDB
索引），不能把两种 ID 混为一谈。

---

## 六、高可用与数据可靠性

### 6.1 Master/Slave 架构

```
NameServer
    │
    ├──→ Broker A (Master) ──→ Slave A1
    │                       └─→ Slave A2
    │
    ├──→ Broker B (Master) ──→ Slave B1
    │
    └──→ Broker C (Master) ──→ Slave C1

说明：
- NameServer 维护 Broker 集群元数据
- 每个 Master 可配置多个 Slave（通常 1-2 个）
- Slave 从 Master 同步 CommitLog 数据
- 消息只写入 Master，Consumer 可从 Master 或 Slave 消费
```

### 6.2 HA 同步机制

**同步复制**（SYNC_MASTER）：

```
步骤1: Producer ──Send Message──→ Master
步骤2: Master ──写入 CommitLog──→ Master
步骤3: needAckNums > 1 时，Master ──复制消息──→ Slave
步骤4: 足够数量的 Slave ──ACK 确认──→ Master
步骤5: 达到配置的 ACK 数后 Master ──Return ACK──→ Producer

优点：可将副本确认纳入发送成功边界
缺点：延迟高、吞吐量低
适用：金融交易、核心业务
```

`SYNC_MASTER` 本身不等于一定等待 Slave。当前 `inSyncReplicas` 默认是 1，且 Master 自身
计入该数量；`needAckNums <= 1` 时 `handleHA` 会直接成功。要等待至少一个 Slave，必须把
所需 ACK 数配置为大于 1，并保证 ISR 数量满足要求。即使如此，同步复制也只是降低副本故障
下的数据丢失概率，不能覆盖所有软硬件故障。

**异步复制**（ASYNC_MASTER）：

```
步骤1: Producer ──Send Message──→ Master
步骤2: Master ──写入 CommitLog──→ Master
步骤3: Master ──Return ACK (立即)──→ Producer

步骤4: Master ──异步复制消息──→ Slave
步骤5: Slave ──ACK 确认──→ Master

优点：延迟低、吞吐量高
缺点：Master 宕机可能丢失数据
适用：日志收集、实时计算
```

**DLedger 模式**（Raft 协议）：

RocketMQ 4.5+ 支持 DLedger 模式，基于 Raft 协议实现多副本数据一致性，自动选主，无需手动配置 Master/Slave。

**核心代码**（[HAService.java](../store/src/main/java/org/apache/rocketmq/store/ha/HAService.java)）：

`HAService`/`HAClient`/`HAConnection` 均为接口，默认实现为 `DefaultHAService`/`DefaultHAClient`/`DefaultHAConnection`（Controller 模式下由 `AutoSwitchHAService` 提供自动主从切换）：

```java
// 接口定义（ha/HAService.java）
public interface HAService {
    interface HAClient {
        // Slave 端：从 Master 拉取 CommitLog 数据并写入本地 CommitLog
    }
    interface HAConnection {
        // Master 端维护的连接，推送数据给 Slave
    }
}
// 默认实现：ha/DefaultHAService.java、ha/DefaultHAClient.java、ha/DefaultHAConnection.java
```

### 6.3 数据完整性保障

**CRC 校验**：

```java
public static final int MESSAGE_MAGIC_CODE = -626843481;  // daa320a7
public static final int BLANK_MAGIC_CODE = -875286124;    // cbd43194
```

每条消息包含：
- **Body CRC**：消息体校验
- **Magic Code**：魔数校验，用于识别有效消息

**文件完整性检查**：

```java
public boolean load() {
    boolean result = this.mappedFileQueue.load();
    this.mappedFileQueue.checkSelf();
    return result;
}
```

启动时扫描 CommitLog 文件，验证魔数和 CRC，修复损坏的数据。

---

## 七、存储优化机制

### 7.1 PageCache 与 Mmap 优化

**PageCache 机制**：

```
操作系统内存布局：
┌─────────────────────────────────────────────────────────┐
│ 应用程序内存 (Application Memory)                       │
└─────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────┐
│ PageCache (文件缓存)                                    │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐     │
│ │  Page 0      │ │  Page 1      │ │  Page 2      │ ... │
│ │  (4KB/8KB)   │ │  (4KB/8KB)   │ │  (4KB/8KB)   │     │
│ └──────────────┘ └──────────────┘ └──────────────┘     │
└─────────────────────────────────────────────────────────┘

数据流向：
应用程序 ──读写──→ PageCache ──pdflush 异步──→ 物理磁盘
物理磁盘 ──预读──→ PageCache

特点：
- 顺序读写性能接近内存（约 1GB/s）
- 读取时自动预读相邻块数据
- 脏页由 pdflush 线程异步刷盘
```

**Mmap 内存映射**：

```
传统 IO (2 次数据拷贝)：
应用程序 ──数据拷贝──→ 内核缓冲区 ──数据拷贝──→ 磁盘

Mmap IO (零拷贝)：
应用程序 ──直接访问──→ MappedByteBuffer ──内存映射──→ 磁盘

实现方式：
MappedByteBuffer mappedByteBuffer = channel.map(MapMode.READ_WRITE, 0, fileSize);

优点：
- 减少数据拷贝，提高 IO 性能
- 应用程序直接操作内存地址，无需系统调用
- 充分利用 PageCache 机制
```

```java
MappedByteBuffer mappedByteBuffer = channel.map(MapMode.READ_WRITE, 0, fileSize);
```

**WriteWithoutMmap 模式**：

当配置 `writeWithoutMmap = true` 时，使用 `FileChannel.write()` 替代 `MappedByteBuffer`，避免 mmap 带来的内存锁问题，适合特定场景。

### 7.2 冷热数据分离

**设计理念**：

```java
class ColdDataCheckService extends ServiceThread {
    @Override
    public void run() {
        while (!this.isStopped()) {
            scanFileAndSetReadMode(LibC.MADV_RANDOM);
        }
    }
}
```

- **热数据**（近期写入）：使用 PageCache 缓存，设置 `MADV_SEQUENTIAL` 模式
- **冷数据**（长时间未访问）：释放 PageCache，设置 `MADV_RANDOM` 模式，减少内存占用

### 7.3 多路径存储

支持将连续的 CommitLog 文件分布在多个磁盘路径，用于容量扩展和磁盘故障隔离：

```java
if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
    this.mappedFileQueue = new MultiPathMappedFileQueue(...);
} else {
    this.mappedFileQueue = new MappedFileQueue(...);
}
```

**配置示例**：
```
storePathCommitLog=/disk1/store/commitlog,/disk2/store/commitlog,/disk3/store/commitlog
```

Broker 仍只有一条全局 CommitLog，append 也受全局 `putMessageLock` 串行化；多路径不会让
多条消息同时写入多个活跃 CommitLog 文件，因此不能简单等同于线性提升写入吞吐量。

### 7.4 RocksDB 存储引擎

RocketMQ 5.x 支持 RocksDB 作为存储引擎，提供更灵活的存储方案：

| 存储类型 | 默认文件存储 | RocksDB 存储 |
|---------|------------|-------------|
| **CommitLog** | 顺序文件 | 不支持 |
| **ConsumeQueue** | 定长索引文件 | 支持（RocksDBConsumeQueue） |
| **Index** | Hash 索引文件 | 支持（IndexRocksDBStore） |
| **Timer** | 时间轮文件 | 支持（TimerMessageRocksDBStore） |
| **Transaction** | 系统 Topic（默认 `RMQ_SYS_TRANS_HALF_TOPIC`） | 支持（`transRocksDBEnable=true` 且 `transWriteOriginTransHalfEnable=false` 时使用 `RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC`，由 TransMessageRocksDBStore 建索引） |

**配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `storeType` | `default` | 可用值为 `default`、`defaultRocksDB`，也可用分号组合 |
| `rocksdbCQDoubleWriteEnable` | false | 是否双写 ConsumeQueue |
| `indexRocksDBEnable` | false | 是否启用 RocksDB 索引 |
| `timerRocksDBEnable` | false | 是否启用 RocksDB 延时存储 |

---

## 八、存储恢复流程

### 8.1 启动恢复流程

```
Broker 启动
    │
    ↓
检查 abort 文件（决定走哪条恢复分支，两种情况都会执行 recover()，
见 DefaultMessageStore.load() 中的 recover(lastExitOK) 调用）
    │
    ├── 存在 ──→ 异常关闭（lastExitOK=false），recoverAbnormally()
    │               │
    │               ├─ 步骤1: 加载 CommitLog
    │               ├─ 步骤2: 校验魔数和 CRC
    │               ├─ 步骤3: 修复损坏消息
    │               └─ 步骤4: 继续以下流程
    │
    └── 不存在 ──→ 正常关闭（lastExitOK=true），recoverNormally()
                    │
                    ↓
步骤5: 加载 ConsumeQueue
步骤6: 加载 IndexFile
步骤7: 加载 TimerLog
步骤8: 读取 StoreCheckpoint
步骤9: 计算最小 dispatch offset
步骤10: 从最小偏移量重新构建索引
步骤11: 启动完成

恢复策略：
- 正常关闭：从 checkpoint 位点做较轻量的校验与推进（recoverNormally），
  仍会校验 CommitLog 尾部并推进 dispatch 位点，并非"完全跳过恢复"
- 异常关闭：可能存在索引不一致，需要根据 CommitLog 重建索引（recoverAbnormally）
```

### 8.2 恢复核心逻辑

```java
public void recover(final boolean lastExitOK) {
    // 恢复 ConsumeQueue
    this.consumeQueueStore.recover(this.brokerConfig.isRecoverConcurrently());
    
    // 计算最小 dispatch offset
    Long dispatchFromPhyOffset = this.consumeQueueStore.getDispatchFromPhyOffset(lastExitOK);
    
    for (CommitLogDispatchStore store : commitLogDispatchStores) {
        dispatchFromPhyOffset = Math.min(dispatchFromPhyOffset, store.getDispatchFromPhyOffset(lastExitOK));
    }
    
    // 从最小偏移量开始重新构建索引
    this.commitLog.recover(dispatchFromPhyOffset);
}
```

---

## 九、典型应用场景与配置建议

### 9.1 场景配置建议

| 场景 | 刷盘策略 | HA 模式 | 存储配置建议 | 原因 |
|-----|---------|--------|-------------|------|
| **金融交易** | SYNC_FLUSH | SYNC_MASTER | 配置 `inSyncReplicas > 1` 并监控 ISR | 将本机刷盘和副本 ACK 纳入成功边界 |
| **日志收集** | ASYNC_FLUSH | ASYNC_MASTER | 开启 TransientStorePool | 高吞吐，允许少量丢失 |
| **实时计算** | ASYNC_FLUSH | SYNC_MASTER | 开启 TransientStorePool | 平衡可靠性与性能 |
| **消息堆积** | ASYNC_FLUSH | ASYNC_MASTER | 增大 ConsumeQueue 缓存 | ConsumeQueue 顺序读，堆积不影响性能 |
| **延时消息** | ASYNC_FLUSH | SYNC_MASTER | 开启 TimerWheel | 精确的时间控制 |

### 9.2 性能优化建议

1. **CommitLog 文件大小**：默认 1GB，SSD 环境可增大至 2GB
2. **刷盘间隔**：异步刷盘时合理设置 `flushIntervalCommitLog`（默认 500ms）
3. **IO 调度算法**：SSD 使用 `deadline` 或 `none`，HDD 使用 `cfq`
4. **内存分配**：确保 PageCache 有足够空间（建议物理内存的 50%）
5. **TransientStorePool**：高并发写入场景开启，设置合理的 `poolSize`
6. **预分配预热**：开启 `warmMapedFileEnable`，减少首次访问延迟
7. **多路径存储**：多磁盘环境配置 `storePathCommitLog` 多路径

### 9.3 容量规划

**CommitLog 容量计算**：

```
存储容量 = 消息吞吐量 × 保留时间 × 消息平均大小 / 有效利用率
```

**示例**：
- 吞吐量：10,000 msg/s
- 保留时间：72 小时
- 消息平均大小：1KB
- 有效利用率：0.8（按 80% 可用空间估算，预留余量）

```
存储容量 = 10,000 × 72 × 3600 × 1KB / 0.8 = 3.24TB
```

---

## 十、消息压缩机制

这里讨论的是 Java Client 普通消息的 **body 压缩**。RocksDB 文件压缩和 Broker 注册信息
压缩是另外两套逻辑，不属于这条消息发送与消费主链路。

### 10.1 端到端流程

```text
原始消息 body
  -> Producer 校验原始消息大小
  -> 达到阈值后压缩 body
  -> sysFlag 标记“已压缩 + 压缩算法”
  -> Broker 不解压，原样写入 CommitLog
  -> Broker Pull 时原样返回 CommitLog 数据
  -> Consumer 根据 sysFlag 自动解压
  -> 业务代码取得原始 body
```

压缩后的数据覆盖了 Producer 到 Broker、Broker 到 Consumer 的网络传输，也以压缩形态
保存在 CommitLog 中并参与 HA 复制。Topic、properties 和 Remoting 请求头不属于消息 body，
不会被这套逻辑一起压缩。

### 10.2 Producer 判断与压缩

普通发送先由 `Validators.checkMessage` 校验原始 body，再进入
[`DefaultMQProducerImpl.tryToCompressMessage`](../client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java)：

```java
if (!(msg instanceof MessageBatch)
    && body.length >= defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
    byte[] data = defaultMQProducer.getCompressor()
        .compress(body, defaultMQProducer.getCompressLevel());
    if (data != null) {
        msg.setBody(data);
        return true;
    }
}
```

压缩成功后，`sendKernelImpl` 设置压缩标志和算法标志：

```java
sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
sysFlag |= defaultMQProducer.getCompressType().getCompressionFlag();
```

随后 [`MQClientAPIImpl`](../client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java)
把当前 `msg.getBody()` 直接设置成 RemotingCommand 的 body。发送结束后，kernel 的
`finally` 会把调用方 `Message` 恢复成原始 body；异步发送会按需 clone 消息，保证网络层
仍持有压缩后的字节。

如果压缩抛出 `IOException`，客户端记录日志并继续发送原始 body，不设置压缩标志。
当前实现也不会比较压缩前后的大小：只要 compressor 返回非 `null`，即使结果更大也会使用。

### 10.3 算法与 `sysFlag`

[`CompressionType`](../common/src/main/java/org/apache/rocketmq/common/compression/CompressionType.java)
支持三种算法：

| 算法 | 编号 | 实现特点 |
| --- | ---: | --- |
| LZ4 | 1 | 使用 LZ4 Frame，当前实现忽略 `compressLevel` |
| ZSTD | 2 | 使用 ZSTD Stream，使用配置的 `compressLevel` |
| ZLIB | 3 | 使用 JDK `Deflater`，默认算法 |

[`MessageSysFlag`](../common/src/main/java/org/apache/rocketmq/common/sysflag/MessageSysFlag.java)
用 bit 0 表示 body 已压缩，用第 8～10 位记录算法：

```java
COMPRESSED_FLAG       = 0x1;
COMPRESSION_LZ4_TYPE  = 0x1 << 8;
COMPRESSION_ZSTD_TYPE = 0x2 << 8;
COMPRESSION_ZLIB_TYPE = 0x3 << 8;
```

旧消息可能只有 `COMPRESSED_FLAG`、没有算法位。算法编号为 0 时按 ZLIB 处理，以兼容旧版本。

### 10.4 Broker 原样存储和传输

[`SendMessageProcessor`](../broker/src/main/java/org/apache/rocketmq/broker/processor/SendMessageProcessor.java)
从请求中取出 body 和 `sysFlag`，直接构造 `MessageExtBrokerInner`，不会在写入前解压。

`CommitLog.asyncPutMessage` 针对收到的压缩 body 计算 `BODYCRC`，随后
[`MessageExtEncoder`](../store/src/main/java/org/apache/rocketmq/store/MessageExtEncoder.java)
把 body、长度和 `sysFlag` 一起编码进 CommitLog。Pull 消息时 Broker 同样不解压，通常
直接把对应的 CommitLog Buffer 发送给客户端。

因此 Tag 和属性过滤不需要解压 body：这些元数据本来就独立保存在 CommitLog 记录中。

### 10.5 Consumer 自动解压

Java Consumer 在 [`PullAPIWrapper`](../client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullAPIWrapper.java)
中调用 [`MessageDecoder`](../common/src/main/java/org/apache/rocketmq/common/message/MessageDecoder.java)
批量解码。默认 `decodeDecompressBody=true`，解码器执行：

```java
if (deCompressBody && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) != 0) {
    Compressor compressor = CompressorFactory.getCompressor(
        MessageSysFlag.getCompressionType(sysFlag));
    body = compressor.decompress(body);
    sysFlag &= ~MessageSysFlag.COMPRESSED_FLAG;
}
```

所以业务消费回调默认看到的是原始 body。关闭自动解压后，客户端会保留压缩 body 和
`COMPRESSED_FLAG`，调用方需要自行按照算法位处理。

### 10.6 配置与边界

Producer 配置位于
[`DefaultMQProducer`](../client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java)：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `compressMsgBodyOverHowmuch` | `4096` bytes | 原始 body 大小大于等于该值时尝试压缩 |
| `compressType` | `ZLIB` | 支持 LZ4、ZSTD、ZLIB |
| `compressLevel` | `5` | 传给所选 compressor；LZ4 当前不使用该参数 |

`decodeDecompressBody` 属于消费端配置，定义在
[`ClientConfig`](../client/src/main/java/org/apache/rocketmq/client/ClientConfig.java)
（系统属性 `com.rocketmq.decompress.body`）：

`compressType` 和 `compressLevel` 也可分别通过 JVM 属性
`rocketmq.message.compressType`、`rocketmq.message.compressLevel` 设置；Consumer 自动解压
可通过 `com.rocketmq.decompress.body` 设置。

还要注意以下边界：

- `MessageBatch` 当前不走这套压缩逻辑，包括显式 Batch 和最终形成 `MessageBatch` 的
  autoBatch 请求。
- Producer 的默认 `4 MiB` 大小限制在压缩前检查，因此不能依靠压缩绕过原始消息限制。
- Broker 存储侧检查的是实际收到的 body 和整条 CommitLog 记录大小；对标准 Java Client
  来说，此时 body 已经是压缩后的字节。

---

## 十一、事务消息存储

### 11.1 事务消息流程

RocketMQ 事务消息采用两阶段提交（2PC）模式：

```
阶段一：Prepare
    Producer ──发送 prepared 消息──→ Broker
                                      │
                                      ↓
        保存真实 Topic/queueId，事务位重置为 NOT，改写到 Half Topic queue 0
                                      │
                                      ↓
                    CommitLog + Half Topic 的 ConsumeQueue
                                      │
                                      ↓
                         Producer 收到确认后执行本地事务

阶段二：Commit/Rollback
    Producer ──发送提交/回滚指令──→ Broker
                                      │
                    ┌─────────────────┴─────────────────┐
                    ↓                                   ↓
              Commit 成功                          Rollback 成功
                    │                                   │
                    ↓                                   ↓
      追加一条恢复真实 Topic 的消息             不追加业务消息
                    │                                   │
                    └─────────────┬─────────────────────┘
                                  ↓
                    向 Op Topic 追加处理标记

阶段三：事务回查
    Broker ──定时扫描超时半消息──→ Producer
                                      │
                                      ↓
                              Producer 回查本地事务状态
                                      │
                              ┌────────┴────────┐
                              ↓                 ↓
                         返回 Commit         返回 Rollback
```

### 11.2 事务消息存储结构

**事务标志与 Half Topic**：

Producer 发来的 prepared 消息使用 `TRANSACTION_PREPARED_TYPE`。Broker 的
`parseHalfMessageInner` 在存储前保存真实 Topic 和 queueId，将事务位重置为
`TRANSACTION_NOT_TYPE`，再把消息改写到内部 Half Topic：

```java
public final static int TRANSACTION_NOT_TYPE = 0;
public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;
// 注意：占用 bit2-bit3。bit0 是 COMPRESSED_FLAG，bit1 是 MULTI_TAGS_FLAG，
// bit4-5 是 BORNHOST_V6_FLAG/STOREHOSTADDRESS_V6_FLAG，bit8-10 是压缩算法位
```

**事务消息存储机制**：

RocketMQ 使用内部系统 Topic 存储事务消息，而非独立的事务表文件：

| 系统 Topic | 用途 |
|-----------|------|
| `RMQ_SYS_TRANS_HALF_TOPIC` | 默认存储半消息（未提交的事务消息）；RocksDB Topic 模式改用 `RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC` |
| `RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC` | `transRocksDBEnable=true` 且 `transWriteOriginTransHalfEnable=false` 时存储半消息 |
| `RMQ_SYS_TRANS_OP_HALF_TOPIC` | 存储操作消息（Commit/Rollback 指令） |
| `RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC` | RocksDB 事务 Topic 模式下存储操作消息 |

**存储实现**（[TransactionalMessageBridge.java](../broker/src/main/java/org/apache/rocketmq/broker/transaction/queue/TransactionalMessageBridge.java)）：

- 默认半消息写入 `RMQ_SYS_TRANS_HALF_TOPIC`，并构建这个内部 Topic 的 ConsumeQueue。
  普通业务 Consumer 看不到它，是因为消息不在业务 Topic，而不是因为没有消费索引。当
  `transRocksDBEnable=true` 且 `transWriteOriginTransHalfEnable=false` 时，改写为
  `RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC`，并由 RocksDB 事务存储建立事务索引。
- 默认 Commit/Rollback 指令写入 `RMQ_SYS_TRANS_OP_HALF_TOPIC`；RocksDB 事务 Topic
  模式对应 `RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC`。
- Commit 不会原地修改原 Half 记录，而是追加一条恢复真实 Topic/queueId 的业务消息；
  Rollback 不追加业务消息。两者都会通过 Op Topic 标记该 Half 消息已处理。原 CommitLog
  记录保持不可变，最终随文件过期清理。

**RocksDB 存储支持**：

当配置 `transRocksDBEnable=true` 时，使用 RocksDB 存储事务消息，提供更好的索引性能：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `transRocksDBEnable` | false | 是否启用 RocksDB 事务存储 |
| `transWriteOriginTransHalfEnable` | true | 启用 RocksDB 时是否仍将半消息写入原事务 Topic；设为 false 才使用 RocksDB 事务 Topic |

### 11.3 事务索引构建

**CommitLogDispatcherBuildTransIndex**（[DefaultMessageStore.java](../store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java)）：

ReputMessageService 通过 `CommitLogDispatcherBuildTransIndex` 构建事务索引，仅当启用 RocksDB 事务存储时生效：

```java
class CommitLogDispatcherBuildTransIndex implements CommitLogDispatcher {
    @Override
    public void dispatch(DispatchRequest request) {
        if (DefaultMessageStore.this.messageStoreConfig.isTransRocksDBEnable()) {
            if (null == request || StringUtils.isEmpty(request.getTopic())) {
                return;
            }
            if (!request.getTopic().equals(TopicValidator.RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC) 
                && !request.getTopic().equals(TopicValidator.RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC)) {
                return;
            }
            DefaultMessageStore.this.transMessageRocksDBStore.buildTransIndex(request);
        }
    }
}
```

### 11.4 事务回查机制

**TransactionalMessageCheckService**：

```
定时任务（`transactionCheckInterval`，默认 30 秒）
    │
    ↓
扫描超时半消息（默认超时时间 6 秒）
    │
    ↓
向 Producer 发送回查请求
    │
    ↓
根据回查结果执行 Commit 或 Rollback
```

**配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `transactionTimeout` | 6s | 事务超时时间 |
| `transactionCheckMax` | 15 | 最大回查次数 |
| `transactionCheckInterval` | 30s | 回查间隔 |

---

## 十二、消息过滤机制

### 12.1 Tag 过滤

**设计原理**：Tag 过滤是 RocketMQ 的基础过滤机制，通过比较 Tag 的哈希值快速过滤。

**过滤流程**（主过滤在 Broker 端完成，Consumer 只做最终精确校验）：

```
Consumer 订阅时指定 Tag
    │
    ↓
Broker 拉取消息时读取 ConsumeQueue 索引条目（getMessage 内由
MessageFilter/DefaultMessageFilter 执行）
    │
    ↓
比较索引条目中的 Tag HashCode（tagsCode）
    │
    ├── 匹配 ──→ 读取 CommitLog 中的完整消息并返回
    │
    └── 不匹配 ──→ 跳过，消息根本不会返回给 Consumer
    │
    ↓
Consumer 收到消息后按 Tag 字符串精确校验（Hash 可能碰撞），不匹配则丢弃
```

**Tag Hash 计算**（[MessageExtBrokerInner.java](../common/src/main/java/org/apache/rocketmq/common/message/MessageExtBrokerInner.java)）：

```java
public static long tagsString2tagsCode(final TopicFilterType filter, final String tags) {
    if (Strings.isNullOrEmpty(tags)) { return 0; }
    return tags.hashCode();
}
```

**特点**：

- **Broker 端过滤**：减少无效网络传输
- **高性能**：仅比较 8 bytes 的 Hash 值
- **局限性**：仅支持精确匹配，不支持模糊查询或范围查询

### 12.2 SQL92 过滤

**设计原理**：SQL92 过滤允许 Consumer 使用 SQL 表达式过滤消息，支持更复杂的过滤条件。

**支持的 SQL 语法**：

```sql
-- 基本比较
tag = 'tagA' AND (a > 100 OR b < 200)

-- 范围查询
num BETWEEN 100 AND 200

-- 空值判断
key IS NOT NULL

-- 字符串匹配
name LIKE '%rocket%'

-- IN 列表
status IN ('SUCCESS', 'FAILED')
```

**SQL 过滤流程**：

```
Consumer 订阅时指定 SQL 表达式
    │
    ↓
Pull 消息时先读取 ConsumeQueue 索引
    │
    ↓
读取 CommitLog 中的完整消息
    │
    ↓
使用 SQL 表达式评估消息属性
    │
    ├── 匹配 ──→ 返回给 Consumer
    │
    └── 不匹配 ──→ 跳过
```

**配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `enablePropertyFilter` | false | 是否启用属性过滤（SQL92） |

### 12.3 ConsumeQueueExt 扩展过滤

**设计原理**：ConsumeQueueExt 是 ConsumeQueue 的扩展索引，存储额外的过滤信息，减少 CommitLog 读取次数。

**扩展索引内容**：

- Tag 位图（Tag Bitmap）
- 消息属性的部分信息

**配置参数**：

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| `enableConsumeQueueExt` | false | 是否启用扩展索引 |

---

## 十三、版本差异说明

### 13.1 4.x vs 5.x 核心差异

| 特性 | RocketMQ 4.x | RocketMQ 5.x |
|-----|-------------|-------------|
| **延时消息** | 18 个固定延迟级别 | 支持任意延迟时间（TimerWheel） |
| **存储引擎** | 仅默认文件存储 | 支持 RocksDB 插件 |
| **消息过滤** | Tag + SQL92 | 支持更复杂的过滤表达式 |
| **事务消息** | 基础 2PC | 增强事务回查机制 |
| **存储优化** | 基础冷热分离 | 多级冷热分离 + 压缩优化 |
| **一致性协议** | Master/Slave（4.5 起支持 DLedger Raft） | 延续 DLedger，新增 Controller 自动主从切换 |

### 13.2 延时消息差异

**RocketMQ 4.x**：

```
延迟级别定义（固定 18 级）：
1s, 5s, 10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h

实现方式：
- 延时消息写入特定的 SCHEDULE_TOPIC_XXXX
- 每个延迟级别对应一个 Queue
- Timer 线程定时扫描并投递到真实 Topic
```

**RocketMQ 5.x**：

```
实现方式：
- 使用 TimerWheel + TimerLog 存储
- 支持任意延迟时间
- 时间精度可配置（默认 1000ms）
- 支持最大延迟 3 天（可配置）
```

### 13.3 存储引擎差异

**RocketMQ 5.x 新增 RocksDB 支持**：

| 组件 | 默认存储 | RocksDB 存储 |
|-----|---------|-------------|
| CommitLog | 顺序文件 | 不支持 |
| ConsumeQueue | 定长索引文件 | 支持 |
| IndexFile | Hash 索引文件 | 支持 |
| TimerLog | 时间轮文件 | 支持 |
| Transaction | 系统 Topic（默认 `RMQ_SYS_TRANS_HALF_TOPIC`；RocksDB Topic 模式为 `RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC`） | 支持 |

---

## 十四、监控指标与调优

### 14.1 核心监控指标

**存储指标**：

| Broker Runtime Key | 说明 |
|-----|------|
| `commitLogDiskRatio` | CommitLog 所在磁盘使用率 |
| `consumeQueueDiskRatio` | ConsumeQueue 所在磁盘使用率 |
| `dispatchBehindBytes` | CommitLog 尚未分发到消费索引的字节数 |
| `commitLogMaxOffset` / `commitLogMinOffset` | CommitLog 有效物理偏移范围 |

**写入指标**：

| Broker Runtime Key | 说明 |
|-----|------|
| `putTps` | 近期消息写入 TPS |
| `putMessageTimesTotal` / `putMessageFailedTimes` | 累计写入与失败次数 |
| `putMessageSizeTotal` | 累计写入字节数 |
| `putLatency99` / `putLatency999` | 写入耗时 P99/P99.9 |
| `putMessageEntireTimeMax` | 统计窗口内最大写入耗时 |

**读取指标**：

| Broker Runtime Key | 说明 |
|-----|------|
| `getFoundTps` / `getMissTps` / `getTotalTps` | 拉取命中、未命中和总 TPS |
| `getTransferredTps` | 实际传输给客户端的消息 TPS |
| `getMessageEntireTimeMax` | 统计窗口内最大读取耗时 |

这些名称来自当前 `StoreStatsService#getRuntimeInfo()` 和 `DefaultMessageStore#getRuntimeInfo()`，
可通过 `mqadmin brokerStatus` 查看。告警阈值应按磁盘容量、消息大小和业务 SLO 建立基线，
源码没有通用的固定 P99 告警线。

### 14.2 性能调优建议

**写入性能调优**：

1. **使用 TransientStorePool**：高并发场景开启，减少 GC 压力
2. **异步刷盘**：非金融场景使用 ASYNC_FLUSH，提升吞吐量
3. **消息压缩**：消息体超过 4KB 时自动压缩，减少磁盘 IO
4. **预分配预热**：开启 `warmMapedFileEnable`，减少首次访问延迟
5. **多路径存储**：多磁盘环境配置多个 CommitLog 路径

**读取性能调优**：

1. **Tag 过滤**：合理设计 Tag，减少无效消息传输
2. **批量拉取**：增大 `pullBatchSize`，减少网络往返
3. **PageCache 优化**：确保足够的 PageCache 空间（建议物理内存的 50%）
4. **冷热分离**：配置合理的冷热数据切换策略

**磁盘 IO 调优**：

1. **使用 SSD**：显著提升随机读写性能
2. **IO 调度算法**：SSD 使用 `deadline` 或 `none`，HDD 使用 `cfq`
3. **文件系统选择**：推荐使用 `ext4` 或 `xfs`
4. **禁用 atime**：减少元数据更新开销

---

## 十五、总结

RocketMQ 存储模型的要点：

1. **混合型存储架构**：CommitLog 集中存储 + ConsumeQueue/IndexFile 索引分离，最大化顺序写效率
2. **零拷贝技术**：Mmap 内存映射减少数据拷贝，TransientStorePool 堆外内存优化
3. **异步索引构建**：ReputMessageService 异步构建索引，不影响写入性能
4. **多级可靠性保障**：同步/异步刷盘 + Master/Slave 复制 + DLedger Raft 协议
5. **灵活的存储管理**：支持多路径、冷热分离、过期删除、RocksDB 插件
6. **完善的恢复机制**：StoreCheckpoint + 魔数/CRC 校验 + 索引重建
