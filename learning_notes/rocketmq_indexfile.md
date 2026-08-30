# RocketMQ IndexFile 工作原理

## 1. IndexFile 是什么

`IndexFile` 是 RocketMQ 为“按消息 Key 查询消息”建立的二级索引文件。它不保存消息正文，只保存：

```text
消息 Key -> CommitLog 物理偏移量
```

按 Key 查询时，先从 IndexFile 找到物理偏移量，再回到 CommitLog 读取消息实体。普通消费者按 Topic/Queue 顺序消费时主要使用 ConsumeQueue，而不是 IndexFile。

当前代码还支持 RocksDB 索引，但传统 IndexFile 是基于内存映射文件的实现。

## 2. 文件位置和大小

文件路径：

```text
$storePath/index/{fileName}
```

文件名使用创建时的时间戳。每个 IndexFile 固定大小，当前默认配置为：

```text
hashSlotNum = 5,000,000              // maxHashSlotNum
indexNum    = 20,000,000             // maxIndexNum，配置中定义为 maxHashSlotNum * 4
```

单文件大小为：

```text
40 + 5,000,000 * 4 + 20,000,000 * 20 = 420,000,040 bytes
```

也就是约 400 MB。`maxIndexNum` 是固定值 2000 万，只调大 `maxHashSlotNum` 时它不会
自动跟随。实现把索引下标 0 作为无效值，因此实际可用索引数约为 `indexNum - 1`。

## 3. 文件结构

```text
IndexFile
├── Header (40 bytes)
│   ├── beginTimestamp  (8 bytes)
│   ├── endTimestamp    (8 bytes)
│   ├── beginPhyOffset  (8 bytes)
│   ├── endPhyOffset    (8 bytes)
│   ├── hashSlotCount   (4 bytes)
│   └── indexCount      (4 bytes)
├── Hash Slot Table
│   └── 每个槽保存对应链表的头部索引下标（4 bytes）
└── Index Data Area
    └── 每条索引项 20 bytes
```

### 3.1 Header

- `beginTimestamp` / `endTimestamp`：该文件第一条和最后一条索引对应的消息存储时间。
- `beginPhyOffset` / `endPhyOffset`：对应消息在 CommitLog 中的起止物理偏移量。
- `hashSlotCount`：实际使用过的哈希槽数量。
- `indexCount`：已经写入的索引数量。

### 3.2 Hash Slot Table

槽表有 `hashSlotNum` 个槽，每个槽 4 字节。槽中保存的是索引区记录的下标，不是字节地址，也不是真实 Key。

### 3.3 Index Data Area

每条索引项占 20 字节：

```text
┌──────────────┬───────────────┬────────────┬────────────────┐
│ Key Hash 4B  │ PhyOffset 8B  │ TimeDiff 4B│ NextIndexPos 4B│
└──────────────┴───────────────┴────────────┴────────────────┘
```

- `Key Hash`：完整索引键的 Java `hashCode()`，转为非负值。
- `PhyOffset`：消息在 CommitLog 中的物理偏移量。
- `TimeDiff`：消息存储时间与本文件 `beginTimestamp` 的差值，单位为秒。
- `NextIndexPos`：同一哈希槽中上一条（更旧的）索引的下标；沿该字段从新到旧遍历链表。

时间只保存差值，是为了节省空间；查询时用 `beginTimestamp + TimeDiff * 1000` 恢复毫秒时间戳。

## 4. 哈希槽和冲突链

索引逻辑类似文件中的 HashMap：

```text
keyHash = hash(key)
slotPos = keyHash % hashSlotNum
```

槽表只保存链表头。写入新索引时，IndexFile 在索引区追加记录，并把新记录插到链表头：

```text
slot[slotPos] -> 最新索引 -> 较旧索引 -> 更旧索引
```

这条链既能处理同一个 Key 对应多条消息，也能处理不同 Key 落到同一槽位的哈希冲突。

注意：索引项只保存哈希值，不保存原始 Key。不同字符串如果发生 Java hash 冲突，存在理论上的误匹配可能；这是该实现的已知边界。

## 5. 索引什么时候构建

消息进入 CommitLog 后，Broker 的 `ReputMessageService` 异步扫描 CommitLog，并通过 `CommitLogDispatcherBuildIndex` 调用 `IndexService.buildIndex`。

因此：

```text
CommitLog 写入成功 != IndexFile 立即可查
```

索引通常会很快跟上，但可能短暂落后于最新消息。

对一条消息，当前实现可能创建以下索引键：

```text
topic#UNIQ_KEY
topic#KEY
topic#INDEX_TAG_TYPE#TAG
```

- `UNIQ_KEY` 存在时，为唯一消息键建立索引。
- `KEYS` 存在时，按空格拆分，每个 Key 单独建立索引。
- `TAGS` 存在时，使用带 `INDEX_TAG_TYPE` 的键建立标签索引。
- 回滚事务消息不会建立该索引；普通、预提交、提交事务消息按当前分支处理。

## 6. 写入一条索引

`IndexFile.putKey` 的核心步骤：

1. 检查当前文件是否已达到 `indexNum`。
2. 计算 `keyHash` 和 `slotPos`。
3. 读取槽表中的旧头指针；非法值（`slotValue <= invalidIndex || slotValue > indexCount`）
   按空链归零处理。
4. 在索引区追加一条 20 字节记录，`NextIndexPos` 字段指向旧头。
5. 将槽表更新为当前索引下标。
6. 更新 Header 的时间、物理偏移和索引计数；其中 `hashSlotCount` 只在原来
   槽为空（新占用一个槽）时递增，begin 字段只在首条索引时设置
   （`IndexFile.java:152-162`）。

伪代码如下：

```java
int keyHash = indexKeyHashMethod(key);
int slotPos = keyHash % hashSlotNum;
int slotValue = mappedByteBuffer.getInt(headerSize + slotPos * 4);
if (slotValue <= invalidIndex || slotValue > indexHeader.getIndexCount()) {
    slotValue = invalidIndex;   // 空链或非法值归零
}

int newIndex = indexHeader.getIndexCount();
int indexPos = headerSize + hashSlotNum * 4 + newIndex * 20;
mappedByteBuffer.putInt(indexPos, keyHash);
mappedByteBuffer.putLong(indexPos + 4, phyOffset);
mappedByteBuffer.putInt(indexPos + 12, timeDiffSeconds);
mappedByteBuffer.putInt(indexPos + 16, slotValue);   // 指向旧头
mappedByteBuffer.putInt(headerSize + slotPos * 4, newIndex);
```

IndexFile 使用 `DefaultMappedFile` 和 `MappedByteBuffer`。写入主要是对内存映射区域进行定点写入；刷盘时调用 `MappedByteBuffer.force()`。

## 7. 查询流程

完整调用链：

```text
QueryMessageProcessor
    -> DefaultMessageStore.queryMessage
    -> IndexService.queryOffset
    -> IndexFile.selectPhyOffset
    -> CommitLog 读取消息正文
```

`IndexService.queryOffset` 从最新 IndexFile 向旧文件遍历：

1. 拼出和写入时一致的索引键，例如 `topic#key`。
2. 对索引键计算哈希并定位槽位。
3. 从槽头开始沿 `NextIndexPos` 字段向更旧的记录遍历。
4. 用 `beginTimestamp + TimeDiff * 1000` 恢复消息时间。
5. 同时满足哈希值和时间范围的记录加入物理偏移量列表。
6. 达到 `maxNum` 或当前文件时间范围不可能命中时停止。
7. 根据这些物理偏移量从 CommitLog 读取消息实体。

查询结果中的物理偏移量会排序后再读取，因此返回消息通常按 CommitLog 顺序处理，而索引链本身是从新到旧遍历的。

## 8. 文件轮换、刷盘和恢复

- 当前 IndexFile 写满后，`IndexService` 创建新的文件。
- 新文件以旧文件的结束物理偏移和结束时间作为起点。
- 创建新文件后，旧文件会异步刷盘。
- 完整文件刷盘后更新 `StoreCheckpoint.indexMsgTimestamp`。
- Broker 启动时按文件名升序加载 IndexFile，并恢复 Header。
- 异常退出时，如果索引文件的结束时间晚于检查点记录的安全索引时间，会删除该文件，避免使用未安全落盘的索引。
- 当最老文件对应的 CommitLog 物理偏移已经过期，IndexService 会删除对应的旧 IndexFile。

## 9. 和其他存储文件的关系

```text
CommitLog       保存消息正文
ConsumeQueue    按 Topic/Queue 建立消费顺序索引
IndexFile       按 Key 建立查询索引
```

一句话总结：

```text
消息 Key
  -> hash slot
  -> NextIndexPos 链（从新到旧）
  -> CommitLog 物理偏移
  -> 读取消息正文
```

## 10. 相关源码

- [`IndexFile.java`](../store/src/main/java/org/apache/rocketmq/store/index/IndexFile.java)
- [`IndexHeader.java`](../store/src/main/java/org/apache/rocketmq/store/index/IndexHeader.java)
- [`IndexService.java`](../store/src/main/java/org/apache/rocketmq/store/index/IndexService.java)
- [`DefaultMessageStore.java`](../store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java)
- [`IndexFileTest.java`](../store/src/test/java/org/apache/rocketmq/store/index/IndexFileTest.java)

