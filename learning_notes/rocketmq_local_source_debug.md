# RocketMQ 本地源码 Debug

本文说明如何在 IDEA 中启动最小单机链路。所有项目路径都使用 IDEA 的
`$PROJECT_DIR$` 宏，因此仓库换目录或换设备后不需要修改绝对路径。

> `$PROJECT_DIR$` 只能填写在 IDEA Run Configuration 中，由 IDEA 在运行时展开；
> 不要把它写进 `broker.conf`，也不要直接在 Shell 中使用。

## 1. 需要启动什么

按以下顺序启动：

| 顺序 | 程序 | 是否必需 | 作用 |
| --- | --- | --- | --- |
| 1 | NameServer | 是 | 保存 Broker 和 Topic 路由 |
| 2 | Broker | 是 | 接收、存储和投递消息 |
| 3 | Consumer | 否 | 验证消费消息 |
| 4 | Producer | 否 | 验证发送消息 |

基础源码调试不需要启动 Proxy、Controller 或 DLedger。

## 2. 准备项目

使用 IDEA 打开仓库根目录的 `pom.xml`，项目及所有 Run Configuration 都选择
**JDK 8**。如果使用其他 JDK 编译时出现 `package sun.misc does not exist`，先检查：

- `File -> Project Structure -> Project SDK` 是否为 JDK 8。
- Maven Runner 使用的 JRE 是否为 Project SDK 或 JDK 8。
- Run Configuration 使用的 JRE 是否为 Project SDK 或 JDK 8。

首次导入或切换分支后，在仓库根目录执行：

```shell
mvn -pl namesrv,broker,example -am -DskipTests compile
```

## 3. Broker 存储目录

无需配置存储路径，也无需修改
[`distribution/conf/broker.conf`](../distribution/conf/broker.conf)。Broker 默认把消息数据写入：

```text
~/store
```

这里的 `~` 是运行 Broker 的 Linux/WSL 用户主目录，不是仓库目录。同一时间只启动
一个使用该目录的 Broker，避免出现存储目录被锁定的问题。

`ROCKETMQ_HOME` 不是消息存储目录。源码调试时在 IDEA 中把它设置为：

```text
ROCKETMQ_HOME=$PROJECT_DIR$
```

## 4. NameServer

在 `Run -> Edit Configurations -> + -> Application` 中创建配置：

| 配置项 | 值 |
| --- | --- |
| Name | `NamesrvStartup` |
| JRE | `Project SDK (JDK 8)` |
| Main class | `org.apache.rocketmq.namesrv.NamesrvStartup` |
| Use classpath of module | `rocketmq-namesrv` |
| Program arguments | 留空 |
| Working directory | `$PROJECT_DIR$` |
| Environment variables | `ROCKETMQ_HOME=$PROJECT_DIR$` |

启动入口：
[`NamesrvStartup.java`](../namesrv/src/main/java/org/apache/rocketmq/namesrv/NamesrvStartup.java)。

使用 Debug 启动，看到以下日志表示成功：

```text
The Name Server boot success. serializeType=JSON, address 0.0.0.0:9876
```

## 5. Broker

NameServer 启动成功后，再创建一个 `Application` 配置：

| 配置项 | 值 |
| --- | --- |
| Name | `BrokerStartup` |
| JRE | `Project SDK (JDK 8)` |
| Main class | `org.apache.rocketmq.broker.BrokerStartup` |
| Use classpath of module | `rocketmq-broker` |
| Program arguments | `-n 127.0.0.1:9876 -c $PROJECT_DIR$/distribution/conf/broker.conf` |
| Working directory | `$PROJECT_DIR$` |
| Environment variables | `ROCKETMQ_HOME=$PROJECT_DIR$` |

参数说明：

- `-n` 指定 NameServer 地址。
- `-c` 使用仓库自带的 Broker 配置。

启动入口：
[`BrokerStartup.java`](../broker/src/main/java/org/apache/rocketmq/broker/BrokerStartup.java)。

使用 Debug 启动，看到以下日志表示成功：

```text
The broker[broker-a, ...:10911] boot success
```

## 6. Consumer

创建一个 `Application` 配置：

| 配置项 | 值 |
| --- | --- |
| Name | `Consumer` |
| JRE | `Project SDK (JDK 8)` |
| Main class | `org.apache.rocketmq.example.quickstart.Consumer` |
| Use classpath of module | `rocketmq-example` |
| Program arguments | 留空 |
| Working directory | `$PROJECT_DIR$` |
| Environment variables | `NAMESRV_ADDR=127.0.0.1:9876` |

启动后看到以下日志表示 Consumer 已就绪：

```text
Consumer Started.
```

## 7. Producer

创建一个 `Application` 配置：

| 配置项 | 值 |
| --- | --- |
| Name | `Producer` |
| JRE | `Project SDK (JDK 8)` |
| Main class | `org.apache.rocketmq.example.quickstart.Producer` |
| Use classpath of module | `rocketmq-example` |
| Program arguments | 留空 |
| Working directory | `$PROJECT_DIR$` |
| Environment variables | `NAMESRV_ADDR=127.0.0.1:9876` |

Producer 会向 `TopicTest` 发送消息。Consumer 控制台出现 `Receive New Messages`
表示完整链路正常。

## 8. 启停顺序

启动顺序：

```text
NameServer -> Broker -> Consumer -> Producer
```

停止顺序：

```text
Producer -> Consumer -> Broker -> NameServer
```

## 9. 换设备后直接使用

`$PROJECT_DIR$` 会自动指向当前设备上的仓库根目录，因此不需要同步任何本机绝对路径。
新设备只需安装 JDK 8，并用 IDEA 重新导入 Maven 项目。

需要把上述 Run Configuration 一起提交到 Git 时，在每个配置中启用
`Store as project file`。IDEA 会把配置保存到仓库的 `.run/` 目录，再正常提交这些
`*.run.xml` 文件即可。不要提交 `.idea/workspace.xml`，它包含本机工作区状态。

## 10. 常见问题

### 提示没有设置 `ROCKETMQ_HOME`

确认 NameServer 和 Broker 各自的 Run Configuration 都设置了：

```text
ROCKETMQ_HOME=$PROJECT_DIR$
```

只在 IDEA Terminal 中设置不会自动传给 Run Configuration。

### Producer 报 `No route info of this topic`

依次确认 NameServer 已监听 `9876`、Broker 已成功注册，以及 Producer 配置了
`NAMESRV_ADDR=127.0.0.1:9876`。

### 端口被占用

NameServer 默认使用 `9876`，Broker 默认使用 `10911`（主 Remoting Server）、
`10909`（fast Remoting Server，`listenPort - 2`）和 `10912`（HA 复制端口，
`haListenPort` 或 listenPort+1；`BrokerStartup` 默认将主端口设为 10911）：

```shell
ss -ltnp | rg ':(9876|10909|10911|10912)\b'
```

停止占用端口的旧进程后再启动。

### Broker 报存储目录被锁定

默认的 `~/store` 同一时间只能由一个 Broker 使用。确认旧 Broker 已完全停止后再启动。

仓库内的官方说明见
[`docs/cn/Debug_In_Idea.md`](../docs/cn/Debug_In_Idea.md)。
