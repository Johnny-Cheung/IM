# bench — 压测工具集

独立于后端模块的压测工具（自己的 `go.mod`），覆盖两类负载：

| 阶段 | 测什么 | 状态 |
| --- | --- | --- |
| **一、WS 长连接** | 服务端能维持多少条并发长连接 | ✅ 已完成，跑到 **20 万连接** |
| **二、消息 QPS 与端到端投递** | 私聊消息的接纳吞吐、异步链路能否跟上、有没有丢/重 | ✅ 已完成，异步链路瓶颈已修复并验证（~12,000–14,800/s） |

## 为什么不用 k6

最初用 k6 写过，60,000 VU 时被 OOM Killer 杀掉（退出码 137）。实测 k6 每个 VU 要独立的 JS 运行时，约 **213–254 KiB/连接**（`--compatibility-mode=base` 只省 3.5%，`GOGC=20` 省 16%，都不够）；60,000 连接就要 12 GB 以上。

Go 的 goroutine 每连接只需约 **21 KiB**，同样规模内存需求低一个数量级，而且能精确控制源 IP 绑定、建连速率和 ping/pong 处理。所以这一套全部用 Go 写，与仓库技术栈一致。

## 目录

```
bench/
├── go.mod                 独立模块，不参与后端构建
├── internal/bench/
│   └── client.go          共用的登录/注册/会话/好友/发消息原语
└── cmd/
    ├── register/          批量注册压测账号（-start/-count，可重入）
    ├── warmup/            批量预登录，把 cookie 写入文件
    ├── pair/              账号两两配对成好友（消息压测的前置）
    ├── wsload/            WS 长连接压测主体
    ├── qpsload/           消息 QPS 压测主体
    └── cleanup/           按前缀或编号区间清理账号及关联数据（含自校验）
```

各命令都支持 `-base`（默认 `http://127.0.0.1:8080`）与 `-prefix`（默认 `bench`），账号名统一为 `<prefix>_<编号>`。

## 前置条件

服务端跑起来，且 4 个依赖在位。本机开发用 Compose：

```bash
docker compose up -d          # MySQL 13308 / Redis 16380 / RabbitMQ 5674
```

构建（模块独立，**在 `bench/` 目录内**执行）：

```bash
cd bench
GOPROXY=https://goproxy.cn,direct go build ./...
```

## 用法

两部分共用 `register`（造账号）。下按阶段分述。

## 第一阶段：WS 长连接

### 1. 注册账号

```bash
go run ./cmd/register -count 200000 -prefix bench -concurrency 10
```

约 **1 万个/分钟**（实测：2000 个用 12.7 秒；受服务端 bcrypt cost 10 拖慢，10 并发已接近本机瓶颈，再提高并发收益很小）。可重入：已存在的账号计为「已存在」，不报错。分批注册用 `-start` 指定起始编号。

### 2. 预热登录

```bash
go run ./cmd/warmup -count 200000 -prefix bench -out /tmp/im-cookies.txt
```

把 cookie 写入文件后，建连阶段就不再发登录请求——否则 bcrypt（cost 10，实测单独登录一次约 **53ms**）的开销会和建连压力搅在一起，测出来的就不是「维持连接的能力」。

在本机做 2000 连接的 A/B 对照（逐登录 vs 预热 cookie，其余参数相同），**墙钟耗时几乎一样**（33.05s vs 33.15s）——因为 `-ramp` 窗口主导了总时长，这个粒度分辨不出差异。登录开销真正会造成麻烦是在**高并发**下：早先用 k6 跑到 6 万连接时，逐登录模式下 `ws_connecting` 的 p95 劣化到 **8.24 秒**、max 31.49 秒（该数字来自 k6 的指标，与本报工具的口径不同，仅作量级参考）。

> **陷阱**：每次登录都会撤销该账号原有的活动会话（服务端 `CreateSession` 会废除旧会话）。所以预热之后**再对同一批账号登录**（包括跑 `wsload -prelogin`、或再跑一次 `warmup`），先前文件里的 cookie 就全部失效了。预热完直接压测。

`wsload` 会读文件并**自动探测过滤掉失效条目**，但过滤不回来的部分会真实减少可用连接数——条件允许时优先用 `wsload -prelogin`（cookie 只留在内存，无外部文件依赖）。

### 3. 压测

```bash
go run ./cmd/wsload \
  -count 200000 -prelogin -prefix bench \
  -src-ips 127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5,127.0.0.6,127.0.0.7,127.0.0.8,127.0.0.9,127.0.0.10 \
  -ramp 300s -hold 60s -retries 5
```

关键参数：

| 参数 | 说明 |
| --- | --- |
| `-count` | 目标连接数 |
| `-src-ips` | 源 IP 列表，连接按序号轮询分配 |
| `-ramp` | 建连错开窗口；全部连接由全局限速器平滑放行 |
| `-hold` | 全部连上后统一保持的时长（绝对截止时刻，保证有「全部同时在线」的窗口） |
| `-rate` | 每秒建连上限；0 表示按 ramp 均摊 |
| `-retries` | 拨号失败重试次数（指数退避；4xx 不重试） |

**为什么必须多源 IP**：单个源 IP 的临时端口上限是 64,512，一个源 IP 超过这个数就连不上了。`127.0.0.0/8` 整个回环网段可用（约 1600 万个地址），每个源 IP 有独立的端口空间，所以单机可以建立远超 64,512 的连接。

**注意**：QPS 压测**不需要**多源 IP——它只开几条连接反复复用，单 IP 足够。多源 IP 是长连接为了绕开端口上限才必需的。

## 清理测试数据

```bash
DSN='im_app:<口令>@tcp(127.0.0.1:13308)/im'
go run ./cmd/cleanup -dsn "$DSN" -prefix <前缀>
```

按外键依赖从叶子到根删 12 张表（含 `messages`、`friendships`、`friend_requests`），删完自校验，删不干净会以非零码退出。

**优先用 `-start`/`-count` 按编号区间清理**，而不是只给 `-prefix`。因为 `-prefix bench` 会匹配 `bench_3`、`bench_30`、`bench_300`…… 任何以 `bench_` 开头的账号——如果库里同时有 `bench_0..199999` 的正式数据和试跑留下的 `bench_300000+`，前缀清理会把不该删的一起删掉。编号区间与 `register`/`warmup` 的 `-start`/`-count` 语义一致，精确且不会误伤。同理 `-prefix ratetest` 会连 `ratetest2_*` 一起匹配掉（实测踩过）。

先用 `-dry-run` 看清会匹配到多少个再动手。

### 顺序有个坑：必须等异步链路排空再删账号

**实测踩到过，症状是 consumer 被卡死。**

`messages` 对 `users` 有 `ON DELETE CASCADE` 外键。如果在**异步链路还没排空**（队列里还有消息）时就删掉账号，那些消息落库时会因 `fk_messages_sender` 找不到用户而失败：

```
dead-letter direct message id=... after 6 attempts:
  persist message: Error 1452 (23000): Cannot add or update a child row:
  a foreign key constraint fails (im.messages, CONSTRAINT fk_messages_sender)
```

后果是**队列里每一条孤儿消息都要重试 6 次才进死信**，而改造前的 consumer 是 `Qos(1)` 串行的——于是整条消费链路被这些注定失败的消息**堵住**（实测卡了 5 分钟以上，队列积压 26,284 条）。

**正确顺序**：

```bash
# 1. 先确认异步链路排空。注意 /ops/infra 的 unrelayed 只反映 Redis Stream，
#    还要单独看 RabbitMQ 队列深度
curl -s http://127.0.0.1:8080/ops/infra | grep -o '"unrelayed":[0-9]*'
docker exec im-local-rabbitmq-1 rabbitmqctl list_queues name messages \
  | grep -E "persist|dead"      # persist 队列必须是 0

# 2. 再清理账号
go run ./cmd/cleanup -dsn "$DSN" -prefix <前缀>

# 若已经产生了孤儿消息，直接清空队列：
docker exec im-local-rabbitmq-1 rabbitmqctl purge_queue im.v12.messages.persist
docker exec im-local-rabbitmq-1 rabbitmqctl purge_queue im.v12.messages.dead
```

> v1.9 批量化后异步链路排空快得多（实测 ~12,000–14,800/s），压测产生的积压通常在清理前就已消化，这个坑不容易再触发；但顺序仍应按上面来——尤其在大规模压测后。

## 消息 QPS 与端到端投递（第二阶段）

### 链路与压测位置

发消息的同步路径与异步路径是分开的，压测只压前半段，后半段靠观测：

```text
客户端 ──POST /api/conversations/:username/messages──▶ Redis Lua 原子接纳 ──▶ 201 返回
                                                              │
                                                     （同步到此为止）
                                                              ▼
                                          Redis Stream ──relay──▶ RabbitMQ ──consumer──▶ MySQL
                                          └──────────── 异步，需要单独观测 ────────────┘
```

**关键**：`201` 只表示消息被 Redis **接纳**，此时还没落库。所以 `qpsload` 测出的是**接纳吞吐**，不等于持久化吞吐。跑完必须查 `/ops/infra` 确认异步链路排空，否则测到的只是一段积压。

**为什么走 HTTP 而不是 WS**：当前发消息入口是 HTTP；WebSocket 只承载 `focus`/`blur` 等已读信号，不再接受发消息（旧版压测走 WS，路径已变，两者的 QPS **不可直接对比**）。

### 用法

```bash
# 1. 注册账号、配好友（好友关系会投影进 Redis，Lua 靠它做 not_friends 校验）
go run ./cmd/register -count 100 -prefix qps -concurrency 10
go run ./cmd/pair -pairs 50 -prefix qps -out /tmp/qps-pairs.txt

# 2. 压测
go run ./cmd/qpsload -conns 10 -duration 15s -pairs /tmp/qps-pairs.txt -prefix qps
```

| 参数 | 说明 |
| --- | --- |
| `-conns` | 并发连接数，每条是一条 keep-alive HTTP 连接 |
| `-duration` | 压测时长 |
| `-pairs` | 好友对文件：`发送方序号<TAB>接收方序号` |
| `-timeout` | 单请求超时 |

**每个连接走 `NewKeepAliveClient`**（与长连接压测相反）。QPS 压测要测消息吞吐，如果每条请求都新建 TCP，测的就是握手开销了。

**`client_message_id` 每条唯一**：服务端按它去重，同 ID 同内容算 duplicate（返回 200 且不新建）。复用 ID 会让 QPS 虚高——工具会把去重命中数单独列出，非零即报错（此时数字不可信）。

### 异步链路验收（必做）

压测结束后用 `/ops/infra` 确认，三项都要归零：

| 指标 | 含义 | 期望 |
| --- | --- | --- |
| `pipeline.direct.unrelayed` | Stream 里还没转发给 RabbitMQ 的条数 | **0** |
| `pipeline.direct.pending` | relay 读了但没 ack 的条数 | **0** |
| `pipeline.direct.dead_letter_count` | 死信数 | **0** |

再加一层数据核对（`unrelayed=0` 只说明转发出去了，不代表落库）：

```bash
# 接纳数（压测输出）应等于 MySQL 行数
docker exec im-local-mysql-1 mysql -uroot -proot123 --default-character-set=utf8mb4 -N -B \
  -e "SELECT COUNT(*) FROM im.messages;"

# seq 连续性：按会话对分组，每个会话的 seq 应从 1 连续无空洞
# （注意 seq 是会话内双向共享的，要 LEAST/GREATEST 合并两个方向再判断）
```

### 实测结果（100 账号 / 50 对好友）

**改造前**（异步链路只有 198/s，会严重积压）：

| 连接数 | QPS(201) | P50 | P95 | P99 | 失败 | 去重 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,461 | 0.76 ms | 1.01 ms | 1.25 ms | 0 | 0 |
| 10 | 9,319 | 1.00 ms | 1.65 ms | 2.12 ms | 0 | 0 |

端到端一致性（排空后核对）：累计接纳 100,507 条 → MySQL 恰好 **100,507 行**、10 个会话对 **seq 零空洞**、死信 0。**零丢失、零重复**。但**排空花了约 4 分钟**——这就是当时发现的瓶颈，见下节。

**改造后**（v1.9 批量链路，异步已不再是瓶颈）：

| 连接数 | QPS(201) | P50 | P99 | 失败 | 去重 |
| --- | --- | --- | --- | --- | --- |
| 10 | 8,045 | 1.14 ms | 2.61 ms | 0 | 0 |
| 50 | 9,897 | 4.85 ms | 9.65 ms | 0 | 0 |
| 200 | 11,798 | 16.43 ms | 24.56 ms | 0 | 0 |

**异步链路在压测结束时就已排空**（不再需要额外等待），详情见下节。

> 核对必须在**异步链路完全排空之后**做——顺序很重要，见下文「清理顺序有个坑」。

**与旧基线对比**（2026-07-05，旧代码走 WS 发消息）：10 连接时旧版 29,532 QPS、P50 0.23ms，本轮 8,045–9,319 QPS、P50 1.00–1.14ms。低约 3 倍，**主要差异是传输方式**（HTTP/1.1 往返 vs WS 帧），且代码与路径都已变化，**不能据此判断"性能退化了"**。

## 已修复：异步投递链路吞吐不足（v1.9，已实测验证）

**这是第二阶段压测最重要的发现，旧压测完全没覆盖**——旧版只测到「MQ publish」为止，没有 Stream → RabbitMQ → MySQL 这一段。

### 改造前的现象

同步接纳能到 9,300/s，异步链路只能处理 ~200/s，**相差约 47 倍**。压测 10 秒堆积 82,380 条在 Stream（`backlog_alert=true`），之后花约 4 分钟才排空。

### 根因：两段都是「单条串行等 IO」

| 环节 | 改造前 | 实测上限 | 根因 |
| --- | --- | --- | --- |
| **relay** | 逐条 `Publish` → 等 broker 确认 → 再 ack | **386/s** | 持久化消息每条要 fsync 落盘，单条 **2.59 ms** |
| **consumer** | `Qos(1)` + 每条一个 MySQL 事务 | **198/s** | 一次只有 1 条在途，且每条一次 commit |

改造前 `relay.go` 是逐条的（`for … { Publish; AwaitConfirm; ackAndDelete }`），`rabbit_consumer.go` 是 `channel.Qos(1, 0, false)`，消费循环也是单 goroutine 逐条处理。

### 改造方案（v1.9 已实现）

- **relay**：`XReadGroup` 批量读取最多 100 条，整批发布后才等待 `DeferredConfirmation`，全部确认成功后用一个 Lua 脚本批量 `XACK + XDEL`；任何一条失败整批不 ack，保留 pending 等待重投。确认等待有 30 秒超时。direct 和 group 都使用同一套批量逻辑。
- **consumer**：每个队列一个 worker，`Qos(100)`，最多 100 条或 10ms 攒批，一个 MySQL 事务多行 `INSERT ... ON DUPLICATE KEY UPDATE id = id`。只有 `RowsAffected == len(messages)` 才 commit；重复、冲突或参数异常回滚并回落现有逐条幂等路径。direct 和 group 同步改造。
- **未启用 consumer 并发**：保持单 worker，靠批量化解决，不按会话分片加 worker。

### 验证结果（2026-11-01，真实链路）

环境：本机裸机服务端（v1.9 二进制）+ 容器化 MySQL 8.4 / Redis / RabbitMQ；100 账号 / 50 对好友。

| 连接数 | 接纳 QPS | P50 | P99 | 失败 | 去重 |
| --- | --- | --- | --- | --- | --- |
| 10 | 8,045 | 1.14 ms | 2.61 ms | 0 | 0 |
| 50 | 9,897 | 4.85 ms | 9.65 ms | 0 | 0 |
| 200 | 11,798 | 16.43 ms | 24.56 ms | 0 | 0 |

**异步链路排空速率**：约 **12,000–14,800/s**（改造前 198/s，**提升约 60–75 倍**）。

最直观的对比是**排空耗时**：

| 场景 | 改造前 | 改造后 |
| --- | --- | --- |
| 10 连接压测产生的 8 万条积压 | 约 **4 分钟** | **压测结束时已排空（0 秒）** |
| 50 连接压测产生的 14.8 万条 | — | **约 12 秒** |
| 200 连接压测产生的 17.7 万条 | — | **约 12 秒** |

**端到端一致性**（每轮都在排空后核对）：累计接纳 **406,049** 条 → MySQL 恰好 **406,049 行**，10 个会话对 **seq 零空洞**，`unrelayed=0 pending=0 in_flight=0`，**死信 0**，RabbitMQ 队列为空。**零丢失、零重复。**

**服务端资源**：200 连接时 CPU 约 32%、RSS 约 48 MB——异步链路不再是瓶颈。

### 结论

批量化把异步持久化从 198/s 提到 ~12,000–14,800/s，**已经超过同步接纳的 9,300–11,800/s**，两者不再有数量级落差。**consumer 单 worker 批量化已经够用，无需增加并发 worker**（这也回避了并发落库会失去「同会话按 seq 顺序落库」的问题）。

### 待补

- 群聊（group）链路的同规模压测——本轮只验证了 direct 链路。
- 故障注入（broker 中断、consumer 崩溃）下的批量 ack 语义。
- 更大规模：1 万账号 / 数千对好友。

### 实测优化空间（改造前的微基准，留作参考）

各环节单独拆开测（基准脚本直接压对应资源，不经完整链路）：

**RabbitMQ 持久化确认**（持久队列 + `DeliveryMode: Persistent`）：

| 方式 | 吞吐 | 相对逐条 |
| --- | --- | --- |
| 逐条等确认（改造前） | 386/s | 1× |
| 流水线 10 条 | 5,077/s | 13× |
| 流水线 50 条 | 12,188/s | 32× |
| **流水线 100 条** | **41,832/s** | **108×** |
| 流水线 500 条 | 63,411/s | 164× |

**MySQL 落库**（`messages` 表，含 `ON DUPLICATE KEY UPDATE`）：

| 方式 | 吞吐 | 相对现状 |
| --- | --- | --- |
| 并发1 × 批量1（改造前） | 351/s | 1× |
| 并发30 × 批量1 | 4,705/s | 13× |
| 并发1 × 批量100 | 15,999/s | 46× |
| 并发8 × 批量100 | **59,489/s** | 169× |
| 并发8 × 批量500 | 97,217/s | 277× |

**批量和并发是两个独立维度、可以叠加**：批量省「每次 IO 的固定开销」，并发省「等待时间」，二者相乘（46× × 13× ≈ 169×）。这两种方式都要先确认瓶颈在哪一环——最初误以为 relay 逐条确认是主因，实测发现 RabbitMQ 确认只要 48µs，真正的大头是**持久化消息落盘**。

### 优化方案

以下方案在 2026-11-01 已落地 relay 批量和 consumer 单 worker 批量；consumer 并发暂未启用，等待真实压测结果决定。下面的吞吐数字是拆开压对应资源的微基准预期，不是当前端到端实测。

**consumer 优先**（收益大、风险低），因为改造前它是**最终瓶颈**——即使 relay 提到 40,000/s，落库仍只有 198/s：

1. `Qos(1)` → `Qos(100)`，先使用单 worker 批量事务；若端到端仍不足，再叠加有限并发 → 微基准约 **5,000/s**
2. 多行批量 `INSERT`（按会话分组）→ 再叠加到微基准约 **~15,000/s**

**relay 其次**（语义要保持，实现要小心）：

3. `Count: 10` → 100，逐条 `Publish+AwaitConfirm` → 批量 `Publish` + 批量等确认 + 整批 ack → 微基准约 **12,000–40,000/s**

**实现注意点**：

- **投递语义可以不变**。当前保证是「未确认绝不 ack」（至少一次投递）。批量化后仍可保持：整批发 → 收齐确认 → **全部成功才批量 ack**；任何一条失败则整批不 ack，交给 `drainPending` 重投。**不做部分 ack**。代价是可能重复投递，但 consumer 的 `ON DUPLICATE KEY UPDATE` 幂等，**重复安全**。
- **`NotifyPublish` 的缓冲只有 1**（`relay.go:123`）。改成流水线却不同步读取这个 channel，会让 `confirms.confirm()` 在 `l <- confirmation` 处阻塞 RabbitMQ 的读取协程，**卡死整条连接**。改用 `PublishWithDeferredConfirmWithContext` 返回的 `DeferredConfirmation.WaitContext` 可绕开；当前实现已经移除 `NotifyPublish` listener，使用 deferred confirmation。
- **`relayStreams` 被两处复用**（新消息与 `drainPending` 重投），批量路径要同时服务这两种场景；当前实现两边都走同一批量函数。
- **consumer 并发会失去「同会话按 seq 顺序落库」**。目前单 goroutine 逐条处理天然保证顺序。最终一致性不受影响（幂等 + `seq` 唯一键），但若有依赖落库顺序的逻辑需先确认。
- **按需优化即可**：同步路径只有 9,300/s 的产出，异步链路优化到 15,000/s 已足够，不必拉满到 85,000/s。

### 批量落库不会踩间隙锁死锁（已实测）

仓库里 `sessions` 和 `friend_requests` 两处会因「无匹配的 UPDATE/INSERT 落在索引末端 supremum 间隙」而死锁（见下文）。**consumer 批量落库不会**，已实测：30 并发单条 INSERT（6,200 行）、多行批量 INSERT（20,000 行）均 **0 死锁**。

原因：那两处是**多语句交叉加锁**（`FOR UPDATE` + `INSERT` 混在一个事务里）才成环；consumer 每事务只有一条或多行 INSERT，纯插入事务之间只会串行等待，不会互相等待成环。

v1.9 最终采用**单 worker + 批量**而非多 worker 并发，所以这个问题在实现里也不存在。

## 复现 20 万连接

以下是一次**实测跑通**的完整参数与结果。

### 环境

| 项 | 值 |
| --- | --- |
| CPU | 24 逻辑核 |
| 内存 | 15 GiB + 8 GiB swap |
| 服务端 | `/tmp/im-server-bare`，`-addr 0.0.0.0:8080` |
| 依赖 | MySQL 8.4.9（13308）、Redis 7.2（16380）、RabbitMQ 3.13（5674）|
| Go | 1.26.2 |

swap 是必需的：15 GiB 物理内存装不下客户端 + 服务端两侧的 20 万连接，实测峰值用掉 **663 MiB** swap。长连接是「冷内存」，被换出后几乎不再访问，所以这一招对这类负载特别合适。

### 步骤

```bash
cd bench
export GOPROXY=https://goproxy.cn,direct

# 1. 注册 20 万个账号（约 20 分钟，本次分 4 批跑以便中途检查）
go run ./cmd/register -count 200000 -prefix bench -concurrency 10

# 2. 压测（-prelogin 在内存里登录 20 万个账号，避免 cookie 文件失效的坑）
go run ./cmd/wsload \
  -count 200000 -prelogin -prefix bench \
  -src-ips 127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5,127.0.0.6,127.0.0.7,127.0.0.8,127.0.0.9,127.0.0.10 \
  -ramp 300s -hold 60s -retries 5
```

### 结果（`-count 200000`）

```
目标连接数:   200000
成功建连:     200000
登录失败:     0
TCP 拨号失败: 0
握手被拒:     0
连接中断:     0
用时:         6m1s
建连延迟:     p50=382µs p95=600µs p99=912µs max=24.266ms
```

服务端侧：fd **200,022**（1:1，无额外消耗）、内存约 **6 GB**、CPU 峰值约 **2.4 核**（共 24 核）。

**服务端始终不是瓶颈**——20 万连接时只用掉 2.4 核。

### 容量阶梯

各档位都是 0 失败，`fd ≈ count + 22`（监听、日志、管道等固定开销）：

| 连接数 | 服务端 fd | 服务端内存 | CPU | 失败 |
| --- | --- | --- | --- | --- |
| 40,000 | 39,924 | 1.3 GiB | 166% | 0 |
| 80,000 | 80,022 | 2.5 GiB | 169% | 0 |
| 140,000 | 140,022 | 4.4 GiB | 242% | 0 |
| 200,000 | 200,022 | ~6 GB | ~2.4 核 | 0 |

14 万连接压测结束后服务端内存从峰值 4.4 GiB 回落到 378 MiB，**连接的内存完全释放，无泄漏**。

## 踩过的坑

排查过程中有几次是**测量方法**出错而非代码问题，记录下来避免重蹈：

- **假象「2 倍连接」**：Docker 的 `userland-proxy` 让宿主机 `ss` 每条逻辑连接显示两个 socket。容器内的 fd 才是 1:1。后来服务端改用裸机跑，就绕开了这层。
- **假象「goroutine 泄漏」**：`docker logs` 累积了多次 SIGQUIT 转储，看起来 goroutine 从 66 涨到 135；单独提取一次转储后是 66→69，且**没有一个 WS goroutine**。
- **假象「SYN 风暴导致拨号失败」**：19 万连接时出现 5,934 个拨号失败，一度归因于 SYN 风暴。真实原因是**服务端被 OOM Killer 杀了**（`journalctl -k`：`Out of memory: Killed process (im-server-bare) total-vm:7533224kB, anon-rss:6193896kB`）。建连速率远超实际处理能力时，最后一批连接会集中爆发——改成全局限速器逐个放行后消失。
- **误判 `somaxconn` 限制**：一度以为 `somaxconn=4096` 卡住了建连，但自建的计数器显示 `ListenOverflows=0`，并非这个原因。

## 已知服务端问题：并发建关系会死锁（两处，同一根因）

**这不是压测工具的缺陷，是服务端在 REPEATABLE READ 下的固有行为**，这里如实记录，因为它是这轮压测发现的问题。

**两处已发现，同一类根因，但在不同表**：

| 触发路径 | 表 / 索引 | 发现于 |
| --- | --- | --- |
| 并发**注册**全新账号 | `sessions` / `idx_sessions_user_active` | WS 长连接阶段压测数据准备时 |
| 并发**加好友** | `friend_requests` / `idx_friend_requests_to` | 消息 QPS 阶段配对好友时 |

> **不要以为只有这两处。** 仓库里有 **21 处** `BeginTx(ctx, nil)`（`moments` 5、`groups` 6、`friends` 3、`seq` 2 等），其余未逐一分析。同样的「多语句交叉加锁 + 新行落在索引末端间隙」组合可能还在别处。

### 现象

10 并发注册全新账号，偶发 HTTP 500：

```
create session after register: create session after 3 retries:
  insert session: Error 1213 (40001): Deadlock found when trying to get lock
```

### 成因

`CreateSession` 的事务里第三条语句是：

```sql
UPDATE sessions SET revoked_at = ?, revoke_reason = ?
 WHERE user_id = ? AND revoked_at IS NULL
```

**全新账号没有任何 session 行**，所以这条 UPDATE 匹配不到记录。在 REPEATABLE READ 下，InnoDB 对「没匹配到」的等值查找会退化成**间隙锁**，而且锁在索引 `idx_sessions_user_active` **末尾的 supremum 伪记录**上（实测 `performance_schema.data_locks` 确认）。

关键在于**新注册账号的 user_id 永远是当前最大值**，于是全部落在这同一个间隙里：

| 事务 | 动作 | 状态 |
| --- | --- | --- |
| A | UPDATE 无匹配 → X gap 锁 on supremum | 持有 |
| B | UPDATE 无匹配 → X gap 锁 on supremum | **也持有**（gap 锁彼此相容） |
| A | INSERT → 申请 insert intention | ✗ 被 B 的 gap 锁挡住 |
| B | INSERT → 申请 insert intention | ✗ 被 A 的 gap 锁挡住 |
| — | — | **闭环 → 1213，回滚一方** |

死锁现场（InnoDB status 与线上一致）：

```
index idx_sessions_user_active of table `im`.`sessions`
  0: len 8; hex 73757072656d756d; asc supremum;;
  双方均 lock_mode X insert intention waiting
```

对照实验：**已有活跃会话的相邻用户重新登录不会死锁**（UPDATE 匹配到行，只锁该行，不加 gap 锁）。所以这只在**注册**和**从未登录过的账号首次登录**路径上出现。

### 第二处：并发加好友（`friend_requests`）

配好友时同样偶发 500：

```
friend operation: insert friend request: Error 1213 (40001): Deadlock found ...
```

成因是**同一类**，只是换了表：`CreateFriendRequest`（`store/friends.go:258`）在事务里先 `lockUsers` 锁 users 行，再连续做两条 `SELECT ... FOR UPDATE` 查 `friend_requests`，最后 INSERT。而**两条 FOR UPDATE 查询都走 `idx_friend_requests_to`**（`EXPLAIN` 确认），新插入的行又落在该索引**末尾 supremum 间隙**——于是两个事务各持 gap 锁、各等对方的 insert intention 锁。

死锁现场与第一处同构：

```
index idx_friend_requests_to of table `im`.`friend_requests`
  双方均 lock_mode X on "hex 73757072656d756d; asc supremum;;"
  双方均 lock_mode X insert intention waiting
```

实测：50 对好友 / 8 并发，**9 对失败（18%）**——比注册那处更容易触发，因为配对是「申请 + 接受」两段事务，窗口更长。

### 严重性

`CreateUser` 是单条 autocommit INSERT，先于 `startSession` 提交。所以 500 响应背后，**users 行其实已经落库**——表现为「账号已存在，但没有 session」。压测时 10 并发下约 8% 概率命中。

### 服务端侧根治方案（尚未实施）

对**两处**都适用：把事务改为 **READ COMMITTED**。

```go
// CreateSession（store/sessions.go）
tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
```

RC 下 UPDATE **和** `SELECT ... FOR UPDATE` 都不加 gap 锁，根源消失；而「同一用户并发登录要串行」「同一对用户并发加好友要串行」的语义仍由事务开头的 `SELECT ... FOR UPDATE` 锁 users 行保证，不受影响。已实测验证：`CreateSession` 的同一并发序列在 RC 下**无任何锁等待**，两条都插入成功。

**当前决定是暂不改动服务端**，只在压测工具侧做幂等规避（见下）——服务端的修复留到正式流程中处理。

### 压测工具侧的规避

`cmd/register` 收到 5xx 后**不直接判定失败**，而是用该账号登录探一次：

- 登录成功 → 账号其实已建，计入「500 后证实已建」，**不重试注册**（重试只会得到 409）
- 登录失败 → 真正失败，带**随机抖动**重试（固定退避没用：并发全部撞在同一次死锁上时会同时醒来再撞）

实测效果：400 个全新账号并发 10 注册，**确实触发了死锁**（InnoDB 记录时间落在运行窗口内、签名一致），但脚本报 **0 失败**。

`cmd/pair` 用同样的策略：收到 5xx 后查好友列表确认关系是否已建立（计入「500 后证实已建」）；若服务端返回 409「已有待处理申请」，则把那条申请找出来补一次接受，而不是新建。实测：50 对 / 8 并发，首轮 9 失败；**重跑后 41 对识别为已存在、9 对补齐、0 失败**，且配对文件无残留。