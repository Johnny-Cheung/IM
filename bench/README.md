# bench — WebSocket 长连接压测

独立于后端模块的压测工具集（自己的 `go.mod`），用于验证服务端维持 WebSocket 长连接的容量。

**当前进度**：仅完成 **WS 长连接**一项。QPS、端到端投递与持久化链路的压测留到后续。

## 为什么不用 k6

最初用 k6 写过，60,000 VU 时被 OOM Killer 杀掉（退出码 137）。实测 k6 每个 VU 要独立的 JS 运行时，约 **213–254 KiB/连接**（`--compatibility-mode=base` 只省 3.5%，`GOGC=20` 省 16%，都不够）；60,000 连接就要 12 GB 以上。

Go 的 goroutine 每连接只需约 **21 KiB**，同样规模内存需求低一个数量级，而且能精确控制源 IP 绑定、建连速率和 ping/pong 处理。所以这一套全部用 Go 写，与仓库技术栈一致。

## 目录

```
bench/
├── go.mod                 独立模块，不参与后端构建
├── internal/bench/
│   └── client.go          共用的 HTTP 登录/注册/会话校验
└── cmd/
    ├── register/          批量注册压测账号（-start/-count，可重入）
    ├── warmup/            批量预登录，把 cookie 写入文件
    ├── wsload/            WS 长连接压测主体
    └── cleanup/           按前缀或编号区间清理账号及关联数据（含自校验）
```

四个命令都支持 `-base`（默认 `http://127.0.0.1:8080`）与 `-prefix`（默认 `bench`），账号名统一为 `<prefix>_<编号>`。

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

### 4. 清理

```bash
DSN='im_app:<口令>@tcp(127.0.0.1:13308)/im'
go run ./cmd/cleanup -dsn "$DSN" -prefix bench -start 300000 -count 1460
```

按外键依赖从叶子到根删 12 张表，删完自校验，删不干净会以非零码退出。

**优先用 `-start`/`-count` 按编号区间清理**，而不是只给 `-prefix`。因为 `-prefix bench` 会匹配 `bench_3`、`bench_30`、`bench_300`…… 任何以 `bench_` 开头的账号——如果库里同时有 `bench_0..199999` 的正式数据和试跑留下的 `bench_300000+`，前缀清理会把不该删的一起删掉。编号区间与 `register`/`warmup` 的 `-start`/`-count` 语义一致，精确且不会误伤。

先用 `-dry-run` 看清会匹配到多少个再动手。

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

## 已知服务端问题：并发注册全新账号会死锁

**这不是压测工具的缺陷，是服务端 `CreateSession` 在 REPEATABLE READ 下的固有行为**，这里如实记录，因为它是这轮压测发现的问题之一。

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

### 严重性

`CreateUser` 是单条 autocommit INSERT，先于 `startSession` 提交。所以 500 响应背后，**users 行其实已经落库**——表现为「账号已存在，但没有 session」。压测时 10 并发下约 8% 概率命中。

### 服务端侧根治方案（尚未实施）

把 `CreateSession` 的事务改为 **READ COMMITTED**：

```go
tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
```

RC 下 UPDATE 不加 gap 锁，根源消失；而「同一用户并发登录要串行」的语义仍由事务开头的 `SELECT ... FOR UPDATE` 锁 users 行保证，不受影响。已实测验证：同一并发序列在 RC 下**无任何锁等待**，两条都插入成功。

**当前决定是暂不改动服务端**，只是在 `cmd/register` 侧做幂等规避（见下）。

### 压测工具侧的规避

`cmd/register` 收到 5xx 后**不直接判定失败**，而是用该账号登录探一次：

- 登录成功 → 账号其实已建，计入「500 后证实已建」，**不重试注册**（重试只会得到 409）
- 登录失败 → 真正失败，带**随机抖动**重试（固定退避没用：并发全部撞在同一次死锁上时会同时醒来再撞）

实测效果：400 个全新账号并发 10 注册，**确实触发了死锁**（InnoDB 记录时间落在运行窗口内、签名一致），但脚本报 **0 失败**。