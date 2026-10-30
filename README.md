<div align="center">

# IM

一个通过浏览器访问的 Web 即时通讯项目：私聊、群聊、已读与朋友圈。

[功能](#功能) · [技术栈](#技术栈) · [架构](#架构) · [快速开始](#快速开始) · [文档](#文档)

![Go](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat-square&logo=go&logoColor=00ADD8)
![React](https://img.shields.io/badge/React-19-61DAFB?style=flat-square&logo=react&logoColor=61DAFB)
![TypeScript](https://img.shields.io/badge/TypeScript-5.7-3178C6?style=flat-square&logo=typescript&logoColor=3178C6)
![Vite](https://img.shields.io/badge/Vite-8-646CFF?style=flat-square&logo=vite&logoColor=646CFF)
![MySQL](https://img.shields.io/badge/MySQL-8.4-4479A1?style=flat-square&logo=mysql&logoColor=4479A1)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=flat-square&logo=redis&logoColor=DC382D)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600?style=flat-square&logo=rabbitmq&logoColor=white)
![Web](https://img.shields.io/badge/Platform-Web-0ea5e9?style=flat-square)
![Version](https://img.shields.io/badge/version-v2.0-6366f1?style=flat-square)

</div>

> **当前状态**：v2.0 朋友圈（文字版）已完成并在 `8199` 隔离验证；正式 `8198` 仍运行 v1.7。v1.8.1–v1.8.3 与 v2.0 尚未切换正式实例；v1.9 容量验证计划转到 Ubuntu（未开始）。

## 功能

IM 是一个单体 Web IM：React 前端负责界面与本地缓存，Go 后端提供 REST API 和 WebSocket；MySQL 是权威数据源，Redis 承担消息接纳与热读，RabbitMQ 负责异步持久化。

- **账号与会话**：注册、登录、退出、昵称修改和密码修改。密码使用 bcrypt 哈希；登录后使用有效期 7 天的服务端会话，浏览器只保存 HttpOnly、SameSite=Lax 的会话 Cookie。
- **好友关系**：通过用户名精确查找用户，发送、接受或拒绝好友申请，删除好友。只有好友之间可以私聊；删除好友后不能继续发送新消息，历史保留。
- **私聊**：HTTP 发送、WebSocket 实时接收，服务端接纳后确认、`client_message_id` 重试去重、历史消息分页、未读与已读、断线重连。
- **群聊**：邀请好友建群，成员上限 100 人（含群主）；成员可邀请自己的好友、退出群聊；群主可改名、移出成员，群主退出时群解散。支持群消息历史与群已读人数。
- **已读**：进入会话或停留在前台聊天窗口时，通过 WebSocket `focus` 隐式推进已读水位；后台页面收到消息不自动已读。群聊返回发送者可见的已读人数，不提供逐条回执或成员名单。完整已读能力需要 Redis 和 RabbitMQ。
- **Web 缓存与增量同步**：浏览器使用 IndexedDB，按账号隔离；冷启动通过 `POST /api/bootstrap` 一次拉齐会话、好友和增量；支持离线只读查看缓存。每个会话保留最近 200 条、全账号最多 10000 条，服务端始终是权威。
- **可靠投递**：会话内连续 `seq`、Redis Lua 原子接纳、客户端幂等、Stream → RabbitMQ → MySQL 异步幂等落库、失败重试与死信、Redis/MySQL 双向对账和 Redis 重建。
- **朋友圈（文字版）**：发布 1–1000 字纯文字动态，选择“好友可见”或“仅自己可见”；按时间倒序分页浏览；点赞/取消点赞；单层评论；删除自己的评论或动态；好友发布非私密动态时导航显示红点。
- **响应式 Web**：桌面和手机浏览器共用同一套页面，不提供桌面或移动客户端。

**当前范围之外：** 图片、音视频、文件和头像上传；消息撤回、删除和搜索；拉黑、群主转让；朋友圈图片/相册、隐私名单、评论回复和通知中心；多实例高可用。

## 技术栈

| 层级 | 技术 | 职责 |
| --- | --- | --- |
| 前端 | React 19 · TypeScript 5.7 · Vite 8 · React Router 7 · 原生 CSS | SPA、路由、页面与响应式样式 |
| 本地缓存 | IndexedDB | 会话与消息缓存、冷启动、离线只读 |
| 后端 | Go 1.26 · Gin | REST API、鉴权、业务逻辑与静态资源托管 |
| 实时通信 | Gorilla WebSocket | 消息推送、在线连接、隐式已读 |
| 权威存储 | MySQL 8.4 | 用户、好友、群组、消息、已读水位、朋友圈 |
| 读模型与接纳 | Redis 7.2 · Lua | 消息实体、会话索引、`seq`、未读/已读、幂等 |
| 异步投递 | RabbitMQ 3.13 | Stream relay、持久化队列、重试与死信 |
| 测试 | Go test · Vitest · Node smoke | 单元/集成、前端、API/浏览器/缓存 smoke |
| 交付 | Linux 发布包 · `imctl`/`infractl` · systemd-run | 构建、部署、备份、重建与回滚 |

## 架构

单个后端进程，前端和 API 同源部署；MySQL 是唯一权威数据源，Redis 是可重建的读模型与接纳层。

```text
浏览器（React SPA + IndexedDB）
        │  /api（同源 Cookie） + /api/ws
        ▼
Go 后端（Gin + WebSocket hub）
   │                  │                    │
   │ 接纳 / 热读       │ 异步持久化           │ 朋友圈
   ▼                  ▼                    ▼
Redis 7.2        Stream → relay →       MySQL 8.4
（可重建）         RabbitMQ → consumer ──────┘
```

### 消息发送与读取

1. 浏览器发送消息时带上 `client_message_id`。后端在 Redis Lua 中原子完成权限校验、幂等检查、分配会话内 `seq`，并写入消息实体、会话索引和 Stream。
2. 接纳成功即返回“已发送”。relay 将 Stream 事件投递到 RabbitMQ，consumer 按服务端消息号和客户端幂等键异步落 MySQL；暂时性错误重试，与消息有关的错误进入死信。
3. WebSocket 把新消息推送给在线成员。历史消息按 `seq` 游标读取，优先使用 Redis 热数据，缺失时回退 MySQL。
4. 已读通过 WebSocket `focus` 推进会话水位；群聊按水位计算发送者可见的已读人数。
5. 浏览器用 IndexedDB 和 `POST /api/bootstrap` 完成冷启动与增量同步；网络不可用时只读展示已缓存的消息。

### 朋友圈

朋友圈直接读写 MySQL，不经过 Redis、RabbitMQ 或后台 worker。点赞、评论和冗余计数在同一事务内更新；分页使用自增 ID 游标；可见性由服务端校验。好友发布非私密动态时，复用现有 WebSocket 推送 `moment.created`，前端只更新导航红点。

### 可靠性

- 每个会话的消息号 `seq` 连续，启动时校准，并检测 Redis 重启后的计数器漂移。
- Redis 数据丢失或回滚后，可从 MySQL 执行 `rebuild-redis` 重建完整投影；重建必须停后端，失败遗留 `rebuild:pending` 时后端拒绝启动。
- 双向对账自动补回 Redis 少量缺失数据、抬高偏低的水位；维护锁阻止在半重建状态下启动。
- 持久化失败按错误类型重试或进入死信；`imctl` 提供队列状态、死信查看/重放/丢弃、备份和恢复检查。

## 项目结构

```text
IM/
├── backend/
│   ├── cmd/server/              # 启动入口与 -backfill-seq / -rebuild-redis / -dlq-* 参数
│   ├── internal/
│   │   ├── auth/                # 用户名、密码、会话令牌
│   │   ├── config/              # MySQL / Redis / RabbitMQ / 命名空间配置
│   │   ├── httpapi/             # REST、WebSocket 与业务处理器
│   │   ├── infra/               # Redis、RabbitMQ 连接（MySQL 由 store 打开）
│   │   ├── messaging/           # 接纳、读模型、relay、consumer、对账与重建
│   │   └── store/               # MySQL 模型、查询与内嵌迁移
│   │       └── migrations/
│   ├── go.mod
│   └── go.sum
├── frontend/
│   ├── src/
│   │   ├── cache/               # IndexedDB 缓存与合并
│   │   ├── components/          # 通用组件
│   │   ├── pages/               # 登录、注册、聊天、好友、朋友圈、设置
│   │   ├── api.ts               # REST / WebSocket 客户端
│   │   ├── App.tsx
│   │   └── index.css
│   ├── package.json
│   └── vite.config.ts
├── deploy/
│   ├── imctl                    # 应用启停、备份、回填、重建、死信
│   └── infractl                 # Redis / RabbitMQ 私有基础设施
├── scripts/                     # 构建、smoke、容量验证与测试数据清理
├── docs/
│   ├── roadmap.md
│   ├── operations.md
│   └── versions/
└── README.md
```

## 快速开始

### 环境要求

- Go 1.26+
- Node 26 + npm
- MySQL 8.0+（推荐 8.4）
- 可选：Redis 7.2、RabbitMQ 3.13（启用完整消息链路与已读时需要）

下面的命令以 macOS/Linux shell 为例。

### 1. 准备 MySQL

在可连接的 MySQL 中创建数据库和具有建表/迁移权限的应用账号：

```sql
CREATE DATABASE im CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE USER 'im_app'@'%' IDENTIFIED BY '<your-password>';
GRANT ALL PRIVILEGES ON im.* TO 'im_app'@'%';
```

不要把自己的真实密码写进仓库或命令历史。示例使用 `'%'` 主机；实际部署可按来源收紧到具体地址。

### 2. 启动后端

用密码文件提供应用密码，不要把密码放进命令行或环境变量：

```bash
mkdir -p "$HOME/.config/im"
install -m 600 /dev/null "$HOME/.config/im/mysql-app"
# 用编辑器把应用密码写入上面的文件
export IM_MYSQL_HOST=127.0.0.1
export IM_MYSQL_PORT=3306
export IM_MYSQL_DB=im
export IM_MYSQL_USER=im_app
export IM_MYSQL_PASSWORD_FILE="$HOME/.config/im/mysql-app"
(cd backend && go run ./cmd/server)
```

后端启动时会自动执行迁移，默认监听 `127.0.0.1:8080`；不指定 `-web-dir` 时只提供 API。

### 3. 启动前端开发服务器

开发时用 Vite 代理后端；第 3 步和第 4 步是两种运行方式，按需二选一。

```bash
cd frontend
npm ci
npm run dev
```

浏览器打开 `http://localhost:5173/`。Vite 固定使用 5173，并把 `/api`（含 WebSocket）和 `/health` 代理到 `http://127.0.0.1:8080`。

### 4. 构建前端并由 Go 托管

```bash
(cd frontend && npm ci && npm run build)
(cd backend && go run ./cmd/server -web-dir ../frontend/dist)
```

浏览器打开 `http://127.0.0.1:8080/`。

### 启用完整消息链路（可选）

只配置 MySQL 时是本地模式：发送、历史和会话列表可用，会话消息号在 MySQL 事务中分配；已读回执、`focus` 和群已读人数不可用，未读数仍会从 MySQL 计算并显示，但不会随阅读清零。

完整链路需要配置 Redis 和 RabbitMQ：设置 `IM_REDIS_URL(_FILE)`、`IM_RABBITMQ_URL(_FILE)` 及对应密码文件，设置 `IM_GROUP_MESSAGING_ENABLED=true` 开启群聊链路；`IM_MESSAGING_REQUIRED=true` 可选，用于基础设施缺失时直接启动失败；`IM_MESSAGING_NAMESPACE` 默认 `im:v12`。首次启动前，如果 Redis 中还没有 `seq:ready`，需要先执行一次：

```bash
(cd backend && go run ./cmd/server -backfill-seq)
```

后端在缺少 `seq:ready` 时会拒绝启动。具体配置、队列、备份和切换步骤见 [运行与发布操作](docs/operations.md)。

## 发布与部署

当前交付路径是 Linux 发布包，不是容器镜像。在干净的 `main` 工作区执行：

```bash
scripts/build-release.sh v2.0 /tmp/im-v2.0
```

产物包含 `bin/im-server`、`web/`、`RELEASE` 和 `SHA256SUMS`。上传到 Linux 机器后，由 `deploy/imctl` 负责启停、健康检查、备份、`backfill-seq`、`rebuild-redis` 和死信操作；基础设施由 `deploy/infractl` 管理。完整步骤、正式/验证实例隔离和回滚规则见 [运行与发布操作](docs/operations.md)。

当前仓库尚未提供 Dockerfile 或 Compose；Docker 打包不在现有交付路径中。

## 测试与验证

```bash
# 后端单元测试
(cd backend && go test -race ./...)

# 前端测试与构建
(cd frontend && npm ci && npm test && npm run build)
```

真实 MySQL 集成测试由环境变量门控，未设置时自动跳过：

```bash
(cd backend && IM_TEST_MYSQL_DSN='<dsn>' go test -race ./internal/store)
```

运行实例的 smoke 需要先构建并托管前端。下面的 `8199` 是 v2.0 验证实例；不设置 `BASE_URL` 时 smoke 默认也打 8199，建议仍然显式指定目标地址：

```bash
BASE_URL=http://127.0.0.1:8199 node scripts/smoke.mjs
BASE_URL=http://127.0.0.1:8199 node scripts/browser-smoke.mjs
BASE_URL=http://127.0.0.1:8199 node scripts/cache-smoke.mjs
```

浏览器 smoke 默认使用 macOS 上的 Google Chrome，可用 `CHROME_PATH` 指定其他 Chrome/Chromium。smoke 会在输出目录写入 `manifest.json`，记录本轮创建的测试账号和群；清理时使用 `scripts/cleanup-test-data.mjs` 按 manifest 精确处理，不要使用宽泛的用户名通配符。

## 版本状态

| 阶段 | 状态 |
| --- | --- |
| v0.1–v0.7 | 已完成并验收，形成基础 IM 与部署基线 |
| v1.1–v1.7 | 已完成；其中 v1.2–v1.7 已切换到正式 `8198` |
| v1.8.1–v1.8.3 | 已完成并在 `8199` 隔离验证；未切换正式 `8198` |
| v1.9 | 容量验证与调优；计划在 Ubuntu 完成（未开始） |
| v2.0 | 朋友圈（文字版）；已完成并在 `8199` 隔离验证；未切换正式 `8198` |
| v2.1+ | 朋友圈图片/相册、隐私与通知等；方向待讨论 |

## 文档

### 规划与运维

- [路线图](docs/roadmap.md)
- [运行与发布操作](docs/operations.md)

### v2.x 社交功能

- [v2.x 总览](docs/versions/v2.md)
- [v2.0 朋友圈（文字版）](docs/versions/v2.0.md)

### v1.x 可靠性、消息链路与已读

<details>
<summary>展开 v1.x 版本文档</summary>

- [v1.8 可靠性与一致性](docs/versions/v1.8.md)
  - [v1.8.1 会话消息号与会话内水位](docs/versions/v1.8.1.md)
  - [v1.8.2 可靠投递与对账](docs/versions/v1.8.2.md)
  - [v1.8.3 Redis 数据重建与双向一致性检查](docs/versions/v1.8.3.md)
- [v1.7 群聊已读与 WebSocket 隐式已读](docs/versions/v1.7.md)
- [v1.6 私聊已读](docs/versions/v1.6.md)
- [v1.5 Web 端本地缓存与增量同步](docs/versions/v1.5.md)
- [v1.4 服务端消息读取链路](docs/versions/v1.4.md)
- [v1.3 群聊消息链路](docs/versions/v1.3.md)
- [v1.2 私聊消息链路](docs/versions/v1.2.md)
- [v1.1 性能基线与基础设施](docs/versions/v1.1.md)

</details>

### v0.x 基础 IM

<details>
<summary>展开 v0.x 版本文档</summary>

- [v0.1 基础工程](docs/versions/v0.1.md)
- [v0.2 用户体系](docs/versions/v0.2.md)
- [v0.3 好友体系](docs/versions/v0.3.md)
- [v0.4 文本私聊](docs/versions/v0.4.md)
- [v0.5 消息体验补全](docs/versions/v0.5.md)
- [v0.6 基础群聊](docs/versions/v0.6.md)
- [v0.7 整体联调与部署](docs/versions/v0.7.md)

</details>

## 边界与限制

- **仅 Web。** 用户通过浏览器访问，不提供需要下载安装的桌面或移动客户端；桌面与手机支持均指响应式网页。
- **单实例。** 一个后端进程、一台 MySQL、一台 Redis 和一台 RabbitMQ，没有跨实例广播和高可用；整机或磁盘故障只能靠备份和 MySQL 重建恢复。
- **Redis 不是备份。** Redis 使用 AOF `appendfsync everysec`，整机掉电时最多约 1 秒内已确认的消息可能丢失；Redis 数据不进入备份，丢失后从 MySQL 重建，重建期间发送和已读不可用。
- **未做容量治理。** 消息和朋友圈没有自动清理或 TTL；v1.9 的容量验证、压测和调优尚未完成。
- **当前不开放公网。** 现有部署不修改防火墙和 Nginx，也没有设计公网域名、TLS、反向代理和访问控制。
- **应用未容器化。** 当前应用交付物是 Linux 发布包和 `imctl`，没有应用镜像或 Compose 编排；MySQL、Redis、RabbitMQ 仍由 `imctl`/`infractl` 放在私有 Docker 中运行。
