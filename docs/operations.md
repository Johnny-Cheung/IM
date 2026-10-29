# 运行与发布操作

本文覆盖当前重构版在 Linux 开发机上的运行、发布和回滚。当前正式运行实例仍为 v1.7：正式 `8198` 和验证 `8199`，MySQL、Redis、RabbitMQ 已就绪。v1.1 基础设施、v1.2 私聊消息链路、v1.3 群聊消息链路、v1.4 服务端读取链路、v1.5 本地缓存、v1.6 私聊已读和 v1.7 群聊已读均已完成正式切换。v1.8.1（会话消息号 `seq`、读模型单位改为 `seq`、删除旧 HTTP 已读接口）仍在开发和隔离验证中，正式切换步骤见下文“v1.8.1 切换步骤与回滚（待执行）”；在该节完成前不要对正式实例执行其步骤。业务接入只在明确配置 Redis/RabbitMQ 的实例启用，高可用、容量方案和最终部署拓扑仍按路线留到后续版本。旧 `/opt/IM` 不在本文范围。

## 环境

- 部署根目录为 `/opt/IM-Rebuild`。后端由临时 systemd 单元 `im-rebuild` 运行，监听 `0.0.0.0:8198`，同时托管 `web/` 页面；控制脚本为 `/opt/IM-Rebuild/imctl`。
- MySQL 8.4 在 `/opt/IM-Rebuild/runtime` 的私有 Docker 中运行，只监听 `127.0.0.1:13307`；数据库为 `im`，应用账号为 `im_app`，数据目录为 `/opt/IM-Rebuild/mysql/data`。
- Mac 只负责写代码、测试和构建，运行环境是 Linux 开发机。Mac 不能连接开发机只监听本机的 MySQL `13307`，也不需要 SSH 隧道；登录开发机和传输文件使用现有 relay 流程。canary 的 HTTP `8199` 可从 Mac 内网访问。
- 不开放公网、不修改防火墙、不配置开机自启。开发机上的 `imctl`、文件切换和备份命令以 root 执行；后端进程仍以 `nobody` 运行。

## 构建发布包

在 Mac 的仓库根目录确认工作区干净且位于 `refactor/im`：

```bash
git status --short
git branch --show-current
scripts/build-release.sh
# 也可以指定版本和输出目录；版本支持两段式和三段式
scripts/build-release.sh v0.7 /tmp/im-v0.7
scripts/build-release.sh v1.8.1 /tmp/im-v1.8.1
```

脚本不会自动切换分支或推送。工作区不干净、分支不是 `refactor/im` 或输出目录已存在时会直接失败。产物目录包含：

```text
bin/im-server
web/
RELEASE
SHA256SUMS
```

`RELEASE` 记录 `version`、`commit`、`built_at`、`go`、`node`、`platform` 和每个产物的 SHA-256；`SHA256SUMS` 还覆盖 `RELEASE`。在 Mac 和开发机上都可以校验（以下用固定输出目录 `/tmp/im-v0.7` 举例；默认输出时改用脚本打印的目录）：

```bash
cd /tmp/im-v0.7
shasum -a 256 -c SHA256SUMS     # macOS
sha256sum -c SHA256SUMS         # Linux
```

通过现有 relay 文件传输把整个目录上传到开发机，例如 `/opt/IM-Rebuild/incoming/im-v0.7`，上传后在开发机再执行一次 `sha256sum -c SHA256SUMS`。

## 健康状态

| 路径 | 含义 |
| --- | --- |
| `/health` | HTTP 进程可响应即返回 200；不检查 MySQL。 |
| `/ready` | 2 秒内成功 ping MySQL 时返回 200（`database=ok`）；否则返回 503（`database=unavailable`）。它仍然只检查 MySQL，不反映消息号闸门。 |
| `/ops/infra` | 基础设施连通性和 v1.8.1 的 seq 就绪/校准状态。`seq` 段包含 `ready`、`calibrating`、`waiting_seq_ready`、`run_id`、`last_calibrated_at`、`conversations`、`last_error`。切换后用它判断接纳是否可用。 |

```bash
/opt/IM-Rebuild/imctl health
/opt/IM-Rebuild/imctl status
curl -fsS http://127.0.0.1:8198/health
curl -fsS http://127.0.0.1:8198/ready
curl -fsS http://127.0.0.1:8198/ops/infra
```

`imctl health` 只在 `/health` 和 `/ready` 都返回 200 时成功，任一失败或连接失败都返回非零。`imctl status` 仅用于查看 backend、MySQL、私有 Docker 和 `RELEASE`，是可读状态展示，不作为发布是否成功的判据。`/health` 为 200 但 `/ready` 为 503 表示进程在、数据库不可用，应先检查 `imctl mysql-status` 和 `imctl logs mysql`。

v1.8.1 起，`/health` 和 `/ready` 全绿不代表发送可用：启动校准完成前、Redis 重启后的重新校准期间、`seq:ready` 丢失或某个会话计数器缺失但索引非空时，发送、`focus`、未读和 `read-stats` 返回 503，历史读取照常。必须同时观察 `/ops/infra` 的 seq 状态与后端日志，不要按 `/ready` 判断切换完成。

## 重启和开机后拉起

```bash
/opt/IM-Rebuild/imctl start
/opt/IM-Rebuild/imctl health
/opt/IM-Rebuild/imctl status
```

项目不配置开机自启。开发机重启后，私有 Docker、MySQL 和临时 systemd 单元都不会自动恢复，需要手工执行 `imctl start`。`imctl restart` 会先确保 runtime 和 MySQL 就绪，再只重启后端，不会重启健康的数据库；完整停止后也可以用它恢复。`imctl backend-start` 和 `imctl restart` 都通过 `systemd-run` 重建临时单元，不假设旧单元仍然存在。`imctl stop` 会依次停止后端、MySQL 和 runtime，但保留全部文件和数据。

```bash
/opt/IM-Rebuild/imctl logs backend 80
/opt/IM-Rebuild/imctl logs mysql 80
/opt/IM-Rebuild/imctl logs runtime 80
```

## 发布切换（仅应用文件）

以下操作在开发机执行，MySQL 始终运行。

1. 备份数据库和当前应用文件，并校验备份：

```bash
BACKUP="$(/opt/IM-Rebuild/imctl backup)"
( cd "$BACKUP" && sha256sum -c SHA256SUMS )
```

`imctl backup` 会打印本次备份目录；备份写入仅 root 可访问的 `/opt/IM-Rebuild/backups/`。目录包含 `database.sql`、行数清单 `counts.tsv`、当前 `bin/`、`web/`、`RELEASE`和 `SHA256SUMS`，保留历史、不自动删除。

2. 校验新包、停止后端并替换文件：

```bash
cd /opt/IM-Rebuild/incoming/im-v0.7
sha256sum -c SHA256SUMS
NEW_WEB="$(mktemp -d /opt/IM-Rebuild/web.new.XXXXXX)"
cp -a web/. "$NEW_WEB/"
OLD_WEB="$(mktemp -d /opt/IM-Rebuild/web.old.XXXXXX)"
/opt/IM-Rebuild/imctl backend-stop
install -m 0755 bin/im-server /opt/IM-Rebuild/bin/im-server
mv /opt/IM-Rebuild/web "$OLD_WEB/web"
mv "$NEW_WEB" /opt/IM-Rebuild/web
install -m 0644 RELEASE /opt/IM-Rebuild/RELEASE
( cd /opt/IM-Rebuild && sha256sum -c /opt/IM-Rebuild/incoming/im-v0.7/SHA256SUMS )
```

3. 启动后端并做严格健康检查：

```bash
/opt/IM-Rebuild/imctl backend-start
/opt/IM-Rebuild/imctl health
/opt/IM-Rebuild/imctl status
curl -fsS http://127.0.0.1:8198/health
curl -fsS http://127.0.0.1:8198/ready
```

新二进制启动时会自动执行迁移。切换只替换应用文件，不停止 MySQL、不恢复数据库；迁移只向前执行，应用回滚时不要回滚或覆盖 MySQL 数据。v1.8.1 改变了 Redis 键和已读单位，不能只按本节替换文件，必须按下一节执行排空队列和 `backfill-seq`。

## v1.8.1 切换步骤与回滚（8199 已验证；开发阶段暂不切 8198）

2026-10-28 决定：项目仍处开发阶段、尚未对外上线，v1.8.1 不切换正式 8198，8198 继续运行 v1.7；8199 保留 v1.8.1，供后续讨论和验证。下面的切换与回滚步骤保留，正式上线前统一执行，届时按当时的数据重新备份和回填。

v1.8.1 把全局消息号改为“会话内连续消息号 `seq`”：私聊两个方向合并成 `dm:pair`，会话计数器为 `dm:seq`/`group:{G}:seq`，已读水位改用 `seq`，三个旧 HTTP 已读接口删除，就绪标记统一为 `seq:ready`。以下步骤尚未执行，执行后把结果补在本节。

### 前置检查

```bash
# 当前版本健康、队列排空（只读；正式 vhost im_v11）
/opt/IM-Rebuild/imctl health
/opt/IM-Rebuild/imctl queue-status
# 备份 MySQL 与应用文件，并保留旧控制脚本供回滚
BACKUP="$(/opt/IM-Rebuild/imctl backup)"
( cd "$BACKUP" && sha256sum -c SHA256SUMS )
/opt/IM-Rebuild/imctl restore-check "$BACKUP"
PRE=/opt/IM-Rebuild/imctl.pre-v1.8.1
if [ ! -e "$PRE" ]; then
  cp -p /opt/IM-Rebuild/imctl "$PRE"
fi
grep -q 'backfill-group-read' "$PRE" \
  || { echo "错误：$PRE 不是 v1.7 控制脚本，回滚将无法执行；请从 refactor/im 的 v1.7 提交取回 deploy/imctl" >&2; exit 1; }
```

本块只执行一次。副本已存在时不会覆盖；`imctl.pre-v1.8.1` 必须是旧脚本，新脚本没有 `backfill-read`/`backfill-group-read`。

- `imctl queue-status` 读取正式 vhost 的 `im.v12.messages.persist` 和 `im.v12.group.messages.persist`，`ready` 与 `unacknowledged` 都为 0 才算排空；`--verify` 形式检查验证 vhost `im_v13_verify`，不会碰正式队列。
- 新 `deploy/imctl` 提供 `backfill-seq` 和 `queue-status`；切换前用 relay 上传并安装为 `/opt/IM-Rebuild/imctl`，旧脚本已复制为 `imctl.pre-v1.8.1`。`imctl` 不在发布包里，也不在 `backup` 中，必须单独保留旧副本。
- `imctl backup` 只包含 MySQL 和应用文件，不含 Redis。`seq` 计数器、`dm:pair`、`seq:ready` 不备份，靠 `backfill-seq` 从 MySQL 重建。
- 保留 `imctl.pre-v1.8.1` 是因为新脚本删除了 `backfill-read`/`backfill-group-read`，回滚只能由旧脚本发起。该副本只生成一次；重复执行上面的块不会把已安装的新脚本复制覆盖它。
- 新包用 `scripts/build-release.sh v1.8.1 <目录>` 构建；脚本接受两段式和三段式版本号。

### 8199 隔离验证结果（2026-10-28）

- 发布包 `v1.8.1`，提交 `8ecd45d`；canary 切换前备份 `/opt/IM-Rebuild/canary-v07/backup-v17-20261028T143510Z`。
- 验证命名空间 `im:v13-verify` 先清空（1340 个旧键），验证库 `im_v07_verify` 为空库。
- 空库回填：写入 `seq:ready`，各计数为 0，内存 2.07M → 2.07M。
- 冒烟：API 114 项、浏览器 56 项、缓存 29 项全部通过（含 `seq` 连续、重复发送同号、并发无空洞、`after_seq` 补缺口、旧已读接口 404/405、`read-stats` 带 `message_id`、群回执实时更新、离线和重连）。
- 数据一致性：146 条私聊、213 条群消息、11 个私聊会话、7 个群；每个会话 `COUNT(*) = MAX(seq)`，无未编号行，MySQL 水位都不大于会话最大 `seq`；持久化队列 0，读事件 Stream 0。
- 回填幂等：在已有数据上停后端重跑两次，会话计数、`dm:pair`、水位指纹完全不变（唯一变化是旧读事件 Stream 被删除后由后端重建）。
- 校准：把 `dm:seq:426:427` 从 44 改成 1 后重启，启动校准抬回 44；`/ops/infra` 的 `seq.conversations=18`、`calibrating=false`。
- 就绪标记丢失：删除 `im:v13-verify:seq:ready` 后 8 秒内发送返回 503 `message_service_unavailable`、会话列表返回 503 `read_model_not_ready`；停后端重跑 `backfill-seq` 并启动后恢复。
- 清理：按 11 份 manifest 删除 45 个测试账号和 16 个测试群；验证库行数回到 0；验证命名空间只保留 `seq:ready`、`index:epoch` 和三个空 Stream。
- 遗留：清理脚本没有覆盖按用户/消息 ID 命名的 `messages:*`、`dedup:*`、`group-dedup:*`、`friends:*`；本次在验证命名空间手工按前缀清除（DB 已空，仅该隔离命名空间）。正式环境同类清理留到 v1.9 容量治理，执行前需单独确认。

### 切换顺序

1. 备份，并在 Redis 容器内记录只读诊断：`INFO memory`、`INFO server` 的 `run_id`，以及 `dm:conv:*`、`group:{G}:inbox`、`index:epoch` 的键数量。不要手工删除正式键。
2. 确认持久化队列已排空：`/opt/IM-Rebuild/imctl queue-status`。非 0 时等待 relay 和 consumer 消费后重试。
3. 停后端：`/opt/IM-Rebuild/imctl backend-stop`。`backfill-seq` 会拒绝在 systemd 单元 active、端口被占用、或存在本目录 `bin/im-server` 进程时执行，也会拒绝在另一个回填持有 Redis `backfill:lock` 时启动。
4. 校验新包并替换 `bin/`、`web/`、`RELEASE`（步骤同“发布切换”第 2 步）。
5. 执行回填：`/opt/IM-Rebuild/imctl backfill-seq`（验证实例为 `/opt/IM-Rebuild/imctl --verify backfill-seq`）。命令先执行迁移，再填充 MySQL 的会话列与 `seq`、重建 Redis 计数器/`dm:pair`/群 inbox、换算水位、校验，全部通过后写入 `seq:ready`（正式 `im:v12:seq:ready`，验证 `im:v13-verify:seq:ready`）；失败时不写 `seq:ready`。回填期间持有 `backfill:lock`（30 分钟 TTL），后端在锁存在时拒绝启动。重跑时只有 `seq:ready` 能证明 Redis 水位已是 `seq`：缺少它就把所有 Redis 水位按旧全局消息号重新换算，MySQL 的 `last_read_seq` 作为下限；已存在 `seq:ready` 时保留 `seq` 值并截断到计数器。崩溃残留的锁最多阻塞启动 30 分钟，可用 `SCAN` 确认后 `UNLINK` 该键。
6. 启动：`/opt/IM-Rebuild/imctl backend-start`。缺少 `seq:ready` 时后端拒绝启动。
7. 启动后验证：
   - `/ready` 200（仍只查 MySQL），并观察 `/ops/infra` 的 `seq` 段：`calibrating:true`（或 `run_id` 与切换前记录的不同）表示 Redis 重启后正在重新校准，等待即可；`ready:false` 且 `waiting_seq_ready:true` 表示 `seq:ready` 丢失，需要停止后端并重新执行 `backfill-seq`。校准完成前发送、`focus`、未读、`read-stats` 返回 503，历史读取正常。
   - `imctl health`、`imctl status`、`imctl logs backend` 无 `seq:ready`、校准或唯一索引错误。
   - `imctl queue-status` 在回归后重新排空；`infractl status` 观察 Redis 内存变化（新的 `dm:seq`/`dm:pair` 取代两个方向的 `dm:conv`，容量治理留到 v1.9）。
   - 用独立验证账号回归**正式切换后的 8198**（必须显式设置 `BASE_URL`；不设置时 smoke 默认打 8199 canary）：`BASE_URL=http://10.12.75.191:8198 node scripts/smoke.mjs`、`BASE_URL=http://10.12.75.191:8198 node scripts/browser-smoke.mjs`、`BASE_URL=http://10.12.75.191:8198 node scripts/cache-smoke.mjs`；在开发机上执行时用 `http://127.0.0.1:8198`。每个会话 `seq` 从 1 连续、重复发送返回同一 `seq`、未读等于 `last_seq - read_seq`、三个旧已读接口返回 404 或 405。本轮 manifest 必须用数据库 `im` 清理。
   - 已读人数核对：`read-stats` 用 `seq` 查询，响应里的 `message_id` 与该 `seq` 的全局消息号一致。
   - 实测统计（消息数、群数、水位、内存、队列）执行后补写，不预先填写。

### 键与容量（v1.8.1 起）

- 新增 `dm:seq:{low}:{high}`（STRING，私聊会话计数器）、`dm:pair:{low}:{high}`（ZSET，成员为全局消息号、分数为 `seq`）、`group:{G}:seq`（STRING）和 `seq:ready`，全部带命名空间前缀（正式 `im:v12:`，验证 `im:v13-verify:`），且都不设 TTL。
- 每个好友对只保留一份 `dm:pair`，取代 v1.7 的两个方向 `dm:conv`；`group:{G}:inbox` 保留但分数改为 `seq`。回填会删除 `dm:conv:*`、`group:{G}:last_accepted_id` 和旧的读事件 Stream；`dm:read:ready`/`group-read:ready` 不再被检查，统一由 `seq:ready` 取代。
- 计数器与会话索引随会话数增长，容量治理和 TTL 留到 v1.9；切换后按 `INFO memory` 记录实际增长，不预先填写数字。

### 503 的含义与观察方式

- 闸门关闭期间（启动校准、检测到 Redis 重启后的重新校准、`seq:ready` 丢失、计数器缺失但会话索引非空），发送、`focus`、未读和 `read-stats` 返回 503；历史读取照常，缺失部分从 MySQL 补。
- Redis 不可用同样返回 503，不回落 MySQL；MySQL 不可用时编号、未读、已读照常，但校准无法完成，发送保持 503。
- `/ready` 与 `imctl health` 不反映闸门，全绿也可能发送 503。以 `/ops/infra`、后端日志和 503 响应码为准。
- `seq:ready` 丢失后必须重新执行 `backfill-seq`，不能手工补键。

### 回滚（v1.8.1 → v1.7）

回滚会丢弃切换后产生的消息和已读数据，不做新旧单位的双写。步骤：

1. `/opt/IM-Rebuild/imctl backend-stop`。
2. 恢复切换前的 MySQL 备份（`$BACKUP/database.sql` 对应的切换前库）和应用文件 `bin/`、`web/`、`RELEASE`（见“回滚应用文件”）。恢复正式库必须单独评审并保留完整备份；`restore-check` 只做演练，不做恢复。
3. 清空整个 v1.8.1 命名空间（仅正式前缀 `im:v12:*`）：用 `SCAN MATCH im:v12:* COUNT 100` 分批 `UNLINK`。必须清空整个命名空间，不能只按新键前缀删除 `dm:seq`、`dm:pair`、`group:{G}:seq`、`seq:ready` 等：`dm:read`、`group:{G}:read` 和 `group:{G}:inbox` 的分数/值已经改成 `seq`，切换后的消息实体、`dm:feed`、`index:epoch` 和两个 Stream 也属于应丢弃的切换后数据；v1.7 的 `backfill-read`/`backfill-group-read` 只补缺和抬高水位（`group_read_reconcile.lua` 永不降低已有分数，校验也只检查 Redis 不小于 MySQL），无法把残留的 `seq` 水位还原成全局消息号，残留会让未读和已读人数算错。**绝不要 `FLUSHDB`/`FLUSHALL`**。旧版启动会从 MySQL 重新播种全局消息号、`index:epoch`、好友投影和群成员投影。
4. 用旧控制脚本和恢复后的旧二进制回填旧模型：
   ```bash
   /opt/IM-Rebuild/imctl.pre-v1.8.1 backfill-read
   /opt/IM-Rebuild/imctl.pre-v1.8.1 backfill-group-read
   ```
   群 inbox 分数和群已读单位在 v1.8.1 已改成 `seq`，因此两个命令都要执行，不能只 `backfill-read`。
5. 启动旧版并验证：`/opt/IM-Rebuild/imctl.pre-v1.8.1 backend-start`，`health`，再用旧版 smoke 回归。回填后抽查单位：`dm:read` 的每个值、`group:{G}:read` 的每个分数都不应大于恢复库中对应会话的最大全局消息号（`messages.id` / `group_messages.id`）。出现更大的值说明第 3 步没有清空整个命名空间，停止回滚并重新执行第 3 步。
6. 启动前确认第 3 步已清空整个命名空间；`seq:ready`、`seq` 计数器、`dm:read`/`group:{G}:read`/`group:{G}:inbox` 的 `seq` 残留必须一起清除，不能留到下一次切换。旧版不读取这些键，但保留它们就意味着第 3 步没有执行，同单位的残留读状态也会一起保留。回滚完成后不要再做额外的通配删除，但第 3 步的 `SCAN`/`UNLINK` 是回滚的必需步骤。

### 恢复测试注意事项

- 验证实例 `8199` 与正式 `8198` 共用同一个 Redis 容器 `im-v11-redis`，只是 DB 与命名空间不同。`infractl redis-restart`/`redis-recreate` 会影响正式实例，必须单独确认并选择窗口后再执行。
- `redis-restart` 保留数据目录，不会制造“计数器落后”。要验证校准：先停 canary，在 `im:v13-verify` 命名空间把某个 `dm:seq:*` 或 `group:*:seq` 改小，再重启 Redis（或等待 run_id 变化）并启动 canary；观察发送先返回 503、校准后从 MySQL 的最大 `seq` 继续，且新消息不撞号。
- 不要在正式命名空间手工改计数器，不要 `FLUSHDB 0`。测试 `seq:ready` 丢失时只删 `im:v13-verify:seq:ready`，并重新执行 `imctl --verify backfill-seq`。
- 掉电丢尾无法在 smoke 中复现，使用人工清单。

### 人工验证清单

- [ ] 掉电丢尾重发：**不要停止、重启或重建共享的 `im-v11-redis` 容器**——正式 8198 的 DB 0 与验证 DB 1 在同一进程和同一份 AOF 内，容器级操作会中断正式实例，并可能丢掉最多约 1 秒的正式写入。在验证命名空间模拟：`/opt/IM-Rebuild/imctl --verify stop`，记下刚发送消息的全局号，在 `im:v13-verify` 删除 `messages:{id}` 或 `group-messages:{id}`、`dm:pair`/`group:{G}:inbox` 中该消息的成员、原 `client_message_id` 对应的 `dedup`/`group-dedup`、Stream 中该消息的事件以及 `dm:conv-last` 中指向它的字段；若该消息已异步落库，同时删除 `im_v07_verify` 中对应的 `messages`/`group_messages` 行；再 `/opt/IM-Rebuild/imctl --verify start` 并刷新浏览器；本地“已发送”消息应变为发送失败，点击重试用原 `client_message_id` 成功且不重复。真正的 SIGKILL/AOF 丢尾只能在独立的 Redis 进程上验证；如果必须对共享容器做容器级恢复测试，按生产维护窗口停 8198、排空队列并单独评审，不能当作只影响验证实例。
- [ ] 客户端缺口恢复：浏览器 smoke 已用合成跳号帧覆盖；人工再用真实弱网或切后台验证一次补拉日志。
- [ ] 服务端回退：在验证命名空间把会话计数器改小，客户端应丢弃本地多出的尾部并按服务端重新同步。
- [ ] 校准期间 503：改小计数器后重启，确认发送/未读/`focus`/`read-stats` 先 503，校准完成后恢复。
- [ ] 旧三个 HTTP 已读接口均返回 404 或 405，浏览器前台收消息不发任何已读 HTTP 请求。
- [ ] 群已读人数：`read-stats` 的 `message_id` 与本地该 `seq` 的全局号一致，`all_read`/分母在踢人后正确。

## 回滚应用文件

用发布前的 `imctl backup` 备份恢复应用文件。先校验整个备份，再停后端、恢复 `bin/`、`web/` 和 `RELEASE`，重新校验应用文件后启动：

```bash
: "${BACKUP:?请设置为本次发布前 backup 返回的确切目录}"
( cd "$BACKUP" && sha256sum -c SHA256SUMS )
NEW_WEB="$(mktemp -d /opt/IM-Rebuild/web.new.XXXXXX)"
cp -a "$BACKUP/web/." "$NEW_WEB/"
OLD_WEB="$(mktemp -d /opt/IM-Rebuild/web.old.XXXXXX)"
/opt/IM-Rebuild/imctl backend-stop
install -m 0755 "$BACKUP/bin/im-server" /opt/IM-Rebuild/bin/im-server
mv /opt/IM-Rebuild/web "$OLD_WEB/web"
mv "$NEW_WEB" /opt/IM-Rebuild/web
install -m 0644 "$BACKUP/RELEASE" /opt/IM-Rebuild/RELEASE
( cd "$BACKUP" && { find bin web -type f; printf 'RELEASE\n'; } | LC_ALL=C sort | xargs sha256sum ) > /tmp/im-app-rollback.sha256
( cd /opt/IM-Rebuild && sha256sum -c /tmp/im-app-rollback.sha256 )
/opt/IM-Rebuild/imctl backend-start
/opt/IM-Rebuild/imctl health
/opt/IM-Rebuild/imctl status
```

确认所选备份确实对应上一版本后再执行。回滚只恢复 `bin/`、`web/` 和 `RELEASE`，不恢复 `database.sql`，不动 MySQL 数据。v1.8.1 的回滚还要求恢复切换前的 MySQL 备份、清空新 Redis 命名空间并用旧脚本执行 `backfill-read` 与 `backfill-group-read`，见上一节。

## 数据库备份与恢复检查

`imctl backup` 通过现有 MySQL 容器和容器内受保护密钥生成一致性 SQL dump 和 SHA-256，保存到仅 root 可访问的 `/opt/IM-Rebuild/backups/` 下按 UTC 时间加后缀命名的子目录，并打印路径。备份包含 `database.sql`、行数清单 `counts.tsv`、当前应用的 `bin/`、`web/`、`RELEASE`和 `SHA256SUMS`；保留历史，不自动删除，不把密码写入参数、环境变量或日志。

备份不包含 Redis。v1.8.1 的 `dm:seq`、`dm:pair`、`group:{G}:seq`、`seq:ready` 都不在备份中：Redis 数据丢失或回滚后，用 `backfill-seq` 从 MySQL 重建；回滚到 v1.7 时则清空新命名空间并用旧脚本回填旧模型。切换前可以在 Redis 容器内记录只读的键数量、内存和 `run_id` 作为诊断，不要导出或修改正式键。

```bash
BACKUP="$(/opt/IM-Rebuild/imctl backup)"
( cd "$BACKUP" && sha256sum -c SHA256SUMS )
```

恢复检查只读备份、只写临时库，不接触正式库：

```bash
/opt/IM-Rebuild/imctl restore-check "$BACKUP"
```

`restore-check` 先校验备份自身的 `SHA256SUMS`，并拒绝包含 `CREATE DATABASE`、`DROP DATABASE` 或 `USE` 的 dump；随后通过容器内受保护的 root optionfile 创建全新的 `im_restore_` 前缀临时库，导入并比较表清单和从 mysqldump 单行 INSERT 解析出的行数清单。成功时打印结果和临时库名，最后只删除本次创建的临时库。它不会写入 `im`，不会覆盖正式数据；不要把正式库恢复当作测试。确需恢复正式数据时必须单独评审并保留完整备份。

应用文件回滚使用上一节从同一备份恢复 `bin/`、`web/`、`RELEASE`；`database.sql` 不参与日常应用回滚，发布和回滚过程中 MySQL 数据保持不变。

## Canary 8199

- 目录 `/opt/IM-Rebuild/canary-v07`，临时 systemd 单元 `im-rebuild-verify`，监听 `0.0.0.0:8199`。canary 仅供内网验证，不做公网暴露；Mac 通过 HTTP `8199` 访问，无需 SSH 隧道。
- 所有 canary 操作使用 `imctl --verify` 前缀（例如 `imctl --verify start`）。`--verify` 固定选择 `APP_ROOT=/opt/IM-Rebuild/canary-v07`、单元 `im-rebuild-verify`、端口 `8199`、库/用户 `im_v07_verify` 和 `/opt/IM-Rebuild/canary-v07/mysql-app`；共享私有 Docker runtime/socket 与 MySQL 保持不变。`--verify start/stop/restart` 只影响 canary 应用，绝不停止共享 MySQL/runtime。
- 使用独立 schema/用户 `im_v07_verify`，应用密码文件为 `/opt/IM-Rebuild/canary-v07/mysql-app`（属主 `nobody`，权限 0400）。不得复用生产库 `im`、账号 `im_app` 或生产凭据。自动化检查只使用本轮创建的账号和群，不登录、不修改 `demo_*` 演示数据。
- v0.6 基线保留为 `/opt/IM-Rebuild/canary-v07/bin/im-server-v06`、`RELEASE.v06` 和初始 v0.6 `web/`。部署候选版本前先备份 canary 当前文件。

canary 应用入口是 `/opt/IM-Rebuild/canary-v07/bin/im-server` 和 `web/`。当前 canary 已部署 v1.7（验证后已停止），其 v1.6 备份为 `bin/im-server-v16`、`RELEASE.v16` 和 `web-v16`；v1.5 备份为 `bin/im-server-v15`、`RELEASE.v15` 和 `web-v15`；v0.6 基线仍保留为 `bin/im-server-v06`、`RELEASE.v06` 和初始 v0.6 `web/`。启动、状态和检查：

```bash
/opt/IM-Rebuild/imctl --verify start
/opt/IM-Rebuild/imctl --verify status
/opt/IM-Rebuild/imctl --verify health
journalctl -u im-rebuild-verify -n 80 --no-pager
curl -fsS http://127.0.0.1:8199/health
curl -fsS http://127.0.0.1:8199/ready
```

首次替换 canary 的 `web/` 前保留基线页面：

```bash
test -d /opt/IM-Rebuild/canary-v07/web-v06 \
  || cp -a /opt/IM-Rebuild/canary-v07/web /opt/IM-Rebuild/canary-v07/web-v06
```

测试候选版本时，把候选文件放到 `canary-v07/bin/im-server` 和 `canary-v07/web/`，然后执行：

```bash
# v1.8.1 候选包必须先回填 seq，再启动；后端缺少 im:v13-verify:seq:ready 会拒绝启动
/opt/IM-Rebuild/imctl --verify backfill-seq
/opt/IM-Rebuild/imctl --verify restart
/opt/IM-Rebuild/imctl --verify health
```

canary 的 Redis DB 1 与正式 `8198` 的 DB 0 在同一个 `im-v11-redis` 容器内，只是命名空间不同。重启或重建该容器、制造 AOF 丢尾等恢复测试都会影响正式实例，必须单独确认后执行；不要用 `FLUSHDB` 清库。

恢复 v0.6 基线时，把 `bin/im-server-v06` 复制为 `bin/im-server`、把 `web-v06` 恢复为 `web`，再执行 `imctl --verify restart`。验证结束后用 `imctl --verify stop` 只停止 canary 应用；不要停止共享 runtime/MySQL。临时单元由 `imctl --verify start` 在需要时重建，不影响生产 `8198`。

## v1.1 基础设施

- 独立目录 `/opt/IM-Rebuild/infra-v11`，使用独立私有 Docker runtime、systemd 单元 `im-infra-docker` 和独立凭据；不影响正式 `8198`、v0.7 runtime/MySQL 或旧 `/opt/IM`。
- Redis `127.0.0.1:26379`（96 MiB），RabbitMQ `127.0.0.1:25676`（512 MiB）；凭据在 `infra-v11/secrets`，离线镜像在 `infra-v11/images`。
- 管理命令：

```bash
/opt/IM-Rebuild/infra-v11/infractl start
/opt/IM-Rebuild/infra-v11/infractl status
/opt/IM-Rebuild/infra-v11/infractl health
/opt/IM-Rebuild/infra-v11/infractl stop
```

- 单组件重启使用 `redis-restart`、`rabbitmq-restart`；日志使用 `logs redis|rabbitmq|runtime`。
- v1.8.1 起后端每 5 秒读取 Redis `INFO server` 的 `run_id` 检测容器重启；启动和检测到重启后都把会话计数器校准到不低于 MySQL 最大 `seq`（只抬不降）。校准期间发送、`focus`、未读和 `read-stats` 返回 503，完成后自动恢复；状态通过 `/ops/infra` 观察。
- `redis-restart` 保留数据目录，不会制造“计数器落后”。恢复测试要在验证命名空间手工把计数器改小（见“v1.8.1 切换步骤与回滚”的恢复测试注意事项），不要操作正式命名空间。
- 当前正式 `8198` 已接入 v1.2 私聊、v1.3 群聊、v1.4 服务端读取链路、v1.5 本地缓存、v1.6 私聊已读和 v1.7 群聊已读；v1.8.1 尚未切换。不配置开机自启，开发机重启后手动执行 `infractl start` 和 `imctl restart`。

## v1.2 私聊消息链路（已切换正式）

- v1.2 后端只有在 Redis 和 RabbitMQ 同时通过 `_FILE` 配置时才启用新链路；未配置时继续使用 MySQL 直写路径。
- `deploy/imctl` 会把 `/opt/IM-Rebuild/infra-v11/secrets` 中的应用凭据复制到 `/opt/IM-Rebuild/config/infra-v11`，以 `nobody:root`、0400 权限提供 `IM_REDIS_URL_FILE`、`IM_REDIS_PASSWORD_FILE`、`IM_RABBITMQ_URL_FILE` 和 `IM_RABBITMQ_PASSWORD_FILE`。凭据不写入命令行、日志或仓库。
- 新链路使用 Redis Lua 接纳、Redis Stream、RabbitMQ 异步落库和服务端消息号；`/ready` 仍只检查 MySQL，基础设施状态通过 `/ops/infra` 观察。
- v1.2 隔离验证使用临时 systemd 单元和独立 `im_v07_verify` 数据库；验证结束后停止临时单元并按 manifest 清理测试数据。隔离验证通过后，正式 `8198` 已切换到 v1.2，正式 API 回归使用独立账号并按 manifest 清理。
- v1.2 不配置开机自启，不做高可用故障转移、压力测试或容量调优。

## v1.3 群聊消息链路（已切换正式）

- 群聊与私聊共享消息号规则、accepted/persisted 语义和基础 worker 骨架，但使用独立的群 Lua、群成员投影、群统一信箱、群消息表、群 Stream 和群 RabbitMQ 拓扑。
- `deploy/imctl --verify` 现在必须注入隔离基础设施：Redis DB 1、RabbitMQ vhost `im_v13_verify`、namespace `im:v13-verify`，并强制 `IM_MESSAGING_REQUIRED=true`。验证实例缺少这些配置时拒绝启动，不再静默回落 MySQL。
- `deploy/infractl` 在 RabbitMQ 启动后幂等创建验证 vhost 并授予 `im_v11` 用户最小可用权限；只新增 vhost，不修改正式 `im_v11` vhost、Redis 数据或生产队列。
- 正式 `8198` 的 `deploy/imctl` 启动参数包含 `IM_GROUP_MESSAGING_ENABLED=true` 和 namespace `im:v12`；正式库迁移到 022 后，私聊与群聊共用消息号水位，分别写入 `messages` 和 `group_messages`。
- 正式 `im_v11` vhost 目前同时存在 `im.v12.messages.persist`（私聊）和 `im.v12.group.messages.persist`（群聊）两个队列；验证只在 `im_v13_verify` vhost 内使用独立群/私聊队列。
- 正式切换前需用 `imctl backup` 备份数据库和应用文件；群消息对外编号从内部自增 ID 切换为共享服务端消息号，属不可逆数据格式变更，应用回滚应使用上一个 v1.3 构建，不要直接回滚到 v1.2 二进制。
- v1.3 隔离验证使用临时单元 `im-rebuild-verify`、`im_v07_verify` 数据库和验证命名空间；验证结束后停止临时单元，按 manifest 清理测试账号、群 ID 对应的 Redis 投影键，并只删除 `im:v13-verify:*` 键。
- v1.3 不做群聊已读、高可用故障转移、pending 接管、死信、对账、压力测试或容量调优。

## v1.4 服务端消息读取链路（已切换正式，v1.8.1 起部分被取代）

- 读取改为按会话索引，另加用户级增量日志 `dm:feed:{user}` 和会话最后消息 `dm:conv-last:{user}`；`inbox`/`outbox` 已停止写入，只保留作回滚兜底。v1.8.1 起私聊的两个方向索引 `dm:conv:{owner}:{peer}` 被单个 `dm:pair:{low}:{high}` 取代（分数为会话 `seq`），`dm:feed` 和私聊增量同步的 `after_id` 仍使用全局消息号。
- `index:epoch` 标记读取索引的完整边界：epoch 以下一律回落 MySQL。首次启动取 `max(落库最大消息号, Redis 计数器)`，重启保留原值，只有计数器落后于落库最大值时才上调。v1.8.1 起 `index:epoch` 只保留给私聊 feed；热读按 `seq` 区间进行并由 MySQL 补齐缺失编号。
- 新增 `GET /api/messages/sync?after_id=N`，一次返回该用户全部私聊增量；`complete=false` 表示补收不完整，需要从会话历史重新同步。
- 消息实体、`dm:feed`、`dm:conv`、`dm:conv-last` 均不设 TTL，也不再裁剪；Redis 内存不再有固定上限行为，发布后需要持续观察 `used_memory`。观察方式是在私有 Redis 容器内读取 `INFO memory` 的 `used_memory_human`、`maxmemory_human` 和 `maxmemory_policy`，凭据仍由容器内受保护文件提供。
- 切换后实测 `used_memory` 约 1.64 MB（上限 64 MB），键总数 232；`dm:feed`/`dm:conv`/`dm:conv-last` 在真实用户产生新消息前为空，属于 epoch 设计的预期现象。
- `inbox`/`outbox` 已停止写入，仅保留作回滚兜底；确认稳定运行一个版本后再按前缀清理。
- 本版未解决会话列表的 MySQL 查询量（当时为 4 条，含 `GROUP BY` 最新消息子查询），也未做服务端性能实测。v1.6 已移除其中的未读和对方已读查询，好友、最新消息和群聊查询仍在；性能实测仍留到 v1.9。
- 回滚：v1.4 只改变 Redis 索引格式，MySQL 仍是权威。回退到 v1.3 二进制时，切换后产生的消息在 MySQL 中完整可用；但新消息没有写入 `inbox`/`outbox`，热窗口会由 MySQL 兜底。
- 验证实例继续使用 `imctl --verify`、Redis DB 1、vhost `im_v13_verify` 和命名空间 `im:v13-verify`。

## v1.5 Web 端本地缓存（已切换正式）

- 新增 `POST /api/bootstrap`，一次返回当前用户、会话列表、好友列表、私聊增量和有变化的群增量；`/api/messages/sync` 增加 `cursor`，单页上限提高到 200。
- 浏览器使用 IndexedDB `im-cache` 按账号缓存会话、好友和最近消息；每个会话最多 200 条，全账号最多 10000 条、100 个会话。
- 冷启动先渲染本地缓存，再通过 bootstrap 对齐；打开没有新消息的会话不再请求历史。退出登录先清除账号提示，再清除该账号缓存。
- v1.5 没有数据库迁移和 Redis 数据格式变更，回滚只需恢复切换前的应用文件。正式切换备份为 `/opt/IM-Rebuild/backups/20261026T103036Z-17TJXE`。
- 正式 smoke 产生的少量 Redis 测试索引未按 ID 清理，测试账号已从 MySQL 删除，不影响真实账号；容量治理时统一处理。
- 回归新增 `node scripts/cache-smoke.mjs`，覆盖冷启动单次 bootstrap、离线只读、断网恢复、退出清理和账号隔离。

## v1.6 私聊已读（已切换正式，v1.8.1 起被取代）

- 私聊已读位置保存为 Redis `dm:read:{userID}` HASH；v1.7 及以前未读用 `dm:conv` 的 `ZCOUNT` 现算。v1.8.1 起 `dm:read` 的值改为 `seq`，未读等于会话计数器的 `last_seq` 减去自己的已读 `seq`（小于 0 按 0），不再对消息索引做 `ZCOUNT`；对方回执比较 `peer_last_read_seq` 与消息的 `seq`。
- 旧的批量接口 `POST /api/read` 和单会话已读接口在 v1.8.1 删除，已读只通过 WebSocket `focus` 推进；`backfill-read` 只在回滚到 v1.7 时使用。
- 已读位置通过 `dm:read:stream` 异步写回 MySQL，worker 使用 `GREATEST` 幂等落库；v1.8.1 写入 `last_read_seq`，旧的 `last_read_message_id` 保留但不再维护。
- 升级到 v1.8.1 前的历史回填由 `backfill-seq` 统一完成（索引历史私聊消息、重建 `dm:pair`、换算已读位置），校验通过后写入 `seq:ready`。启用消息链路时，后端缺少该标记会拒绝启动；Redis 丢数据后需要重新回填。v1.7 及以前使用 `dm:read:ready`，该标记在 v1.8.1 中已被 `seq:ready` 取代。
- 正式切换前备份为 `/opt/IM-Rebuild/backups/20261027T150410Z-4Z7wA7`；回填统计为 50 条私聊、8 条已读、9 个方向，Redis 内存 1.94 MB → 1.96 MB。
- 正式环境回归：API smoke 62 项、浏览器 smoke 39 项、缓存 smoke 18 项全部通过；本轮 10 个测试账号按 manifest 删除，生产队列 ready/unack 均为 0。
- v1.5 不读取新的 Redis 键，MySQL 已读位置由异步 worker 持续写入，回退后继续可用。
- 验证实例 `8199` 已停止；验证库测试账号和测试用 `dm:read` 键已清除。测试消息的 `dm:conv` 和实体索引留到 v1.9 容量治理。
- v1.7 用 WebSocket 隐式已读取代了前台持续上报；v1.8.1 又删除了遗留的 HTTP 已读接口，批量接口不再是回滚路径的一部分（回滚到 v1.7 请使用旧控制脚本的 `backfill-read`）。

## v1.7 群聊已读与 WebSocket 隐式已读（已切换正式，v1.8.1 起被取代）

- 群聊已读位置保存为 Redis `group:{G}:read` ZSET；v1.7 未读用 `group:{G}:inbox` 的 `ZCOUNT` 现算，已读人数由 `GET /api/groups/:id/read-stats` 现算。v1.8.1 起 `group:{G}:inbox` 的分数和 `group:{G}:read` 的值都改为群会话 `seq`，未读等于群计数器 `last_seq` 减去自己的已读 `seq`；`read-stats` 改用 `?seq=` 查询并返回 `{seq, message_id, read_count, member_count, all_read}`。位置通过 `group-read:stream` 异步写回 `group_members.last_read_seq`（`GREATEST`，同一成员合并写）。
- 前台已读继续使用 WebSocket 隐式已读：客户端发 `focus`/`blur`，服务端的 `focus.ack` 和 `messages.read` 使用 `last_read_seq`。v1.8.1 删除了 `POST /api/read`、`POST /api/conversations/:username/read`、`POST /api/groups/:id/read`，浏览器只会通过 `focus` 推进已读。
- v1.8.1 的群聊历史回填补并进 `backfill-seq`，不再有独立的 `backfill-group-read`：
  ```bash
  /opt/IM-Rebuild/imctl --verify backfill-seq
  /opt/IM-Rebuild/imctl backfill-seq
  ```
  回填按（群，`seq`）重建 `group:{G}:inbox` 和计数器、换算成员位置，校验通过后写入 `seq:ready`。启用群消息链路时，后端缺少该标记会拒绝启动；Redis 丢数据后需要重新回填。v1.7 当时写入的是 `group-read:ready`，v1.8.1 已统一为 `seq:ready`；旧的 `backfill-group-read` 只保留在回滚路径（用 `imctl.pre-v1.8.1` 执行）。
- v1.7 当时的切换顺序为“备份 → 停后端 → 替换文件 → `imctl backfill-group-read` → 启动”（历史记录；v1.8.1 起统一改为 `backfill-seq`，不再有独立命令）。正式切换前备份为 `/opt/IM-Rebuild/backups/20261028T073828Z-gsbcbz`；回填统计为 19 条群消息、3 个群（ID 6、7、8）、10 个成员位置，其中 1 个位置被抬高；回填当场 Redis 内存 2.48 MB，没有明显变化，清理测试数据后约 2.68 MB（含本轮私聊测试消息的遗留索引，留到 v1.9）。
- 正式环境回归：API smoke 94 项、浏览器 smoke 49 项、缓存 smoke 25 项全部通过；本轮 16 个测试账号和 7 个测试群按 manifest 删除（MySQL 与 Redis），账号、私聊、群消息、群成员、已读行数与切换前一致，`group-read:stream` 为空，队列积压为 0。
- 群成员变更（建群、邀请、踢人、退群、解散）在同一群内串行写 Redis 投影；失败时 `group:{G}:sync_required` 保持置位，由后台修复循环按 MySQL 快照重建，期间该群的发送、已读和未读返回 503。当前串行锁在进程内，多实例留到 v1.8。
- 回滚应用文件仍使用切换前备份。v1.7 没有数据库迁移，v1.6 不读取新的 Redis 键；v1.7 写入 MySQL 的已读位置只增不减，回退后继续可用。
- 验证实例 `8199` 已停止；验证库 64 个测试账号和 31 个测试群已清除，测试用 Redis 键已按清单删除。

## 死信队列处理

v1.8.2 为私聊和群聊各增加一个死信队列（正式 `<前缀>` 为 `im.v12`，即 `im.v12.messages.dead`、`im.v12.group.messages.dead`；验证为 `im.v13-verify.*`），持久、不设 TTL、不自动消费。consumer 落库失败时按错误类型处理：暂时错误（连接中断、超时、死锁）不计次数并持续退避重试；与消息有关的错误重试 5 次（约 31 秒）后进死信；无法解码、正文冲突和 `seq` 冲突不重试、直接进死信。死信消息体与原消息一致，消息头记录原因、最后一次错误、尝试次数、首次失败时间和来源链路。

| 原因 | 含义 |
| --- | --- |
| `malformed` | 消息无法解码，通常是正文格式错误或版本不匹配。 |
| `conflict` | 正文或客户端消息号与已落库消息冲突（`ErrMessageConflict`）。 |
| `seq_conflict` | 会话内 `seq` 冲突或非法（`ErrSeqConflict`/`ErrSeqInvalid`）。 |
| `retries_exhausted` | 与消息有关的错误重试 5 次后仍未落库，例如外键失败或数据超长。 |

`imctl` 提供以下命令，均可加 `--verify` 操作验证实例（vhost `im_v13_verify`、命名空间 `im:v13-verify`）。这些命令只读取 RabbitMQ 配置，不需要 MySQL 或 Redis，也不需要停止后端：

| 命令 | 作用 |
| --- | --- |
| `imctl dlq-list` | 列出死信并全部重新入队，只读。 |
| `imctl dlq-replay [--id N]` | 按条或全部重放到主交换机，broker 确认后从死信队列删除。 |
| `imctl dlq-discard [--id N] --yes` | 按条或全部确认删除，必须显式确认。 |

```bash
/opt/IM-Rebuild/imctl dlq-list
/opt/IM-Rebuild/imctl dlq-list --kind group --limit 50
/opt/IM-Rebuild/imctl dlq-replay --id 123
/opt/IM-Rebuild/imctl dlq-replay --yes
/opt/IM-Rebuild/imctl dlq-discard --id 123 --yes
/opt/IM-Rebuild/imctl --verify dlq-list
```

- `--kind direct|group|all` 选择链路，默认 `all`；`--id` 是服务端消息号（消息头 `MessageId`），0 表示全部；`--limit` 默认 100、最大 10000，表示一次最多检查的消息数；指定 `--id` 时 `dlq-replay`/`dlq-discard` 会在该上限内自动把窗口扩大到队列深度。
- `dlq-list` 逐条打印消息号、`seq`、链路、原因、尝试次数、首次失败时间和简短错误，不输出正文，然后全部重新入队。`dlq-replay` 把原消息重新发布到主交换机，确认后才删除死信；某条发布失败时该条重新入队并停止，已经重放的不会重复。`dlq-discard` 直接确认删除，必须加 `--yes`；不带 `--id` 的重放同样必须加 `--yes`。
- 一次运行只检查有界窗口（`--limit`）；`basic.get` 重新入队的消息会回到队首原来的位置（或接近队首），因此多次 `dlq-list` 会重复扫描同一段队首，不是稳定快照。`--id` 指定单条时窗口会扩大到队列深度（最多 10000 条）；目标仍在该上限之外时命令报错并以非 0 退出，需要先重放/丢弃队首或提高 `--limit`，不会把“窗口内没找到”当成成功。扫描期间消息保持未确认，不会因为重读同一批而无限循环。
- 重放不修改消息内容。如果导致落库失败的原因（如外键缺失、字段超长）没有消除，消息会在再次重试 5 次后重新进入死信队列。先修复原因，或确认无用后用 `dlq-discard` 明确丢弃。
- `imctl queue-status` 会额外打印两个死信队列的 `ready`/`unacknowledged`；死信数为非 0 只打印 warning，不影响“持久化队列已排空”的判断，因为死信队列不会自动消费。

## 回归检查

以下命令在 Mac 仓库根目录执行，默认目标为 `8199` 的独立验证库：

```bash
node scripts/smoke.mjs
node scripts/browser-smoke.mjs
node scripts/cache-smoke.mjs
node scripts/capacity-smoke.mjs
node scripts/perf-baseline.mjs
bash deploy/imctl_test.sh
bash deploy/infractl_test.sh
```

API 和浏览器脚本使用 Node 26；浏览器脚本默认使用 `/Applications/Google Chrome.app/Contents/MacOS/Google Chrome`，可通过 `CHROME_PATH` 指定本机 Chrome。通过 `BASE_URL` 指定其他已确认的验证地址。容量脚本会创建 101 个独立账号并验证 100 人边界，仅用于隔离验证库。性能基线脚本串行采样发送、会话列表、历史和 WebSocket `focus`，只输出 p50/p95/max，不设阈值，也不做压力测试。

v1.8.1 起 smoke 断言会话内 `seq`：每个会话从 1 连续、私聊两个方向共用一个编号、重复发送返回同一 `seq`、并发发送无重复无空洞、`after_seq` 补拉缺口、未读等于 `last_seq - read_seq`、`read-stats` 用 `seq` 并返回 `message_id`、三个旧 HTTP 已读接口返回 404 或 405。浏览器 smoke 用已安装的 WebSocket 包装层丢弃真实推送并合成一帧跳号消息，验证客户端按 `after_seq` 补拉后恢复。缓存 smoke 覆盖 v1.7 缓存升级清理、缓存消息携带 `seq`、未读按服务端计数和重连补读。掉电丢尾重发无法在 smoke 中复现，见“v1.8.1 切换步骤与回滚”的人工验证清单。

脚本随机生成本轮账号和密码，密码只在进程内存中使用。完成后打印独立报告目录，`manifest.json` 只记录服务地址、本轮账号名、群 ID 和检查数；失败截图不包含登录表单。脚本退出非零表示检查未通过；账号清理需按本轮清单执行，不使用宽泛用户名通配符，不操作 `demo_*`。正式发布后的少量写入检查也必须使用本轮独立账号并按清单清理，不重置演示账号或它们的未读位置。

清理脚本生成 SQL 和同名的 `*.redis.sh`，不直接连接数据库；它只接受已登记的随机测试账号名和群 ID，并核对清单服务端口与所选数据库一致：

```bash
node scripts/cleanup-test-data.mjs im_v07_verify /tmp/im-cleanup.sql /path/to/manifest.json
# 多轮清单可依次追加；正式8198的本轮清单需显式选择 im
```

输出文件不会覆盖已有文件。核对 SQL 中的数据库和账号清单后，用既有 relay 流程上传到开发机。**先执行 Redis 清理，再执行 SQL**：私聊的 `dm:seq`/`dm:pair`/`dm:read`/`dm:conv-last` 以内部用户 ID 命名，Redis 脚本需要账号仍然存在，才能按清单里的精确用户名从 MySQL 查出 ID 再拼键（示例使用已上传的文件）：

```bash
bash /opt/IM-Rebuild/canary-v07/reports/cleanup.redis.sh
bash -c 'source /opt/IM-Rebuild/imctl; mysql_command mysql < /opt/IM-Rebuild/canary-v07/reports/cleanup.sql'
```

SQL 仅按明确用户名删除，表名包含固定数据库前缀，删除账号会级联清理这些账号的测试关系、消息和群；不会按用户名通配符清理，也不会修改真实账号或演示账号。`*.redis.sh` 只删除清单账号对应的私聊键和清单中群 ID 对应的验证命名空间键（成员集合、已读位置、inbox、`group:{G}:seq`、水位、消息实体及两个 Stream 中的群事件），不影响其他账号、群或正式数据，也不会删除 `seq:ready`。清单账号数必须与 MySQL 查到的一致，否则脚本拒绝执行。

## 凭据和演示数据

- 应用数据库密码使用已有受保护文件，不写进命令行、环境变量值、发布包、备份清单或对话。生产密码文件为 `/opt/IM-Rebuild/mysql/secrets/mysql-app`；`imctl restart` 会把它复制为 `/opt/IM-Rebuild/config/mysql-app`（`nobody`，0400），后端通过 `IM_MYSQL_PASSWORD_FILE` 引用。canary 使用自己的 `/opt/IM-Rebuild/canary-v07/mysql-app`。
- relay 凭据只由既有加密配置和交互认证管理，不进入参数、环境变量、文件或回复。MySQL root 密钥只由备份/核验脚本在容器内读取，生成受保护的短期 optionfile 并自动清理，不输出给操作人员。发布包及应用回退不包含密码文件。
- 演示账号为 `demo_alice`、`demo_bob`、`demo_carol`、`demo_dave`，默认密码 `Demo123456`，仅用于演示。发布流程不会自动写入演示数据；需要时手动执行 `BASE_URL=http://127.0.0.1:8198 node scripts/seed-demo.mjs`。脚本会创建或复用演示账号、好友、群和消息，详见 [v0.6 演示数据](versions/v0.6.md)。
