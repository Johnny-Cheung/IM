# 运行与发布操作

本文覆盖当前重构版在 Linux 开发机上的运行、发布和回滚。当前正式运行实例已切换为 v1.6 私聊已读：MySQL、正式 `8198` 和验证 `8199`。v1.1 基础设施已就绪，v1.2 私聊消息链路、v1.3 群聊消息链路、v1.4 服务端读取链路、v1.5 本地缓存和 v1.6 私聊已读均已完成正式切换。业务接入只在明确配置 Redis/RabbitMQ 的实例启用，高可用、容量方案和最终部署拓扑仍按路线留到后续版本。旧 `/opt/IM` 不在本文范围。

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
# 也可以指定版本和输出目录
scripts/build-release.sh v0.7 /tmp/im-v0.7
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
| `/ready` | 2 秒内成功 ping MySQL 时返回 200（`database=ok`）；否则返回 503（`database=unavailable`）。 |

```bash
/opt/IM-Rebuild/imctl health
/opt/IM-Rebuild/imctl status
curl -fsS http://127.0.0.1:8198/health
curl -fsS http://127.0.0.1:8198/ready
```

`imctl health` 只在 `/health` 和 `/ready` 都返回 200 时成功，任一失败或连接失败都返回非零。`imctl status` 仅用于查看 backend、MySQL、私有 Docker 和 `RELEASE`，是可读状态展示，不作为发布是否成功的判据。`/health` 为 200 但 `/ready` 为 503 表示进程在、数据库不可用，应先检查 `imctl mysql-status` 和 `imctl logs mysql`。

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

新二进制启动时会自动执行迁移。切换只替换应用文件，不停止 MySQL、不恢复数据库；迁移只向前执行，应用回滚时不要回滚或覆盖 MySQL 数据。

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

确认所选备份确实对应上一版本后再执行。回滚只恢复 `bin/`、`web/` 和 `RELEASE`，不恢复 `database.sql`，不动 MySQL 数据。

## 数据库备份与恢复检查

`imctl backup` 通过现有 MySQL 容器和容器内受保护密钥生成一致性 SQL dump 和 SHA-256，保存到仅 root 可访问的 `/opt/IM-Rebuild/backups/` 下按 UTC 时间加后缀命名的子目录，并打印路径。备份包含 `database.sql`、行数清单 `counts.tsv`、当前应用的 `bin/`、`web/`、`RELEASE`和 `SHA256SUMS`；保留历史，不自动删除，不把密码写入参数、环境变量或日志。

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

canary 应用入口是 `/opt/IM-Rebuild/canary-v07/bin/im-server` 和 `web/`。当前 canary 已部署 v1.6，其 v1.5 备份为 `bin/im-server-v15`、`RELEASE.v15` 和 `web-v15`；v0.6 基线仍保留为 `bin/im-server-v06`、`RELEASE.v06` 和初始 v0.6 `web/`。启动、状态和检查：

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
/opt/IM-Rebuild/imctl --verify restart
/opt/IM-Rebuild/imctl --verify health
```

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
- 当前正式 `8198` 已接入 v1.2 私聊、v1.3 群聊、v1.4 服务端读取链路和 v1.5 本地缓存，群聊已读仍按路线留在 v1.7；不配置开机自启，开发机重启后手动执行 `infractl start` 和 `imctl restart`。

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

## v1.4 服务端消息读取链路（已切换正式）

- 读取改为按会话的方向索引 `dm:conv:{owner}:{peer}`，另加用户级增量日志 `dm:feed:{user}` 和会话最后消息 `dm:conv-last:{user}`；`inbox`/`outbox` 已停止写入，只保留作回滚兜底。
- `index:epoch` 标记读取索引的完整边界：epoch 以下一律回落 MySQL。首次启动取 `max(落库最大消息号, Redis 计数器)`，重启保留原值，只有计数器落后于落库最大值时才上调。
- 新增 `GET /api/messages/sync?after_id=N`，一次返回该用户全部私聊增量；`complete=false` 表示补收不完整，需要从会话历史重新同步。
- 消息实体、`dm:feed`、`dm:conv`、`dm:conv-last` 均不设 TTL，也不再裁剪；Redis 内存不再有固定上限行为，发布后需要持续观察 `used_memory`。观察方式是在私有 Redis 容器内读取 `INFO memory` 的 `used_memory_human`、`maxmemory_human` 和 `maxmemory_policy`，凭据仍由容器内受保护文件提供。
- 切换后实测 `used_memory` 约 1.64 MB（上限 64 MB），键总数 232；`dm:feed`/`dm:conv`/`dm:conv-last` 在真实用户产生新消息前为空，属于 epoch 设计的预期现象。
- `inbox`/`outbox` 已停止写入，仅保留作回滚兜底；确认稳定运行一个版本后再按前缀清理。
- 本版未解决会话列表的 MySQL 查询量（仍为 4 条，含 `GROUP BY` 最新消息子查询），也未做服务端性能实测，统一留到 v1.6 和 v1.9。
- 回滚：v1.4 只改变 Redis 索引格式，MySQL 仍是权威。回退到 v1.3 二进制时，切换后产生的消息在 MySQL 中完整可用；但新消息没有写入 `inbox`/`outbox`，热窗口会由 MySQL 兜底。
- 验证实例继续使用 `imctl --verify`、Redis DB 1、vhost `im_v13_verify` 和命名空间 `im:v13-verify`。

## v1.5 Web 端本地缓存（已切换正式）

- 新增 `POST /api/bootstrap`，一次返回当前用户、会话列表、好友列表、私聊增量和有变化的群增量；`/api/messages/sync` 增加 `cursor`，单页上限提高到 200。
- 浏览器使用 IndexedDB `im-cache` 按账号缓存会话、好友和最近消息；每个会话最多 200 条，全账号最多 10000 条、100 个会话。
- 冷启动先渲染本地缓存，再通过 bootstrap 对齐；打开没有新消息的会话不再请求历史。退出登录先清除账号提示，再清除该账号缓存。
- v1.5 没有数据库迁移和 Redis 数据格式变更，回滚只需恢复切换前的应用文件。正式切换备份为 `/opt/IM-Rebuild/backups/20261026T103036Z-17TJXE`。
- 正式 smoke 产生的少量 Redis 测试索引未按 ID 清理，测试账号已从 MySQL 删除，不影响真实账号；容量治理时统一处理。
- 回归新增 `node scripts/cache-smoke.mjs`，覆盖冷启动单次 bootstrap、离线只读、断网恢复、退出清理和账号隔离。

## v1.6 私聊已读（已切换正式）

- 私聊已读位置保存为 Redis `dm:read:{userID}` HASH；未读用 `dm:conv` 的 `ZCOUNT` 现算，对方回执直接读取对方的位置。已读和未读请求不再查询 `conversation_reads`。
- 新增批量接口 `POST /api/read`（最多 50 项），保留单会话已读接口；已读位置通过 `dm:read:stream` 异步写回 MySQL，worker 使用 `GREATEST` 幂等落库。
- 升级前必须执行一次历史回填：
  ```bash
  /opt/IM-Rebuild/imctl --verify backfill-read
  /opt/IM-Rebuild/imctl backfill-read
  ```
  回填会索引历史私聊消息和已读位置，校验通过后写入 `dm:read:ready`。启用消息链路时，后端缺少该标记会拒绝启动；Redis 丢数据后也需要重新回填。
- 正式切换前备份为 `/opt/IM-Rebuild/backups/20261027T150410Z-4Z7wA7`；回填统计为 50 条私聊、8 条已读、9 个方向，Redis 内存 1.94 MB → 1.96 MB。
- 正式环境回归：API smoke 62 项、浏览器 smoke 39 项、缓存 smoke 18 项全部通过；本轮 10 个测试账号按 manifest 删除，生产队列 ready/unack 均为 0。
- 回滚应用文件仍使用切换前备份。v1.5 不读取新的 Redis 键，MySQL 已读位置由异步 worker 持续写入，回退后继续可用。
- 验证实例 `8199` 已停止；验证库测试账号和测试用 `dm:read` 键已清除。测试消息的 `dm:conv` 和实体索引留到 v1.9 容量治理。

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

API 和浏览器脚本使用 Node 26；浏览器脚本默认使用 `/Applications/Google Chrome.app/Contents/MacOS/Google Chrome`，可通过 `CHROME_PATH` 指定本机 Chrome。通过 `BASE_URL` 指定其他已确认的验证地址。容量脚本会创建 101 个独立账号并验证 100 人边界，仅用于隔离验证库。性能基线脚本串行采样现有 API，只输出 p50/p95/max，不设阈值，也不做压力测试。

脚本随机生成本轮账号和密码，密码只在进程内存中使用。完成后打印独立报告目录，`manifest.json` 只记录服务地址、本轮账号名、群 ID 和检查数；失败截图不包含登录表单。脚本退出非零表示检查未通过；账号清理需按本轮清单执行，不使用宽泛用户名通配符，不操作 `demo_*`。正式发布后的少量写入检查也必须使用本轮独立账号并按清单清理，不重置演示账号或它们的未读位置。

清理脚本先生成 SQL，不直接连接数据库；它只接受已登记的随机测试账号名，并核对清单服务端口与所选数据库一致：

```bash
node scripts/cleanup-test-data.mjs im_v07_verify /tmp/im-cleanup.sql /path/to/manifest.json
# 多轮清单可依次追加；正式8198的本轮清单需显式选择 im
```

输出文件不会覆盖已有文件。核对 SQL 中的数据库和账号清单后，用既有 relay 流程上传到开发机，再在开发机执行（示例使用已上传的文件）：

```bash
bash -c 'source /opt/IM-Rebuild/imctl; mysql_command mysql < /opt/IM-Rebuild/canary-v07/reports/cleanup.sql'
```

SQL 仅按明确用户名删除，表名包含固定数据库前缀，删除账号会级联清理这些账号的测试关系、消息和群；不会按用户名通配符清理，也不会修改真实账号或演示账号。

## 凭据和演示数据

- 应用数据库密码使用已有受保护文件，不写进命令行、环境变量值、发布包、备份清单或对话。生产密码文件为 `/opt/IM-Rebuild/mysql/secrets/mysql-app`；`imctl restart` 会把它复制为 `/opt/IM-Rebuild/config/mysql-app`（`nobody`，0400），后端通过 `IM_MYSQL_PASSWORD_FILE` 引用。canary 使用自己的 `/opt/IM-Rebuild/canary-v07/mysql-app`。
- relay 凭据只由既有加密配置和交互认证管理，不进入参数、环境变量、文件或回复。MySQL root 密钥只由备份/核验脚本在容器内读取，生成受保护的短期 optionfile 并自动清理，不输出给操作人员。发布包及应用回退不包含密码文件。
- 演示账号为 `demo_alice`、`demo_bob`、`demo_carol`、`demo_dave`，默认密码 `Demo123456`，仅用于演示。发布流程不会自动写入演示数据；需要时手动执行 `BASE_URL=http://127.0.0.1:8198 node scripts/seed-demo.mjs`。脚本会创建或复用演示账号、好友、群和消息，详见 [v0.6 演示数据](versions/v0.6.md)。
