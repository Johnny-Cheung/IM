# IM

一个通过浏览器访问的 Web 即时通讯项目。

## 项目边界

- 用户通过网址使用，不制作需要下载安装的桌面或移动客户端。
- `backend/` 用于后端服务，`frontend/` 用于浏览器前端。
- 桌面和手机支持均指桌面浏览器、手机浏览器中的响应式网页体验。
- 根目录用于放置跨模块配置、文档和部署编排文件。

## 当前状态

项目正在 `refactor/im` 分支从基础工程开始重构。[v0.1 基础工程](docs/versions/v0.1.md)、[v0.2 用户体系](docs/versions/v0.2.md) 和 [v0.3 好友体系](docs/versions/v0.3.md)（含好友记录）均已通过验收；[v0.4 文本私聊](docs/versions/v0.4.md) 和 [v0.5 消息体验补全](docs/versions/v0.5.md) 已通过联合验收；[v0.6 基础群聊](docs/versions/v0.6.md) 已通过验收。先建设 v0.x 基础 IM，后续再根据实际结果推进 v1.x 和 v2.x。

## 路线图

完整版本规划见 [docs/roadmap.md](docs/roadmap.md)。

版本只使用 `v0.1`、`v0.2` 这样的两段式编号，用于表达建设顺序。具体版本开始前再讨论范围、方案和验收标准；每个可验证的小步完成后提交并推送到远程分支。

## 本地运行

代码编辑、测试和构建在 Mac 完成，部署运行在 Linux 开发机。Mac 不连接开发机的私有 Docker/MySQL；若要在 Mac 本地启动后端，需要一台当前 Mac 可连接的 MySQL 8.0+，并显式提供连接配置。开发机 MySQL 只监听自身 `127.0.0.1:13307`，Mac 无法直接连接，不能把本地后端指向开发机数据库，也不要用生产数据库做本地开发。

先在可连接的 MySQL 中准备数据库和具有建表/迁移权限的应用账号，然后用密码文件提供应用密码（不要把密码放进命令行或仓库）：

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

后端启动时会自动执行迁移。也可以设置 `IM_MYSQL_DSN` 显式指定完整 DSN，它优先于上面的分项配置和密码文件。默认监听 `127.0.0.1:8080`，可用 `-addr` 修改；未指定 `-web-dir` 时只提供 API。

前端开发：

```bash
cd frontend
npm ci
npm run dev
```

浏览器打开 `http://localhost:5173/`。Vite 固定使用 5173，并把 `/api`（含 WebSocket）和 `/health` 代理到 `http://127.0.0.1:8080`；只有本地后端在该地址运行时代理才可用。

构建前端并由 Go 在本地托管：

```bash
(cd frontend && npm ci && npm run build)
(cd backend && go run ./cmd/server -web-dir ../frontend/dist)
```

此时浏览器打开 `http://127.0.0.1:8080/`。

## 发布构建

在干净的 `refactor/im` 工作区运行：

```bash
scripts/build-release.sh              # 默认 v0.7，输出到仓库外的临时目录
scripts/build-release.sh v0.7 /tmp/im-v0.7
```

脚本要求源码已提交且工作区干净，会校验当前分支，但不会自动切换分支或推送。它用 `npm ci` 和 `npm run build` 构建前端，用 `CGO_ENABLED=0 GOOS=linux GOARCH=amd64` 构建 Linux 二进制，产物目录包含 `bin/im-server`、`web/`、`RELEASE` 和 `SHA256SUMS`。`RELEASE` 记录版本、提交号、构建时间、Go/Node 版本以及所有产物的 SHA-256。部署、切换和回滚步骤见 [docs/operations.md](docs/operations.md)。
