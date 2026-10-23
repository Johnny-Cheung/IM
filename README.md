# IM

一个通过浏览器访问的 Web 即时通讯项目。

## 项目边界

- 用户通过网址使用，不制作需要下载安装的桌面或移动客户端。
- `backend/` 用于后端服务，`frontend/` 用于浏览器前端。
- 桌面和手机支持均指桌面浏览器、手机浏览器中的响应式网页体验。
- 根目录用于放置跨模块配置、文档和部署编排文件。

## 当前状态

项目正在 `refactor/im` 分支从基础工程开始重构。[v0.1 基础工程](docs/versions/v0.1.md) 和 [v0.2 用户体系](docs/versions/v0.2.md) 均已通过验收，[v0.3 好友体系](docs/versions/v0.3.md)（含好友记录）已部署验证，等待验收；先建设 v0.x 基础 IM，后续再根据实际结果推进 v1.x 和 v2.x。

## 路线图

完整版本规划见 [docs/roadmap.md](docs/roadmap.md)。

版本只使用 `v0.1`、`v0.2` 这样的两段式编号，用于表达建设顺序。具体版本开始前再讨论范围、方案和验收标准；每个可验证的小步完成后提交并推送到远程分支。

## v0.1 本地运行

后端和前端分别启动：

```bash
cd backend && go run ./cmd/server
cd frontend && npm install && npm run dev
```

浏览器打开 `http://localhost:5173/`。Vite 会将 `/health` 请求代理到 `http://127.0.0.1:8080`。

构建并由 Go 统一托管前端：

```bash
cd frontend && npm run build
cd backend && go run ./cmd/server -web-dir ../frontend/dist
```

此时浏览器打开 `http://127.0.0.1:8080/`。后端也可以使用 `-addr` 参数指定监听地址。

Linux amd64 构建：

```bash
cd backend
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/im-server-linux-amd64 ./cmd/server
```
