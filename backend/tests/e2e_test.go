//go:build e2e

// Package e2e 包含 GoIM 服务器的端到端集成测试。
// 这些测试覆盖完整技术栈：HTTP API、WebSocket、MQ 消费者、
// Redis、MySQL 和 RabbitMQ。测试要求 Docker 服务处于运行状态
// （按 configs/config.test.yaml 中的配置）。
//
// 运行命令：go test ./tests/... -v -tags e2e -timeout 120s
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	goredisv9 "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/api"
	"github.com/goim/goim/internal/config"
	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/consumer"
	"github.com/goim/goim/internal/infra"
	"github.com/goim/goim/internal/middleware"
	goredis "github.com/goim/goim/internal/redis"
	"github.com/goim/goim/internal/repository"
	"github.com/goim/goim/internal/service"
	"github.com/goim/goim/internal/ws"
)

// ──────────────────────────────────────────────────────
// 测试环境搭建
// ──────────────────────────────────────────────────────

// testEnv 保存完整的测试环境：服务器、连接、基础 URL。
type testEnv struct {
	baseURL string
	db      *sql.DB
	rdb     *goredisv9.Client
	mqConn  *amqp.Connection
	mqCh    *amqp.Channel
	logger  *zap.Logger
	server  *httptest.Server
	cancel  context.CancelFunc
}

var env *testEnv

// TestMain 为 E2E 测试搭建完整的 GoIM 服务器。
// 需要 MySQL、Redis 和 RabbitMQ 在本地运行
// （按 configs/config.test.yaml 中的配置）。
func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)

	// 加载测试配置
	cfgPath := "../configs/config.test.yaml"
	if v := os.Getenv("GOIM_CONFIG"); v != "" {
		cfgPath = v
	}
	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置: %v\n", err)
		os.Exit(1)
	}

	// 覆盖服务器端口（httptest 会自动分配）
	cfg.Server.Port = 0
	uploadDir, err := os.MkdirTemp("", "goim-e2e-uploads-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create upload directory: %v\n", err)
		os.Exit(1)
	}
	cfg.Server.UploadDir = uploadDir
	cfg.File.UploadDir = uploadDir

	// 初始化日志
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化日志: %v\n", err)
		os.Exit(1)
	}

	// ── 连接 MySQL ──
	db, err := infra.NewMySQLPool(&cfg.MySQL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "连接 MySQL: %v\n", err)
		os.Exit(1)
	}
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "MySQL 连通检测: %v\n", err)
		os.Exit(1)
	}
	logger.Info("MySQL 已连接，准备 E2E 测试")

	// ── 连接 Redis ──
	rdb, err := infra.NewRedisClient(&cfg.Redis)
	if err != nil {
		fmt.Fprintf(os.Stderr, "连接 Redis: %v\n", err)
		os.Exit(1)
	}
	logger.Info("Redis 已连接，准备 E2E 测试")

	// ── 连接 RabbitMQ ──
	mqConn, mqCh, err := infra.NewRabbitMQConn(&cfg.RabbitMQ)
	if err != nil {
		fmt.Fprintf(os.Stderr, "连接 RabbitMQ: %v\n", err)
		os.Exit(1)
	}
	if err := infra.DeclareQueues(mqCh); err != nil {
		fmt.Fprintf(os.Stderr, "声明队列: %v\n", err)
		os.Exit(1)
	}
	logger.Info("RabbitMQ 已连接，准备 E2E 测试")

	// ── 加载 Lua 脚本 ──
	ctx := context.Background()
	if err := goredis.LoadLuaScripts(rdb, ctx); err != nil {
		fmt.Fprintf(os.Stderr, "加载 Lua 脚本: %v\n", err)
		os.Exit(1)
	}

	// ── 构建仓库层 ──
	mysqlRepo := repository.NewMySQLRepo(db)
	redisRepo := repository.NewRedisRepo(rdb)
	mqRepo := repository.NewMQRepo(mqCh)

	// ── 构建连接管理器 ──
	cm := conn.NewConnectionManager()

	// ── 构建服务层 ──
	msgSvc := service.NewMsgService(redisRepo, mqRepo, cm, logger)
	authSvc := service.NewAuthService(mysqlRepo, cfg.JWT.Secret, cfg.JWT.AccessExpHours, cfg.JWT.RefreshExpDays)
	friendSvc := service.NewFriendService(mysqlRepo, redisRepo, logger)
	groupSvc := service.NewGroupService(mysqlRepo, redisRepo, logger)
	momentSvc := service.NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Duration(cfg.Moment.LikeCacheTTLHours)*time.Hour)
	msgOpSvc := service.NewMsgOpService(mysqlRepo, redisRepo, logger)
	settingsSvc := service.NewSettingsService(mysqlRepo, logger)

	// ── 构建 WS 消息分发器 ──
	dispatcher := ws.NewMessageDispatcher(msgSvc, friendSvc)

	// ── 构建 HTTP 处理器 ──
	authHandler := api.NewAuthHandler(authSvc)
	friendHandler := api.NewFriendHandler(friendSvc, rdb)
	groupHandler := api.NewGroupHandler(groupSvc)
	momentHandler := api.NewMomentHandler(momentSvc)
	msgOpHandler := api.NewMsgOpHandler(msgOpSvc)
	settingsHandler := api.NewSettingsHandler(settingsSvc)
	uploadHandler := api.NewUploadHandler(cfg.Server.UploadDir, cfg.File.MaxSizeMB, cfg.File.AllowedExts, mysqlRepo)
	avatarHandler := api.NewAvatarHandler()

	// ── 构建 Gin 路由器 ──
	router := buildRouter(cfg, rdb, cm, dispatcher, logger,
		authHandler, friendHandler, groupHandler,
		momentHandler, msgOpHandler, settingsHandler, uploadHandler, avatarHandler)

	// ── 启动 MQ 消费者 ──
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	privateMsgConsumer := consumer.NewPrivateMsgConsumer(mqCh, mysqlRepo, redisRepo, cm, logger)
	groupMsgConsumer := consumer.NewGroupMsgConsumer(mqCh, mysqlRepo, redisRepo, cm, logger)
	momentFeedConsumer := consumer.NewMomentFeedConsumer(mqCh, mysqlRepo, redisRepo, logger, cfg.Moment.BigUserFriendThreshold, cfg.Moment.TimelineMaxLen)

	if err := privateMsgConsumer.Start(consumerCtx); err != nil {
		fmt.Fprintf(os.Stderr, "启动私聊消息消费者: %v\n", err)
		os.Exit(1)
	}
	if err := groupMsgConsumer.Start(consumerCtx); err != nil {
		fmt.Fprintf(os.Stderr, "启动群组消息消费者: %v\n", err)
		os.Exit(1)
	}
	if err := momentFeedConsumer.Start(consumerCtx); err != nil {
		fmt.Fprintf(os.Stderr, "启动动态流消费者: %v\n", err)
		os.Exit(1)
	}
	logger.Info("MQ 消费者已启动，准备 E2E 测试")

	// ── 启动清理任务 ──
	infra.StartCleanupTask(rdb, logger, 1*time.Hour)

	// ── 启动 httptest 服务器 ──
	server := httptest.NewServer(router)

	env = &testEnv{
		baseURL: server.URL,
		db:      db,
		rdb:     rdb,
		mqConn:  mqConn,
		mqCh:    mqCh,
		logger:  logger,
		server:  server,
		cancel:  consumerCancel,
	}

	logger.Info("GoIM E2E 服务器已启动", zap.String("url", env.baseURL))

	// 执行测试
	code := m.Run()

	// ── 清理 ──
	consumerCancel()
	server.Close()
	os.RemoveAll(uploadDir)
	mqConn.Close()
	rdb.Close()
	db.Close()
	logger.Sync()
	os.Exit(code)
}

// buildRouter 创建用于 E2E 测试的 Gin 路由器，并挂载所有路由。
func buildRouter(
	cfg *config.Config,
	rdb *goredisv9.Client,
	cm *conn.ConnectionManager,
	dispatcher *ws.MessageDispatcher,
	logger *zap.Logger,
	authHandler *api.AuthHandler,
	friendHandler *api.FriendHandler,
	groupHandler *api.GroupHandler,
	momentHandler *api.MomentHandler,
	msgOpHandler *api.MsgOpHandler,
	settingsHandler *api.SettingsHandler,
	uploadHandler *api.UploadHandler,
	avatarHandler *api.AvatarHandler,
) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	// ── 健康检查 ──
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "goim"})
	})

	// ── 公开路由 ──
	public := r.Group("/api/v1")
	authHandler.RegisterRoutes(public)
	avatarHandler.RegisterRoutes(public)

	// ── 受保护路由 ──
	protected := r.Group("/api/v1")
	protected.Use(middleware.JWTAuthMiddleware(cfg.JWT.Secret))
	friendHandler.RegisterRoutes(protected)
	groupHandler.RegisterRoutes(protected)
	momentHandler.RegisterRoutes(protected)
	msgOpHandler.RegisterRoutes(protected)
	settingsHandler.RegisterRoutes(protected)
	uploadHandler.RegisterRoutes(protected)

	// ── WebSocket 端点 ──
	wsHandler := ws.ServeWebSocket(cfg.JWT.Secret, rdb, cm, dispatcher.Callback())
	r.GET(cfg.Server.WsPath, wsHandler)
	r.Static("/uploads", cfg.Server.UploadDir)

	return r
}

// ──────────────────────────────────────────────────────
// E2E 测试用例
// ──────────────────────────────────────────────────────

// TestE2E_HealthCheck 验证 /health 端点返回 ok。
func TestE2E_HealthCheck(t *testing.T) {
	status, body := doRequest(t, http.MethodGet, env.baseURL, "/health", nil, "")
	if status != http.StatusOK {
		t.Fatalf("健康检查: status=%d body=%s", status, body)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("反序列化健康检查响应: %v", err)
	}
	if resp["status"] != "ok" {
		t.Fatalf("期望 status=ok, 得到 %v", resp["status"])
	}
}

func TestE2E_AvatarAndUpload(t *testing.T) {
	avatarResp, err := http.Get(env.baseURL + "/api/v1/avatar/1?name=alice")
	if err != nil {
		t.Fatalf("get generated avatar: %v", err)
	}
	defer avatarResp.Body.Close()
	avatarBody, err := io.ReadAll(avatarResp.Body)
	if err != nil {
		t.Fatalf("read generated avatar: %v", err)
	}
	if avatarResp.StatusCode != http.StatusOK || avatarResp.Header.Get("Content-Type") != "image/svg+xml" || !bytes.Contains(avatarBody, []byte(">A<")) {
		t.Fatalf("unexpected generated avatar: status=%d type=%q body=%s", avatarResp.StatusCode, avatarResp.Header.Get("Content-Type"), avatarBody)
	}

	username := fmt.Sprintf("e2e_upload_%d", time.Now().UnixNano())
	registerUser(t, env.baseURL, username, "pass1234")
	token := loginUser(t, env.baseURL, username, "pass1234")

	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	part, err := writer.CreateFormFile("file", "avatar.png")
	if err != nil {
		t.Fatalf("create multipart file: %v", err)
	}
	if _, err := part.Write([]byte("png test payload")); err != nil {
		t.Fatalf("write multipart file: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, env.baseURL+"/api/v1/upload/avatar", &payload)
	if err != nil {
		t.Fatalf("create upload request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload avatar: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read upload response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload avatar: status=%d body=%s", resp.StatusCode, body)
	}
	var upload struct {
		URL  string `json:"url"`
		Size int64  `json:"size"`
	}
	decodeSuccessResponse(t, body, &upload)
	if upload.URL == "" || upload.Size == 0 {
		t.Fatalf("invalid upload response: %+v", upload)
	}

	fileResp, err := http.Get(env.baseURL + upload.URL)
	if err != nil {
		t.Fatalf("get uploaded avatar: %v", err)
	}
	defer fileResp.Body.Close()
	stored, err := io.ReadAll(fileResp.Body)
	if err != nil {
		t.Fatalf("read uploaded avatar: %v", err)
	}
	if fileResp.StatusCode != http.StatusOK || !bytes.Equal(stored, []byte("png test payload")) {
		t.Fatalf("uploaded avatar mismatch: status=%d body=%s", fileResp.StatusCode, stored)
	}
	status, loginBody := doRequest(t, http.MethodPost, env.baseURL, "/api/v1/auth/login", map[string]string{"username": username, "password": "pass1234"}, "")
	if status != http.StatusOK {
		t.Fatalf("login after avatar upload: status=%d body=%s", status, loginBody)
	}
	var login authLoginResp
	decodeSuccessResponse(t, loginBody, &login)
	if login.AvatarURL != upload.URL {
		t.Fatalf("avatar not persisted in login response: got=%q want=%q", login.AvatarURL, upload.URL)
	}
}

func TestE2E_WebSocketSingleDeviceKick(t *testing.T) {
	username := fmt.Sprintf("e2e_ws_kick_%d", time.Now().UnixNano())
	registerUser(t, env.baseURL, username, "pass1234")
	token := loginUser(t, env.baseURL, username, "pass1234")

	first := connectWS(t, env.baseURL, token)
	defer first.Close()
	second := connectWS(t, env.baseURL, token)
	defer closeWS(t, second)

	first.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, rawBytes, err := first.ReadMessage()
	if err != nil {
		t.Fatalf("read kick message: %v", err)
	}
	var raw struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(rawBytes, &raw); err != nil || raw.Reason != "new_login" {
		t.Fatalf("unexpected kick message: %s", rawBytes)
	}
}

// TestE2E_AuthFlow 测试完整的认证生命周期：
// 注册 → 登录 → 刷新令牌 → 使用刷新后的令牌。
func TestE2E_AuthFlow(t *testing.T) {
	username := fmt.Sprintf("e2e_auth_%d", time.Now().UnixNano())
	password := "testpass123"

	// 步骤 1：注册
	userID := registerUser(t, env.baseURL, username, password)
	t.Logf("已注册用户: id=%d username=%s", userID, username)
	if userID == 0 {
		t.Fatal("期望非零 user ID")
	}

	// 步骤 2：登录
	loginResp := loginUserFull(t, env.baseURL, username, password)
	t.Logf("已登录: token=%s...", loginResp.AccessToken[:20])

	// 步骤 3：在受保护端点上使用访问令牌
	status, _ := doAuthedRequest(t, http.MethodGet, env.baseURL,
		"/api/v1/friend/list", nil, loginResp.AccessToken)
	if status == http.StatusUnauthorized {
		t.Fatal("访问令牌应可在受保护端点上使用")
	}

	// 步骤 4：刷新令牌
	refreshResp := refreshToken(t, env.baseURL, loginResp.RefreshToken)
	t.Logf("刷新后的令牌: %s...", refreshResp.AccessToken[:20])

	// 步骤 5：使用刷新后的令牌
	status, _ = doAuthedRequest(t, http.MethodGet, env.baseURL,
		"/api/v1/friend/list", nil, refreshResp.AccessToken)
	if status == http.StatusUnauthorized {
		t.Fatal("刷新后的令牌应可在受保护端点上使用")
	}

	// 步骤 6：重复注册应失败
	status, _ = doRequest(t, http.MethodPost, env.baseURL, "/api/v1/auth/register",
		map[string]string{"username": username, "password": password}, "")
	if status != http.StatusConflict {
		t.Fatalf("重复注册应返回 409, 实际得到 %d", status)
	}
}

// TestE2E_FriendFlow 测试完整的好友生命周期：
// 发送请求 → 接受 → 列表 → 屏蔽 → 取消屏蔽 → 删除。
func TestE2E_FriendFlow(t *testing.T) {
	u1Name := fmt.Sprintf("e2e_fr1_%d", time.Now().UnixNano())
	u2Name := fmt.Sprintf("e2e_fr2_%d", time.Now().UnixNano())
	_ = registerUser(t, env.baseURL, u1Name, "pass1234")
	u2ID := registerUser(t, env.baseURL, u2Name, "pass1234")
	u1Token := loginUser(t, env.baseURL, u1Name, "pass1234")
	u2Token := loginUser(t, env.baseURL, u2Name, "pass1234")

	// 步骤 1：发送好友请求（u1 → u2）
	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": u2ID, "message": "let's be friends"}, u1Token)
	if status != http.StatusCreated {
		t.Fatalf("发送好友请求: status=%d body=%s", status, body)
	}
	var frResp friendRequestResp
	if err := json.Unmarshal(body, &frResp); err != nil {
		t.Fatalf("反序列化好友请求: %v", err)
	}
	t.Logf("好友请求: id=%d from=%d to=%d", frResp.RequestID, frResp.FromUserID, frResp.ToUserID)

	// 步骤 2：接受（u2）
	decodeSuccessResponse(t, body, &frResp)
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept",
		map[string]interface{}{"request_id": frResp.RequestID}, u2Token)
	if status != http.StatusOK {
		t.Fatalf("接受好友请求: status=%d body=%s", status, body)
	}
	var acceptResp acceptFriendResp
	if err := json.Unmarshal(body, &acceptResp); err != nil {
		t.Fatalf("反序列化接受响应: %v", err)
	}
	t.Logf("已接受好友: user=%d friend=%d", acceptResp.UserID, acceptResp.FriendID)

	// 步骤 3：获取好友列表（u1）
	decodeSuccessResponse(t, body, &acceptResp)
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, "/api/v1/friend/list", nil, u1Token)
	if status != http.StatusOK {
		t.Fatalf("获取好友列表: status=%d body=%s", status, body)
	}
	t.Logf("u1 的好友列表: %s", body)

	// 步骤 4：屏蔽（u1 屏蔽 u2）
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/block",
		map[string]interface{}{"blocked_id": u2ID}, u1Token)
	if status != http.StatusOK {
		t.Fatalf("屏蔽用户: status=%d body=%s", status, body)
	}

	// 步骤 5：取消屏蔽
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/unblock",
		map[string]interface{}{"blocked_id": u2ID}, u1Token)
	if status != http.StatusOK {
		t.Fatalf("取消屏蔽用户: status=%d body=%s", status, body)
	}

	// 步骤 6：删除好友
	status, body = doAuthedRequest(t, http.MethodDelete, env.baseURL,
		fmt.Sprintf("/api/v1/friend/%d", u2ID), nil, u1Token)
	if status != http.StatusOK {
		t.Fatalf("删除好友: status=%d body=%s", status, body)
	}
	t.Logf("已删除好友 %d", u2ID)
}

// TestE2E_GroupMessaging 测试群组创建、成员管理及 WS 消息收发。
func TestE2E_GroupMessaging(t *testing.T) {
	u1Name := fmt.Sprintf("e2e_grp1_%d", time.Now().UnixNano())
	u2Name := fmt.Sprintf("e2e_grp2_%d", time.Now().UnixNano())
	u1ID := registerUser(t, env.baseURL, u1Name, "pass1234")
	u2ID := registerUser(t, env.baseURL, u2Name, "pass1234")
	u1Token := loginUser(t, env.baseURL, u1Name, "pass1234")
	u2Token := loginUser(t, env.baseURL, u2Name, "pass1234")

	// 群成员只能从群主的好友中邀请，先建立测试所需的好友关系。
	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": u2ID, "message": "join test group"}, u1Token)
	if status != http.StatusCreated {
		t.Fatalf("创建好友请求: from=%d to=%d status=%d body=%s", u1ID, u2ID, status, body)
	}
	var friendRequest friendRequestResp
	decodeSuccessResponse(t, body, &friendRequest)
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept",
		map[string]interface{}{"request_id": friendRequest.RequestID}, u2Token)
	if status != http.StatusOK {
		t.Fatalf("接受好友请求: status=%d body=%s", status, body)
	}

	// 步骤 1：创建群组（u1 为群主）
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/group",
		map[string]interface{}{"name": "E2E Test Group", "notice": "test group"}, u1Token)
	if status != http.StatusCreated {
		t.Fatalf("创建群组: status=%d body=%s", status, body)
	}
	var grpResp createGroupResp
	if err := json.Unmarshal(body, &grpResp); err != nil {
		t.Fatalf("反序列化创建群组响应: %v", err)
	}
	decodeSuccessResponse(t, body, &grpResp)
	groupID := grpResp.GroupID
	t.Logf("群组已创建: id=%d", groupID)

	// 步骤 2：添加成员 u2
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL,
		fmt.Sprintf("/api/v1/group/%d/member", groupID),
		map[string]interface{}{"member_id": u2ID}, u1Token)
	if status != http.StatusOK {
		t.Fatalf("添加成员: status=%d body=%s", status, body)
	}

	// 步骤 3：获取群组信息
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL,
		fmt.Sprintf("/api/v1/group/%d", groupID), nil, u1Token)
	if status != http.StatusOK {
		t.Fatalf("获取群组信息: status=%d body=%s", status, body)
	}

	// 步骤 4：获取成员列表
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL,
		fmt.Sprintf("/api/v1/group/%d/members", groupID), nil, u1Token)
	if status != http.StatusOK {
		t.Fatalf("获取成员列表: status=%d body=%s", status, body)
	}

	// 步骤 5：u1 通过 WS 连接，发送群组消息
	wsConn1 := connectWS(t, env.baseURL, u1Token)
	defer closeWS(t, wsConn1)
	time.Sleep(500 * time.Millisecond)

	clientMsgID := fmt.Sprintf("e2e_grp_%d", time.Now().UnixNano())
	sendWSMessage(t, wsConn1, "msg", sendMsgData{
		ClientMsgID: clientMsgID,
		ConvType:    2,
		ToID:        groupID,
		MsgType:     1,
		Content:     "hello group from E2E",
		Timestamp:   time.Now().Unix(),
	})

	// 读取 serverAck
	ack := readWSMessageType(t, wsConn1, "serverAck", 10*time.Second)
	var ackData serverAckData
	if err := json.Unmarshal(ack.Data, &ackData); err != nil {
		t.Fatalf("反序列化 serverAck: %v", err)
	}
	if ackData.ServerMsgID == 0 {
		t.Fatal("期望非零 serverMsgID")
	}
	t.Logf("群组消息已确认: serverMsgID=%d groupSeq=%d", ackData.ServerMsgID, ackData.GroupSeq)

	// 步骤 6：u2 通过 WS 连接，同步消息
	wsConn2 := connectWS(t, env.baseURL, u2Token)
	defer closeWS(t, wsConn2)
	time.Sleep(500 * time.Millisecond)

	sendWSMessage(t, wsConn2, "syncReq", map[string]interface{}{
		"lastSyncTime": 0,
		"batchSize":    100,
	})

	// 读取同步响应
	syncMsg := readWSMessage(t, wsConn2, 10*time.Second)
	dataStr := string(syncMsg.Data)
	if len(dataStr) > 200 {
		dataStr = dataStr[:200]
	}
	t.Logf("u2 同步: type=%s data=%s", syncMsg.Type, dataStr)
}

// TestE2E_PrivateMessaging 测试两个好友之间通过 WS 的私聊消息。
func TestE2E_PrivateMessaging(t *testing.T) {
	u1Name := fmt.Sprintf("e2e_pm1_%d", time.Now().UnixNano())
	u2Name := fmt.Sprintf("e2e_pm2_%d", time.Now().UnixNano())
	u1ID := registerUser(t, env.baseURL, u1Name, "pass1234")
	u2ID := registerUser(t, env.baseURL, u2Name, "pass1234")
	u1Token := loginUser(t, env.baseURL, u1Name, "pass1234")
	u2Token := loginUser(t, env.baseURL, u2Name, "pass1234")

	// 将两人设为好友（私聊消息需要验证好友关系 —— Lua 检查）
	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": u2ID, "message": "let's chat"}, u1Token)
	if status != http.StatusCreated {
		t.Fatalf("好友请求: status=%d body=%s", status, body)
	}
	var frResp friendRequestResp
	decodeSuccessResponse(t, body, &frResp)

	status, _ = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept",
		map[string]interface{}{"request_id": frResp.RequestID}, u2Token)
	if status != http.StatusOK {
		t.Fatalf("接受好友: status=%d", status)
	}

	// u1 和 u2 通过 WS 连接
	wsConn1 := connectWS(t, env.baseURL, u1Token)
	defer closeWS(t, wsConn1)
	time.Sleep(500 * time.Millisecond)

	wsConn2 := connectWS(t, env.baseURL, u2Token)
	defer closeWS(t, wsConn2)
	time.Sleep(500 * time.Millisecond)

	// 从 u1 向 u2 发送私聊消息
	clientMsgID := fmt.Sprintf("e2e_pm_%d", time.Now().UnixNano())
	sendWSMessage(t, wsConn1, "msg", sendMsgData{
		ClientMsgID: clientMsgID,
		ConvType:    1,
		ToID:        u2ID,
		MsgType:     1,
		Content:     "hello from E2E private msg",
		Timestamp:   time.Now().Unix(),
	})

	// 在 u1 上读取 serverAck
	ack := readWSMessageType(t, wsConn1, "serverAck", 10*time.Second)
	var ackData serverAckData
	if err := json.Unmarshal(ack.Data, &ackData); err != nil {
		t.Fatalf("反序列化 serverAck: %v", err)
	}
	if ackData.ServerMsgID == 0 {
		t.Fatal("期望非零 serverMsgID")
	}
	t.Logf("私聊消息已确认: serverMsgID=%d", ackData.ServerMsgID)

	// u2 应收到消息（推送模型 —— 消费者推送到 u2 的 WS）
	msgOnU2 := readWSMessageType(t, wsConn2, "msg", 15*time.Second)
	dataStr := string(msgOnU2.Data)
	if len(dataStr) > 200 {
		dataStr = dataStr[:200]
	}
	t.Logf("u2 收到消息: data=%s", dataStr)

	// 从 u2 发送 deliverAck
	var receivedMsg struct {
		MsgID  int64  `json:"msgId"`
		ConvID string `json:"convId"`
	}
	if err := json.Unmarshal(msgOnU2.Data, &receivedMsg); err == nil && receivedMsg.MsgID > 0 {
		sendWSMessage(t, wsConn2, "deliverAck", map[string]interface{}{
			"serverMsgId": receivedMsg.MsgID,
		})
	}

	// 从 u2 发送 readAck
	convID := fmt.Sprintf("p_%d_%d", min64(u1ID, u2ID), max64(u1ID, u2ID))
	sendWSMessage(t, wsConn2, "readAck", map[string]interface{}{
		"convId": convID,
	})
	t.Logf("u2 发送 readAck, convId=%s", convID)
}

// TestE2E_MomentFlow 测试动态生命周期：发布 → 点赞 → 评论 → 动态流。
func TestE2E_MomentFlow(t *testing.T) {
	u1Name := fmt.Sprintf("e2e_mom1_%d", time.Now().UnixNano())
	u2Name := fmt.Sprintf("e2e_mom2_%d", time.Now().UnixNano())
	_ = registerUser(t, env.baseURL, u1Name, "pass1234")
	u2ID := registerUser(t, env.baseURL, u2Name, "pass1234")
	u1Token := loginUser(t, env.baseURL, u1Name, "pass1234")
	u2Token := loginUser(t, env.baseURL, u2Name, "pass1234")

	// 将两人设为好友（动态流分发需要）
	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": u2ID, "message": "share moments"}, u1Token)
	if status != http.StatusCreated {
		t.Fatalf("好友请求: status=%d body=%s", status, body)
	}
	var frResp friendRequestResp
	decodeSuccessResponse(t, body, &frResp)
	doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept",
		map[string]interface{}{"request_id": frResp.RequestID}, u2Token)

	// 步骤 1：发布动态（u1）
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/moment",
		map[string]interface{}{
			"content":    "E2E test moment!",
			"visibility": 2,
		}, u1Token)
	if status != http.StatusCreated {
		t.Fatalf("发布动态: status=%d body=%s", status, body)
	}
	var momResp publishMomentResp
	if err := json.Unmarshal(body, &momResp); err != nil {
		t.Fatalf("反序列化动态: %v", err)
	}
	decodeSuccessResponse(t, body, &momResp)
	momentID := momResp.MomentID
	t.Logf("动态已发布: id=%d", momentID)

	// 步骤 2：按 ID 获取动态
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL,
		fmt.Sprintf("/api/v1/moment/%d", momentID), nil, u1Token)
	if status != http.StatusOK {
		t.Fatalf("获取动态: status=%d body=%s", status, body)
	}

	// 步骤 3：点赞动态（u2）
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL,
		fmt.Sprintf("/api/v1/moment/%d/like", momentID), nil, u2Token)
	if status != http.StatusOK {
		t.Fatalf("点赞动态: status=%d body=%s", status, body)
	}

	// 步骤 4：取消点赞动态（u2）
	status, body = doAuthedRequest(t, http.MethodDelete, env.baseURL,
		fmt.Sprintf("/api/v1/moment/%d/like", momentID), nil, u2Token)
	if status != http.StatusOK {
		t.Fatalf("取消点赞动态: status=%d body=%s", status, body)
	}

	// 步骤 5：评论（u2）
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL,
		fmt.Sprintf("/api/v1/moment/%d/comment", momentID),
		map[string]interface{}{"content": "nice post!"}, u2Token)
	if status != http.StatusCreated {
		t.Fatalf("评论动态: status=%d body=%s", status, body)
	}
	var commentResp struct {
		CommentID int64 `json:"comment_id"`
	}
	decodeSuccessResponse(t, body, &commentResp)
	t.Logf("评论已添加: id=%d", commentResp.CommentID)

	// 步骤 6：获取动态流（u2）—— 等待 MQ 消费者分发完成
	time.Sleep(2 * time.Second)
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL,
		"/api/v1/moment/feed?limit=10", nil, u2Token)
	if status != http.StatusOK {
		t.Fatalf("获取动态流: status=%d body=%s", status, body)
	}
	t.Logf("u2 动态流: %s", body)

	// 步骤 7：删除评论
	if commentResp.CommentID > 0 {
		status, body = doAuthedRequest(t, http.MethodDelete, env.baseURL,
			fmt.Sprintf("/api/v1/moment/comment/%d", commentResp.CommentID), nil, u2Token)
		if status != http.StatusOK {
			t.Fatalf("删除评论: status=%d body=%s", status, body)
		}
	}
}

func TestE2E_FriendRequestManagement(t *testing.T) {
	aName := fmt.Sprintf("e2e_reject_a_%d", time.Now().UnixNano())
	bName := fmt.Sprintf("e2e_reject_b_%d", time.Now().UnixNano())
	_ = registerUser(t, env.baseURL, aName, "pass1234")
	bID := registerUser(t, env.baseURL, bName, "pass1234")
	aToken := loginUser(t, env.baseURL, aName, "pass1234")
	bToken := loginUser(t, env.baseURL, bName, "pass1234")

	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request", map[string]interface{}{"to_user_id": bID}, aToken)
	if status != http.StatusCreated {
		t.Fatalf("create request: status=%d body=%s", status, body)
	}
	var request friendRequestResp
	decodeSuccessResponse(t, body, &request)

	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, "/api/v1/friend/requests?limit=1", nil, bToken)
	if status != http.StatusOK {
		t.Fatalf("list requests: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/reject", map[string]interface{}{"request_id": request.RequestID}, bToken)
	if status != http.StatusOK {
		t.Fatalf("reject request: status=%d body=%s", status, body)
	}

	// 被拒绝后允许同方向再次申请，并复用原记录重置为待处理。
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": bID, "message": "try again"}, aToken)
	if status != http.StatusCreated {
		t.Fatalf("reapply after rejection: status=%d body=%s", status, body)
	}
	var reopened friendRequestResp
	decodeSuccessResponse(t, body, &reopened)
	if reopened.RequestID != request.RequestID || reopened.Status != 0 {
		t.Fatalf("reopened request mismatch: old=%d reopened=%+v", request.RequestID, reopened)
	}

	// 重置为待处理后再次重复申请是幂等操作，返回同一待处理记录。
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": bID}, aToken)
	if status != http.StatusCreated {
		t.Fatalf("duplicate pending request: status=%d body=%s", status, body)
	}
	var duplicate friendRequestResp
	decodeSuccessResponse(t, body, &duplicate)
	if duplicate.RequestID != request.RequestID || duplicate.Status != 0 {
		t.Fatalf("duplicate request should return existing pending record: %+v", duplicate)
	}
}

func TestE2E_GroupManagement(t *testing.T) {
	ownerName := fmt.Sprintf("e2e_group_owner_%d", time.Now().UnixNano())
	memberName := fmt.Sprintf("e2e_group_member_%d", time.Now().UnixNano())
	ownerID := registerUser(t, env.baseURL, ownerName, "pass1234")
	memberID := registerUser(t, env.baseURL, memberName, "pass1234")
	ownerToken := loginUser(t, env.baseURL, ownerName, "pass1234")
	memberToken := loginUser(t, env.baseURL, memberName, "pass1234")

	// 群成员邀请仅允许从好友中选择，先建立好友关系。
	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request", map[string]int64{"to_user_id": memberID}, ownerToken)
	if status != http.StatusCreated {
		t.Fatalf("create owner-member friendship request: status=%d body=%s", status, body)
	}
	var friendRequest friendRequestResp
	decodeSuccessResponse(t, body, &friendRequest)
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept", map[string]int64{"request_id": friendRequest.RequestID}, memberToken)
	if status != http.StatusOK {
		t.Fatalf("accept owner-member friendship request: status=%d body=%s", status, body)
	}
	if ownerID <= 0 {
		t.Fatal("invalid owner id")
	}

	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/group", map[string]string{"name": "management group"}, ownerToken)
	if status != http.StatusCreated {
		t.Fatalf("create group: status=%d body=%s", status, body)
	}
	var created createGroupResp
	decodeSuccessResponse(t, body, &created)
	groupPath := fmt.Sprintf("/api/v1/group/%d", created.GroupID)

	status, body = doAuthedRequest(t, http.MethodPut, env.baseURL, groupPath, map[string]string{"name": "updated group", "notice": "updated"}, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("update group: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, groupPath, nil, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("get group: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, groupPath+"/member", map[string]int64{"member_id": memberID}, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("add member: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, groupPath+"/members?limit=1", nil, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("list members: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPut, env.baseURL, fmt.Sprintf("%s/member/%d/role", groupPath, memberID), map[string]int{"role": 1}, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("promote member: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPut, env.baseURL, fmt.Sprintf("%s/member/%d/role", groupPath, memberID), map[string]int{"role": 0}, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("demote member to role 0: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPut, env.baseURL, fmt.Sprintf("%s/member/%d/role", groupPath, memberID), map[string]interface{}{}, ownerToken)
	if status != http.StatusBadRequest {
		t.Fatalf("missing role should be rejected: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, groupPath+"/leave", nil, memberToken)
	if status != http.StatusOK {
		t.Fatalf("leave group: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, groupPath+"/member", map[string]int64{"member_id": memberID}, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("re-add member: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodDelete, env.baseURL, fmt.Sprintf("%s/member/%d", groupPath, memberID), nil, ownerToken)
	if status != http.StatusOK {
		t.Fatalf("remove member: status=%d body=%s", status, body)
	}
}

func TestE2E_UserMoments(t *testing.T) {
	username := fmt.Sprintf("e2e_user_moments_%d", time.Now().UnixNano())
	userID := registerUser(t, env.baseURL, username, "pass1234")
	token := loginUser(t, env.baseURL, username, "pass1234")
	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/moment", map[string]interface{}{"content": "author timeline", "visibility": 2}, token)
	if status != http.StatusCreated {
		t.Fatalf("publish moment: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, fmt.Sprintf("/api/v1/moment/user/%d?limit=1", userID), nil, token)
	if status != http.StatusOK {
		t.Fatalf("get user moments: status=%d body=%s", status, body)
	}
}

func TestE2E_MomentVisibilityValidation(t *testing.T) {
	username := fmt.Sprintf("e2e_moment_visibility_%d", time.Now().UnixNano())
	registerUser(t, env.baseURL, username, "pass1234")
	token := loginUser(t, env.baseURL, username, "pass1234")

	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/moment",
		map[string]interface{}{"content": "default visibility"}, token)
	if status != http.StatusCreated {
		t.Fatalf("missing visibility should use default: status=%d body=%s", status, body)
	}
	var created publishMomentResp
	decodeSuccessResponse(t, body, &created)
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL,
		fmt.Sprintf("/api/v1/moment/%d", created.MomentID), nil, token)
	if status != http.StatusOK {
		t.Fatalf("get default visibility moment: status=%d body=%s", status, body)
	}
	var envelope apiResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("parse default visibility response: %v", err)
	}
	var moment struct {
		Visibility int `json:"visibility"`
	}
	if err := json.Unmarshal(envelope.Data, &moment); err != nil || moment.Visibility != 2 {
		t.Fatalf("missing visibility should persist as 2: visibility=%d err=%v", moment.Visibility, err)
	}

	for _, visibility := range []int{0, 1, 4} {
		status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/moment",
			map[string]interface{}{"content": "invalid visibility", "visibility": visibility}, token)
		if status != http.StatusBadRequest {
			t.Fatalf("visibility=%d should be rejected: status=%d body=%s", visibility, status, body)
		}
		var resp apiResponse
		if err := json.Unmarshal(body, &resp); err != nil || resp.Code != api.CodeInvalidVisibility {
			t.Fatalf("visibility=%d wrong response: code=%d err=%v body=%s", visibility, resp.Code, err, body)
		}
	}
}

func TestE2E_MomentEmptyContentCodes(t *testing.T) {
	username := fmt.Sprintf("e2e_moment_empty_%d", time.Now().UnixNano())
	registerUser(t, env.baseURL, username, "pass1234")
	token := loginUser(t, env.baseURL, username, "pass1234")

	assertEmptyContent := func(path string, body map[string]interface{}) {
		status, responseBody := doAuthedRequest(t, http.MethodPost, env.baseURL, path, body, token)
		if status != http.StatusBadRequest {
			t.Fatalf("empty content path=%s: status=%d body=%s", path, status, responseBody)
		}
		var response apiResponse
		if err := json.Unmarshal(responseBody, &response); err != nil || response.Code != api.CodeMomentContentEmpty {
			t.Fatalf("empty content path=%s: code=%d err=%v body=%s", path, response.Code, err, responseBody)
		}
	}

	assertEmptyContent("/api/v1/moment", map[string]interface{}{"content": "", "visibility": 2})
	assertEmptyContent("/api/v1/moment", map[string]interface{}{"visibility": 2})

	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/moment",
		map[string]interface{}{"content": "comment target", "visibility": 2}, token)
	if status != http.StatusCreated {
		t.Fatalf("create comment target: status=%d body=%s", status, body)
	}
	var created publishMomentResp
	decodeSuccessResponse(t, body, &created)
	commentPath := fmt.Sprintf("/api/v1/moment/%d/comment", created.MomentID)
	assertEmptyContent(commentPath, map[string]interface{}{"content": ""})
	assertEmptyContent(commentPath, map[string]interface{}{})
}

func TestE2E_MomentPrivacyAcrossReadEndpoints(t *testing.T) {
	stamp := time.Now().UnixNano()
	authorName := fmt.Sprintf("e2e_privacy_author_%d", stamp)
	friendName := fmt.Sprintf("e2e_privacy_friend_%d", stamp)
	strangerName := fmt.Sprintf("e2e_privacy_stranger_%d", stamp)
	authorID := registerUser(t, env.baseURL, authorName, "pass1234")
	friendID := registerUser(t, env.baseURL, friendName, "pass1234")
	registerUser(t, env.baseURL, strangerName, "pass1234")
	authorToken := loginUser(t, env.baseURL, authorName, "pass1234")
	friendToken := loginUser(t, env.baseURL, friendName, "pass1234")
	strangerToken := loginUser(t, env.baseURL, strangerName, "pass1234")

	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": friendID}, authorToken)
	if status != http.StatusCreated {
		t.Fatalf("create privacy friendship request: status=%d body=%s", status, body)
	}
	var request friendRequestResp
	decodeSuccessResponse(t, body, &request)
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept",
		map[string]interface{}{"request_id": request.RequestID}, friendToken)
	if status != http.StatusOK {
		t.Fatalf("accept privacy friendship: status=%d body=%s", status, body)
	}

	publish := func(content string, visibility int) int64 {
		status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/moment",
			map[string]interface{}{"content": content, "visibility": visibility}, authorToken)
		if status != http.StatusCreated {
			t.Fatalf("publish visibility=%d: status=%d body=%s", visibility, status, body)
		}
		var response publishMomentResp
		decodeSuccessResponse(t, body, &response)
		return response.MomentID
	}
	friendsID := publish("privacy friends", 2)
	privateID := publish("privacy private", 3)
	time.Sleep(100 * time.Millisecond)

	assertDetail := func(token string, momentID int64, wantStatus, wantCode int) {
		status, body := doAuthedRequest(t, http.MethodGet, env.baseURL,
			fmt.Sprintf("/api/v1/moment/%d", momentID), nil, token)
		if status != wantStatus {
			t.Fatalf("detail moment=%d: status=%d want=%d body=%s", momentID, status, wantStatus, body)
		}
		var response apiResponse
		if err := json.Unmarshal(body, &response); err != nil || response.Code != wantCode {
			t.Fatalf("detail moment=%d: code=%d want=%d err=%v body=%s", momentID, response.Code, wantCode, err, body)
		}
	}
	for _, id := range []int64{friendsID, privateID} {
		assertDetail(authorToken, id, http.StatusOK, 0)
	}
	assertDetail(friendToken, friendsID, http.StatusOK, 0)
	assertDetail(friendToken, privateID, http.StatusNotFound, api.CodeMomentNotFound)
	assertDetail(strangerToken, friendsID, http.StatusNotFound, api.CodeMomentNotFound)
	assertDetail(strangerToken, privateID, http.StatusNotFound, api.CodeMomentNotFound)

	listIDs := func(token string) map[int64]bool {
		status, body := doAuthedRequest(t, http.MethodGet, env.baseURL,
			fmt.Sprintf("/api/v1/moment/user/%d?limit=20&offset=0", authorID), nil, token)
		if status != http.StatusOK {
			t.Fatalf("get user moments: status=%d body=%s", status, body)
		}
		var response struct {
			Data struct {
				Items []struct {
					ID int64 `json:"id"`
				} `json:"items"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatalf("parse user moments: %v", err)
		}
		ids := make(map[int64]bool)
		for _, item := range response.Data.Items {
			ids[item.ID] = true
		}
		return ids
	}
	authorIDs := listIDs(authorToken)
	friendIDs := listIDs(friendToken)
	strangerIDs := listIDs(strangerToken)
	if !authorIDs[friendsID] || !authorIDs[privateID] {
		t.Fatalf("author list missing privacy moments: %+v", authorIDs)
	}
	if !friendIDs[friendsID] || friendIDs[privateID] {
		t.Fatalf("friend list visibility mismatch: %+v", friendIDs)
	}
	if strangerIDs[friendsID] || strangerIDs[privateID] {
		t.Fatalf("stranger list visibility mismatch: %+v", strangerIDs)
	}

	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, "/api/v1/moment/feed?limit=20", nil, friendToken)
	if status != http.StatusOK {
		t.Fatalf("friend feed: status=%d body=%s", status, body)
	}
	var feed struct {
		Data struct {
			Moments []struct {
				ID int64 `json:"id"`
			} `json:"moments"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &feed); err != nil {
		t.Fatalf("parse friend feed: %v", err)
	}
	feedIDs := make(map[int64]bool)
	for _, item := range feed.Data.Moments {
		feedIDs[item.ID] = true
	}
	if !feedIDs[friendsID] || feedIDs[privateID] {
		t.Fatalf("friend feed visibility mismatch: %+v", feedIDs)
	}
}

func TestE2E_MessageOperations(t *testing.T) {
	senderName := fmt.Sprintf("e2e_msgop_sender_%d", time.Now().UnixNano())
	receiverName := fmt.Sprintf("e2e_msgop_receiver_%d", time.Now().UnixNano())
	senderID := registerUser(t, env.baseURL, senderName, "pass1234")
	receiverID := registerUser(t, env.baseURL, receiverName, "pass1234")
	senderToken := loginUser(t, env.baseURL, senderName, "pass1234")
	receiverToken := loginUser(t, env.baseURL, receiverName, "pass1234")

	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request", map[string]int64{"to_user_id": receiverID}, senderToken)
	if status != http.StatusCreated {
		t.Fatalf("create friendship request: status=%d body=%s", status, body)
	}
	var friendRequest friendRequestResp
	decodeSuccessResponse(t, body, &friendRequest)
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept", map[string]int64{"request_id": friendRequest.RequestID}, receiverToken)
	if status != http.StatusOK {
		t.Fatalf("accept friendship request: status=%d body=%s", status, body)
	}

	senderWS := connectWS(t, env.baseURL, senderToken)
	defer closeWS(t, senderWS)
	receiverWS := connectWS(t, env.baseURL, receiverToken)
	defer closeWS(t, receiverWS)
	convID := fmt.Sprintf("p_%d_%d", min64(senderID, receiverID), max64(senderID, receiverID))

	send := func(clientMsgID, content string) int64 {
		sendWSMessage(t, senderWS, "msg", sendMsgData{ClientMsgID: clientMsgID, ConvType: 1, ToID: receiverID, MsgType: 1, Content: content, Timestamp: time.Now().UnixMilli()})
		ack := readWSMessageType(t, senderWS, "serverAck", 10*time.Second)
		var ackData serverAckData
		if err := json.Unmarshal(ack.Data, &ackData); err != nil {
			t.Fatalf("parse server acknowledgement: %v", err)
		}
		if ackData.ServerMsgID == 0 {
			t.Fatal("expected server message ID")
		}
		readWSMessageType(t, receiverWS, "msg", 10*time.Second)
		return ackData.ServerMsgID
	}

	firstID := send(fmt.Sprintf("e2e_msgop_search_%d", time.Now().UnixNano()), "searchable integration message")
	// serverAck 只在消费者完成 Redis/MySQL 写入后发送，因此这里无需等待即可查询和撤回。
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, "/api/v1/msg/search?q=searchable&limit=10", nil, senderToken)
	if status != http.StatusOK {
		t.Fatalf("search messages: status=%d body=%s", status, body)
	}
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/msg/revoke", map[string]interface{}{"convId": convID, "msgId": firstID}, senderToken)
	if status != http.StatusOK {
		t.Fatalf("revoke message: status=%d body=%s", status, body)
	}

	secondID := send(fmt.Sprintf("e2e_msgop_delete_%d", time.Now().UnixNano()), "deletable integration message")
	// 收到 serverAck 后立即删除，验证不存在 MQ 消费时序窗口。
	status, body = doAuthedRequest(t, http.MethodDelete, env.baseURL, fmt.Sprintf("/api/v1/msg/%d?convId=%s", secondID, convID), nil, senderToken)
	if status != http.StatusOK {
		t.Fatalf("delete message: status=%d body=%s", status, body)
	}
}

func TestE2E_SyncCompositeCursorSameTimestamp(t *testing.T) {
	stamp := time.Now().UnixNano()
	senderName := fmt.Sprintf("e2e_sync_sender_%d", stamp)
	receiverName := fmt.Sprintf("e2e_sync_receiver_%d", stamp)
	registerUser(t, env.baseURL, senderName, "pass1234")
	receiverID := registerUser(t, env.baseURL, receiverName, "pass1234")
	senderToken := loginUser(t, env.baseURL, senderName, "pass1234")
	receiverToken := loginUser(t, env.baseURL, receiverName, "pass1234")

	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": receiverID}, senderToken)
	if status != http.StatusCreated {
		t.Fatalf("create sync friendship request: status=%d body=%s", status, body)
	}
	var request friendRequestResp
	decodeSuccessResponse(t, body, &request)
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept",
		map[string]interface{}{"request_id": request.RequestID}, receiverToken)
	if status != http.StatusOK {
		t.Fatalf("accept sync friendship: status=%d body=%s", status, body)
	}

	senderWS := connectWS(t, env.baseURL, senderToken)
	defer closeWS(t, senderWS)
	receiverWS := connectWS(t, env.baseURL, receiverToken)
	defer closeWS(t, receiverWS)
	sharedTimestamp := time.Now().UnixMilli()
	sentIDs := make([]int64, 0, 3)
	for i := 0; i < 3; i++ {
		sendWSMessage(t, senderWS, "msg", sendMsgData{
			ClientMsgID: fmt.Sprintf("e2e-sync-%d-%d", stamp, i),
			ConvType:    1,
			ToID:        receiverID,
			MsgType:     1,
			Content:     fmt.Sprintf("same timestamp %d", i),
			Timestamp:   sharedTimestamp,
		})
		ack := readWSMessageType(t, senderWS, "serverAck", 10*time.Second)
		var ackData serverAckData
		if err := json.Unmarshal(ack.Data, &ackData); err != nil {
			t.Fatalf("parse sync message ack: %v", err)
		}
		sentIDs = append(sentIDs, ackData.ServerMsgID)
		readWSMessageType(t, receiverWS, "msg", 10*time.Second)
	}

	type syncBatchData struct {
		Messages []struct {
			MsgID int64 `json:"msgId"`
		} `json:"msgs"`
		HasMore   bool  `json:"hasMore"`
		SyncTime  int64 `json:"syncTime"`
		SyncMsgID int64 `json:"syncMsgId"`
	}
	sendWSMessage(t, receiverWS, "syncReq", map[string]interface{}{
		"lastSyncTime": sharedTimestamp,
		"batchSize":    2,
	})
	firstRaw := readWSMessageType(t, receiverWS, "syncBatch", 10*time.Second)
	var first syncBatchData
	if err := json.Unmarshal(firstRaw.Data, &first); err != nil {
		t.Fatalf("parse first sync page: %v", err)
	}
	readWSMessageType(t, receiverWS, "convSync", 10*time.Second)
	if len(first.Messages) != 2 || !first.HasMore || first.SyncTime != sharedTimestamp || first.SyncMsgID == 0 {
		t.Fatalf("first sync page mismatch: %+v", first)
	}

	sendWSMessage(t, receiverWS, "syncReq", map[string]interface{}{
		"lastSyncTime":  first.SyncTime,
		"lastSyncMsgId": first.SyncMsgID,
		"batchSize":     2,
	})
	secondRaw := readWSMessageType(t, receiverWS, "syncBatch", 10*time.Second)
	var second syncBatchData
	if err := json.Unmarshal(secondRaw.Data, &second); err != nil {
		t.Fatalf("parse second sync page: %v", err)
	}
	if len(second.Messages) != 1 || second.HasMore {
		t.Fatalf("second sync page mismatch: %+v", second)
	}

	got := []int64{first.Messages[0].MsgID, first.Messages[1].MsgID, second.Messages[0].MsgID}
	sort.Slice(sentIDs, func(i, j int) bool { return sentIDs[i] < sentIDs[j] })
	if !reflect.DeepEqual(got, sentIDs) {
		t.Fatalf("sync pages duplicated or omitted messages: got=%v want=%v", got, sentIDs)
	}
}

// TestE2E_SettingsFlow 测试用户设置的生命周期。
func TestE2E_SettingsFlow(t *testing.T) {
	username := fmt.Sprintf("e2e_set_%d", time.Now().UnixNano())
	friendName := fmt.Sprintf("e2e_setf_%d", time.Now().UnixNano())
	meID := registerUser(t, env.baseURL, username, "pass1234")
	token := loginUser(t, env.baseURL, username, "pass1234")
	friendID := registerUser(t, env.baseURL, friendName, "pass1234")
	friendToken := loginUser(t, env.baseURL, friendName, "pass1234")

	// 静音校验要求会话双方为好友：先建立好友关系（修复预存断裂）
	status, body := doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/request",
		map[string]interface{}{"to_user_id": friendID, "message": "settings flow"}, token)
	if status != http.StatusCreated {
		t.Fatalf("发送好友申请: status=%d body=%s", status, body)
	}
	var reqResp struct {
		Data struct {
			RequestID int64 `json:"request_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &reqResp); err != nil || reqResp.Data.RequestID == 0 {
		t.Fatalf("解析好友申请响应: err=%v body=%s", err, body)
	}
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/friend/accept",
		map[string]interface{}{"request_id": reqResp.Data.RequestID}, friendToken)
	if status != http.StatusOK {
		t.Fatalf("接受好友申请: status=%d body=%s", status, body)
	}

	// 步骤 1：获取默认设置
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, "/api/v1/settings", nil, token)
	if status != http.StatusOK {
		t.Fatalf("获取设置: status=%d body=%s", status, body)
	}

	// 步骤 2：更新设置
	status, body = doAuthedRequest(t, http.MethodPut, env.baseURL, "/api/v1/settings",
		map[string]interface{}{
			"notification_enabled": true,
			"msg_preview_enabled":  false,
			"mute_list":            "",
		}, token)
	if status != http.StatusOK {
		t.Fatalf("更新设置: status=%d body=%s", status, body)
	}

	// 步骤 3：静音会话（使用真实好友会话）
	convID := fmt.Sprintf("p_%d_%d", min64(meID, friendID), max64(meID, friendID))
	status, body = doAuthedRequest(t, http.MethodPost, env.baseURL, "/api/v1/settings/mute",
		map[string]interface{}{"convId": convID}, token)
	if status != http.StatusOK {
		t.Fatalf("静音会话: status=%d body=%s", status, body)
	}

	// 步骤 4：取消静音会话
	status, body = doAuthedRequest(t, http.MethodDelete, env.baseURL,
		fmt.Sprintf("/api/v1/settings/mute/%s", convID), nil, token)
	if status != http.StatusOK {
		t.Fatalf("取消静音会话: status=%d body=%s", status, body)
	}

	// 步骤 5：验证设置
	status, body = doAuthedRequest(t, http.MethodGet, env.baseURL, "/api/v1/settings", nil, token)
	if status != http.StatusOK {
		t.Fatalf("获取设置: status=%d body=%s", status, body)
	}
	t.Logf("最终设置: %s", body)
}

// ── 辅助函数 ──

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
