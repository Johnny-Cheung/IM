// Package main 是 GoIM 服务端入口。
// 它组装所有基础设施组件（配置、MySQL、Redis、RabbitMQ），
// 并启动 Gin HTTP 服务器 + WebSocket 处理器 + MQ 消费者。
//
// @title                       GoIM API
// @version                     1.0
// @description                 GoIM 即时通讯系统 API — 支持私聊、群聊、朋友圈动态。
// @host                        localhost:8080
// @BasePath                    /api/v1
//
// @securityDefinitions.apikey  BearerAuth
// @in                          header
// @name                        Authorization
// @description                 在 Authorization 头中携带 JWT 访问令牌，格式：Bearer <token>
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	goredisv9 "github.com/redis/go-redis/v9"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	_ "github.com/goim/goim/docs"
	"github.com/goim/goim/internal/api"
	"github.com/goim/goim/internal/config"
	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/consumer"
	"github.com/goim/goim/internal/infra"
	"github.com/goim/goim/internal/middleware"
	goredis "github.com/goim/goim/internal/redis"
	"github.com/goim/goim/internal/relay"
	"github.com/goim/goim/internal/repository"
	"github.com/goim/goim/internal/service"
	"github.com/goim/goim/internal/ws"
)

func main() {
	configPath := flag.String("c", "configs/config.yaml", "配置文件路径")
	flag.Parse()

	// ── 加载配置 ──
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// ── 初始化日志 ──
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("初始化日志失败: %v", err)
	}
	defer logger.Sync()

	// 统一管理服务退出：系统信号和 RabbitMQ 异常都会走同一套优雅关闭流程。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── 初始化 MySQL ──
	db, err := infra.NewMySQLPool(&cfg.MySQL)
	if err != nil {
		logger.Fatal("连接 MySQL 失败", zap.Error(err))
	}
	if err := db.Ping(); err != nil {
		logger.Fatal("Ping MySQL 失败", zap.Error(err))
	}
	logger.Info("MySQL 已连接")

	// ── 初始化 Redis ──
	rdb, err := infra.NewRedisClient(&cfg.Redis)
	if err != nil {
		logger.Fatal("连接 Redis 失败", zap.Error(err))
	}
	logger.Info("Redis 已连接")

	// ── 初始化 RabbitMQ ──
	mqConn, mqCh, err := infra.ConnectRabbitMQWithRetry(
		ctx,
		&cfg.RabbitMQ,
		time.Second,
		30*time.Second,
		func(connectErr error, retryAfter time.Duration) {
			logger.Warn("连接 RabbitMQ 失败，等待重试",
				zap.Error(connectErr),
				zap.Duration("retryAfter", retryAfter),
			)
		},
	)
	if err != nil {
		logger.Error("RabbitMQ 初始化终止", zap.Error(err))
		return
	}
	defer mqConn.Close()
	logger.Info("RabbitMQ 已连接，队列已声明")
	mqFailure := monitorRabbitMQ(ctx, mqConn, mqCh)
	var relayPublishFailure <-chan error

	// ── 加载 Lua 脚本到 Redis ──
	if err := goredis.LoadLuaScripts(rdb, ctx); err != nil {
		logger.Fatal("加载 Redis Lua 脚本失败", zap.Error(err))
	}
	logger.Info("Redis Lua 脚本已加载")

	// ── 构建仓库层 ──
	mysqlRepo := repository.NewMySQLRepo(db)
	redisRepo := repository.NewRedisRepo(rdb)
	mqRepo := repository.NewMQRepo(mqCh)
	// Publisher confirm is ordered per AMQP channel.  Keep the Stream relay on
	// a dedicated channel so confirms for unrelated publishers (moments/likes)
	// can never be mistaken for a message persistence confirmation.
	var persistMQRepo *repository.MQRepoImpl
	if cfg.Message.AtomicPipeline {
		persistCh, err := mqConn.Channel()
		if err != nil {
			logger.Fatal("创建消息 relay RabbitMQ 通道失败", zap.Error(err))
		}
		defer persistCh.Close()
		persistMQRepo = repository.NewMQRepo(persistCh)
		persistClosed := persistCh.NotifyClose(make(chan *amqp.Error, 1))
		persistFailure := make(chan error, 1)
		relayPublishFailure = persistFailure
		go func() {
			select {
			case <-ctx.Done():
			case closeErr, ok := <-persistClosed:
				if ok && closeErr != nil {
					select {
					case persistFailure <- fmt.Errorf("RabbitMQ 消息 relay 发布通道已关闭: %w", closeErr):
					default:
					}
				}
			}
		}()
	}

	// ── 初始化连接管理器 ──
	cm := conn.NewConnectionManager()

	// ── 初始化服务层 ──
	var msgSvc *service.MsgService
	if cfg.Message.AtomicPipeline {
		msgSvc = service.NewMsgServiceWithMySQL(redisRepo, mqRepo, mysqlRepo, cm, logger)
	} else {
		msgSvc = service.NewMsgService(redisRepo, mqRepo, cm, logger)
		logger.Warn("message atomic pipeline is disabled; using legacy consumers")
	}
	authSvc := service.NewAuthService(mysqlRepo, cfg.JWT.Secret, cfg.JWT.AccessExpHours, cfg.JWT.RefreshExpDays)
	friendSvc := service.NewFriendService(mysqlRepo, redisRepo, logger)
	groupSvc := service.NewGroupService(mysqlRepo, redisRepo, logger)
	momentSvc := service.NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Duration(cfg.Moment.LikeCacheTTLHours)*time.Hour)
	msgOpSvc := service.NewMsgOpService(mysqlRepo, redisRepo, logger)
	settingsSvc := service.NewSettingsService(mysqlRepo, logger)

	// ── 初始化 WebSocket 分发器 ──
	dispatcher := ws.NewMessageDispatcher(msgSvc, friendSvc)

	// ── 初始化 HTTP 处理器 ──
	authHandler := api.NewAuthHandler(authSvc)
	friendHandler := api.NewFriendHandler(friendSvc, rdb)
	groupHandler := api.NewGroupHandler(groupSvc, cm)
	momentHandler := api.NewMomentHandler(momentSvc)
	msgOpHandler := api.NewMsgOpHandler(msgOpSvc)
	settingsHandler := api.NewSettingsHandler(settingsSvc)
	uploadHandler := api.NewUploadHandler(cfg.Server.UploadDir, cfg.File.MaxSizeMB, cfg.File.AllowedExts, mysqlRepo)
	avatarHandler := api.NewAvatarHandler()

	// ── 设置 Gin 路由 ──
	router := setupRouter(
		cfg, mysqlRepo, redisRepo, rdb, cm,
		func() bool { return !mqConn.IsClosed() && !mqCh.IsClosed() },
		dispatcher, logger,
		authHandler, friendHandler, groupHandler,
		momentHandler, msgOpHandler, settingsHandler,
		uploadHandler, avatarHandler,
	)

	// ── 启动 MQ 消费者 ──
	momentFeedConsumer := consumer.NewMomentFeedConsumer(mqCh, mysqlRepo, redisRepo, logger, cfg.Moment.BigUserFriendThreshold, cfg.Moment.TimelineMaxLen)
	likePersistConsumer := consumer.NewLikePersistConsumer(mqCh, mysqlRepo, logger, cfg.Moment.LikePersistBatchSize, cfg.Moment.LikePersistFlushMs)

	if cfg.Message.AtomicPipeline {
		messagePersistConsumer := consumer.NewMessagePersistConsumer(mqCh, mysqlRepo, logger)
		messageRelay := relay.NewMessageRelay(redisRepo, persistMQRepo, cm, logger)
		if err := messagePersistConsumer.Start(ctx); err != nil {
			logger.Fatal("启动消息持久化消费者失败", zap.Error(err))
		}
		if err := messageRelay.Start(ctx); err != nil {
			logger.Fatal("启动消息 Stream relay 失败", zap.Error(err))
		}
	} else {
		privateMsgConsumer := consumer.NewPrivateMsgConsumer(mqCh, mysqlRepo, redisRepo, cm, logger)
		groupMsgConsumer := consumer.NewGroupMsgConsumer(mqCh, mysqlRepo, redisRepo, cm, logger)
		if err := privateMsgConsumer.Start(ctx); err != nil {
			logger.Fatal("启动私聊消息消费者失败", zap.Error(err))
		}
		if err := groupMsgConsumer.Start(ctx); err != nil {
			logger.Fatal("启动群聊消息消费者失败", zap.Error(err))
		}
	}
	if err := momentFeedConsumer.Start(ctx); err != nil {
		logger.Fatal("启动动态推送消费者失败", zap.Error(err))
	}
	if err := likePersistConsumer.Start(ctx); err != nil {
		logger.Fatal("启动点赞持久化消费者失败", zap.Error(err))
	}
	logger.Info("MQ 消费者已启动")

	// ── 启动清理任务 ──
	infra.StartCleanupTask(rdb, logger, 1*time.Hour)

	// ── 启动 pprof 调试端口 ──
	if cfg.Server.PprofPort > 0 {
		go func() {
			pprofAddr := fmt.Sprintf(":%d", cfg.Server.PprofPort)
			logger.Info("pprof 已启动", zap.String("addr", pprofAddr))
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				logger.Warn("pprof 服务器错误", zap.Error(err))
			}
		}()
	}

	// ── 启动 HTTP 服务器 ──
	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{Addr: addr, Handler: router}

	go func() {
		logger.Info("正在启动 HTTP 服务器", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP 服务器启动失败", zap.Error(err))
		}
	}()

	logger.Info("GoIM 服务器已启动", zap.Int("port", cfg.Server.Port))

	// ── 优雅关闭 ──
	select {
	case <-ctx.Done():
		logger.Info("收到退出信号，正在关闭服务器...")
	case mqErr := <-mqFailure:
		logger.Error("RabbitMQ 连接异常，正在关闭服务器等待容器重启", zap.Error(mqErr))
		stop()
	case mqErr := <-relayPublishFailure:
		logger.Error("RabbitMQ relay 发布通道异常，正在关闭服务器等待容器重启", zap.Error(mqErr))
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("服务器强制关闭", zap.Error(err))
	}

	logger.Info("服务器已退出")
}

// ──────────────────────────────────────────────────────
// 路由设置 — 绑定所有 HTTP 路由 + WebSocket 端点
// ──────────────────────────────────────────────────────

func setupRouter(
	cfg *config.Config,
	mysqlRepo repository.MySQLRepo,
	redisRepo repository.RedisRepo,
	rdb *goredisv9.Client,
	cm *conn.ConnectionManager,
	mqReady func() bool,
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
	r := gin.Default()

	// ── CORS 中间件 ──
	// 开发环境：空切片 = 允许所有来源
	// 生产环境：传入前端域名，如 []string{"https://your-domain.com"}
	r.Use(middleware.CORS(nil))

	// ── 健康检查 ──
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "goim"})
	})
	r.GET("/ready", func(c *gin.Context) {
		if !mqReady() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "service": "goim", "rabbitmq": "disconnected"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready", "service": "goim", "rabbitmq": "connected"})
	})

	// ── Swagger 文档 ──
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// ── 公开路由（无需认证）──
	public := r.Group("/api/v1")
	authHandler.RegisterRoutes(public)
	avatarHandler.RegisterRoutes(public)

	// ── 受保护路由（需要 JWT 认证）──
	protected := r.Group("/api/v1")
	protected.Use(middleware.JWTAuthMiddleware(cfg.JWT.Secret))
	authHandler.RegisterAccountRoutes(protected)
	friendHandler.RegisterRoutes(protected)
	groupHandler.RegisterRoutes(protected)
	momentHandler.RegisterRoutes(protected)
	msgOpHandler.RegisterRoutes(protected)
	settingsHandler.RegisterRoutes(protected)
	uploadHandler.RegisterRoutes(protected)

	// ── WebSocket 端点 ──
	wsHandler := ws.ServeWebSocket(cfg.JWT.Secret, rdb, cm, dispatcher.Callback())
	r.GET(cfg.Server.WsPath, wsHandler)

	// ── 静态文件：上传目录 ──
	r.Static("/uploads", cfg.Server.UploadDir)

	// ── 前端 SPA（生产环境）──
	// 如果 frontend/dist 目录存在，托管前端静态资源并启用 SPA fallback
	frontendDir := "frontend/dist"
	if _, err := os.Stat(frontendDir); err == nil {
		r.Static("/assets", frontendDir+"/assets")
		r.StaticFile("/favicon.ico", frontendDir+"/favicon.ico")
		r.GET("/", func(c *gin.Context) {
			c.File(frontendDir + "/index.html")
		})
		// SPA fallback：API/WebSocket 路径返回 JSON 404，其余返回 index.html
		r.NoRoute(func(c *gin.Context) {
			path := c.Request.URL.Path
			if strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/ws") {
				c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "not found"})
				return
			}
			c.File(frontendDir + "/index.html")
		})
	}

	return r
}

// monitorRabbitMQ 将连接或主通道的异常关闭转换为单一故障信号。
// 正常关闭时 ctx 会先取消，因此不会触发重复的异常退出流程。
func monitorRabbitMQ(ctx context.Context, conn *amqp.Connection, ch *amqp.Channel) <-chan error {
	failure := make(chan error, 1)
	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	channelClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

	report := func(component string, notifications <-chan *amqp.Error) {
		select {
		case <-ctx.Done():
			return
		case closeErr, ok := <-notifications:
			if !ok || closeErr == nil {
				return
			}
			select {
			case failure <- fmt.Errorf("RabbitMQ %s 已关闭: %w", component, closeErr):
			default:
			}
		}
	}

	go report("连接", connClosed)
	go report("通道", channelClosed)
	return failure
}
