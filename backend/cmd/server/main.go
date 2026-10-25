package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/httpapi"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

const (
	defaultAddr = "127.0.0.1:8080"
	serviceName = "im-backend"
)

type serverOptions struct {
	addr   string
	webDir string
}

func main() {
	options := serverOptions{}
	flag.StringVar(&options.addr, "addr", defaultAddr, "HTTP server address")
	flag.StringVar(&options.webDir, "web-dir", "", "directory containing the frontend build")
	flag.Parse()

	if err := run(options); err != nil {
		log.Fatal(err)
	}
}

func run(options serverOptions) error {
	dsn, err := config.MySQLDSN()
	if err != nil {
		return fmt.Errorf("load MySQL configuration: %w", err)
	}

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelStartup()

	dataStore, err := store.Open(startupCtx, dsn)
	if err != nil {
		return fmt.Errorf("connect MySQL: %w", err)
	}
	defer dataStore.Close()

	if err := dataStore.Migrate(startupCtx); err != nil {
		return fmt.Errorf("migrate MySQL: %w", err)
	}
	log.Printf("MySQL ready; migrations applied")

	infraConfig, err := config.LoadInfra()
	if err != nil {
		return fmt.Errorf("load optional infrastructure configuration: %w", err)
	}
	infraManager, err := infra.New(infraConfig)
	if err != nil {
		return fmt.Errorf("configure optional infrastructure: %w", err)
	}
	if infraManager != nil {
		infraManager.Start(context.Background())
		defer func() {
			if err := infraManager.Close(); err != nil {
				log.Printf("close optional infrastructure: %v", err)
			}
		}()
		log.Printf("optional infrastructure enabled")
	}

	var directAcceptor *messaging.RedisAcceptor
	var workerContext context.Context
	var cancelWorkers context.CancelFunc
	var workers sync.WaitGroup
	if infraManager != nil && infraManager.RedisEnabled() && infraConfig.RabbitMQ.Enabled() {
		seedFloor, err := dataStore.MaxServerMessageID(startupCtx)
		if err != nil {
			return fmt.Errorf("seed direct message ID: %w", err)
		}
		projection := messaging.NewFriendshipProjection(infraManager.Redis())
		pairs, err := dataStore.ListFriendshipPairs(startupCtx)
		if err != nil {
			return fmt.Errorf("load friendship projection: %w", err)
		}
		if err := projection.Rebuild(startupCtx, pairs); err != nil {
			return fmt.Errorf("rebuild friendship projection: %w", err)
		}
		directAcceptor = messaging.NewRedisAcceptor(
			infraManager.Redis(),
			messaging.RedisAcceptorOptions{SeedFloor: seedFloor},
		)

		workerContext, cancelWorkers = context.WithCancel(context.Background())
		defer func() {
			cancelWorkers()
			workers.Wait()
		}()

		relay := messaging.NewStreamRelay(
			infraManager.Redis(),
			infraConfig.RabbitMQ,
			infra.DialRabbit,
		)
		consumer := messaging.NewRabbitMessageConsumer(
			infraConfig.RabbitMQ,
			infra.DialRabbit,
			dataStore,
		)
		workers.Add(2)
		go func() {
			defer workers.Done()
			if err := relay.Run(workerContext); err != nil && workerContext.Err() == nil {
				log.Printf("direct message stream relay stopped: %v", err)
			}
		}()
		go func() {
			defer workers.Done()
			if err := consumer.Run(workerContext); err != nil && workerContext.Err() == nil {
				log.Printf("direct message consumer stopped: %v", err)
			}
		}()
		log.Printf("direct message pipeline enabled")
	} else if infraManager != nil && (infraManager.RedisEnabled() || infraConfig.RabbitMQ.Enabled()) {
		log.Printf("direct message pipeline disabled: Redis and RabbitMQ must both be configured")
	}

	router, err := newRouter(options.webDir, dataStore.Ping)
	if err != nil {
		return err
	}
	if infraManager != nil {
		registerInfraStatus(router, infraManager)
	}
	apiOptions := make([]httpapi.Option, 0, 1)
	if directAcceptor != nil {
		apiOptions = append(apiOptions, httpapi.WithDirectMessageAcceptor(directAcceptor))
	}
	httpapi.New(dataStore, apiOptions...).Register(router)

	server := &http.Server{Addr: options.addr, Handler: router}
	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("%s listening on %s", serviceName, options.addr)
		serverErrors <- server.ListenAndServe()
	}()

	stopContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-stopContext.Done():
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelShutdown()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP: %w", err)
		}
		return nil
	}
}

type infraStatusProvider interface {
	Snapshot() infra.Snapshot
}

func registerInfraStatus(router *gin.Engine, provider infraStatusProvider) {
	router.GET("/ops/infra", func(c *gin.Context) {
		c.JSON(http.StatusOK, provider.Snapshot())
	})
}

func newRouter(webDir string, ready func(context.Context) error) (*gin.Engine, error) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"service": serviceName,
		})
	})

	router.GET("/ready", func(c *gin.Context) {
		if ready == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":   "degraded",
				"database": "unavailable",
			})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":   "degraded",
				"database": "unavailable",
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"status":   "ok",
			"database": "ok",
		})
	})

	if webDir == "" {
		return router, nil
	}

	absoluteWebDir, err := filepath.Abs(webDir)
	if err != nil {
		return nil, fmt.Errorf("resolve web directory: %w", err)
	}
	indexPath := filepath.Join(absoluteWebDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		return nil, fmt.Errorf("frontend index not found at %s: %w", indexPath, err)
	}

	fileServer := http.FileServer(http.Dir(absoluteWebDir))
	router.NoRoute(func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.Status(http.StatusMethodNotAllowed)
			return
		}
		requestPath := c.Request.URL.Path
		if requestPath == "/api" || strings.HasPrefix(requestPath, "/api/") ||
			requestPath == "/ops" || strings.HasPrefix(requestPath, "/ops/") {
			c.Status(http.StatusNotFound)
			return
		}
		if filepath.Ext(requestPath) != "" {
			fileServer.ServeHTTP(c.Writer, c.Request)
			return
		}
		c.File(indexPath)
	})
	return router, nil
}
