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
	addr          string
	webDir        string
	backfillRead  bool
	backfillBatch int
}

func main() {
	options := serverOptions{}
	flag.StringVar(&options.addr, "addr", defaultAddr, "HTTP server address")
	flag.StringVar(&options.webDir, "web-dir", "", "directory containing the frontend build")
	flag.BoolVar(&options.backfillRead, "backfill-read", false, "backfill the direct read model and exit")
	flag.IntVar(&options.backfillBatch, "backfill-batch", 1000, "rows per backfill batch")
	flag.Parse()

	if options.backfillRead {
		if err := runBackfill(options); err != nil {
			log.Fatal(err)
		}
		return
	}
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

	namespace, err := messaging.NewNamespace(infraConfig.MessagingNamespace)
	if err != nil {
		return fmt.Errorf("configure messaging namespace: %w", err)
	}
	keyspace := messaging.NewKeyspace(namespace)
	pipelineEnabled := infraManager != nil && infraManager.RedisEnabled() && infraConfig.RabbitMQ.Enabled()
	if (infraConfig.MessagingRequired || infraConfig.GroupMessagingEnabled) && !pipelineEnabled {
		return fmt.Errorf("message pipeline is required but Redis and RabbitMQ are not both configured")
	}

	var directAcceptor *messaging.RedisAcceptor
	var directReader *messaging.RedisMessageReader
	var directReadTracker *messaging.DirectReadTracker
	var friendshipProjector *messaging.FriendshipProjection
	var groupAcceptor *messaging.GroupRedisAcceptor
	var groupReader *messaging.GroupMessageReader
	var groupProjector *messaging.GroupProjection
	var workerContext context.Context
	var cancelWorkers context.CancelFunc
	var workers sync.WaitGroup
	if pipelineEnabled {
		seedFloor, err := dataStore.MaxServerMessageID(startupCtx)
		if err != nil {
			return fmt.Errorf("seed message ID: %w", err)
		}
		indexEpoch, err := messaging.EnsureIndexEpoch(
			startupCtx, infraManager.Redis(), keyspace, seedFloor,
		)
		if err != nil {
			return fmt.Errorf("prepare index epoch: %w", err)
		}
		friendshipProjector = messaging.NewFriendshipProjectionWithKeyspace(infraManager.Redis(), keyspace)
		pairs, err := dataStore.ListFriendshipPairs(startupCtx)
		if err != nil {
			return fmt.Errorf("load friendship projection: %w", err)
		}
		if err := friendshipProjector.Rebuild(startupCtx, pairs); err != nil {
			return fmt.Errorf("rebuild friendship projection: %w", err)
		}
		directAcceptor = messaging.NewRedisAcceptor(
			infraManager.Redis(),
			messaging.RedisAcceptorOptions{SeedFloor: seedFloor, Keyspace: keyspace},
		)
		directReader = messaging.NewRedisMessageReaderWithKeyspace(infraManager.Redis(), keyspace, indexEpoch)
		directReadTracker = messaging.NewDirectReadTracker(
			infraManager.Redis(),
			messaging.DirectReadTrackerOptions{Keyspace: keyspace},
		)
		directReadPersister := messaging.NewDirectReadPersister(
			infraManager.Redis(),
			keyspace,
			namespace,
			dataStore,
		)
		if directReadTracker != nil {
			ready, err := directReadTracker.ReadModelReady(startupCtx)
			if err != nil {
				return fmt.Errorf("check direct read model: %w", err)
			}
			if !ready {
				if infraConfig.MessagingRequired {
					return fmt.Errorf(
						"direct read model is not ready; run with -backfill-read first",
					)
				}
				log.Printf("direct read model is not ready; using MySQL read state")
				directReadTracker = nil
			}
		}

		if infraConfig.GroupMessagingEnabled {
			groupProjector = messaging.NewGroupProjection(infraManager.Redis(), keyspace)
			storedGroups, err := dataStore.ListGroupMembershipProjections(startupCtx)
			if err != nil {
				return fmt.Errorf("load group membership projections: %w", err)
			}
			projections := make([]messaging.GroupMembershipProjection, 0, len(storedGroups))
			for _, group := range storedGroups {
				projections = append(projections, messaging.GroupMembershipProjection{
					GroupID:   group.GroupID,
					MemberIDs: group.MemberIDs,
					Dissolved: group.Dissolved,
				})
			}
			if err := groupProjector.Rebuild(startupCtx, projections); err != nil {
				return fmt.Errorf("rebuild group projection: %w", err)
			}
			groupAcceptor = messaging.NewGroupRedisAcceptor(
				infraManager.Redis(),
				keyspace,
				messaging.RedisAcceptorOptions{SeedFloor: seedFloor, Keyspace: keyspace},
			)
			groupReader = messaging.NewGroupMessageReader(infraManager.Redis(), keyspace, indexEpoch)
		}

		workerContext, cancelWorkers = context.WithCancel(context.Background())
		defer func() {
			cancelWorkers()
			workers.Wait()
		}()

		relay := messaging.NewStreamRelayWithNamespace(
			infraManager.Redis(), infraConfig.RabbitMQ, infra.DialRabbit, keyspace, namespace,
		)
		consumer := messaging.NewRabbitMessageConsumerWithNamespace(
			infraConfig.RabbitMQ, infra.DialRabbit, dataStore, namespace,
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
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := directReadPersister.Run(workerContext); err != nil && workerContext.Err() == nil {
				log.Printf("direct read persister stopped: %v", err)
			}
		}()
		log.Printf("direct message pipeline enabled namespace=%s", namespace.Base())

		if infraConfig.GroupMessagingEnabled {
			groupRelay := messaging.NewGroupStreamRelay(
				infraManager.Redis(), infraConfig.RabbitMQ, infra.DialRabbit, keyspace, namespace,
			)
			groupConsumer := messaging.NewGroupMessageConsumer(
				infraConfig.RabbitMQ, infra.DialRabbit, dataStore, namespace,
			)
			workers.Add(2)
			go func() {
				defer workers.Done()
				if err := groupRelay.Run(workerContext); err != nil && workerContext.Err() == nil {
					log.Printf("group message stream relay stopped: %v", err)
				}
			}()
			go func() {
				defer workers.Done()
				if err := groupConsumer.Run(workerContext); err != nil && workerContext.Err() == nil {
					log.Printf("group message consumer stopped: %v", err)
				}
			}()
			log.Printf("group message pipeline enabled namespace=%s", namespace.Base())
		}
	} else if infraManager != nil && (infraManager.RedisEnabled() || infraConfig.RabbitMQ.Enabled()) {
		log.Printf("message pipeline disabled: Redis and RabbitMQ must both be configured")
	}

	router, err := newRouter(options.webDir, dataStore.Ping)
	if err != nil {
		return err
	}
	if infraManager != nil {
		registerInfraStatus(router, infraManager)
	}
	apiOptions := make([]httpapi.Option, 0, 2)
	if directAcceptor != nil {
		apiOptions = append(apiOptions, httpapi.WithDirectMessageAcceptor(directAcceptor))
	}
	if friendshipProjector != nil {
		apiOptions = append(apiOptions, httpapi.WithFriendshipProjector(friendshipProjector))
	}
	if directReader != nil {
		apiOptions = append(apiOptions, httpapi.WithDirectMessageReader(directReader))
	}
	if directReadTracker != nil {
		apiOptions = append(apiOptions, httpapi.WithDirectReadTracker(directReadTracker))
	}
	if groupAcceptor != nil {
		apiOptions = append(apiOptions, httpapi.WithGroupMessageAcceptor(groupAcceptor))
	}
	if groupReader != nil {
		apiOptions = append(apiOptions, httpapi.WithGroupMessageReader(groupReader))
	}
	if groupProjector != nil {
		apiOptions = append(apiOptions,
			httpapi.WithGroupProjector(groupProjector),
			httpapi.WithGroupAcceptedWatermarkReader(groupProjector),
		)
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
