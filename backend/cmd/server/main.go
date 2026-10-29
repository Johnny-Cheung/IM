package main

import (
	"context"
	"errors"
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
	"github.com/redis/go-redis/v9"
)

const (
	defaultAddr = "127.0.0.1:8080"
	serviceName = "im-backend"
)

type serverOptions struct {
	addr                string
	webDir              string
	backfillSeq         bool
	backfillBatch       int
	rebuildRedis        bool
	rebuildBatch        int
	rebuildQueueTimeout time.Duration
	dlqList             bool
	dlqReplay           bool
	dlqDiscard          bool
	dlqKind             string
	dlqID               uint64
	dlqLimit            int
	dlqYes              bool
}

func main() {
	options := serverOptions{}
	flag.StringVar(&options.addr, "addr", defaultAddr, "HTTP server address")
	flag.StringVar(&options.webDir, "web-dir", "", "directory containing the frontend build")
	flag.BoolVar(&options.backfillSeq, "backfill-seq", false, "number conversation history and write seq:ready, then exit")
	flag.IntVar(&options.backfillBatch, "backfill-batch", 1000, "rows per backfill batch")
	flag.BoolVar(&options.rebuildRedis, "rebuild-redis", false, "rebuild the Redis messaging projection from MySQL, then exit")
	flag.IntVar(&options.rebuildBatch, "rebuild-batch", defaultRedisRebuildBatch, "rows per rebuild batch")
	flag.DurationVar(&options.rebuildQueueTimeout, "rebuild-queue-timeout", defaultRedisRebuildQueueTimeout, "how long the rebuild waits for the persist queues to drain")
	flag.BoolVar(&options.dlqList, "dlq-list", false, "list dead letters and exit")
	flag.BoolVar(&options.dlqReplay, "dlq-replay", false, "replay dead letters to the main exchange and exit")
	flag.BoolVar(&options.dlqDiscard, "dlq-discard", false, "discard dead letters and exit")
	flag.StringVar(&options.dlqKind, "dlq-kind", "all", "dead letter chain: direct, group or all")
	flag.Uint64Var(&options.dlqID, "dlq-id", 0, "server message id to replay or discard; 0 means all")
	flag.IntVar(&options.dlqLimit, "dlq-limit", dlqDefaultLimit, "maximum dead letters to scan per chain")
	flag.BoolVar(&options.dlqYes, "dlq-yes", false, "confirm discard or replay of every dead letter")
	flag.Parse()

	if err := validateCommandModes(options); err != nil {
		log.Fatal(err)
	}
	if options.dlqList || options.dlqReplay || options.dlqDiscard {
		if err := runDeadLetterAdmin(options); err != nil {
			log.Fatal(err)
		}
		return
	}
	if options.backfillSeq {
		if err := runSeqBackfill(options); err != nil {
			log.Fatal(err)
		}
		return
	}
	if options.rebuildRedis {
		if err := runRedisRebuild(options); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := run(options); err != nil {
		log.Fatal(err)
	}
}

// validateCommandModes rejects one-shot command flags selected together.
func validateCommandModes(options serverOptions) error {
	selected := make([]string, 0, 5)
	if options.backfillSeq {
		selected = append(selected, "-backfill-seq")
	}
	if options.rebuildRedis {
		selected = append(selected, "-rebuild-redis")
	}
	if options.dlqList {
		selected = append(selected, "-dlq-list")
	}
	if options.dlqReplay {
		selected = append(selected, "-dlq-replay")
	}
	if options.dlqDiscard {
		selected = append(selected, "-dlq-discard")
	}
	if len(selected) > 1 {
		return fmt.Errorf("%s cannot be combined", strings.Join(selected, ", "))
	}
	return nil
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
	var groupReadTracker *messaging.GroupReadTracker
	var seqWatcher *messaging.SeqWatcher
	var pipelineMonitor *messaging.PipelineMonitor
	var workerContext context.Context
	var cancelWorkers context.CancelFunc
	var workers sync.WaitGroup
	if pipelineEnabled {
		seedFloor, err := dataStore.MaxServerMessageID(startupCtx)
		if err != nil {
			return fmt.Errorf("seed message ID: %w", err)
		}
		if err := checkMaintenanceLocks(startupCtx, infraManager.Redis(), keyspace); err != nil {
			return err
		}
		readyChecker := messaging.NewDirectReadTracker(
			infraManager.Redis(),
			messaging.DirectReadTrackerOptions{Keyspace: keyspace},
		)
		ready, err := readyChecker.ReadModelReady(startupCtx)
		if err != nil {
			return fmt.Errorf("check conversation sequence readiness: %w", err)
		}
		if !ready {
			return fmt.Errorf(
				"conversation sequence is not ready; stop the backend and run `imctl backfill-seq` first",
			)
		}

		indexEpoch, err := messaging.EnsureIndexEpoch(
			startupCtx, infraManager.Redis(), keyspace, seedFloor,
		)
		if err != nil {
			return fmt.Errorf("prepare index epoch: %w", err)
		}
		gate := messaging.NewSeqGate()
		gate.Close()
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
			messaging.RedisAcceptorOptions{SeedFloor: seedFloor, Keyspace: keyspace, Gate: gate},
		)
		directReader = messaging.NewRedisMessageReaderWithKeyspace(infraManager.Redis(), keyspace, indexEpoch)
		directReadTracker = messaging.NewDirectReadTracker(
			infraManager.Redis(),
			messaging.DirectReadTrackerOptions{Keyspace: keyspace, Gate: gate},
		)
		directReadPersister := messaging.NewDirectReadPersister(
			infraManager.Redis(),
			keyspace,
			namespace,
			dataStore,
		)

		if infraConfig.GroupMessagingEnabled {
			groupReadTracker = messaging.NewGroupReadTracker(
				infraManager.Redis(),
				messaging.GroupReadTrackerOptions{Keyspace: keyspace, Gate: gate},
			)

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
			for _, group := range storedGroups {
				if err := groupReadTracker.ReconcileGroup(
					startupCtx, group.GroupID, group.Positions,
				); err != nil {
					return fmt.Errorf(
						"reconcile group %d read model: %w", group.GroupID, err,
					)
				}
			}
			groupAcceptor = messaging.NewGroupRedisAcceptor(
				infraManager.Redis(),
				keyspace,
				messaging.RedisAcceptorOptions{SeedFloor: seedFloor, Keyspace: keyspace, Gate: gate},
			)
			groupReader = messaging.NewGroupMessageReader(infraManager.Redis(), keyspace)
		}

		// Capture the run_id before calibration so a Redis restart during or
		// just after startup calibration is still detected by the first check.
		runIDBeforeCalibration, err := messaging.ReadRunID(
			startupCtx, infraManager.Redis(),
		)
		if err != nil {
			return fmt.Errorf("read redis run id: %w", err)
		}
		calibrator := messaging.NewSeqCalibrator(
			infraManager.Redis(), keyspace, dataStore, options.backfillBatch,
		)
		calibrationCtx, cancelCalibration := context.WithTimeout(context.Background(), 5*time.Minute)
		calibrated, err := calibrator.Calibrate(calibrationCtx)
		cancelCalibration()
		if err != nil {
			return fmt.Errorf("calibrate conversation counters: %w", err)
		}
		calibratedAt := time.Now().UTC()
		gate.Open()
		log.Printf("conversation counters calibrated conversations=%d", calibrated)
		seqWatcher = messaging.NewSeqWatcher(infraManager.Redis(), messaging.SeqWatcherOptions{
			Keyspace:             keyspace,
			Gate:                 gate,
			Calibrator:           calibrator,
			InitialRunID:         runIDBeforeCalibration,
			InitialCalibratedAt:  calibratedAt,
			InitialConversations: calibrated,
		})

		workerContext, cancelWorkers = context.WithCancel(context.Background())
		defer func() {
			cancelWorkers()
			workers.Wait()
		}()
		workers.Add(1)
		go func() {
			defer workers.Done()
			seqWatcher.Run(workerContext)
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

		var groupRelayStatus func() messaging.RelayStatus
		var groupConsumerStatus func() messaging.ConsumerStatus
		if infraConfig.GroupMessagingEnabled {
			groupRelay := messaging.NewGroupStreamRelay(
				infraManager.Redis(), infraConfig.RabbitMQ, infra.DialRabbit, keyspace, namespace,
			)
			groupConsumer := messaging.NewGroupMessageConsumer(
				infraConfig.RabbitMQ, infra.DialRabbit, dataStore, namespace,
			)
			groupRelayStatus = func() messaging.RelayStatus { return groupRelay.Status() }
			groupConsumerStatus = func() messaging.ConsumerStatus { return groupConsumer.Status() }
			groupReadPersister := messaging.NewGroupReadPersister(
				infraManager.Redis(), keyspace, namespace, dataStore,
			)
			workers.Add(3)
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
			go func() {
				defer workers.Done()
				if err := groupReadPersister.Run(workerContext); err != nil && workerContext.Err() == nil {
					log.Printf("group read persister stopped: %v", err)
				}
			}()
			log.Printf("group message pipeline enabled namespace=%s", namespace.Base())
		}

		// The audit repair path writes the same Redis projection as the rebuild,
		// so reverse inconsistency is repaired in place instead of only reported.
		projectionWriter := messaging.NewProjectionWriter(
			infraManager.Redis(),
			keyspace,
			messaging.ProjectionWriteOptions{},
		)
		reconciler := messaging.NewReconciler(
			infraManager.Redis(),
			dataStore,
			messaging.ReconcileOptions{
				Keyspace: keyspace,
				Gate:     gate,
				Repair:   projectionWriter,
			},
		)
		workers.Add(1)
		go func() {
			defer workers.Done()
			reconciler.Run(workerContext)
		}()
		log.Printf("message reconciler started")

		pipelineMonitor = messaging.NewPipelineMonitor(
			infraManager.Redis(),
			infraConfig.RabbitMQ,
			infra.DialRabbit,
			messaging.PipelineMonitorOptions{
				Keyspace:       keyspace,
				Namespace:      namespace,
				DirectRelay:    func() messaging.RelayStatus { return relay.Status() },
				DirectConsumer: func() messaging.ConsumerStatus { return consumer.Status() },
				GroupRelay:     groupRelayStatus,
				GroupConsumer:  groupConsumerStatus,
				Reconcile:      func() messaging.ReconcileStatus { return reconciler.Status() },
			},
		)
		workers.Add(1)
		go func() {
			defer workers.Done()
			pipelineMonitor.Run(workerContext)
		}()
		log.Printf("pipeline monitor started")
	} else if infraManager != nil && (infraManager.RedisEnabled() || infraConfig.RabbitMQ.Enabled()) {
		log.Printf("message pipeline disabled: Redis and RabbitMQ must both be configured")
	}

	router, err := newRouter(options.webDir, dataStore.Ping)
	if err != nil {
		return err
	}
	if infraManager != nil {
		registerInfraStatus(
			router,
			infraManager,
			func() *messaging.SeqWatcherStatus {
				if seqWatcher == nil {
					return nil
				}
				status := seqWatcher.Status()
				return &status
			},
			func() *messaging.PipelineStatus {
				if pipelineMonitor == nil {
					return nil
				}
				status := pipelineMonitor.Status()
				return &status
			},
		)
	}
	apiOptions := make([]httpapi.Option, 0, 8)
	if directAcceptor != nil {
		var acceptor httpapi.DirectMessageAcceptor = directAcceptor
		if seqWatcher != nil {
			acceptor = notifyingDirectAcceptor{inner: acceptor, watcher: seqWatcher}
		}
		apiOptions = append(apiOptions, httpapi.WithDirectMessageAcceptor(acceptor))
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
		var acceptor httpapi.GroupMessageAcceptor = groupAcceptor
		if seqWatcher != nil {
			acceptor = notifyingGroupAcceptor{inner: acceptor, watcher: seqWatcher}
		}
		apiOptions = append(apiOptions, httpapi.WithGroupMessageAcceptor(acceptor))
	}
	if groupReader != nil {
		apiOptions = append(apiOptions, httpapi.WithGroupMessageReader(groupReader))
	}
	if groupProjector != nil {
		apiOptions = append(apiOptions,
			httpapi.WithGroupProjector(groupProjector),
			httpapi.WithGroupSequenceReader(groupProjector),
		)
	}
	if groupReadTracker != nil {
		apiOptions = append(apiOptions, httpapi.WithGroupReadTracker(groupReadTracker))
	}
	api := httpapi.New(dataStore, apiOptions...)
	api.Register(router)
	api.Start()
	defer api.Stop()

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

type infraStatusResponse struct {
	infra.Snapshot
	Seq      *messaging.SeqWatcherStatus `json:"seq,omitempty"`
	Pipeline *messaging.PipelineStatus   `json:"pipeline,omitempty"`
}

func registerInfraStatus(
	router *gin.Engine,
	provider infraStatusProvider,
	seqStatus func() *messaging.SeqWatcherStatus,
	pipelineStatus func() *messaging.PipelineStatus,
) {
	router.GET("/ops/infra", func(c *gin.Context) {
		response := infraStatusResponse{Snapshot: provider.Snapshot()}
		if seqStatus != nil {
			response.Seq = seqStatus()
		}
		// The endpoint only serves the monitor's last sample; it never queries
		// Redis or RabbitMQ on the request path.
		if pipelineStatus != nil {
			response.Pipeline = pipelineStatus()
		}
		c.JSON(http.StatusOK, response)
	})
}

// notifySeqWatcher asks the watcher to recheck Redis after a failure that is
// not a business rejection. ErrSeqNotReady is expected while the gate is
// closed, so it must not feed the check loop.
func notifySeqWatcher(watcher *messaging.SeqWatcher, err error) {
	if watcher == nil || err == nil {
		return
	}
	switch {
	case errors.Is(err, messaging.ErrSeqNotReady),
		errors.Is(err, messaging.ErrSeqOverflow),
		errors.Is(err, store.ErrCannotMessageSelf),
		errors.Is(err, store.ErrNotFriends),
		errors.Is(err, store.ErrMessageConflict),
		errors.Is(err, store.ErrMessageIDExhausted),
		errors.Is(err, store.ErrGroupNotFound):
		return
	}
	watcher.Notify()
}

type notifyingDirectAcceptor struct {
	inner   httpapi.DirectMessageAcceptor
	watcher *messaging.SeqWatcher
}

func (a notifyingDirectAcceptor) Accept(
	ctx context.Context,
	request messaging.DirectMessageRequest,
) (messaging.DirectMessageResult, error) {
	result, err := a.inner.Accept(ctx, request)
	notifySeqWatcher(a.watcher, err)
	return result, err
}

type notifyingGroupAcceptor struct {
	inner   httpapi.GroupMessageAcceptor
	watcher *messaging.SeqWatcher
}

func (a notifyingGroupAcceptor) Accept(
	ctx context.Context,
	request messaging.GroupMessageRequest,
) (messaging.GroupMessageResult, error) {
	result, err := a.inner.Accept(ctx, request)
	notifySeqWatcher(a.watcher, err)
	return result, err
}

// checkMaintenanceLocks refuses startup while a one-shot maintenance command
// holds its lock; serving a half-rebuilt namespace is worse than being down.
func checkMaintenanceLocks(
	ctx context.Context,
	client redis.UniversalClient,
	keyspace messaging.Keyspace,
) error {
	backfillLock := messaging.BackfillLockKey(keyspace)
	backfilling, err := client.Exists(ctx, backfillLock).Result()
	if err != nil {
		return fmt.Errorf("check seq backfill lock: %w", err)
	}
	if backfilling > 0 {
		return fmt.Errorf(
			"seq backfill is in progress (lock %s); wait for it to finish before starting the backend",
			backfillLock,
		)
	}
	rebuildLock := messaging.RebuildLockKey(keyspace)
	rebuilding, err := client.Exists(ctx, rebuildLock).Result()
	if err != nil {
		return fmt.Errorf("check Redis rebuild lock: %w", err)
	}
	if rebuilding > 0 {
		return fmt.Errorf(
			"Redis rebuild is in progress (lock %s); wait for it to finish, or confirm no "+
				"rebuild is running and delete the lock, before starting the backend",
			rebuildLock,
		)
	}
	return nil
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
