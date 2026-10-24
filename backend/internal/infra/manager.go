package infra

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	componentRedis    = "redis"
	componentRabbitMQ = "rabbitmq"
	probeInterval     = 5 * time.Second
	reconnectInterval = time.Second
)

type Manager struct {
	config config.InfraConfig
	redis  *redis.Client
	status *statusStore

	mu        sync.Mutex
	rabbit    *amqp091.Connection
	cancel    context.CancelFunc
	started   bool
	closeOnce sync.Once
	workers   sync.WaitGroup
}

func New(config config.InfraConfig) (*Manager, error) {
	if !config.Redis.Enabled() && !config.RabbitMQ.Enabled() {
		return nil, nil
	}

	manager := &Manager{
		config: config,
		status: newStatusStore(config.Redis.Enabled(), config.RabbitMQ.Enabled()),
	}
	if config.Redis.Enabled() {
		client, err := newRedisClient(config.Redis)
		if err != nil {
			return nil, fmt.Errorf("parse Redis URL: %w", safeError(err, config.Redis.URL))
		}
		manager.redis = client
	}
	if config.RabbitMQ.Enabled() {
		if err := validateRabbitURL(config.RabbitMQ.URL); err != nil {
			if manager.redis != nil {
				_ = manager.redis.Close()
			}
			return nil, err
		}
	}
	return manager, nil
}

func (m *Manager) Start(parent context.Context) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel
	m.started = true
	m.mu.Unlock()

	if m.redis != nil {
		m.workers.Add(1)
		go m.redisLoop(ctx)
	}
	if m.config.RabbitMQ.Enabled() {
		m.workers.Add(1)
		go m.rabbitLoop(ctx)
	}
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	var closeErr error
	m.closeOnce.Do(func() {
		m.mu.Lock()
		if m.cancel != nil {
			m.cancel()
		}
		m.mu.Unlock()
		m.workers.Wait()
		if m.redis != nil {
			closeErr = m.redis.Close()
		}
		m.closeRabbit()
	})
	return closeErr
}

func (m *Manager) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return m.status.snapshotCopy()
}

func (m *Manager) redisLoop(ctx context.Context) {
	defer m.workers.Done()
	for {
		if err := m.redis.Ping(ctx).Err(); err != nil {
			m.status.failure(componentRedis, safeError(err, m.config.Redis.URL))
			if !waitFor(ctx, reconnectInterval) {
				return
			}
			m.status.reconnect(componentRedis)
			continue
		}
		m.status.success(componentRedis)
		if !waitFor(ctx, probeInterval) {
			return
		}
	}
}

func (m *Manager) rabbitLoop(ctx context.Context) {
	defer m.workers.Done()
	for {
		connection, err := dialRabbit(m.config.RabbitMQ)
		if err != nil {
			m.status.failure(componentRabbitMQ, safeError(err, m.config.RabbitMQ.URL))
			if !waitFor(ctx, reconnectInterval) {
				return
			}
			m.status.reconnect(componentRabbitMQ)
			continue
		}
		m.setRabbit(connection)
		m.status.success(componentRabbitMQ)

		closed := connection.NotifyClose(make(chan *amqp091.Error, 1))
		select {
		case <-ctx.Done():
			_ = connection.Close()
			return
		case err := <-closed:
			if err == nil {
				m.status.failure(componentRabbitMQ, fmt.Errorf("connection closed"))
			} else {
				m.status.failure(componentRabbitMQ, safeError(err, m.config.RabbitMQ.URL))
			}
			m.closeRabbit()
			if !waitFor(ctx, reconnectInterval) {
				return
			}
			m.status.reconnect(componentRabbitMQ)
		}
	}
}

func (m *Manager) setRabbit(connection *amqp091.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rabbit = connection
}

func (m *Manager) closeRabbit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rabbit != nil {
		_ = m.rabbit.Close()
		m.rabbit = nil
	}
}

func newRedisClient(config config.RedisConfig) (*redis.Client, error) {
	options, err := redis.ParseURL(config.URL)
	if err != nil {
		return nil, err
	}
	options.Password = config.Password
	options.DB = config.DB
	options.DialTimeout = config.DialTimeout
	options.ReadTimeout = config.ReadTimeout
	options.WriteTimeout = config.WriteTimeout
	return redis.NewClient(options), nil
}

func dialRabbit(config config.RabbitMQConfig) (*amqp091.Connection, error) {
	return amqp091.DialConfig(config.URL, amqp091.Config{
		Heartbeat: config.Heartbeat,
		Dial: func(network, address string) (net.Conn, error) {
			return net.DialTimeout(network, address, config.DialTimeout)
		},
	})
}

func validateRabbitURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "amqp" && parsed.Scheme != "amqps") || parsed.Host == "" {
		return fmt.Errorf("RabbitMQ URL must use amqp or amqps with a host")
	}
	return nil
}

func safeError(err error, rawURL string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if parsed, parseErr := url.Parse(rawURL); parseErr == nil && parsed.User != nil {
		message = strings.ReplaceAll(message, parsed.User.String(), "<credentials>")
	}
	message = strings.ReplaceAll(message, rawURL, "<redacted-url>")
	return fmt.Errorf("%s", message)
}

func waitFor(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
