package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultInfraTimeout        = 5 * time.Second
	defaultRabbitHeartbeat     = 10 * time.Second
	defaultMomentsFeedLimit    = 500
	defaultMomentsFeedTTL      = 7 * 24 * time.Hour
	defaultMomentsLikeTTL      = 7 * 24 * time.Hour
	defaultMomentsCacheTimeout = 300 * time.Millisecond
	defaultMomentsPullSources  = 100
	DefaultMessagingNamespace  = "im:v12"
)

// MomentsCacheConfig controls the derived Redis feed and like cache. The cache
// is optional: when Redis is absent or the cache is disabled, Moments keep
// using MySQL directly.
type MomentsCacheConfig struct {
	Enabled          bool
	PullThreshold    int
	FeedWarmLimit    int
	FeedTTL          time.Duration
	LikeTTL          time.Duration
	OperationTimeout time.Duration
	MaxPullSources   int
}

type RedisConfig struct {
	URL          string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c RedisConfig) Enabled() bool {
	return c.URL != ""
}

type RabbitMQConfig struct {
	URL         string
	Password    string
	DialTimeout time.Duration
	Heartbeat   time.Duration
}

func (c RabbitMQConfig) Enabled() bool {
	return c.URL != ""
}

type InfraConfig struct {
	Redis                 RedisConfig
	RabbitMQ              RabbitMQConfig
	MessagingNamespace    string
	MessagingRequired     bool
	GroupMessagingEnabled bool
	MomentsCache          MomentsCacheConfig
}

func LoadInfra() (InfraConfig, error) {
	namespace, err := messagingNamespace()
	if err != nil {
		return InfraConfig{}, err
	}
	messagingRequired, err := optionalBool("IM_MESSAGING_REQUIRED", false)
	if err != nil {
		return InfraConfig{}, err
	}
	groupMessagingEnabled, err := optionalBool("IM_GROUP_MESSAGING_ENABLED", false)
	if err != nil {
		return InfraConfig{}, err
	}
	momentsCache, err := loadMomentsCacheConfig()
	if err != nil {
		return InfraConfig{}, err
	}

	redisURL, err := valueOrFile("IM_REDIS_URL", "IM_REDIS_URL_FILE")
	if err != nil {
		return InfraConfig{}, err
	}
	rabbitURL, err := valueOrFile("IM_RABBITMQ_URL", "IM_RABBITMQ_URL_FILE")
	if err != nil {
		return InfraConfig{}, err
	}

	config := InfraConfig{
		MessagingNamespace:    namespace,
		MessagingRequired:     messagingRequired,
		GroupMessagingEnabled: groupMessagingEnabled,
		MomentsCache:          momentsCache,
	}
	if redisURL != "" {
		redisPassword, err := valueOrFile("IM_REDIS_PASSWORD", "IM_REDIS_PASSWORD_FILE")
		if err != nil {
			return InfraConfig{}, err
		}
		redisDB, err := optionalInt("IM_REDIS_DB", 0)
		if err != nil {
			return InfraConfig{}, err
		}
		redisTimeouts, err := redisTimeouts()
		if err != nil {
			return InfraConfig{}, err
		}
		config.Redis = RedisConfig{
			URL:          redisURL,
			Password:     redisPassword,
			DB:           redisDB,
			DialTimeout:  redisTimeouts.dial,
			ReadTimeout:  redisTimeouts.read,
			WriteTimeout: redisTimeouts.write,
		}
	}

	if rabbitURL != "" {
		rabbitPassword, err := valueOrFile("IM_RABBITMQ_PASSWORD", "IM_RABBITMQ_PASSWORD_FILE")
		if err != nil {
			return InfraConfig{}, err
		}
		dialTimeout, err := optionalDuration("IM_RABBITMQ_DIAL_TIMEOUT", defaultInfraTimeout)
		if err != nil {
			return InfraConfig{}, err
		}
		heartbeat, err := optionalDuration("IM_RABBITMQ_HEARTBEAT", defaultRabbitHeartbeat)
		if err != nil {
			return InfraConfig{}, err
		}
		config.RabbitMQ = RabbitMQConfig{
			URL:         rabbitURL,
			Password:    rabbitPassword,
			DialTimeout: dialTimeout,
			Heartbeat:   heartbeat,
		}
	}

	return config, nil
}

type timeoutValues struct {
	dial  time.Duration
	read  time.Duration
	write time.Duration
}

func loadMomentsCacheConfig() (MomentsCacheConfig, error) {
	enabled, err := optionalBool("IM_MOMENTS_CACHE_ENABLED", true)
	if err != nil {
		return MomentsCacheConfig{}, err
	}
	threshold, err := optionalInt("IM_MOMENTS_PULL_THRESHOLD", 500)
	if err != nil {
		return MomentsCacheConfig{}, err
	}
	if threshold <= 0 {
		return MomentsCacheConfig{}, fmt.Errorf("IM_MOMENTS_PULL_THRESHOLD must be positive")
	}
	feedLimit, err := optionalInt("IM_MOMENTS_FEED_WARM_LIMIT", defaultMomentsFeedLimit)
	if err != nil {
		return MomentsCacheConfig{}, err
	}
	if feedLimit <= 0 {
		return MomentsCacheConfig{}, fmt.Errorf("IM_MOMENTS_FEED_WARM_LIMIT must be positive")
	}
	pullSources, err := optionalInt("IM_MOMENTS_MAX_PULL_SOURCES", defaultMomentsPullSources)
	if err != nil {
		return MomentsCacheConfig{}, err
	}
	if pullSources <= 0 {
		return MomentsCacheConfig{}, fmt.Errorf("IM_MOMENTS_MAX_PULL_SOURCES must be positive")
	}
	feedTTL, err := optionalDuration("IM_MOMENTS_FEED_TTL", defaultMomentsFeedTTL)
	if err != nil {
		return MomentsCacheConfig{}, err
	}
	likeTTL, err := optionalDuration("IM_MOMENTS_LIKE_TTL", defaultMomentsLikeTTL)
	if err != nil {
		return MomentsCacheConfig{}, err
	}
	operationTimeout, err := optionalDuration(
		"IM_MOMENTS_CACHE_TIMEOUT",
		defaultMomentsCacheTimeout,
	)
	if err != nil {
		return MomentsCacheConfig{}, err
	}
	return MomentsCacheConfig{
		Enabled:          enabled,
		PullThreshold:    threshold,
		FeedWarmLimit:    feedLimit,
		FeedTTL:          feedTTL,
		LikeTTL:          likeTTL,
		OperationTimeout: operationTimeout,
		MaxPullSources:   pullSources,
	}, nil
}

func redisTimeouts() (timeoutValues, error) {
	dial, err := optionalDuration("IM_REDIS_DIAL_TIMEOUT", defaultInfraTimeout)
	if err != nil {
		return timeoutValues{}, err
	}
	read, err := optionalDuration("IM_REDIS_READ_TIMEOUT", defaultInfraTimeout)
	if err != nil {
		return timeoutValues{}, err
	}
	write, err := optionalDuration("IM_REDIS_WRITE_TIMEOUT", defaultInfraTimeout)
	if err != nil {
		return timeoutValues{}, err
	}
	return timeoutValues{dial: dial, read: read, write: write}, nil
}

func valueOrFile(valueKey, fileKey string) (string, error) {
	value := strings.TrimSpace(os.Getenv(valueKey))
	file := strings.TrimSpace(os.Getenv(fileKey))
	if value != "" && file != "" {
		return "", fmt.Errorf("%s and %s cannot both be set", valueKey, fileKey)
	}
	if value != "" {
		return value, nil
	}
	if file == "" {
		return "", nil
	}
	contents, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", fileKey, err)
	}
	value = strings.TrimSpace(string(contents))
	if value == "" {
		return "", fmt.Errorf("%s points to an empty file", fileKey)
	}
	return value, nil
}

func optionalInt(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return parsed, nil
}

func optionalDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return parsed, nil
}

func optionalBool(key string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return parsed, nil
}

func messagingNamespace() (string, error) {
	value := strings.TrimSpace(os.Getenv("IM_MESSAGING_NAMESPACE"))
	if value == "" {
		return DefaultMessagingNamespace, nil
	}
	if len(value) > 64 {
		return "", fmt.Errorf("IM_MESSAGING_NAMESPACE is too long")
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == ':' || character == '-' || character == '_' {
			continue
		}
		return "", fmt.Errorf("IM_MESSAGING_NAMESPACE contains invalid characters")
	}
	value = strings.TrimSuffix(value, ":")
	if value == "" {
		return "", fmt.Errorf("IM_MESSAGING_NAMESPACE is empty")
	}
	return value, nil
}
