package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultInfraTimeout    = 5 * time.Second
	defaultRabbitHeartbeat = 10 * time.Second
)

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
	DialTimeout time.Duration
	Heartbeat   time.Duration
}

func (c RabbitMQConfig) Enabled() bool {
	return c.URL != ""
}

type InfraConfig struct {
	Redis    RedisConfig
	RabbitMQ RabbitMQConfig
}

func LoadInfra() (InfraConfig, error) {
	redisURL, err := valueOrFile("IM_REDIS_URL", "IM_REDIS_URL_FILE")
	if err != nil {
		return InfraConfig{}, err
	}
	rabbitURL, err := valueOrFile("IM_RABBITMQ_URL", "IM_RABBITMQ_URL_FILE")
	if err != nil {
		return InfraConfig{}, err
	}

	config := InfraConfig{}
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
