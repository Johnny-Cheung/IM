package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadInfraDisabledByDefault(t *testing.T) {
	clearInfraEnv(t)

	config, err := LoadInfra()
	if err != nil {
		t.Fatalf("LoadInfra() error = %v", err)
	}
	if config.Redis.Enabled() || config.RabbitMQ.Enabled() {
		t.Fatalf("config = %#v, want both components disabled", config)
	}
}

func TestLoadInfraReadsFilesAndDefaults(t *testing.T) {
	clearInfraEnv(t)
	dir := t.TempDir()
	redisPasswordFile := filepath.Join(dir, "redis-password")
	rabbitURLFile := filepath.Join(dir, "rabbit-url")
	if err := os.WriteFile(redisPasswordFile, []byte("redis-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rabbitURLFile, []byte("amqp://im:secret@127.0.0.1:5672/im-v11\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IM_REDIS_URL", "redis://127.0.0.1:6379")
	t.Setenv("IM_REDIS_PASSWORD_FILE", redisPasswordFile)
	t.Setenv("IM_REDIS_DB", "2")
	t.Setenv("IM_RABBITMQ_URL_FILE", rabbitURLFile)

	config, err := LoadInfra()
	if err != nil {
		t.Fatalf("LoadInfra() error = %v", err)
	}
	if !config.Redis.Enabled() || config.Redis.Password != "redis-secret" || config.Redis.DB != 2 {
		t.Fatalf("Redis config = %#v", config.Redis)
	}
	if !config.RabbitMQ.Enabled() || config.RabbitMQ.URL != "amqp://im:secret@127.0.0.1:5672/im-v11" {
		t.Fatalf("RabbitMQ config = %#v", config.RabbitMQ)
	}
	if config.Redis.DialTimeout != defaultInfraTimeout || config.RabbitMQ.Heartbeat != defaultRabbitHeartbeat {
		t.Fatalf("config defaults = %#v / %#v", config.Redis, config.RabbitMQ)
	}
}

func TestLoadInfraRejectsInvalidSettings(t *testing.T) {
	tests := []struct {
		name string
		set  func(*testing.T)
	}{
		{
			name: "conflicting URL sources",
			set: func(t *testing.T) {
				t.Setenv("IM_REDIS_URL", "redis://127.0.0.1:6379")
				t.Setenv("IM_REDIS_URL_FILE", filepath.Join(t.TempDir(), "url"))
			},
		},
		{
			name: "invalid database",
			set: func(t *testing.T) {
				t.Setenv("IM_REDIS_URL", "redis://127.0.0.1:6379")
				t.Setenv("IM_REDIS_DB", "-1")
			},
		},
		{
			name: "invalid timeout",
			set: func(t *testing.T) {
				t.Setenv("IM_RABBITMQ_URL", "amqp://127.0.0.1:5672")
				t.Setenv("IM_RABBITMQ_HEARTBEAT", "0s")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearInfraEnv(t)
			tt.set(t)
			if _, err := LoadInfra(); err == nil {
				t.Fatal("LoadInfra() error = nil, want error")
			}
		})
	}
}

func TestLoadInfraCustomTimeouts(t *testing.T) {
	clearInfraEnv(t)
	t.Setenv("IM_REDIS_URL", "redis://127.0.0.1:6379")
	t.Setenv("IM_REDIS_DIAL_TIMEOUT", "700ms")
	t.Setenv("IM_REDIS_READ_TIMEOUT", "800ms")
	t.Setenv("IM_REDIS_WRITE_TIMEOUT", "900ms")
	t.Setenv("IM_RABBITMQ_URL", "amqp://127.0.0.1:5672")
	t.Setenv("IM_RABBITMQ_DIAL_TIMEOUT", "1s")
	t.Setenv("IM_RABBITMQ_HEARTBEAT", "2s")

	config, err := LoadInfra()
	if err != nil {
		t.Fatalf("LoadInfra() error = %v", err)
	}
	if config.Redis.DialTimeout != 700*time.Millisecond || config.Redis.ReadTimeout != 800*time.Millisecond || config.Redis.WriteTimeout != 900*time.Millisecond {
		t.Fatalf("Redis timeouts = %#v", config.Redis)
	}
	if config.RabbitMQ.DialTimeout != time.Second || config.RabbitMQ.Heartbeat != 2*time.Second {
		t.Fatalf("RabbitMQ timeouts = %#v", config.RabbitMQ)
	}
}

func clearInfraEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"IM_REDIS_URL", "IM_REDIS_URL_FILE", "IM_REDIS_PASSWORD", "IM_REDIS_PASSWORD_FILE", "IM_REDIS_DB",
		"IM_REDIS_DIAL_TIMEOUT", "IM_REDIS_READ_TIMEOUT", "IM_REDIS_WRITE_TIMEOUT",
		"IM_RABBITMQ_URL", "IM_RABBITMQ_URL_FILE", "IM_RABBITMQ_DIAL_TIMEOUT", "IM_RABBITMQ_HEARTBEAT",
	} {
		t.Setenv(key, "")
	}
}
