package infra

import (
	"testing"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
)

func TestNewDisabled(t *testing.T) {
	manager, err := New(config.InfraConfig{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if manager != nil {
		t.Fatal("New() returned manager for disabled configuration")
	}
}

func TestNewValidatesRedisURL(t *testing.T) {
	_, err := New(config.InfraConfig{Redis: config.RedisConfig{URL: "not-a-redis-url"}})
	if err == nil {
		t.Fatal("New() error = nil, want invalid Redis URL error")
	}
}

func TestNewValidatesRabbitMQURL(t *testing.T) {
	_, err := New(config.InfraConfig{RabbitMQ: config.RabbitMQConfig{URL: "http://127.0.0.1:5672"}})
	if err == nil {
		t.Fatal("New() error = nil, want invalid RabbitMQ URL error")
	}
}

func TestSnapshotStartsDisabled(t *testing.T) {
	manager := &Manager{status: newStatusStore(true, true)}
	snapshot := manager.Snapshot()
	if !snapshot.Redis.Enabled || snapshot.Redis.Connected {
		t.Fatalf("Redis snapshot = %#v", snapshot.Redis)
	}
	if !snapshot.RabbitMQ.Enabled || snapshot.RabbitMQ.Connected {
		t.Fatalf("RabbitMQ snapshot = %#v", snapshot.RabbitMQ)
	}
}

func TestSafeErrorRedactsCredentials(t *testing.T) {
	err := safeError(
		&testError{message: "dial amqp://user:secret@127.0.0.1:5672/vhost failed"},
		"amqp://user:secret@127.0.0.1:5672/vhost",
	)
	if err == nil || contains(err.Error(), "secret") || contains(err.Error(), "amqp://user:secret") {
		t.Fatalf("safeError() = %v, credentials were not redacted", err)
	}
}

type testError struct {
	message string
}

func (e *testError) Error() string { return e.message }

func contains(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}
