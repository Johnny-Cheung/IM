package infra

import (
	"sync"
	"time"
)

type ComponentStatus struct {
	Enabled       bool      `json:"enabled"`
	Connected     bool      `json:"connected"`
	LastError     string    `json:"last_error,omitempty"`
	Reconnects    uint64    `json:"reconnects"`
	LastSuccessAt time.Time `json:"last_success_at,omitempty"`
	QueueBacklog  int       `json:"queue_backlog"`
}

type Snapshot struct {
	Redis    ComponentStatus `json:"redis"`
	RabbitMQ ComponentStatus `json:"rabbitmq"`
}

type statusStore struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func newStatusStore(redisEnabled, rabbitEnabled bool) *statusStore {
	return &statusStore{snapshot: Snapshot{
		Redis:    ComponentStatus{Enabled: redisEnabled},
		RabbitMQ: ComponentStatus{Enabled: rabbitEnabled},
	}}
}

func (s *statusStore) snapshotCopy() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

func (s *statusStore) success(component string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.component(component)
	status.Connected = true
	status.LastError = ""
	status.LastSuccessAt = time.Now().UTC()
}

func (s *statusStore) failure(component string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.component(component)
	status.Connected = false
	if err != nil {
		status.LastError = err.Error()
	}
}

func (s *statusStore) reconnect(component string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.component(component).Reconnects++
}

func (s *statusStore) component(component string) *ComponentStatus {
	if component == "redis" {
		return &s.snapshot.Redis
	}
	return &s.snapshot.RabbitMQ
}
