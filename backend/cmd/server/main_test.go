package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
)

type fakeInfraStatus struct {
	snapshot infra.Snapshot
}

func (f fakeInfraStatus) Snapshot() infra.Snapshot {
	return f.snapshot
}

func TestInfraStatusExposesSnapshotWithoutChangingReadiness(t *testing.T) {
	router, err := newRouter("", func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}
	registerInfraStatus(router, fakeInfraStatus{snapshot: infra.Snapshot{
		Redis:    infra.ComponentStatus{Enabled: true, Connected: true},
		RabbitMQ: infra.ComponentStatus{Enabled: true, Connected: false},
	}}, func() *messaging.SeqWatcherStatus {
		return &messaging.SeqWatcherStatus{Ready: true, Conversations: 3}
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ops/infra", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var snapshot struct {
		infra.Snapshot
		Seq *messaging.SeqWatcherStatus `json:"seq"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !snapshot.Redis.Connected || snapshot.RabbitMQ.Connected {
		t.Fatalf("snapshot = %#v", snapshot.Snapshot)
	}
	if snapshot.Seq == nil || !snapshot.Seq.Ready || snapshot.Seq.Conversations != 3 {
		t.Fatalf("seq status = %#v, want ready with 3 conversations", snapshot.Seq)
	}

	ready := httptest.NewRecorder()
	router.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want %d", ready.Code, http.StatusOK)
	}
}

func TestInfraStatusAbsentWhenNotRegistered(t *testing.T) {
	router, err := newRouter("", nil)
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ops/infra", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestHealth(t *testing.T) {
	router, err := newRouter("", nil)
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	var response struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ok" || response.Service != serviceName {
		t.Fatalf("response = %#v, want status ok and service %q", response, serviceName)
	}
}

func TestReady(t *testing.T) {
	tests := []struct {
		name       string
		ready      func(context.Context) error
		wantStatus int
		wantState  string
	}{
		{
			name:       "database available",
			ready:      func(context.Context) error { return nil },
			wantStatus: http.StatusOK,
			wantState:  "ok",
		},
		{
			name:       "database unavailable",
			ready:      func(context.Context) error { return errors.New("database down") },
			wantStatus: http.StatusServiceUnavailable,
			wantState:  "degraded",
		},
		{
			name:       "database check not configured",
			ready:      nil,
			wantStatus: http.StatusServiceUnavailable,
			wantState:  "degraded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router, err := newRouter("", tt.ready)
			if err != nil {
				t.Fatalf("newRouter() error = %v", err)
			}

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/ready", nil)
			router.ServeHTTP(recorder, request)

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}

			var response struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response.Status != tt.wantState {
				t.Fatalf("status field = %q, want %q", response.Status, tt.wantState)
			}
		})
	}
}

func TestStaticFiles(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("status page"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	router, err := newRouter(webDir, nil)
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Body.String() != "status page" {
		t.Fatalf("body = %q, want %q", recorder.Body.String(), "status page")
	}
}

func TestStaticDirectoryRequiresIndex(t *testing.T) {
	_, err := newRouter(t.TempDir(), nil)
	if err == nil {
		t.Fatal("newRouter() error = nil, want missing index error")
	}
}

func TestSPAFallback(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("app shell"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	router, err := newRouter(webDir, nil)
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	for _, requestPath := range []string{"/", "/login", "/register", "/settings"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, requestPath, nil)
		router.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want %d", requestPath, recorder.Code, http.StatusOK)
		}
		if recorder.Body.String() != "app shell" {
			t.Fatalf("%s body = %q, want app shell", requestPath, recorder.Body.String())
		}
	}
}

func TestSPAFallbackDoesNotHandleAPI(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("app shell"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	router, err := newRouter(webDir, nil)
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/unknown", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if recorder.Body.String() == "app shell" {
		t.Fatal("API route returned the SPA shell")
	}
}

func TestSPAFallbackDoesNotHandleOps(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("app shell"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	router, err := newRouter(webDir, nil)
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ops/infra", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if recorder.Body.String() == "app shell" {
		t.Fatal("ops route returned the SPA shell")
	}
}

func TestSPAFallbackLeavesMissingAssets404(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("app shell"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	router, err := newRouter(webDir, nil)
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/missing.js", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}
