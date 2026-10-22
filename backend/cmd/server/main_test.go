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
)

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
