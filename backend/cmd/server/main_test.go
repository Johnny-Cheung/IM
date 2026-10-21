package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHealth(t *testing.T) {
	router, err := newRouter("")
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

func TestStaticFiles(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("status page"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	router, err := newRouter(webDir)
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
	_, err := newRouter(t.TempDir())
	if err == nil {
		t.Fatal("newRouter() error = nil, want missing index error")
	}
}
