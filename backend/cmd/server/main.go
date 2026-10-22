package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/httpapi"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

const (
	defaultAddr = "127.0.0.1:8080"
	serviceName = "im-backend"
)

type serverOptions struct {
	addr   string
	webDir string
}

func main() {
	options := serverOptions{}
	flag.StringVar(&options.addr, "addr", defaultAddr, "HTTP server address")
	flag.StringVar(&options.webDir, "web-dir", "", "directory containing the frontend build")
	flag.Parse()

	dsn, err := config.MySQLDSN()
	if err != nil {
		log.Fatalf("load MySQL configuration: %v", err)
	}

	startupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dataStore, err := store.Open(startupCtx, dsn)
	if err != nil {
		log.Fatalf("connect MySQL: %v", err)
	}
	defer dataStore.Close()

	if err := dataStore.Migrate(startupCtx); err != nil {
		log.Fatalf("migrate MySQL: %v", err)
	}
	log.Printf("MySQL ready; migrations applied")

	router, err := newRouter(options.webDir, dataStore.Ping)
	if err != nil {
		log.Fatal(err)
	}
	httpapi.New(dataStore).Register(router)

	log.Printf("%s listening on %s", serviceName, options.addr)
	if err := http.ListenAndServe(options.addr, router); err != nil {
		log.Fatal(err)
	}
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
		if requestPath == "/api" || strings.HasPrefix(requestPath, "/api/") {
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
