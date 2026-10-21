package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

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

	router, err := newRouter(options.webDir)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s listening on %s", serviceName, options.addr)
	if err := http.ListenAndServe(options.addr, router); err != nil {
		log.Fatal(err)
	}
}

func newRouter(webDir string) (*gin.Engine, error) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"service": serviceName,
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
		fileServer.ServeHTTP(c.Writer, c.Request)
	})
	return router, nil
}
