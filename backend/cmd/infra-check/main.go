package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
)

func main() {
	timeout := flag.Duration("timeout", 15*time.Second, "timeout for each infrastructure check")
	flag.Parse()

	infraConfig, err := config.LoadInfra()
	if err != nil {
		log.Fatalf("load infrastructure configuration: %v", err)
	}
	if !infraConfig.Redis.Enabled() && !infraConfig.RabbitMQ.Enabled() {
		fmt.Fprintln(os.Stderr, "no infrastructure configured; set IM_REDIS_URL and/or IM_RABBITMQ_URL")
		os.Exit(2)
	}

	exitCode := 0
	if infraConfig.Redis.Enabled() {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		err := infra.RedisRoundTrip(ctx, infraConfig.Redis)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL redis: %v\n", err)
			exitCode = 1
		} else {
			fmt.Println("PASS redis set/get/del")
		}
	}
	if infraConfig.RabbitMQ.Enabled() {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		err := infra.RabbitRoundTrip(ctx, infraConfig.RabbitMQ)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL rabbitmq: %v\n", err)
			exitCode = 1
		} else {
			fmt.Println("PASS rabbitmq publish/consume/ack")
		}
	}
	os.Exit(exitCode)
}
