package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"channeld/internal/config"
	"channeld/internal/realtime"
)

func main() {
	path := os.Getenv("CHANNEL_CONFIG")
	if path == "" {
		path = "configs/channeld.json"
	}
	cfg, err := config.Load(path)
	if err != nil {
		log.Fatal(err)
	}
	running, err := realtime.Start(cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("channeld %s listening on http://127.0.0.1:%d", cfg.Version, running.Port)
	log.Printf("websocket  /connection/websocket")
	log.Printf("http api   POST /api/publish")
	if cfg.Insecure {
		log.Printf("http api key check is off (http_api.insecure)")
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := running.Close(ctx); err != nil {
		log.Fatal(err)
	}
}
