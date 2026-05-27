package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/DivyanshuShekhar55/catfish/backpressure"
	"github.com/DivyanshuShekhar55/catfish/config"
	"github.com/DivyanshuShekhar55/catfish/proxy"
)

func main() {
	configPath := flag.String("config", "/etc/catfish/config.yml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sem := backpressure.NewSemaphore(cfg.Tiers, cfg.MaxConcurrent)
	defer sem.Close()

	server, err := proxy.New(ctx, cfg, sem)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	// listen for ctrl+c or docker stop signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("shutting down...")
		cancel() // cancel contexct first
		server.Close() // then close the server
	}()

	log.Printf("catfish listening on %s", cfg.ListenerAddr)
	if err := server.Listen(); err != nil {
		log.Fatalf("server error: %v", err)
	}
	// is server.Closed() is called
	log.Println("catfish stopped")
}
