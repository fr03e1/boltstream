package main

import (
	"github.com/fr03e1/boltstream/internal/platform/config"
	"github.com/fr03e1/boltstream/internal/platform/httpx"
	"github.com/fr03e1/boltstream/internal/platform/readiness"
	"github.com/fr03e1/boltstream/internal/producer"
	"log"
	"net/http"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	st := producer.NewState()
	g := readiness.New(true)
	router := producer.NewRouter(st, g)
	srv := httpx.NewServer(cfg.HTTPAddr, router)

	log.Printf("producer listening on %s", cfg.HTTPAddr)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}
