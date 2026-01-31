package main

import (
	"context"
	"errors"
	"github.com/fr03e1/boltstream/internal/platform/config"
	"github.com/fr03e1/boltstream/internal/platform/httpx"
	"github.com/fr03e1/boltstream/internal/platform/kafkax"
	"github.com/fr03e1/boltstream/internal/platform/metrics"
	"github.com/fr03e1/boltstream/internal/platform/readiness"
	"github.com/fr03e1/boltstream/internal/producer"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	kWriter, err := kafkax.NewWriter(&cfg)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}

	metrics.RegisterHTTP()
	metrics.RegisterProducer()
	metrics.RegisterIngest()

	st := producer.NewState()
	g := readiness.New(true)
	mng := producer.NewManager(kWriter, cfg)

	router := producer.NewRouter(st, g, kWriter, mng)
	srv := httpx.NewServer(cfg.HTTPAddr, router)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("producer listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	mng.StartWriter()

	<-ctx.Done()
	log.Printf("shutdown: signal received, draining...")

	graceCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	g.Set(false)
	if err := srv.Shutdown(graceCtx); err != nil {
		log.Printf("shutdown graceful failed: %v, forcing close", err)
		_ = srv.Close()
	}
	if err := mng.StopAndDrain(graceCtx); err != nil {
		log.Printf("drain buffer failed: %v", err)
	}
	if err := kWriter.Close(); err != nil {
		log.Printf("kafka close failed: %v", err)
	}

	log.Println("shutdown: app drained")
}
