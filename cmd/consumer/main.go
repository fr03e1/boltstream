package main

import (
	"context"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	topic := getenv("TOPIC", "events")
	groupID := os.Getenv("GROUP_ID")
	if groupID == "" {
		log.Fatal("GROUP_ID must be set")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{os.Getenv("KAFKA_BROKERS")},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1 << 10,
		MaxBytes:       10 << 20,
		CommitInterval: 0,
	})
	defer func() {
		_ = reader.Close()
		log.Println("reader.closed")
	}()

	log.Printf("consumer.start topic=%s group=%s", topic, groupID)

	for {
		msg, err := reader.FetchMessage(ctx)

		if err != nil {
			if ctx.Err() != nil {
				log.Println("shutdown.start")
				break
			}

			log.Printf("fetch.error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		log.Printf("msg topic=%s partition=%d offset=%d key=%dB value=%dB",
			msg.Topic, msg.Partition, msg.Offset, len(msg.Key), len(msg.Value))

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit.error: %v", err)
		}
	}

	log.Println("shutdown.done")
}
