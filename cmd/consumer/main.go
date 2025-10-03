package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type MsgStruct struct {
	EventID string          `json:"event_id"`
	UserID  string          `json:"user_id"`
	TS      string          `json:"ts"`
	Payload json.RawMessage `json:"payload"`
}

var (
	ErrInvalidJSON  = fmt.Errorf("invalid_json")
	ErrMissingEvent = fmt.Errorf("missing_event_id")
	ErrMissingUser  = fmt.Errorf("missing_user_id")
	ErrBadTS        = fmt.Errorf("bad_timestamp")
	ErrBadPayload   = fmt.Errorf("bad_payload")
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

		if err := ValidateAndProcess(msg.Value); err != nil {
			log.Printf("process.error: %v len=%d head=%q", err, len(msg.Value), head(msg.Value, 96))
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

func head(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

func ValidateAndProcess(value []byte) error {
	var m MsgStruct

	if err := json.Unmarshal(value, &m); err != nil {
		return ErrInvalidJSON
	}

	if _, err := time.Parse(time.RFC3339Nano, m.TS); err != nil {
		if _, err2 := time.Parse(time.RFC3339, m.TS); err2 != nil {
			return ErrBadTS
		}
	}

	if len(m.Payload) == 0 || string(m.Payload) == "null" || m.Payload[0] != '{' {
		return ErrBadPayload
	}

	return nil
}
