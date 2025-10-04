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
	"strconv"
	"strings"
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
	attempts, _ := strconv.Atoi(getenv("RETRY_ATTEMPTS", "3"))
	baseMs, _ := strconv.Atoi(getenv("RETRY_BACKOFF_MS", "200"))
	mode := getenv("RETRY_BACKOFF_MODE", "exponential") // а не "200"
	if attempts < 1 {
		attempts = 1
	}
	if baseMs < 1 {
		baseMs = 1
	}

	brokers := splitBrokers(os.Getenv("KAFKA_BROKERS"))

	if len(brokers) == 0 {
		log.Fatal("KAFKA_BROKERS must be set")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1 << 10,
		MaxBytes:       10 << 20,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
		Logger:         log.New(os.Stdout, "reader ", 0),
		ErrorLogger:    log.New(os.Stderr, "reader.err ", 0),
		MaxWait:        200 * time.Millisecond,
	})

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        getenv("DLQ_TOPIC", "events.dlq"),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchBytes:   1 << 20,
		BatchTimeout: 50 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}
	defer dlqWriter.Close()

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

		for attempt := 1; attempt <= attempts; attempt++ {
			if err := ValidateAndProcess(msg.Value); err == nil {
				if err := reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("commit.error: %v", err)
				} else {
					log.Printf("commit.ok topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
				}
				break
			}

			log.Printf("process.error attempt=%d/%d err=%v len=%d head=%q",
				attempt, attempts, err, len(msg.Value), head(msg.Value, 96))

			if attempt == attempts {
				log.Printf("retry.exhausted topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)

				dlqHeaders := []kafka.Header{
					{Key: "original-topic", Value: []byte(msg.Topic)},
					{Key: "original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
					{Key: "original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
					{Key: "retries", Value: []byte(strconv.Itoa(attempt))},
					{Key: "error", Value: []byte(truncate(err.Error(), 200))},
					{Key: "ts_deadlettered", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
				}

				dlqMsg := kafka.Message{
					Key:     append([]byte(nil), msg.Key...),
					Value:   append([]byte(nil), msg.Value...),
					Time:    time.Now(),
					Headers: dlqHeaders,
				}

				wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				errDLQ := dlqWriter.WriteMessages(wctx, dlqMsg)
				cancel()

				if errDLQ != nil {
					log.Printf("dlq.send.error: %v", errDLQ)

				} else {
					log.Printf("dlq.send.ok topic=%s retries=%d", dlqWriter.Topic, attempt)
					if err := reader.CommitMessages(ctx, msg); err != nil {
						log.Printf("commit.error: %v", err)
					} else {
						log.Printf("commit.ok topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
					}
				}

				break
			}

			var delay time.Duration

			if mode == "linear" {
				delay = time.Duration(baseMs*attempt) * time.Millisecond
			} else {
				delay = time.Duration(baseMs<<uint(attempt-1)) * time.Millisecond
				if delay > 2*time.Second {
					delay = 2 * time.Second
				}
			}

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				log.Println("shutdown.start")
				return
			}
		}
	}

	log.Println("shutdown.done")
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
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

	if m.EventID == "" {
		return ErrMissingEvent
	}

	if m.UserID == "" {
		return ErrMissingUser
	}

	return nil
}
