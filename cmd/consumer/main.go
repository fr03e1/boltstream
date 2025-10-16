package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/fr03e1/boltstream/internal/platform/metrics"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
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
	metrics.RegisterConsumer()
	metrics.ConsumerUp.Set(1)
	defer metrics.ConsumerUp.Set(0)

	topic := getenv("TOPIC", "events")
	groupID := os.Getenv("GROUP_ID")
	if groupID == "" {
		log.Fatal("GROUP_ID must be set")
	}
	attempts, _ := strconv.Atoi(getenv("RETRY_ATTEMPTS", "3"))
	baseMs, _ := strconv.Atoi(getenv("RETRY_BACKOFF_MS", "200"))
	mode := getenv("RETRY_BACKOFF_MODE", "exponential")
	brokers := splitBrokers(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		log.Fatal("KAFKA_BROKERS must be set")
	}

	go func() {
		addr := getenv("CONSUMER_METRICS_ADDR", ":8082")
		log.Printf("metrics listening on %s", addr)
		if err := http.ListenAndServe(addr, promhttp.Handler()); err != nil {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1 << 10,
		MaxBytes:       10 << 20,
		CommitInterval: 0,
		Logger:         log.New(os.Stdout, "reader ", 0),
		ErrorLogger:    log.New(os.Stderr, "reader.err ", 0),
	})
	defer reader.Close()

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        getenv("DLQ_TOPIC", "events.dlq"),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchBytes:   1 << 20,
		BatchTimeout: 50 * time.Millisecond,
	}
	defer dlqWriter.Close()

	go updateLag(ctx, reader, topic)

	log.Printf("consumer.start topic=%s group=%s", topic, groupID)

	for {
		start := time.Now()

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("shutdown.start")
				break
			}
			metrics.ConsumerErrorAdd("fetch", topic, 1)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if err := processMessage(ctx, msg, reader, dlqWriter, attempts, baseMs, mode, start); err != nil {
			log.Printf("message.error: %v", err)
		}
	}

	log.Println("shutdown.done")
}

func processMessage(ctx context.Context, msg kafka.Message, reader *kafka.Reader, dlq *kafka.Writer,
	attempts, baseMs int, mode string, start time.Time) error {

	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ValidateAndProcess(msg.Value); err == nil {
			if err := reader.CommitMessages(ctx, msg); err != nil {
				metrics.ConsumerErrorAdd("commit", msg.Topic, 1)
			} else {
				metrics.ConsumerCommitAdd(msg.Topic, 1)
				metrics.ConsumerAdd(msg.Topic, 1)
				metrics.ObserveProcessing(msg.Topic, time.Since(start).Seconds())
				metrics.ObserveBatchBytes(msg.Topic, len(msg.Value))
			}
			return nil
		}

		if attempt == attempts {
			return sendToDLQ(ctx, msg, reader, dlq, attempt, start)
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
			return ctx.Err()
		}
	}
	return nil
}

func sendToDLQ(ctx context.Context, msg kafka.Message, reader *kafka.Reader, dlq *kafka.Writer, attempt int, start time.Time) error {
	headers := []kafka.Header{
		{Key: "original-topic", Value: []byte(msg.Topic)},
		{Key: "partition", Value: []byte(strconv.Itoa(msg.Partition))},
		{Key: "offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		{Key: "retries", Value: []byte(strconv.Itoa(attempt))},
		{Key: "ts_deadlettered", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	}

	dlqMsg := kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Time:    time.Now(),
		Headers: headers,
	}

	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := dlq.WriteMessages(wctx, dlqMsg)
	cancel()

	if err != nil {
		metrics.ConsumerErrorAdd("dlq", msg.Topic, 1)
		return err
	}

	if err := reader.CommitMessages(ctx, msg); err != nil {
		metrics.ConsumerErrorAdd("commit", msg.Topic, 1)
		return err
	}

	metrics.ConsumerCommitAdd(msg.Topic, 1)
	metrics.ConsumerAdd(msg.Topic, 1)
	metrics.ObserveProcessing(msg.Topic, time.Since(start).Seconds())
	metrics.ObserveBatchBytes(msg.Topic, len(msg.Value))
	return nil
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

func updateLag(ctx context.Context, r *kafka.Reader, topic string) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			stats := r.Stats()
			metrics.ConsumerLagSet(topic, "-1", float64(stats.Lag))
		}
	}
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
