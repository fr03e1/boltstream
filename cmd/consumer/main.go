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

type Committer interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type DLQWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

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

		if err := ProcessMessage(ctx, msg, reader, dlqWriter, attempts, baseMs, mode, start); err != nil {
			log.Printf("message.error: %v", err)
		}
	}

	log.Println("shutdown.done")
}

func ProcessMessage(ctx context.Context, msg kafka.Message, reader Committer, dlq DLQWriter,
	attempts, baseMs int, mode string, start time.Time) error {

	for attempt := 1; attempt <= attempts; attempt++ {
		err := ValidateAndProcess(msg)
		if err == nil {
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

		if IsPermanent(err) {
			metrics.ConsumerErrorAdd("permanent", msg.Topic, 1)
			return sendToDLQ(ctx, msg, reader, dlq, attempt, start, err, "permanent")
		}

		if attempt == attempts {
			return sendToDLQ(ctx, msg, reader, dlq, attempt, start, err, "transient")
		}

		delay := calcDelay(mode, baseMs, attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func calcDelay(mode string, baseMs, attempt int) time.Duration {
	if mode == "linear" {
		return time.Duration(baseMs*attempt) * time.Millisecond
	}
	delay := time.Duration(baseMs<<uint(attempt-1)) * time.Millisecond
	if delay > 2*time.Second {
		delay = 2 * time.Second
	}
	return delay
}

func IsPermanent(err error) bool {
	switch err {
	case ErrInvalidJSON, ErrMissingEvent, ErrMissingUser, ErrBadTS, ErrBadPayload:
		return true
	default:
		return false
	}
}

func sendToDLQ(ctx context.Context, msg kafka.Message, reader Committer, dlq DLQWriter, attempt int, start time.Time, cause error, errorKind string) error {
	errStr := errorCode(cause)

	headers := []kafka.Header{
		{Key: "original-topic", Value: []byte(msg.Topic)},
		{Key: "partition", Value: []byte(strconv.Itoa(msg.Partition))},
		{Key: "offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		{Key: "retries", Value: []byte(strconv.Itoa(attempt))},
		{Key: "ts_deadlettered", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		{Key: "error", Value: []byte(errStr)},
		{Key: "error_kind", Value: []byte(errorKind)},
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
	metrics.ConsumerDLQAdd(msg.Topic, errorKind, errStr, 1)

	return nil
}

func ValidateAndProcess(msg kafka.Message) error {
	var m MsgStruct

	if err := json.Unmarshal(msg.Value, &m); err != nil {
		metrics.ConsumerErrorAdd("decode", msg.Topic, 1)
		return ErrInvalidJSON
	}
	if _, err := time.Parse(time.RFC3339Nano, m.TS); err != nil {
		if _, err2 := time.Parse(time.RFC3339, m.TS); err2 != nil {
			metrics.ConsumerErrorAdd("validate", msg.Topic, 1)
			return ErrBadTS
		}
	}
	if len(m.Payload) == 0 || string(m.Payload) == "null" || m.Payload[0] != '{' {
		metrics.ConsumerErrorAdd("validate", msg.Topic, 1)
		return ErrBadPayload
	}
	if m.EventID == "" {
		metrics.ConsumerErrorAdd("validate", msg.Topic, 1)
		return ErrMissingEvent
	}
	if m.UserID == "" {
		metrics.ConsumerErrorAdd("validate", msg.Topic, 1)
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
			metrics.ConsumerLagSet(topic, "all", float64(stats.Lag))
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

func errorCode(err error) string {
	switch err {
	case ErrInvalidJSON:
		return "invalid_json"
	case ErrMissingEvent:
		return "missing_event_id"
	case ErrMissingUser:
		return "missing_user_id"
	case ErrBadTS:
		return "bad_timestamp"
	case ErrBadPayload:
		return "bad_payload"
	default:
		return "unknown"
	}
}
