package kafkax

import (
	"context"
	"errors"
	"fmt"
	"github.com/fr03e1/boltstream/internal/platform/config"
	"github.com/segmentio/kafka-go"
	"time"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type Writer struct {
	w       *kafka.Writer
	brokers []string
	topic   string
}

func NewWriter(cfg *config.Config) (*Writer, error) {
	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("kafka: empty brokers")
	}

	if cfg.KafkaTopic == "" {
		return nil, errors.New("kafka: empty topic")
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		BatchBytes:   cfg.WriteBatchBytes,
		BatchTimeout: cfg.WriterBatchTime,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}

	return &Writer{
		w:       w,
		brokers: cfg.KafkaBrokers,
		topic:   cfg.KafkaTopic,
	}, nil
}

func (w *Writer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	return w.w.WriteMessages(ctx, msgs...)
}

func (w *Writer) Close() error {
	if w == nil || w.w == nil {
		return nil
	}
	return w.w.Close()
}

func (w *Writer) Ping(ctx context.Context) error {
	if len(w.brokers) == 0 {
		return errors.New("kafka: no brokers configured")
	}
	conn, err := kafka.DialContext(ctx, "tcp", w.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions(w.topic)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return fmt.Errorf("kafka: topic %q not found or has no partitions", w.topic)
	}
	return nil
}

func (w *Writer) W() *kafka.Writer { return w.w }
func (w *Writer) Topic() string    { return w.topic }
