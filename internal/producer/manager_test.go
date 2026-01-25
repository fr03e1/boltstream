package producer

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"sync/atomic"
	"testing"
	"time"
)

type mockWriter struct {
	topic string
}

func (m *mockWriter) Topic() string                                                  { return m.topic }
func (m *mockWriter) Close() error                                                   { return nil }
func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error { return nil }

func requireNotClosedWithin(t *testing.T, ch <-chan struct{}, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("expected NOT closed within %s: %s", d, msg)
	case <-time.After(d):

	}
}

func requireClosedWithin(t *testing.T, ch <-chan struct{}, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(d):
		t.Fatalf("expected closed within %s: %s", d, msg)
	}
}

func TestStopAndDrain_WritesAllQueuedMessages(t *testing.T) {
	m := newManagerWithMock()

	var wrote int64
	var usedCanceled atomic.Bool

	m.writeFn = func(ctx context.Context, batch []kafka.Message) error {
		if ctx.Err() != nil {
			usedCanceled.Store(true)
		}
		atomic.AddInt64(&wrote, int64(len(batch)))
		return nil
	}

	for i := 0; i < 5; i++ {
		_ = m.Enqueue(kafka.Message{Value: []byte("X")})
	}

	graceCtx, graceCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer graceCancel()

	m.StartWriter()
	err := m.StopAndDrain(graceCtx)

	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	if usedCanceled.Load() {
		t.Fatalf("writer used canceled context during drain")
	}
	if wrote != 5 {
		t.Fatalf("expected to write 5 messages, wrote %d", wrote)
	}
}

func TestStopAndDrain_Timeout(t *testing.T) {
	entered := make(chan struct{})

	m := newManagerWithMock()

	m.writeFn = func(ctx context.Context, batch []kafka.Message) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		select {}
	}

	_ = m.Enqueue(kafka.Message{Value: []byte("X")})
	m.StartWriter()

	select {
	case <-entered:

	case <-time.After(200 * time.Millisecond):
		t.Fatalf("writer did not enter writeFn")
	}

	graceCtx, graceCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer graceCancel()

	err := m.StopAndDrain(graceCtx)

	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got: %v", err)
	}
}

func TestWriter_DoesNotExitUntilChannelClosed(t *testing.T) {
	m := newManagerWithMock()

	var wrote int64
	m.writeFn = func(ctx context.Context, batch []kafka.Message) error {
		atomic.AddInt64(&wrote, int64(len(batch)))
		return nil
	}

	exited := make(chan struct{})
	go func() {
		m.writer()
		close(exited)
	}()

	m.msg <- kafka.Message{Value: []byte("X")}

	requireNotClosedWithin(t, exited, 30*time.Millisecond, "writer exited while channel still open")

	close(m.msg)
	requireClosedWithin(t, exited, 200*time.Millisecond, "writer did not exit after channel close")

	if atomic.LoadInt64(&wrote) == 0 {
		t.Fatalf("expected writer to write at least 1 message, wrote %d", wrote)
	}
}

func TestWriter_FlushesAllQueuedMessagesOnClose(t *testing.T) {
	m := newManagerWithMock()

	var wrote int64
	m.writeFn = func(ctx context.Context, batch []kafka.Message) error {
		atomic.AddInt64(&wrote, int64(len(batch)))
		return nil
	}

	exited := make(chan struct{})
	go func() {
		m.writer()
		close(exited)
	}()

	want := int64(5)
	for i := int64(0); i < want; i++ {
		m.msg <- kafka.Message{Value: []byte("X")}
	}

	close(m.msg)
	requireClosedWithin(t, exited, 200*time.Millisecond, "writer did not exit after close")

	got := atomic.LoadInt64(&wrote)
	if got != want {
		t.Fatalf("expected to write %d messages, wrote %d", want, got)
	}
}

func newManagerWithMock() *Manager {
	mockW := &mockWriter{
		topic: "test_topic",
	}
	return NewManager(mockW, 10)
}
