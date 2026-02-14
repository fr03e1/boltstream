package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"testing"
	"time"
)

type fakeCommitter struct {
	calls int
}

func (f *fakeCommitter) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.calls++
	return nil
}

type fakeDLQ struct {
	calls int
}

func (f *fakeDLQ) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.calls++
	return nil
}

func TestProcessMessage_PermanentError_GoesToDLQAndCommits(t *testing.T) {
	ctx := context.Background()
	reader := &fakeCommitter{}
	dlq := &fakeDLQ{}

	msg := kafka.Message{
		Topic: "events",
		Value: []byte(`{"event_id":"1","user_id":","ts":"2026-02-14T00:00:00Z","payload":{}}`),
	}

	err := ProcessMessage(ctx, msg, reader, dlq, 3, 200, "exponential", time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reader.calls != 1 {
		t.Fatalf("expected 1 commit, got %d", reader.calls)
	}

	if dlq.calls != 1 {
		t.Fatalf("expected 1 dlq write, got %d", dlq.calls)
	}
}

func TestProcessMessage_Success_CommitsWithoutDLQ(t *testing.T) {
	ctx := context.Background()
	reader := &fakeCommitter{}
	dlq := &fakeDLQ{}

	msg := kafka.Message{
		Topic: "events",
		Value: []byte(`{"event_id":"1","user_id":"1","ts":"2026-02-14T00:00:00Z","payload":{}}`),
	}

	err := ProcessMessage(ctx, msg, reader, dlq, 3, 200, "exponential", time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reader.calls != 1 {
		t.Fatalf("expected 1 commit, got %d", reader.calls)
	}

	if dlq.calls != 0 {
		t.Fatalf("expected 0 dlq writes, got %d", dlq.calls)
	}
}
