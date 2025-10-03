package producer

import (
	"context"
	"errors"
	"fmt"
	"github.com/fr03e1/boltstream/internal/platform/kafkax"
	"github.com/fr03e1/boltstream/internal/platform/metrics"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var ErrAlreadyRunning = errors.New("producer: already running")
var ErrBackpressure = errors.New("backpressure: queue is full")

type Manager struct {
	w *kafkax.Writer

	running       atomic.Bool
	runID         atomic.Value // string
	rpsTarget     atomic.Int64
	producedTotal atomic.Int64
	rpsActual     atomic.Int64
	startedAt     atomic.Value //time.Time
	writerStarted atomic.Bool

	mu       sync.Mutex
	cancel   context.CancelFunc
	writerWG sync.WaitGroup
	genWG    sync.WaitGroup

	msg chan kafka.Message
}

func NewManager(w *kafkax.Writer, bufCap int) *Manager {
	if bufCap <= 0 {
		bufCap = 10_000
	}
	return &Manager{
		w:   w,
		msg: make(chan kafka.Message, bufCap),
	}
}

func (m *Manager) StartWriter(ctx context.Context) {
	if m.writerStarted.Swap(true) {
		return
	}

	m.writerWG.Add(1)

	go func() {
		defer m.writerWG.Done()
		defer m.writerStarted.Store(false)
		log.Println("writer.loop.start")
		m.writer(ctx)
		log.Println("writer.loop.stop")
	}()
}

func (m *Manager) Start(rps int) (string, error) {
	if rps <= 0 {
		return "", fmt.Errorf("invalid rps: %d", rps)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running.Load() {
		return "", ErrAlreadyRunning
	}

	runID := uuid.NewString()
	m.runID.Store(runID)
	m.rpsTarget.Store(int64(rps))
	m.producedTotal.Store(0)
	m.rpsActual.Store(0)
	m.startedAt.Store(time.Now())

	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.running.Store(true)

	m.genWG.Add(1)
	go m.generator(runCtx, rps)

	return runID, nil
}

func (m *Manager) Stop(wait context.Context) error {
	m.mu.Lock()
	if !m.running.Load() {
		m.mu.Unlock()
		return nil
	}
	cancel := m.cancel
	m.cancel = nil
	m.running.Store(false)
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() { defer close(done); m.genWG.Wait() }()

	select {
	case <-done:
		return nil
	case <-wait.Done():
		return wait.Err()
	}
}

func (m *Manager) generator(ctx context.Context, rps int) {
	defer m.genWG.Done()

	lim := rate.NewLimiter(rate.Limit(rps), rps)
	var i int64

	for {
		if err := lim.Wait(ctx); err != nil {
			return
		}

		userID := fmt.Sprintf("u-%06d", i%100000)
		i++

		val := []byte(fmt.Sprintf(`{"event_id":"%s","user_id":"%s","ts":"%s","payload":{"kind":"arrow","power":1}}`,
			uuid.NewString(), userID, time.Now().UTC().Format(time.RFC3339Nano)))

		msg := kafka.Message{
			Key:   []byte(userID),
			Value: val,
			Time:  time.Now(),
		}

		select {
		case <-ctx.Done():
			return
		case m.msg <- msg:
		}
	}
}

func (m *Manager) writer(ctx context.Context) {
	const flushEvery = 50 * time.Millisecond
	const maxBatch = 1000

	t := time.NewTimer(flushEvery)
	defer t.Stop()

	lastFlush := time.Now()
	tick1s := time.NewTicker(time.Second)
	defer tick1s.Stop()
	var secCount int64

	batch := make([]kafka.Message, 0, maxBatch)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		start := time.Now()

		if err := m.w.W().WriteMessages(ctx, batch...); err != nil {
			log.Printf("kafka write failed (batch=%d): %v", len(batch), err)
			metrics.ProduceErrorInc(m.w.Topic())
		} else {
			n := int64(len(batch))
			m.producedTotal.Add(n)
			secCount += n

			metrics.ProducedAdd(m.w.Topic(), int(n))
			metrics.ObserveBatchSize(m.w.Topic(), int(n))
			metrics.ObserveFlushLatency(m.w.Topic(), time.Since(start))
		}

		batch = batch[:0]
		_ = t.Stop()
		t.Reset(flushEvery)
		lastFlush = time.Now()
	}

	drain := func() {
		for {
			select {
			case msg, ok := <-m.msg:
				if !ok {
					flush()
					return
				}
				batch = append(batch, msg)
				if len(batch) >= maxBatch {
					flush()
				}
			default:
				flush()
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			drain()
			return
		case msg, ok := <-m.msg:
			if !ok {
				flush()
				return
			}
			batch = append(batch, msg)
			if len(batch) >= maxBatch {
				flush()
			}
			if time.Since(lastFlush) >= flushEvery {
				flush()
			}
		case <-t.C:
			flush()
		case <-tick1s.C:
			m.rpsActual.Store(secCount)
			secCount = 0

			if time.Since(lastFlush) >= flushEvery {
				flush()
			}
		}
	}
}

func (m *Manager) Enqueue(msg kafka.Message) error {
	select {
	case m.msg <- msg:
		metrics.IngestAcceptedTotal.Inc()
	default:
		metrics.IngestRejectedTotal.Inc()
		return ErrBackpressure
	}

	metrics.IngestQueueDepth.Set(float64(len(m.msg)))
	return nil
}

func (m *Manager) StartedAt() time.Time {
	if v := m.startedAt.Load(); v != nil {
		if t, ok := v.(time.Time); ok {
			return t
		}
	}
	return time.Time{}
}

func (m *Manager) RunID() string {
	if v := m.runID.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (m *Manager) ProducedTotal() int64 { return m.producedTotal.Load() }
func (m *Manager) RPSActual() int       { return int(m.rpsActual.Load()) }
