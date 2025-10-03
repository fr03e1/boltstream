package producer

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/fr03e1/boltstream/internal/platform/kafkax"
	"github.com/fr03e1/boltstream/internal/platform/metrics"
	"github.com/fr03e1/boltstream/internal/platform/prom"
	"github.com/fr03e1/boltstream/internal/platform/readiness"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"net/http"
	"os"
	"time"
)

type Handler struct {
	st   *State
	gate *readiness.Gate
	ping kafkax.Pinger
	mng  *Manager
}

type KafkaHealthResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type emitRequest struct {
	EventID string          `json:"event_id,omitempty"`
	UserID  string          `json:"user_id,omitempty"`
	TS      string          `json:"ts,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

func NewHandler(st *State, gate *readiness.Gate, ping kafkax.Pinger, mng *Manager) *Handler {
	return &Handler{st: st, gate: gate, ping: ping, mng: mng}
}

func (h *Handler) status(w http.ResponseWriter, _ *http.Request) {
	h.st.mu.RLock()
	uptimeSec := int64(time.Since(h.st.startedAt).Seconds())
	h.st.mu.RUnlock()

	resp := Status{
		Running:       h.mng.running.Load(),
		RunID:         h.mng.RunID(),
		RPSTarget:     int(h.mng.rpsTarget.Load()),
		RPSActual:     h.mng.RPSActual(),
		ProducedTotal: h.mng.ProducedTotal(),
		UptimeSec:     uptimeSec,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) readyz(w http.ResponseWriter, _ *http.Request) {
	if !h.gate.Ready() {
		writeErr(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) kafkaHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.ping.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, KafkaHealthResp{OK: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, KafkaHealthResp{OK: true})
}

func (h *Handler) start(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req struct {
		RPS int `json:"rps"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.RPS < 1 || req.RPS > 5000 {
		writeErr(w, http.StatusBadRequest, "rps must be between 1 and 5000")
		return
	}

	if !h.gate.Ready() {
		writeErr(w, http.StatusServiceUnavailable, "service not ready")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.ping.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "kafka unavailable: "+err.Error())
		return
	}

	runID, err := h.mng.Start(req.RPS)
	if err != nil {
		if errors.Is(err, ErrAlreadyRunning) {
			writeErr(w, http.StatusConflict, "already running")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, struct {
		RunID     string    `json:"run_id"`
		RPS       int       `json:"rps"`
		StartedAt time.Time `json:"started_at"`
	}{
		RunID:     runID,
		RPS:       req.RPS,
		StartedAt: h.mng.StartedAt(),
	})
}

func (h *Handler) stop(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	runID := h.mng.RunID()
	if err := h.mng.Stop(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeErr(w, http.StatusGatewayTimeout, "stop timeout")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, struct {
		Stopped       bool   `json:"stopped"`
		RunID         string `json:"run_id"`
		ProducedTotal int64  `json:"produced_total"`
	}{
		Stopped:       true,
		RunID:         runID,
		ProducedTotal: h.mng.ProducedTotal(),
	})
}

func (h *Handler) emit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req emitRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(&req); err != nil {
		metrics.IngestRejectedTotal.Inc()
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.EventID == "" {
		req.EventID = uuid.NewString()
	}
	if req.UserID == "" {
		req.UserID = "u-anon"
	}
	if req.TS == "" {
		req.TS = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		req.Payload = json.RawMessage(`{}`)
	}

	val, _ := json.Marshal(struct {
		EventID string          `json:"event_id"`
		UserID  string          `json:"user_id"`
		TS      string          `json:"ts"`
		Payload json.RawMessage `json:"payload"`
	}{req.EventID, req.UserID, req.TS, req.Payload})

	msg := kafka.Message{
		Key:   []byte(req.UserID),
		Value: val,
		Time:  time.Now(),
	}

	if err := h.mng.Enqueue(msg); err != nil {
		if errors.Is(err, ErrBackpressure) {
			writeErr(w, http.StatusTooManyRequests, "queue is full, try later")
			return
		}
		writeErr(w, http.StatusInternalServerError, "enqueue failed")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"event_id": req.EventID,
		"queued":   true,
	})
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	base := os.Getenv("PROMETHEUS_URL")
	client := prom.New(base)

	ctx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
	defer cancel()

	const (
		qRPS     = `sum(rate(producer_messages_total[1m]))`
		qP95ms   = `1000 * histogram_quantile(0.95, sum(rate(producer_flush_latency_seconds_bucket[5m])) by (le))`
		qBatch50 = `histogram_quantile(0.5, sum(rate(producer_batch_size_bucket[5m])) by (le))`
		qErrs    = `sum(rate(producer_errors_total[5m])) or 0`
		qQueue   = `max(ingest_queue_depth) or 0`
	)

	rps, err := client.Query(ctx, qRPS)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "prometheus unavailable")
		return
	}
	p95ms, _ := client.Query(ctx, qP95ms)
	batch50, _ := client.Query(ctx, qBatch50)
	errs, _ := client.Query(ctx, qErrs)
	queue, _ := client.Query(ctx, qQueue)

	writeJSON(w, http.StatusOK, map[string]any{
		"rps":         rps,
		"p95_ms":      p95ms,
		"batch_p50":   batch50,
		"errors_s":    errs,
		"queue_depth": queue,
		"source":      "prometheus",
		"ts":          time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}
