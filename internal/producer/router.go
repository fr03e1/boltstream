package producer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

type KafkaHealthResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}
