package producer

import (
	"github.com/fr03e1/boltstream/internal/platform/kafkax"
	"github.com/fr03e1/boltstream/internal/platform/metrics"
	"github.com/fr03e1/boltstream/internal/platform/readiness"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"net/http"
	"time"
)

type Handler struct {
	st   *State
	gate *readiness.Gate
	ping kafkax.Pinger
	mng  *Manager
}

func NewHandler(st *State, gate *readiness.Gate, ping kafkax.Pinger, mng *Manager) *Handler {
	return &Handler{st: st, gate: gate, ping: ping, mng: mng}
}

func NewRouter(st *State, g *readiness.Gate, ping kafkax.Pinger, mrg *Manager) http.Handler {
	h := NewHandler(st, g, ping, mrg)
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer)

	r.Get("/healthz", h.healthz)
	r.Get("/readyz", h.readyz)
	r.Method(http.MethodGet, "/metrics", metrics.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/status", h.status)
		r.Route("/kafka", func(r chi.Router) {
			r.Get("/health", h.kafkaHealth)
		})
		r.Post("/start", h.start)
		r.Post("/stop", h.stop)
	})

	return r
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
