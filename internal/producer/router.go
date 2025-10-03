package producer

import (
	"github.com/fr03e1/boltstream/internal/platform/kafkax"
	"github.com/fr03e1/boltstream/internal/platform/metrics"
	"github.com/fr03e1/boltstream/internal/platform/readiness"
	"github.com/fr03e1/boltstream/internal/platform/static"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"net/http"
)

func NewRouter(st *State, g *readiness.Gate, ping kafkax.Pinger, mrg *Manager) http.Handler {
	h := NewHandler(st, g, ping, mrg)
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer)

	r.Use(metrics.HTTPMiddleware)

	r.Mount("/debug", middleware.Profiler())

	r.Get("/healthz", h.healthz)
	r.Get("/readyz", h.readyz)
	r.Get("/kafka/health", h.kafkaHealth)
	r.Method(http.MethodGet, "/metrics", metrics.Handler())

	r.Get("/", func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "/ui/", http.StatusFound)
	})
	r.Handle("/ui/*", http.StripPrefix("/ui/", static.Handler()))

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/status", h.status)
		r.Get("/stats", h.stats)
		r.Post("/start", h.start)
		r.Post("/stop", h.stop)

		r.Post("/emit", h.emit)
	})

	return r
}
