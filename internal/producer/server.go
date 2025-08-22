package producer

import (
	"encoding/json"
	"github.com/fr03e1/boltstream/internal/platform/metrics"
	"github.com/fr03e1/boltstream/internal/platform/readiness"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"net/http"
)

func NewRouter(st *State, g *readiness.Gate) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})

	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !g.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	r.Get("/status", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(st.Snapshot())
	})

	r.Method(http.MethodGet, "/metrics", metrics.Handler())

	return r
}
