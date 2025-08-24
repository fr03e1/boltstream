package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"strconv"
	"time"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests by method/path/status",
		},
		[]string{"method", "path", "code"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func RegisterHTTP() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration)
}

func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		sw := &statusWriter{ResponseWriter: writer}
		start := time.Now()

		next.ServeHTTP(sw, request)

		status := sw.status

		if status == 0 {
			status = http.StatusOK
		}

		path := request.URL.Path
		httpRequestsTotal.WithLabelValues(request.Method, path, strconv.Itoa(status)).Inc()
		httpRequestDuration.WithLabelValues(request.Method, path).Observe(time.Since(start).Seconds())
	})
}
