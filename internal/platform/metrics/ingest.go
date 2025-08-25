package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	IngestAcceptedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ingest_accepted_total",
			Help: "Total number of accepted ingest requests (enqueued).",
		},
	)
	IngestRejectedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ingest_rejected_total",
			Help: "Total number of rejected ingest requests (backpressure or bad request).",
		},
	)
	IngestQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ingest_queue_depth",
			Help: "Current in-memory queue depth (pending messages before flush).",
		},
	)
)

func RegisterIngest() {
	prometheus.MustRegister(
		IngestAcceptedTotal,
		IngestRejectedTotal,
		IngestQueueDepth,
	)
}
