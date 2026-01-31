package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	producedMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "producer_messages_total",
			Help: "Total messages successfully produced",
		},
		[]string{"topic"},
	)
	produceErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "producer_errors_total",
			Help: "Number of produce errors",
		},
		[]string{"topic"},
	)
	produceBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "producer_batch_size",
			Help:    "Number of messages per flush",
			Buckets: []float64{1, 10, 50, 100, 250, 500, 1000},
		},
		[]string{"topic"},
	)
	produceFlushLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "producer_flush_latency_seconds",
			Help:    "Flush latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic"},
	)
	produceRetryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "producer",
			Name:      "write_retries_total",
			Help:      "Total number of producer write retries",
		},
		[]string{"topic"},
	)
	produceDropTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "producer",
			Name:      "write_drops_total",
			Help:      "Total number of producer write drops after retries",
		},
		[]string{"topic"},
	)
)

func RegisterProducer() {
	prometheus.MustRegister(
		producedMessagesTotal,
		produceErrorsTotal,
		produceBatchSize,
		produceFlushLatency,
		produceRetryTotal,
		produceDropTotal,
	)
}

func ProducedAdd(topic string, n int) {
	if n > 0 {
		producedMessagesTotal.WithLabelValues(topic).Add(float64(n))
	}
}

func ProduceErrorInc(topic string) {
	produceErrorsTotal.WithLabelValues(topic).Inc()
}

func ObserveBatchSize(topic string, size int) {
	if size > 0 {
		produceBatchSize.WithLabelValues(topic).Observe(float64(size))
	}
}

func ObserveFlushLatency(topic string, d time.Duration) {
	produceFlushLatency.WithLabelValues(topic).Observe(d.Seconds())
}

func RetryTotalAdd(topic string, n int) {
	produceRetryTotal.WithLabelValues(topic).Add(float64(n))
}

func RetryTotalInc(topic string) {
	produceRetryTotal.WithLabelValues(topic).Inc()
}

func RetryDropTotalInc(topic string) {
	produceDropTotal.WithLabelValues(topic).Inc()
}
