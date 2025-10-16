package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ConsumerReadTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumer_read_total",
			Help: "Total messages successfully consumed and committed",
		},
		[]string{"topic"},
	)

	ConsumerCommitTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumer_commit_total",
			Help: "Total successful offset commits",
		},
		[]string{"topic"},
	)

	ConsumerErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumer_errors_total",
			Help: "Total errors by stage (fetch/decode/process/commit/dlq)",
		},
		[]string{"stage", "topic"},
	)

	ConsumerProcessingSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "consumer_processing_seconds",
			Help:    "Message processing latency",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic"},
	)

	ConsumerBatchBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "consumer_batch_bytes",
			Help:    "Message payload size in bytes",
			Buckets: []float64{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576},
		},
		[]string{"topic"},
	)

	ConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "consumer_lag",
			Help: "Current consumer lag (sum or per partition)",
		},
		[]string{"topic", "partition"},
	)

	ConsumerUp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "consumer_up",
			Help: "Is consumer loop running (1) or stopped (0)",
		},
	)
)

func RegisterConsumer() {
	prometheus.MustRegister(
		ConsumerReadTotal,
		ConsumerCommitTotal,
		ConsumerErrorsTotal,
		ConsumerProcessingSeconds,
		ConsumerBatchBytes,
		ConsumerLag,
		ConsumerUp,
	)
}

func ConsumerAdd(topic string, n int) {
	ConsumerReadTotal.WithLabelValues(topic).Add(float64(n))
}

func ConsumerCommitAdd(topic string, n int) {
	ConsumerCommitTotal.WithLabelValues(topic).Add(float64(n))
}

func ConsumerErrorAdd(stage, topic string, n int) {
	ConsumerErrorsTotal.WithLabelValues(stage, topic).Add(float64(n))
}

func ObserveProcessing(topic string, seconds float64) {
	ConsumerProcessingSeconds.WithLabelValues(topic).Observe(seconds)
}

func ObserveBatchBytes(topic string, size int) {
	ConsumerBatchBytes.WithLabelValues(topic).Observe(float64(size))
}

func ConsumerLagSet(topic, partition string, lag float64) {
	ConsumerLag.WithLabelValues(topic, partition).Set(lag)
}
