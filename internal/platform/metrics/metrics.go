package metrics

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func Handler() http.Handler {
	return promhttp.Handler()
}
