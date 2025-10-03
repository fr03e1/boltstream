package static

import "net/http"

func Handler() http.Handler {
	return http.FileServer(http.Dir("./ui"))
}
