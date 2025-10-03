package prom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	base string
	http *http.Client
}

func New(base string) *Client {
	if base == "" {
		base = "http://localhost:9090"
	}
	return &Client{
		base: base,
		http: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   1 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *Client) Query(ctx context.Context, expr string) (float64, error) {
	u, _ := url.Parse(c.base)
	u.Path = "/api/v1/query"
	q := u.Query()
	q.Set("query", expr)
	u.RawQuery = q.Encode()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus: status %d", resp.StatusCode)
	}

	var x struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&x); err != nil {
		return 0, err
	}

	if x.Status != "success" {
		return 0, errors.New("prometheus: not success")
	}

	if len(x.Data.Result) == 0 || len(x.Data.Result[0].Value) < 2 {
		return 0, nil
	}

	str, ok := x.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, errors.New("prometheus: bad value")
	}
	var f float64
	_, err = fmt.Sscanf(str, "%f", &f)
	return f, err
}
