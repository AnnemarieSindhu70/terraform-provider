package ghclient

import (
	"fmt"
	"net/http"
)

// Client wraps an http.Client with a concurrency limiter.
type Client struct {
	httpClient *http.Client
	semaphore  chan struct{}
}

// NewClient creates a new Client with the specified concurrency limit.
func NewClient(httpClient *http.Client, maxConcurrency int) *Client {
	return &Client{
		httpClient: httpClient,
		semaphore:  make(chan struct{}, maxConcurrency),
	}
}

// Do executes the HTTP request, respecting the concurrency limit.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	c.semaphore <- struct{}{}
	defer func() {
		<-c.semaphore
	}()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("api error: %s", resp.Status)
	}

	return resp, nil
}
