package ghclient

import (
	"context"
	"fmt"
	"net/http"
)

type Client struct {
	httpClient *http.Client
	semaphore  chan struct{}
}

func NewClient(httpClient *http.Client, maxConcurrency int) *Client {
	return &Client{
		httpClient: httpClient,
		semaphore:  make(chan struct{}, maxConcurrency),
	}
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	select {
	case c.semaphore <- struct{}{}:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	defer func() {
		<-c.semaphore
	}()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return resp, nil
}
