package ghclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestClientConcurrencyPermitRelease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	maxConcurrency := 2
	client := NewClient(server.Client(), maxConcurrency)

	n := 5
	var wg sync.WaitGroup
	errorsChan := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
			if err != nil {
				errorsChan <- err
				return
			}
			_, err = client.Do(req)
			errorsChan <- err
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("test timed out: requests blocked indefinitely, indicating a permit leak")
	}

	close(errorsChan)
	for err := range errorsChan {
		if err == nil {
			t.Error("expected error for non-2xx response, got nil")
		}
	}

	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	successClient := NewClient(successServer.Client(), maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
		_, err := successClient.Do(req)
		if err == nil {
			t.Fatal("expected error from 500 server")
		}
	}

	req, err := http.NewRequestWithContext(context.Background(), "GET", successServer.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	respChan := make(chan *http.Response, 1)
	errChan := make(chan error, 1)
	go func() {
		resp, err := successClient.Do(req)
		if err != nil {
			errChan <- err
		} else {
			respChan <- resp
		}
	}()

	select {
	case resp := <-respChan:
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
	case err := <-errChan:
		t.Fatalf("failed to dispatch N+1 request: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("N+1 request blocked, indicating permits were not fully reclaimed")
	}
}
