package ghclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientConcurrencyPermitRelease(t *testing.T) {
	// Create a mock server that returns 500 Internal Server Error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// Set concurrency limit to 2
	client := NewClient(server.Client(), 2)

	// Fire 3 requests. If permits leak, the 3rd request will block indefinitely.
	for i := 0; i < 3; i++ {
		req, err := http.NewRequest("GET", server.URL, nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		// Use a channel to detect timeout/blocking
		done := make(chan struct{})
		var respErr error
		go func() {
			_, respErr = client.Do(req)
			close(done)
		}()

		select {
		case <-done:
			if respErr == nil {
				t.Error("expected error for 500 status code, got nil")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("request %d blocked indefinitely (permit leak)", i+1)
		}
	}
}
