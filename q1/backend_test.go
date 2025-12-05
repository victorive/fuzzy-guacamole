package q1

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryBackendGetSetDeleteAndTTL(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(0, 0)
	mem := NewMemoryBackendWithClock(func() time.Time { return now }, 0)

	if err := mem.Set(ctx, "k", []byte("value"), 10*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := mem.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("Get = %q, want value", got)
	}

	// Get must return a copy; mutating it must not corrupt the cache.
	got[0] = 'X'
	again, _ := mem.Get(ctx, "k")
	if string(again) != "value" {
		t.Fatalf("Get returned shared memory: %q", again)
	}

	now = now.Add(11 * time.Second)
	if _, err := mem.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired Get error = %v, want ErrNotFound", err)
	}
	if mem.Len() != 0 {
		t.Fatalf("Len = %d, want 0", mem.Len())
	}

	if err := mem.Set(ctx, "k", []byte("value"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := mem.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := mem.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after Delete Get error = %v, want ErrNotFound", err)
	}
}

func TestHTTPBackendRoundTrip(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryBackend()
	srv := httptest.NewServer(mem)
	defer srv.Close()

	backend, err := NewHTTPBackend(HTTPBackendConfig{Name: "node-a", BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("NewHTTPBackend: %v", err)
	}
	defer backend.Close()

	if err := backend.Set(ctx, "k", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := backend.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("Get = %q, want value", got)
	}

	if err := backend.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := backend.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete error = %v, want ErrNotFound", err)
	}
}

func TestHTTPBackendReusesConnections(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryBackend()
	var newConns atomic.Int64

	srv := httptest.NewUnstartedServer(mem)
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	backend, err := NewHTTPBackend(HTTPBackendConfig{
		Name:    "node-a",
		BaseURL: srv.URL,
		Client:  &http.Client{Transport: NewTransport()},
	})
	if err != nil {
		t.Fatalf("NewHTTPBackend: %v", err)
	}
	defer backend.Close()

	if err := backend.Set(ctx, "k", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := backend.Get(ctx, "k"); err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
	}

	if got := newConns.Load(); got != 1 {
		t.Fatalf("new connections = %d, want 1 (connection pooling failed)", got)
	}
}

func TestHTTPBackendClassifiesErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("5xx is retryable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer srv.Close()

		backend, err := NewHTTPBackend(HTTPBackendConfig{Name: "node-a", BaseURL: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		_, err = backend.Get(ctx, "k")
		var backendErr *BackendError
		if !errors.As(err, &backendErr) {
			t.Fatalf("error = %v, want *BackendError", err)
		}
		if backendErr.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d", backendErr.StatusCode)
		}
		if !backendErr.Retryable() {
			t.Fatal("5xx should be retryable")
		}
	})

	t.Run("4xx is not retryable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "bad request", http.StatusBadRequest)
		}))
		defer srv.Close()

		backend, err := NewHTTPBackend(HTTPBackendConfig{Name: "node-a", BaseURL: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		_, err = backend.Get(ctx, "k")
		var backendErr *BackendError
		if !errors.As(err, &backendErr) {
			t.Fatalf("error = %v, want *BackendError", err)
		}
		if backendErr.Retryable() {
			t.Fatal("4xx should not be retryable")
		}
	})

	t.Run("body limit", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("123456789"))
		}))
		defer srv.Close()

		backend, err := NewHTTPBackend(HTTPBackendConfig{
			Name:         "node-a",
			BaseURL:      srv.URL,
			MaxBodyBytes: 4,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = backend.Get(ctx, "k")
		if !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("error = %v, want ErrBodyTooLarge", err)
		}
	})
}

func TestMemoryBackendRejectsOversizedPut(t *testing.T) {
	mem := NewMemoryBackend()
	body := bytes.Repeat([]byte("x"), maxCacheValueBytes+1)
	req := httptest.NewRequest(http.MethodPut, "/v1/cache?key=big", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	mem.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if mem.Len() != 0 {
		t.Fatalf("oversized value was stored (len = %d)", mem.Len())
	}
}
