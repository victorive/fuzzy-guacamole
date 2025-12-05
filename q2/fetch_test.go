package q2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchAllSuccess(t *testing.T) {
	servers := make([]*httptest.Server, 0, 3)
	urls := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		i := i
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintf(w, "body-%d", i)
		}))
		servers = append(servers, srv)
		urls = append(urls, srv.URL)
	}
	defer func() {
		for _, srv := range servers {
			srv.Close()
		}
	}()

	results, err := FetchAll(context.Background(), urls, FetchOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatalf("FetchAll: %v", err)
	}
	if len(results) != len(urls) {
		t.Fatalf("results = %d, want %d", len(results), len(urls))
	}
	for i, result := range results {
		if result.Err != nil {
			t.Fatalf("result %d: %v", i, result.Err)
		}
		if result.StatusCode != http.StatusOK {
			t.Fatalf("result %d status = %d", i, result.StatusCode)
		}
		if got, want := string(result.Body), fmt.Sprintf("body-%d", i); got != want {
			t.Fatalf("result %d body = %q, want %q", i, got, want)
		}
	}
}

func TestFetchAllPartialFailure(t *testing.T) {
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer okServer.Close()

	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer badServer.Close()

	results, err := FetchAll(context.Background(), []string{okServer.URL, badServer.URL}, FetchOptions{})
	if err == nil {
		t.Fatal("expected an aggregate error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error = %v, want *HTTPStatusError", err)
	}
	if statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d", statusErr.StatusCode)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	if results[0].Err != nil || string(results[0].Body) != "ok" {
		t.Fatalf("successful result lost: %+v", results[0])
	}
	if results[1].Err == nil {
		t.Fatal("failing result should carry an error")
	}
}

func TestFetchAllPerCallTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte("late"))
	}))
	defer srv.Close()

	start := time.Now()
	results, err := FetchAll(context.Background(), []string{srv.URL}, FetchOptions{
		PerCallTimeout: 30 * time.Millisecond,
		OverallTimeout: time.Second,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("FetchAll took %v, want per-call timeout", elapsed)
	}
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("results = %+v", results)
	}
}

func TestFetchAllOverallTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte("late"))
	}))
	defer srv.Close()

	start := time.Now()
	_, err := FetchAll(context.Background(), []string{srv.URL}, FetchOptions{
		PerCallTimeout: time.Second,
		OverallTimeout: 30 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("FetchAll took %v, want overall timeout", elapsed)
	}
}

func TestFetchAllFailFast(t *testing.T) {
	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer badServer.Close()

	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		_, _ = w.Write([]byte("slow"))
	}))
	defer slowServer.Close()

	start := time.Now()
	_, err := FetchAll(context.Background(), []string{badServer.URL, slowServer.URL}, FetchOptions{
		MaxConcurrency: 2,
		PerCallTimeout: time.Second,
		OverallTimeout: 2 * time.Second,
		FailFast:       true,
	})
	if !errors.Is(err, ErrFailFast) {
		t.Fatalf("error = %v, want ErrFailFast", err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("fail-fast took %v, want cancellation of the slow request", elapsed)
	}
}

func TestFetchAllConcurrencyLimit(t *testing.T) {
	var inFlight atomic.Int64
	var maxInFlight atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		current := inFlight.Add(1)
		for {
			max := maxInFlight.Load()
			if current <= max || maxInFlight.CompareAndSwap(max, current) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		inFlight.Add(-1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	urls := make([]string, 20)
	for i := range urls {
		urls[i] = srv.URL
	}
	if _, err := FetchAll(context.Background(), urls, FetchOptions{MaxConcurrency: 3}); err != nil {
		t.Fatalf("FetchAll: %v", err)
	}
	if got := maxInFlight.Load(); got > 3 {
		t.Fatalf("max in flight = %d, want <= 3", got)
	} else if got < 2 {
		t.Fatalf("max in flight = %d, want real concurrency", got)
	}
}

func TestFetchAllBodyLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("012345678901234567890123456789"))
	}))
	defer srv.Close()

	_, err := FetchAll(context.Background(), []string{srv.URL}, FetchOptions{MaxBodyBytes: 10})
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("error = %v, want ErrBodyTooLarge", err)
	}
}

func TestFetchAllContextValues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Request-ID") != "req-123" {
			http.Error(w, "missing request id", http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-Trace-ID") != "trace-456" {
			http.Error(w, "missing trace id", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	ctx := WithRequestID(context.Background(), "req-123")
	ctx = WithTraceID(ctx, "trace-456")
	results, err := FetchAll(ctx, []string{srv.URL}, FetchOptions{})
	if err != nil {
		t.Fatalf("FetchAll: %v", err)
	}
	if string(results[0].Body) != "ok" {
		t.Fatalf("body = %q", results[0].Body)
	}
}

func TestFetchAllCallerCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte("late"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := FetchAll(ctx, []string{srv.URL}, FetchOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("results = %+v", results)
	}
}

func TestFetchAllEmptyAndNilContext(t *testing.T) {
	results, err := FetchAll(context.Background(), nil, FetchOptions{})
	if err != nil {
		t.Fatalf("empty FetchAll: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("results = %d, want 0", len(results))
	}

	if _, err := FetchAll(nil, []string{"http://example.com"}, FetchOptions{}); err == nil {
		t.Fatal("expected nil-context error")
	}
}

func TestFetchAllInvalidURL(t *testing.T) {
	results, err := FetchAll(context.Background(), []string{"://not-a-url"}, FetchOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("results = %+v", results)
	}
}

func TestFetchAllStatusErrorBodyIsBounded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	results, err := FetchAll(context.Background(), []string{srv.URL}, FetchOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error = %v", err)
	}
	if statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", statusErr.StatusCode)
	}
	if string(statusErr.Body) != "not found" {
		t.Fatalf("body = %q", statusErr.Body)
	}
	if results[0].StatusCode != http.StatusNotFound {
		t.Fatalf("result status = %d", results[0].StatusCode)
	}
}

func TestFetchAllFailFastSingleURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := FetchAll(context.Background(), []string{srv.URL}, FetchOptions{FailFast: true})
	if !errors.Is(err, ErrFailFast) {
		t.Fatalf("error = %v, want ErrFailFast", err)
	}
}
