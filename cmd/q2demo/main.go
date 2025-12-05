package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"test/q2"
)

func main() {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "fast response for %s", r.Header.Get("X-Request-ID"))
	}))
	defer fast.Close()

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("slow response"))
	}))
	defer slow.Close()

	ctx := q2.WithRequestID(context.Background(), "demo-request-1")
	ctx = q2.WithTraceID(ctx, "demo-trace-1")

	results, err := q2.FetchAll(ctx, []string{fast.URL, slow.URL}, q2.FetchOptions{
		MaxConcurrency: 2,
		PerCallTimeout: 50 * time.Millisecond,
		OverallTimeout: time.Second,
		MaxBodyBytes:   1 << 20,
	})
	for _, result := range results {
		fmt.Printf("%s -> status=%d body=%q err=%v\n", result.URL, result.StatusCode, result.Body, result.Err)
	}
	fmt.Printf("aggregate error: %v\n", err)
}
