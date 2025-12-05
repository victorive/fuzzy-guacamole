package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"time"

	"test/q1"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	metrics := q1.NewMemoryMetrics()

	servers := make(map[string]*httptest.Server, 3)
	backends := make([]q1.BackendConfig, 0, 3)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("node-%d", i)
		srv := httptest.NewServer(q1.NewMemoryBackend())
		servers[name] = srv
		backends = append(backends, q1.BackendConfig{
			Name:    name,
			BaseURL: srv.URL,
		})
	}
	defer func() {
		for _, srv := range servers {
			if srv != nil {
				srv.Close()
			}
		}
	}()

	cfg := q1.DefaultConfig()
	cfg.Backends = backends
	cfg.ReplicationFactor = 3
	cfg.WriteReplicas = true
	cfg.Logger = logger
	cfg.Metrics = metrics
	cfg.Breaker.FailureThreshold = 2
	cfg.Breaker.OpenTimeout = 50 * time.Millisecond

	client, err := q1.New(cfg)
	if err != nil {
		logger.Error("create client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Set(ctx, "user:1", []byte("alice"), time.Minute); err != nil {
		logger.Error("set", "error", err)
		os.Exit(1)
	}
	value, err := client.Get(ctx, "user:1")
	if err != nil {
		logger.Error("get", "error", err)
		os.Exit(1)
	}
	fmt.Printf("get: %s\n", value)

	candidates := client.BackendsForKey("user:1")
	primary := candidates[0]
	if srv := servers[primary]; srv != nil {
		srv.Close()
		servers[primary] = nil
	}
	fmt.Printf("stopped primary %s; candidates were %v\n", primary, candidates)

	value, err = client.Get(ctx, "user:1")
	if err != nil {
		logger.Error("get after failover", "error", err)
		os.Exit(1)
	}
	fmt.Printf("get after failover: %s\n", value)
	fmt.Printf("failovers for %s: %.0f\n",
		primary,
		metrics.Counter(q1.MetricFailovers, q1.Labels{"op": "get", "backend": primary}),
	)

	result := <-client.GetAsync("user:1")
	if result.Error != nil {
		logger.Error("async get", "error", result.Error)
		os.Exit(1)
	}
	fmt.Printf("async get: %s\n", result.Value)
}
