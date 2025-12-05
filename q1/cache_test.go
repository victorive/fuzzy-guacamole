package q1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, backends []Backend, mutate func(*Config)) (*Client, *MemoryMetrics) {
	t.Helper()
	metrics := NewMemoryMetrics()
	cfg := DefaultConfig()
	cfg.Backends = make([]BackendConfig, len(backends))
	for i, backend := range backends {
		cfg.Backends[i] = BackendConfig{
			Name:    "node-" + string(rune('a'+i)),
			Backend: backend,
		}
	}
	cfg.Metrics = metrics
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.OperationTimeout = 500 * time.Millisecond
	cfg.AsyncTimeout = 2 * time.Second
	cfg.Breaker = BreakerConfig{
		FailureThreshold: 100,
		SuccessThreshold: 1,
		OpenTimeout:      time.Hour,
		HalfOpenMax:      1,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client, metrics
}

type flakyBackend struct {
	Backend
	fail atomic.Bool
}

func (f *flakyBackend) Get(ctx context.Context, key string) ([]byte, error) {
	if f.fail.Load() {
		return nil, &BackendError{Backend: f.Name(), Op: "get", StatusCode: 500, Err: errors.New("backend down")}
	}
	return f.Backend.Get(ctx, key)
}

func (f *flakyBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if f.fail.Load() {
		return &BackendError{Backend: f.Name(), Op: "set", StatusCode: 500, Err: errors.New("backend down")}
	}
	return f.Backend.Set(ctx, key, value, ttl)
}

func (f *flakyBackend) Delete(ctx context.Context, key string) error {
	if f.fail.Load() {
		return &BackendError{Backend: f.Name(), Op: "delete", StatusCode: 500, Err: errors.New("backend down")}
	}
	return f.Backend.Delete(ctx, key)
}

type blockingBackend struct {
	Backend
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingBackend(inner Backend) *blockingBackend {
	return &blockingBackend{
		Backend: inner,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingBackend) Get(ctx context.Context, key string) ([]byte, error) {
	b.startedOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
		return b.Backend.Get(ctx, key)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *blockingBackend) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func TestClientGetSetDelete(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	ctx := context.Background()

	if err := client.Set(ctx, "user:1", []byte("alice"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	value, err := client.Get(ctx, "user:1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(value) != "alice" {
		t.Fatalf("Get = %q, want alice", value)
	}

	if err := client.Delete(ctx, "user:1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = client.Get(ctx, "user:1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete error = %v, want ErrNotFound", err)
	}
	if got := KindOf(err); got != KindNotFound {
		t.Fatalf("KindOf = %v, want not_found", got)
	}
}

func TestClientGetFailover(t *testing.T) {
	backends := []*flakyBackend{
		{Backend: NewMemoryBackend()},
		{Backend: NewMemoryBackend()},
		{Backend: NewMemoryBackend()},
	}
	client, metrics := newTestClient(t, []Backend{backends[0], backends[1], backends[2]}, func(cfg *Config) {
		cfg.ReplicationFactor = 3
		cfg.WriteReplicas = true
	})
	ctx := context.Background()

	if err := client.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	candidates := client.BackendsForKey("key")
	if len(candidates) != 3 {
		t.Fatalf("candidates = %v, want 3", candidates)
	}
	primary := candidates[0]
	backends[indexForName(primary)].fail.Store(true)

	value, err := client.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get after primary failure: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("Get = %q, want value", value)
	}
	if got := metrics.Counter(MetricFailovers, Labels{"op": "get", "backend": primary}); got < 1 {
		t.Fatalf("failover metric = %v, want >= 1", got)
	}
}

func TestClientSetFailover(t *testing.T) {
	backends := []*flakyBackend{
		{Backend: NewMemoryBackend()},
		{Backend: NewMemoryBackend()},
	}
	client, _ := newTestClient(t, []Backend{backends[0], backends[1]}, func(cfg *Config) {
		cfg.ReplicationFactor = 2
		cfg.WriteReplicas = false
	})
	ctx := context.Background()

	candidates := client.BackendsForKey("key")
	primary := candidates[0]
	backends[indexForName(primary)].fail.Store(true)

	if err := client.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set with primary down: %v", err)
	}
	value, err := client.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get with primary down: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("Get = %q, want value", value)
	}
}

func TestClientCircuitBreakerFailsFast(t *testing.T) {
	backend := &flakyBackend{Backend: NewMemoryBackend()}
	client, metrics := newTestClient(t, []Backend{backend}, func(cfg *Config) {
		cfg.ReplicationFactor = 1
		cfg.Breaker.FailureThreshold = 2
		cfg.Breaker.OpenTimeout = time.Hour
	})
	ctx := context.Background()

	if err := client.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	backend.fail.Store(true)

	if _, err := client.Get(ctx, "key"); err == nil {
		t.Fatal("expected first failure")
	}
	if _, err := client.Get(ctx, "key"); err == nil {
		t.Fatal("expected second failure")
	}

	start := time.Now()
	_, err := client.Get(ctx, "key")
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("error = %v, want ErrCircuitOpen", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("circuit-open call took %v, want fail-fast", elapsed)
	}
	if got := KindOf(err); got != KindCircuitOpen {
		t.Fatalf("KindOf = %v, want circuit_open", got)
	}
	if got := metrics.Gauge(MetricBreakerState, Labels{"backend": "node-a"}); got != breakerOpen.metricValue() {
		t.Fatalf("breaker gauge = %v, want open", got)
	}
}

func TestClientAsync(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	ctx := context.Background()

	if err := client.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	result := <-client.GetAsync("key")
	if result.Error != nil {
		t.Fatalf("GetAsync: %v", result.Error)
	}
	if string(result.Value) != "value" {
		t.Fatalf("GetAsync = %q, want value", result.Value)
	}

	missing := <-client.GetAsync("missing")
	if !errors.Is(missing.Error, ErrNotFound) {
		t.Fatalf("missing GetAsync error = %v, want ErrNotFound", missing.Error)
	}
}

func TestClientAsyncQueueFull(t *testing.T) {
	blocking := newBlockingBackend(NewMemoryBackend())
	client, _ := newTestClient(t, []Backend{blocking}, func(cfg *Config) {
		cfg.AsyncWorkers = 1
		cfg.AsyncQueueSize = 1
		cfg.AsyncTimeout = 5 * time.Second
		cfg.OperationTimeout = 5 * time.Second
	})
	defer blocking.unblock()

	first := client.GetAsync("first")
	<-blocking.started
	second := client.GetAsync("second")
	third := client.GetAsync("third")

	result := <-third
	if !errors.Is(result.Error, ErrQueueFull) {
		t.Fatalf("third result error = %v, want ErrQueueFull", result.Error)
	}

	blocking.unblock()
	<-first
	<-second
}

func TestClientAsyncAfterClose(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	result := <-client.GetAsync("key")
	if !errors.Is(result.Error, ErrClosed) {
		t.Fatalf("GetAsync after Close error = %v, want ErrClosed", result.Error)
	}
}

func TestGetOrLoadSingleflight(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	ctx := context.Background()

	var loads atomic.Int64
	loader := func(ctx context.Context, key string) ([]byte, time.Duration, error) {
		loads.Add(1)
		time.Sleep(30 * time.Millisecond)
		return []byte("loaded"), time.Minute, nil
	}

	const goroutines = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	values := make([][]byte, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			values[i], errs[i] = client.GetOrLoad(ctx, "key", loader)
		}(i)
	}
	close(start)
	wg.Wait()

	if got := loads.Load(); got != 1 {
		t.Fatalf("loader called %d times, want 1", got)
	}
	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if string(values[i]) != "loaded" {
			t.Fatalf("goroutine %d value = %q", i, values[i])
		}
	}
}

func TestWarm(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	ctx := context.Background()

	var loads atomic.Int64
	loader := func(ctx context.Context, key string) ([]byte, time.Duration, error) {
		loads.Add(1)
		return []byte("value:" + key), time.Minute, nil
	}
	keys := []string{"a", "b", "c", "d", "e"}

	report, err := client.Warm(ctx, keys, loader, WarmOptions{Concurrency: 2, TTLJitter: 0.1})
	if err != nil {
		t.Fatalf("Warm: %v", err)
	}
	if report.Warmed != len(keys) || report.Failed != 0 {
		t.Fatalf("report = %+v", report)
	}
	for _, key := range keys {
		value, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if string(value) != "value:"+key {
			t.Fatalf("Get %s = %q", key, value)
		}
	}

	report, err = client.Warm(ctx, keys, loader, WarmOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("second Warm: %v", err)
	}
	if report.Skipped != len(keys) || report.Warmed != 0 {
		t.Fatalf("second report = %+v", report)
	}
	if got := loads.Load(); got != int64(len(keys)) {
		t.Fatalf("loads = %d, want %d", got, len(keys))
	}

	report, err = client.Warm(ctx, keys, loader, WarmOptions{Concurrency: 2, Force: true})
	if err != nil {
		t.Fatalf("forced Warm: %v", err)
	}
	if report.Warmed != len(keys) {
		t.Fatalf("forced report = %+v", report)
	}
	if got := loads.Load(); got != int64(2*len(keys)) {
		t.Fatalf("loads after force = %d, want %d", got, 2*len(keys))
	}
}

func TestClientMetrics(t *testing.T) {
	client, metrics := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	ctx := context.Background()

	if _, err := client.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get error = %v", err)
	}
	if err := client.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := client.Get(ctx, "key"); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got := metrics.Counter(MetricMisses, Labels{"backend": "node-a"}); got != 1 {
		t.Fatalf("misses = %v, want 1", got)
	}
	if got := metrics.Counter(MetricHits, Labels{"backend": "node-a"}); got != 1 {
		t.Fatalf("hits = %v, want 1", got)
	}
	if got := metrics.Counter(MetricRequests, Labels{"op": "get", "backend": "node-a", "status": "ok"}); got != 1 {
		t.Fatalf("get ok requests = %v, want 1", got)
	}
}

func TestClientNilContext(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	//nolint:staticcheck // deliberately exercising the nil-context guard
	_, err := client.Get(nil, "key")
	if got := KindOf(err); got != KindCanceled {
		t.Fatalf("KindOf(error) = %v, want canceled (err = %v)", got, err)
	}
	if errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want canceled, not closed", err)
	}
}

func TestWarmNilContextDoesNotPanic(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	loader := func(context.Context, string) ([]byte, time.Duration, error) {
		return []byte("value"), time.Minute, nil
	}

	//nolint:staticcheck // deliberately exercising the nil-context guard
	report, err := client.Warm(nil, []string{"key"}, loader, WarmOptions{Force: true})
	if KindOf(err) != KindCanceled {
		t.Fatalf("KindOf(error) = %v, want canceled (err = %v)", KindOf(err), err)
	}
	if report.Warmed != 0 {
		t.Fatalf("report = %+v, want no warmed keys", report)
	}
}

func TestClientDeleteAllCandidates(t *testing.T) {
	memories := []*MemoryBackend{NewMemoryBackend(), NewMemoryBackend(), NewMemoryBackend()}
	client, _ := newTestClient(t, []Backend{memories[0], memories[1], memories[2]}, func(cfg *Config) {
		cfg.ReplicationFactor = 3
		cfg.WriteReplicas = true
	})
	ctx := context.Background()

	if err := client.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Delete(ctx, "key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for i, memory := range memories {
		if _, err := memory.Get(ctx, "key"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("memory %d still has key: %v", i, err)
		}
	}
}

func TestClientReadReplicasOnMiss(t *testing.T) {
	memories := []*MemoryBackend{NewMemoryBackend(), NewMemoryBackend()}
	client, _ := newTestClient(t, []Backend{memories[0], memories[1]}, func(cfg *Config) {
		cfg.ReplicationFactor = 2
		cfg.WriteReplicas = false
		cfg.ReadReplicasOnMiss = true
	})
	ctx := context.Background()

	candidates := client.BackendsForKey("key")
	secondary := candidates[1]
	// Simulate a write that landed only on the replica.
	if err := memories[indexForName(secondary)].Set(ctx, "key", []byte("replica-value"), time.Minute); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	value, err := client.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(value) != "replica-value" {
		t.Fatalf("Get = %q, want replica-value", value)
	}
}

func indexForName(name string) int {
	return int(name[len(name)-1] - 'a')
}

func TestClientSyncAfterClose(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if _, err := client.Get(ctx, "key"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close error = %v, want ErrClosed", err)
	}
	if err := client.Set(ctx, "key", []byte("value"), time.Minute); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close error = %v, want ErrClosed", err)
	}
	if err := client.Delete(ctx, "key"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close error = %v, want ErrClosed", err)
	}
}

type bodyTooLargeBackend struct {
	Backend
	calls atomic.Int64
}

func (b *bodyTooLargeBackend) Get(context.Context, string) ([]byte, error) {
	b.calls.Add(1)
	return nil, ErrBodyTooLarge
}

func TestClientBodyTooLargeDoesNotFailOver(t *testing.T) {
	first := &bodyTooLargeBackend{Backend: NewMemoryBackend()}
	second := &bodyTooLargeBackend{Backend: NewMemoryBackend()}
	client, _ := newTestClient(t, []Backend{first, second}, func(cfg *Config) {
		cfg.ReplicationFactor = 2
	})

	if _, err := client.Get(context.Background(), "key"); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("Get error = %v, want ErrBodyTooLarge", err)
	}
	if got := first.calls.Load() + second.calls.Load(); got != 1 {
		t.Fatalf("backend calls = %d, want 1 (no failover)", got)
	}
}

func TestWarmTreatsEmptyValueAsPresent(t *testing.T) {
	client, _ := newTestClient(t, []Backend{NewMemoryBackend()}, nil)
	ctx := context.Background()
	if err := client.Set(ctx, "empty", []byte{}, time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var loads atomic.Int64
	loader := func(context.Context, string) ([]byte, time.Duration, error) {
		loads.Add(1)
		return []byte("loaded"), time.Minute, nil
	}
	report, err := client.Warm(ctx, []string{"empty"}, loader, WarmOptions{})
	if err != nil {
		t.Fatalf("Warm: %v", err)
	}
	if report.Skipped != 1 || report.Warmed != 0 {
		t.Fatalf("report = %+v, want one skipped key", report)
	}
	if got := loads.Load(); got != 0 {
		t.Fatalf("loads = %d, want 0", got)
	}
}

func TestJitterTTLStaysPositive(t *testing.T) {
	const ttl = time.Nanosecond
	for i := 0; i < 1000; i++ {
		if got := jitterTTL(ttl, 1); got <= 0 {
			t.Fatalf("jitterTTL(1ns, 1) = %v, want > 0", got)
		}
		if got := jitterTTL(ttl, 2); got <= 0 {
			t.Fatalf("jitterTTL(1ns, 2) = %v, want > 0", got)
		}
	}
}
