package q1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

type CacheClient interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	GetAsync(key string) <-chan Result
}

type Result struct {
	Value []byte
	Error error
}

const (
	MetricRequests           = "cache_requests_total"
	MetricDuration           = "cache_request_duration_seconds"
	MetricHits               = "cache_hits_total"
	MetricMisses             = "cache_misses_total"
	MetricFailovers          = "cache_failovers_total"
	MetricBackendErrors      = "cache_backend_errors_total"
	MetricBreakerState       = "cache_breaker_state"
	MetricBreakerTransitions = "cache_breaker_transitions_total"
	MetricAsyncRejected      = "cache_async_rejected_total"
	MetricAsyncDepth         = "cache_async_queue_depth"
	MetricWarm               = "cache_warm_total"
)

type BackendConfig struct {
	Name    string
	BaseURL string
	Weight  int
	Client  *http.Client
	// Backend, when set, is caller-owned: Client.Close does not close it.
	Backend Backend
}

type Config struct {
	Backends []BackendConfig

	Replicas          int
	ReplicationFactor int
	OperationTimeout  time.Duration
	// WriteReplicas writes to every failover candidate instead of stopping at
	// the first success. This improves availability at the cost of coherence.
	WriteReplicas bool
	// ReadReplicasOnMiss continues to the next candidate when the primary
	// reports a miss. Useful when writes may have landed on a replica.
	ReadReplicasOnMiss bool

	AsyncWorkers   int
	AsyncQueueSize int
	AsyncTimeout   time.Duration

	Breaker BreakerConfig
	Logger  *slog.Logger
	Metrics Metrics
	Tracer  Tracer
	Now     func() time.Time
}

func DefaultConfig() Config {
	return Config{
		Replicas:          100,
		ReplicationFactor: 2,
		OperationTimeout:  2 * time.Second,
		AsyncWorkers:      8,
		AsyncQueueSize:    256,
		AsyncTimeout:      2 * time.Second,
		Breaker:           defaultBreakerConfig(),
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Replicas <= 0 {
		c.Replicas = d.Replicas
	}
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = d.ReplicationFactor
	}
	if c.OperationTimeout <= 0 {
		c.OperationTimeout = d.OperationTimeout
	}
	if c.AsyncWorkers <= 0 {
		c.AsyncWorkers = d.AsyncWorkers
	}
	if c.AsyncQueueSize <= 0 {
		c.AsyncQueueSize = d.AsyncQueueSize
	}
	if c.AsyncTimeout <= 0 {
		c.AsyncTimeout = d.AsyncTimeout
	}
	c.Breaker = c.Breaker.withDefaults()
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Metrics == nil {
		c.Metrics = NoopMetrics{}
	}
	if c.Tracer == nil {
		c.Tracer = NoopTracer{}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

type backendState struct {
	backend Backend
	breaker *breaker
	owned   bool
}

type asyncJob struct {
	key    string
	result chan Result
}

type Client struct {
	cfg      Config
	ring     *ring
	backends map[string]*backendState
	flights  *flightGroup

	jobs    chan asyncJob
	baseCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	asyncMu sync.RWMutex
	closed  bool
}

var _ CacheClient = (*Client)(nil)

func New(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if len(cfg.Backends) == 0 {
		return nil, errors.New("q1: at least one backend is required")
	}

	baseCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:      cfg,
		ring:     newRing(cfg.Replicas),
		backends: make(map[string]*backendState, len(cfg.Backends)),
		flights:  newFlightGroup(),
		jobs:     make(chan asyncJob, cfg.AsyncQueueSize),
		baseCtx:  baseCtx,
		cancel:   cancel,
	}

	owned := make([]Backend, 0, len(cfg.Backends))
	fail := func(err error) (*Client, error) {
		cancel()
		for _, b := range owned {
			_ = b.Close()
		}
		return nil, err
	}

	weights := make(map[string]int, len(cfg.Backends))
	for _, bc := range cfg.Backends {
		name := strings.TrimSpace(bc.Name)
		if name == "" {
			return fail(errors.New("q1: backend name is required"))
		}
		if _, exists := c.backends[name]; exists {
			return fail(fmt.Errorf("q1: duplicate backend %q", name))
		}

		var backend Backend
		isOwned := bc.Backend == nil
		if bc.Backend != nil {
			backend = bc.Backend
		} else {
			if bc.BaseURL == "" {
				return fail(fmt.Errorf("q1: backend %q needs BaseURL or Backend", name))
			}
			httpBackend, err := NewHTTPBackend(HTTPBackendConfig{
				Name:    name,
				BaseURL: bc.BaseURL,
				Client:  bc.Client,
			})
			if err != nil {
				return fail(err)
			}
			backend = httpBackend
			owned = append(owned, httpBackend)
		}

		weight := bc.Weight
		if weight <= 0 {
			weight = 1
		}
		weights[name] = weight
		c.backends[name] = &backendState{backend: backend, owned: isOwned}
	}
	c.ring.AddAll(weights)

	for name, bs := range c.backends {
		bs.breaker = newBreaker(name, cfg.Breaker, cfg.Now, c.breakerChanged)
		c.metrics().SetGauge(MetricBreakerState, breakerClosed.metricValue(), Labels{"backend": name})
	}

	for i := 0; i < cfg.AsyncWorkers; i++ {
		c.wg.Add(1)
		go c.asyncWorker()
	}
	return c, nil
}

func (c *Client) metrics() Metrics {
	return c.cfg.Metrics
}

func (c *Client) isClosed() bool {
	c.asyncMu.RLock()
	defer c.asyncMu.RUnlock()
	return c.closed
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	return c.do(ctx, "get", key, func(ctx context.Context, b Backend) ([]byte, error) {
		return b.Get(ctx, key)
	})
}

func (c *Client) do(ctx context.Context, op, key string, fn func(context.Context, Backend) ([]byte, error)) ([]byte, error) {
	if ctx == nil {
		return nil, &Error{Kind: KindCanceled, Op: op, KeyHash: hashKey(key), Err: errors.New("q1: nil context")}
	}
	keyHash := hashKey(key)
	if c.isClosed() {
		return nil, &Error{Kind: KindClosed, Op: op, KeyHash: keyHash, Err: ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return nil, contextError(op, keyHash, err)
	}

	candidates := c.ring.Candidates(key, c.cfg.ReplicationFactor)
	if len(candidates) == 0 {
		return nil, &Error{Kind: KindNoBackend, Op: op, KeyHash: keyHash, Err: ErrNoBackend}
	}

	var errs []error
	for attempt, name := range candidates {
		if err := ctx.Err(); err != nil {
			errs = append(errs, contextError(op, keyHash, err))
			break
		}
		bs := c.backends[name]
		if bs == nil {
			continue
		}
		if !bs.breaker.Allow() {
			c.metrics().IncCounter(MetricRequests, Labels{"op": op, "backend": name, "status": "circuit_open"})
			errs = append(errs, &Error{Kind: KindCircuitOpen, Op: op, Backend: name, KeyHash: keyHash, Err: ErrCircuitOpen})
			continue
		}

		attemptCtx, cancel := c.operationContext(ctx)
		start := time.Now()
		spanCtx, span := c.cfg.Tracer.Start(attemptCtx, "cache."+op,
			Attr{Key: "cache.backend", Value: name},
			Attr{Key: "cache.key_hash", Value: keyHash},
			Attr{Key: "cache.attempt", Value: attempt},
		)

		var value []byte
		var err error
		func() {
			defer cancel()
			defer span.End()
			value, err = fn(spanCtx, bs.backend)
			if err != nil {
				span.RecordError(err)
			}
		}()

		duration := time.Since(start)
		success := err == nil || errors.Is(err, ErrNotFound)
		bs.breaker.Record(success)

		status := "ok"
		switch {
		case errors.Is(err, ErrNotFound):
			status = "miss"
			c.metrics().IncCounter(MetricMisses, Labels{"backend": name})
		case err != nil:
			status = "error"
			c.metrics().IncCounter(MetricBackendErrors, Labels{"backend": name, "class": errorClass(err)})
		default:
			if op == "get" {
				c.metrics().IncCounter(MetricHits, Labels{"backend": name})
			}
		}
		c.metrics().IncCounter(MetricRequests, Labels{"op": op, "backend": name, "status": status})
		c.metrics().Observe(MetricDuration, duration.Seconds(), Labels{"op": op, "backend": name})
		c.cfg.Logger.DebugContext(ctx, "cache operation",
			"op", op, "backend", name, "key_hash", keyHash,
			"status", status, "duration_ms", float64(duration.Microseconds())/1000.0,
		)

		if err == nil {
			return value, nil
		}
		if errors.Is(err, ErrNotFound) {
			if op == "get" && c.cfg.ReadReplicasOnMiss {
				errs = append(errs, err)
				continue
			}
			return nil, err
		}
		if ctx.Err() != nil {
			errs = append(errs, contextError(op, keyHash, ctx.Err()))
			break
		}
		if errors.Is(err, context.Canceled) {
			errs = append(errs, err)
			break
		}
		if errors.Is(err, ErrBodyTooLarge) {
			errs = append(errs, err)
			break
		}
		var backendErr *BackendError
		if errors.As(err, &backendErr) && !backendErr.Retryable() {
			errs = append(errs, err)
			break
		}
		c.metrics().IncCounter(MetricFailovers, Labels{"op": op, "backend": name})
		errs = append(errs, err)
	}

	return nil, joinErrors(op, keyHash, errs)
}

func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ctx == nil {
		return &Error{Kind: KindCanceled, Op: "set", KeyHash: hashKey(key), Err: errors.New("q1: nil context")}
	}
	keyHash := hashKey(key)
	if c.isClosed() {
		return &Error{Kind: KindClosed, Op: "set", KeyHash: keyHash, Err: ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return contextError("set", keyHash, err)
	}

	candidates := c.ring.Candidates(key, c.cfg.ReplicationFactor)
	if len(candidates) == 0 {
		return &Error{Kind: KindNoBackend, Op: "set", KeyHash: keyHash, Err: ErrNoBackend}
	}

	var errs []error
	successes := 0
	for _, name := range candidates {
		if err := ctx.Err(); err != nil {
			errs = append(errs, contextError("set", keyHash, err))
			break
		}
		bs := c.backends[name]
		if bs == nil {
			continue
		}
		if !bs.breaker.Allow() {
			c.metrics().IncCounter(MetricRequests, Labels{"op": "set", "backend": name, "status": "circuit_open"})
			errs = append(errs, &Error{Kind: KindCircuitOpen, Op: "set", Backend: name, KeyHash: keyHash, Err: ErrCircuitOpen})
			continue
		}

		attemptCtx, cancel := c.operationContext(ctx)
		start := time.Now()
		spanCtx, span := c.cfg.Tracer.Start(attemptCtx, "cache.set",
			Attr{Key: "cache.backend", Value: name},
			Attr{Key: "cache.key_hash", Value: keyHash},
		)
		var err error
		func() {
			defer cancel()
			defer span.End()
			err = bs.backend.Set(spanCtx, key, value, ttl)
			if err != nil {
				span.RecordError(err)
			}
		}()

		duration := time.Since(start)
		bs.breaker.Record(err == nil)
		status := "ok"
		if err != nil {
			status = "error"
			c.metrics().IncCounter(MetricBackendErrors, Labels{"backend": name, "class": errorClass(err)})
		}
		c.metrics().IncCounter(MetricRequests, Labels{"op": "set", "backend": name, "status": status})
		c.metrics().Observe(MetricDuration, duration.Seconds(), Labels{"op": "set", "backend": name})

		if err == nil {
			successes++
			if !c.cfg.WriteReplicas {
				return nil
			}
			continue
		}
		var backendErr *BackendError
		if errors.As(err, &backendErr) && !backendErr.Retryable() {
			errs = append(errs, err)
			break
		}
		c.metrics().IncCounter(MetricFailovers, Labels{"op": "set", "backend": name})
		errs = append(errs, err)
	}

	if successes > 0 {
		return nil
	}
	return joinErrors("set", keyHash, errs)
}

func (c *Client) Delete(ctx context.Context, key string) error {
	if ctx == nil {
		return &Error{Kind: KindCanceled, Op: "delete", KeyHash: hashKey(key), Err: errors.New("q1: nil context")}
	}
	keyHash := hashKey(key)
	if c.isClosed() {
		return &Error{Kind: KindClosed, Op: "delete", KeyHash: keyHash, Err: ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return contextError("delete", keyHash, err)
	}

	candidates := c.ring.Candidates(key, c.cfg.ReplicationFactor)
	if len(candidates) == 0 {
		return &Error{Kind: KindNoBackend, Op: "delete", KeyHash: keyHash, Err: ErrNoBackend}
	}

	var errs []error
	for _, name := range candidates {
		if err := ctx.Err(); err != nil {
			errs = append(errs, contextError("delete", keyHash, err))
			break
		}
		bs := c.backends[name]
		if bs == nil {
			continue
		}
		if !bs.breaker.Allow() {
			c.metrics().IncCounter(MetricRequests, Labels{"op": "delete", "backend": name, "status": "circuit_open"})
			errs = append(errs, &Error{Kind: KindCircuitOpen, Op: "delete", Backend: name, KeyHash: keyHash, Err: ErrCircuitOpen})
			continue
		}

		attemptCtx, cancel := c.operationContext(ctx)
		start := time.Now()
		spanCtx, span := c.cfg.Tracer.Start(attemptCtx, "cache.delete",
			Attr{Key: "cache.backend", Value: name},
			Attr{Key: "cache.key_hash", Value: keyHash},
		)
		var err error
		func() {
			defer cancel()
			defer span.End()
			err = bs.backend.Delete(spanCtx, key)
			if err != nil {
				span.RecordError(err)
			}
		}()

		duration := time.Since(start)
		success := err == nil || errors.Is(err, ErrNotFound)
		bs.breaker.Record(success)
		status := "ok"
		if !success {
			status = "error"
			c.metrics().IncCounter(MetricBackendErrors, Labels{"backend": name, "class": errorClass(err)})
		}
		c.metrics().IncCounter(MetricRequests, Labels{"op": "delete", "backend": name, "status": status})
		c.metrics().Observe(MetricDuration, duration.Seconds(), Labels{"op": "delete", "backend": name})

		if success {
			continue
		}
		var backendErr *BackendError
		if errors.As(err, &backendErr) && !backendErr.Retryable() {
			errs = append(errs, err)
			break
		}
		c.metrics().IncCounter(MetricFailovers, Labels{"op": "delete", "backend": name})
		errs = append(errs, err)
	}

	if len(errs) == 0 {
		return nil
	}
	return joinErrors("delete", keyHash, errs)
}

func (c *Client) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.cfg.OperationTimeout > 0 {
		return context.WithTimeout(ctx, c.cfg.OperationTimeout)
	}
	return context.WithCancel(ctx)
}

func (c *Client) asyncContext() (context.Context, context.CancelFunc) {
	if c.cfg.AsyncTimeout > 0 {
		return context.WithTimeout(c.baseCtx, c.cfg.AsyncTimeout)
	}
	return context.WithCancel(c.baseCtx)
}

func (c *Client) GetAsync(key string) <-chan Result {
	result := make(chan Result, 1)

	c.asyncMu.RLock()
	defer c.asyncMu.RUnlock()
	if c.closed {
		result <- Result{Error: &Error{Kind: KindClosed, Op: "get_async", KeyHash: hashKey(key), Err: ErrClosed}}
		close(result)
		return result
	}

	select {
	case c.jobs <- asyncJob{key: key, result: result}:
		c.metrics().SetGauge(MetricAsyncDepth, float64(len(c.jobs)), nil)
	default:
		c.metrics().IncCounter(MetricAsyncRejected, Labels{"reason": "queue_full"})
		result <- Result{Error: &Error{Kind: KindQueueFull, Op: "get_async", KeyHash: hashKey(key), Err: ErrQueueFull}}
		close(result)
	}
	return result
}

func (c *Client) asyncWorker() {
	defer c.wg.Done()
	for job := range c.jobs {
		c.metrics().SetGauge(MetricAsyncDepth, float64(len(c.jobs)), nil)
		c.runAsyncJob(job)
	}
}

func (c *Client) runAsyncJob(job asyncJob) {
	var result Result
	func() {
		defer func() {
			if r := recover(); r != nil {
				result = Result{Error: &Error{
					Kind:    KindBackend,
					Op:      "get_async",
					KeyHash: hashKey(job.key),
					Err:     fmt.Errorf("q1: async panic: %v", r),
				}}
			}
		}()
		ctx, cancel := c.asyncContext()
		defer cancel()
		value, err := c.Get(ctx, job.key)
		result = Result{Value: value, Error: err}
	}()
	job.result <- result
	close(job.result)
}

func (c *Client) Close() error {
	c.asyncMu.Lock()
	if c.closed {
		c.asyncMu.Unlock()
		return nil
	}
	c.closed = true
	close(c.jobs)
	c.cancel()
	c.asyncMu.Unlock()

	c.wg.Wait()

	var errs []error
	for _, bs := range c.backends {
		if !bs.owned {
			continue
		}
		if err := bs.backend.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Client) BackendsForKey(key string) []string {
	return c.ring.Candidates(key, c.cfg.ReplicationFactor)
}

type Loader func(ctx context.Context, key string) ([]byte, time.Duration, error)

func (c *Client) GetOrLoad(ctx context.Context, key string, loader Loader) ([]byte, error) {
	if loader == nil {
		return nil, errors.New("q1: nil loader")
	}
	value, err := c.Get(ctx, key)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	return c.flights.Do(ctx, key, func() ([]byte, error) {
		// Another goroutine may have populated the cache while we waited.
		value, err := c.Get(ctx, key)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		value, ttl, err := loader(ctx, key)
		if err != nil {
			return nil, err
		}
		if err := c.Set(ctx, key, value, ttl); err != nil {
			c.cfg.Logger.WarnContext(ctx, "cache: set after load failed",
				"op", "get_or_load", "key_hash", hashKey(key), "error", err.Error())
		}
		return value, nil
	})
}

type WarmOptions struct {
	Concurrency   int
	Force         bool
	TTLJitter     float64
	PerKeyTimeout time.Duration
}

// WarmReport summarizes a warming run. Errors are aggregated separately so one
// bad key does not hide the successful ones.
type WarmReport struct {
	Warmed  int
	Skipped int
	Failed  int
	Errors  []error
}

func (c *Client) Warm(ctx context.Context, keys []string, loader Loader, opts WarmOptions) (WarmReport, error) {
	if ctx == nil {
		return WarmReport{}, &Error{Kind: KindCanceled, Op: "warm", Err: errors.New("q1: nil context")}
	}
	if loader == nil {
		return WarmReport{}, errors.New("q1: nil loader")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 8
	}
	if opts.PerKeyTimeout <= 0 {
		opts.PerKeyTimeout = c.cfg.OperationTimeout
	}
	if opts.TTLJitter < 0 {
		opts.TTLJitter = 0
	}

	var report WarmReport
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, opts.Concurrency)

	add := func(warmed, skipped, failed int, err error) {
		mu.Lock()
		defer mu.Unlock()
		report.Warmed += warmed
		report.Skipped += skipped
		report.Failed += failed
		if err != nil {
			report.Errors = append(report.Errors, err)
		}
		if warmed > 0 {
			c.metrics().IncCounter(MetricWarm, Labels{"result": "warmed"})
		}
		if skipped > 0 {
			c.metrics().IncCounter(MetricWarm, Labels{"result": "skipped"})
		}
		if failed > 0 {
			c.metrics().IncCounter(MetricWarm, Labels{"result": "failed"})
		}
	}

	for _, key := range keys {
		key := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				add(0, 0, 1, ctx.Err())
				return
			}

			if !opts.Force {
				_, err := c.Get(ctx, key)
				if err == nil {
					add(0, 1, 0, nil)
					return
				}
				if !errors.Is(err, ErrNotFound) {
					add(0, 0, 1, err)
					return
				}
			}

			keyCtx, cancel := context.WithTimeout(ctx, opts.PerKeyTimeout)
			defer cancel()
			_, err := c.flights.Do(keyCtx, key, func() ([]byte, error) {
				value, ttl, err := loader(keyCtx, key)
				if err != nil {
					return nil, err
				}
				ttl = jitterTTL(ttl, opts.TTLJitter)
				if err := c.Set(keyCtx, key, value, ttl); err != nil {
					return nil, err
				}
				return value, nil
			})
			if err != nil {
				add(0, 0, 1, err)
				return
			}
			add(1, 0, 0, nil)
		}()
	}

	wg.Wait()
	return report, errors.Join(report.Errors...)
}

func (c *Client) breakerChanged(name string, state breakerState) {
	c.metrics().SetGauge(MetricBreakerState, state.metricValue(), Labels{"backend": name})
	c.metrics().IncCounter(MetricBreakerTransitions, Labels{"backend": name, "state": state.String()})

	level := slog.LevelInfo
	if state == breakerOpen {
		level = slog.LevelWarn
	}
	c.cfg.Logger.Log(context.Background(), level, "cache: circuit breaker state changed",
		"backend", name, "state", state.String(),
	)
}

func jitterTTL(ttl time.Duration, jitter float64) time.Duration {
	if ttl <= 0 || jitter <= 0 {
		return ttl
	}
	if jitter > 1 {
		jitter = 1
	}
	factor := 1 + (rand.Float64()*2-1)*jitter
	adjusted := time.Duration(float64(ttl) * factor)
	if adjusted <= 0 {
		return ttl
	}
	return adjusted
}

func hashKey(key string) string {
	return fmt.Sprintf("%016x", hashString(key))
}

func joinErrors(op, keyHash string, errs []error) error {
	if len(errs) == 0 {
		return &Error{Kind: KindNoBackend, Op: op, KeyHash: keyHash, Err: ErrNoBackend}
	}
	return errors.Join(errs...)
}

func errorClass(err error) string {
	var backendErr *BackendError
	if errors.As(err, &backendErr) {
		switch {
		case backendErr.StatusCode == 0:
			return "transport"
		case backendErr.StatusCode == 429:
			return "rate_limited"
		case backendErr.StatusCode >= 500:
			return "http_5xx"
		case backendErr.StatusCode >= 400:
			return "http_4xx"
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrBodyTooLarge):
		return "body_too_large"
	default:
		return "unknown"
	}
}
