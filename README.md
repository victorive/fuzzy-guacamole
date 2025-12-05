Question 2: Explain Go's context package and its role in managing request lifecycles. 
Design a function that performs multiple concurrent HTTP calls with proper timeout 
handling and graceful cancellation. Discuss how context values should be used and the 
potential pitfalls of context misuse.


Question 3: Go's error handling is often criticized as verbose. Design a robust error 
handling strategy for a microservice that needs to distinguish between different error types 
(validation, authorization, external service failures). Explain when you would use error
wrapping, custom error types, and how you'd implement proper logging and observability.


Question 1: Implement a distributed cache client with the following requirements:
1. Support multiple backend servers with consistent hashing
2. Implement connection pooling and circuit breaker functionality
3. Handle server failures gracefully with automatic failover
4. Provide both synchronous and asynchronous APIs
5. Include proper instrumentation (metrics, logging, tracing)
Requirements:
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
Follow-up Questions:
• How would you handle cache warming and preloading strategies?
• What metrics would you expose for monitoring cache performance?
• How would you implement cache coherence across multiple client instances?

## Layout

```text
q1/                 distributed cache client
  cache.go          CacheClient, Client, config, failover, async, warming
  ring.go           consistent hash ring
  breaker.go        per-backend circuit breaker
  backend.go        Backend interface, HTTP backend, in-memory backend
  errors.go         sentinels and structured errors
  metrics.go        Metrics interface, no-op and in-memory implementations
  tracing.go        Tracer/Span interface, no-op and slog implementations
  singleflight.go   stdlib singleflight for stampede protection
  *_test.go

q2/                 context and concurrent HTTP
  fetch.go          FetchAll with timeouts, bounded concurrency, fail-fast
  context.go        typed request/trace ID helpers
  *_test.go

q3/                 error handling strategy
  errors.go         Kind, Error, ValidationError, ExternalError
  mapping.go        HTTP status and safe JSON responses
  handler.go        request ID, panic recovery, logging, metrics
  retry.go          retry policy with backoff, jitter, and Retry-After
  *_test.go

cmd/
  q1demo/           cache failover demo using in-process HTTP servers
  q2demo/           concurrent fetch with a per-call timeout
  q3demo/           error classification, logging, and HTTP response demo
```

## Quick start

```bash
go test ./... -race -count=1
go vet ./...
go run ./cmd/q1demo
go run ./cmd/q2demo
go run ./cmd/q3demo
```
