package q2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	ErrBodyTooLarge = errors.New("q2: response body too large")
	ErrFailFast     = errors.New("q2: fail-fast cancellation")
)

type FetchOptions struct {
	MaxConcurrency int
	PerCallTimeout time.Duration
	OverallTimeout time.Duration
	MaxBodyBytes   int64
	FailFast       bool
	Client         *http.Client
}

func (o FetchOptions) withDefaults() FetchOptions {
	if o.MaxConcurrency <= 0 {
		o.MaxConcurrency = 8
	}
	if o.PerCallTimeout <= 0 {
		o.PerCallTimeout = 5 * time.Second
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 1 << 20
	}
	if o.Client == nil {
		o.Client = defaultHTTPClient
	}
	return o
}

type FetchResult struct {
	URL        string
	StatusCode int
	Body       []byte
	Err        error
}

type HTTPStatusError struct {
	URL        string
	StatusCode int
	Status     string
	Body       []byte
}

func (e *HTTPStatusError) Error() string {
	status := e.Status
	if status == "" {
		status = fmt.Sprintf("%d", e.StatusCode)
	}
	return fmt.Sprintf("q2: %s returned %s", e.URL, status)
}

func NewHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			MaxConnsPerHost:       50,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
		},
	}
}

var defaultHTTPClient = NewHTTPClient()

func FetchAll(ctx context.Context, urls []string, opts FetchOptions) ([]FetchResult, error) {
	if ctx == nil {
		return nil, errors.New("q2: nil context")
	}
	opts = opts.withDefaults()

	results := make([]FetchResult, len(urls))
	for i, u := range urls {
		results[i].URL = u
	}
	if len(urls) == 0 {
		return results, nil
	}

	if opts.OverallTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.OverallTimeout)
		defer cancel()
	}

	var cancelCause context.CancelCauseFunc
	if opts.FailFast {
		ctx, cancelCause = context.WithCancelCause(ctx)
		defer cancelCause(nil)
	}

	workers := opts.MaxConcurrency
	if workers > len(urls) {
		workers = len(urls)
	}

	var wg sync.WaitGroup
	errs := make([]error, len(urls))
	jobs := make(chan int)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				select {
				case <-ctx.Done():
					err := cause(ctx)
					results[idx].Err = err
					errs[idx] = err
					continue
				default:
				}

				res := fetchOne(ctx, opts.Client, urls[idx], opts)
				results[idx] = res
				if res.Err == nil {
					continue
				}
				err := res.Err
				if cancelCause != nil {
					err = fmt.Errorf("%w: %s: %w", ErrFailFast, urls[idx], res.Err)
					// The first cancel wins; later calls are no-ops.
					cancelCause(err)
				}
				errs[idx] = err
			}
		}()
	}

	for i := range urls {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	return results, errors.Join(errs...)
}

func cause(ctx context.Context) error {
	if c := context.Cause(ctx); c != nil {
		return c
	}
	return ctx.Err()
}

func fetchOne(ctx context.Context, client *http.Client, rawURL string, opts FetchOptions) FetchResult {
	res := FetchResult{URL: rawURL}

	callCtx, cancel := context.WithTimeout(ctx, opts.PerCallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		res.Err = fmt.Errorf("q2: build request: %w", err)
		return res
	}
	if id := RequestIDFrom(ctx); id != "" {
		req.Header.Set("X-Request-ID", id)
	}
	if id := TraceIDFrom(ctx); id != "" {
		req.Header.Set("X-Trace-ID", id)
	}

	resp, err := client.Do(req)
	if err != nil {
		res.Err = fmt.Errorf("q2: request %s: %w", rawURL, err)
		return res
	}
	defer resp.Body.Close()

	res.StatusCode = resp.StatusCode
	body, err := io.ReadAll(io.LimitReader(resp.Body, opts.MaxBodyBytes+1))
	if err != nil {
		res.Err = fmt.Errorf("q2: read body from %s: %w", rawURL, err)
		return res
	}
	if int64(len(body)) > opts.MaxBodyBytes {
		res.Err = fmt.Errorf("%w: %s", ErrBodyTooLarge, rawURL)
		return res
	}
	res.Body = body

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		res.Err = &HTTPStatusError{
			URL:        rawURL,
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       truncate(body, 512),
		}
	}
	return res
}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return append([]byte(nil), b...)
	}
	return append([]byte(nil), b[:n]...)
}
