package q1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Backend interface {
	Name() string
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Close() error
}

const maxCacheValueBytes = 4 << 20

type HTTPBackendConfig struct {
	Name    string
	BaseURL string
	// Client, when set, is caller-owned: Close does not close its idle
	// connections.
	Client       *http.Client
	MaxBodyBytes int64
}

// HTTPBackend talks to a small cache HTTP protocol:
//
//	GET    /v1/cache?key=...  -> 200 value | 404 miss
//	PUT    /v1/cache?key=...  -> 204, X-Cache-TTL-Ms header
//	DELETE /v1/cache?key=...  -> 204
//
// The real value is the transport: it uses a shared *http.Transport with
// bounded idle/open connections per host, which is the same pooling model used
// by production HTTP clients.
type HTTPBackend struct {
	name         string
	baseURL      *url.URL
	client       *http.Client
	maxBodyBytes int64
	ownsClient   bool
}

func NewHTTPBackend(cfg HTTPBackendConfig) (*HTTPBackend, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("q1: backend name is required")
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("q1: parse backend URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("q1: backend URL %q must be absolute", cfg.BaseURL)
	}
	ownsClient := cfg.Client == nil
	if cfg.Client == nil {
		cfg.Client = &http.Client{Transport: NewTransport()}
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = maxCacheValueBytes
	}
	return &HTTPBackend{
		name:         cfg.Name,
		baseURL:      u,
		client:       cfg.Client,
		maxBodyBytes: cfg.MaxBodyBytes,
		ownsClient:   ownsClient,
	}, nil
}

// NewTransport returns an HTTP transport tuned for a cache client. The
// per-host limits prevent one bad backend from consuming all connections.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   20,
		MaxConnsPerHost:       50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
	}
}

func (b *HTTPBackend) Name() string { return b.name }

func (b *HTTPBackend) endpoint(key string) string {
	u := *b.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/cache"
	q := u.Query()
	q.Set("key", key)
	u.RawQuery = q.Encode()
	return u.String()
}

func (b *HTTPBackend) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := b.do(ctx, http.MethodGet, key, nil, 0)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, b.maxBodyBytes+1))
		if err != nil {
			return nil, &BackendError{Backend: b.name, Op: "get", Err: err}
		}
		if int64(len(body)) > b.maxBodyBytes {
			return nil, ErrBodyTooLarge
		}
		return body, nil
	case http.StatusNotFound:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil, ErrNotFound
	default:
		return nil, b.statusError("get", resp)
	}
}

func (b *HTTPBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	resp, err := b.do(ctx, http.MethodPut, key, bytes.NewReader(value), ttl)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil
	default:
		return b.statusError("set", resp)
	}
}

func (b *HTTPBackend) Delete(ctx context.Context, key string) error {
	resp, err := b.do(ctx, http.MethodDelete, key, nil, 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil
	default:
		return b.statusError("delete", resp)
	}
}

func (b *HTTPBackend) Close() error {
	if b.ownsClient {
		if tr, ok := b.client.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
	return nil
}

func (b *HTTPBackend) do(ctx context.Context, method, key string, body io.Reader, ttl time.Duration) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, b.endpoint(key), body)
	if err != nil {
		return nil, &BackendError{Backend: b.name, Op: strings.ToLower(method), Err: err}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	if ttl > 0 {
		req.Header.Set("X-Cache-TTL-Ms", strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, &BackendError{Backend: b.name, Op: strings.ToLower(method), Err: err}
	}
	return resp, nil
}

func (b *HTTPBackend) statusError(op string, resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	return &BackendError{
		Backend:    b.name,
		Op:         op,
		StatusCode: resp.StatusCode,
		Err:        fmt.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(msg))),
	}
}

type MemoryBackend struct {
	mu       sync.Mutex
	items    map[string]memoryItem
	now      func() time.Time
	maxBytes int64
	bytes    int64
}

type memoryItem struct {
	value     []byte
	expiresAt time.Time
}

func NewMemoryBackend() *MemoryBackend {
	return NewMemoryBackendWithClock(time.Now, 0)
}

func NewMemoryBackendWithClock(now func() time.Time, maxBytes int64) *MemoryBackend {
	if now == nil {
		now = time.Now
	}
	return &MemoryBackend{
		items:    make(map[string]memoryItem),
		now:      now,
		maxBytes: maxBytes,
	}
}

func (m *MemoryBackend) Name() string { return "memory" }

func (m *MemoryBackend) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	item, ok := m.items[key]
	if !ok {
		return nil, ErrNotFound
	}
	if !item.expiresAt.IsZero() && !m.now().Before(item.expiresAt) {
		delete(m.items, key)
		m.bytes -= int64(len(item.value))
		return nil, ErrNotFound
	}
	return append([]byte(nil), item.value...), nil
}

func (m *MemoryBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if old, ok := m.items[key]; ok {
		m.bytes -= int64(len(old.value))
	}
	copied := append([]byte(nil), value...)
	item := memoryItem{value: copied}
	if ttl > 0 {
		item.expiresAt = m.now().Add(ttl)
	}
	m.items[key] = item
	m.bytes += int64(len(copied))
	m.evictLocked()
	return nil
}

func (m *MemoryBackend) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if item, ok := m.items[key]; ok {
		delete(m.items, key)
		m.bytes -= int64(len(item.value))
	}
	return nil
}

func (m *MemoryBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = make(map[string]memoryItem)
	m.bytes = 0
	return nil
}

func (m *MemoryBackend) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for key, item := range m.items {
		if !item.expiresAt.IsZero() && !now.Before(item.expiresAt) {
			delete(m.items, key)
			m.bytes -= int64(len(item.value))
		}
	}
	return len(m.items)
}

func (m *MemoryBackend) evictLocked() {
	if m.maxBytes <= 0 {
		return
	}
	now := m.now()
	for key, item := range m.items {
		if !item.expiresAt.IsZero() && !now.Before(item.expiresAt) {
			delete(m.items, key)
			m.bytes -= int64(len(item.value))
		}
	}
	if m.bytes <= m.maxBytes {
		return
	}
	type entry struct {
		key  string
		size int64
	}
	entries := make([]entry, 0, len(m.items))
	for key, item := range m.items {
		entries = append(entries, entry{key: key, size: int64(len(item.value))})
	}
	for _, e := range entries {
		if m.bytes <= m.maxBytes {
			break
		}
		delete(m.items, e.key)
		m.bytes -= e.size
	}
}

func (m *MemoryBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		value, err := m.Get(r.Context(), key)
		switch {
		case errors.Is(err, ErrNotFound):
			http.NotFound(w, r)
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(value)
		}
	case http.MethodPut:
		r.Body = http.MaxBytesReader(w, r.Body, maxCacheValueBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var ttl time.Duration
		if raw := r.Header.Get("X-Cache-TTL-Ms"); raw != "" {
			ms, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || ms < 0 {
				http.Error(w, "invalid X-Cache-TTL-Ms", http.StatusBadRequest)
				return
			}
			ttl = time.Duration(ms) * time.Millisecond
		}
		if err := m.Set(r.Context(), key, body, ttl); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := m.Delete(r.Context(), key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
