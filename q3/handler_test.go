package q3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeMetrics struct {
	mu           sync.Mutex
	counters     map[string]int
	observations map[string]int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		counters:     make(map[string]int),
		observations: make(map[string]int),
	}
}

func (m *fakeMetrics) IncCounter(name string, _ map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name]++
}

func (m *fakeMetrics) Observe(name string, _ float64, _ map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observations[name]++
}

func (m *fakeMetrics) counter(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

func (m *fakeMetrics) observation(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.observations[name]
}

func TestRequestIDMiddleware(t *testing.T) {
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(RequestIDFrom(r.Context())))
	}))

	t.Run("preserves incoming ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Request-ID", "req-123")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if got := rec.Header().Get("X-Request-ID"); got != "req-123" {
			t.Fatalf("response request ID = %q", got)
		}
		if got := rec.Body.String(); got != "req-123" {
			t.Fatalf("context request ID = %q", got)
		}
	})

	t.Run("generates missing ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		header := rec.Header().Get("X-Request-ID")
		if header == "" {
			t.Fatal("expected generated request ID")
		}
		if got := rec.Body.String(); got != header {
			t.Fatalf("context ID %q != response header %q", got, header)
		}
	})
}

func TestErrorHandlerValidation(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	metrics := newFakeMetrics()

	handler := RequestIDMiddleware(ErrorHandler(logger, metrics, func(_ http.ResponseWriter, _ *http.Request) error {
		return NewValidation("invalid_email", "email is invalid", FieldError{
			Field:   "email",
			Code:    "invalid_format",
			Message: "must contain @",
		})
	}))

	req := httptest.NewRequest(http.MethodPost, "/users", nil)
	req.Header.Set("X-Request-ID", "req-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var resp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Code != "invalid_email" || resp.Message != "email is invalid" {
		t.Fatalf("response = %+v", resp)
	}
	if resp.RequestID != "req-1" {
		t.Fatalf("request ID = %q", resp.RequestID)
	}
	if len(resp.Fields) != 1 || resp.Fields[0].Field != "email" {
		t.Fatalf("fields = %+v", resp.Fields)
	}

	logOutput := logs.String()
	if !strings.Contains(logOutput, `"error_kind":"validation"`) {
		t.Fatalf("log missing error kind: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"request_id":"req-1"`) {
		t.Fatalf("log missing request ID: %s", logOutput)
	}
	if got := metrics.counter("http_errors_total"); got != 1 {
		t.Fatalf("error counter = %d", got)
	}
	if got := metrics.observation("http_request_duration_seconds"); got != 1 {
		t.Fatalf("duration observations = %d", got)
	}
}

func TestErrorHandlerPanicRecovery(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	metrics := newFakeMetrics()

	handler := ErrorHandler(logger, metrics, func(_ http.ResponseWriter, _ *http.Request) error {
		panic("boom")
	})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var resp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Code != "internal" {
		t.Fatalf("code = %q", resp.Code)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, "panic recovered") || !strings.Contains(logOutput, "boom") {
		t.Fatalf("panic was not logged: %s", logOutput)
	}
	if strings.Contains(rec.Body.String(), "boom") {
		t.Fatal("panic details leaked to the client")
	}
}

func TestErrorHandlerSuccess(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	metrics := newFakeMetrics()

	handler := ErrorHandler(logger, metrics, func(w http.ResponseWriter, _ *http.Request) error {
		w.WriteHeader(http.StatusNoContent)
		return nil
	})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ok", nil))

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	if logs.Len() != 0 {
		t.Fatalf("unexpected log: %s", logs.String())
	}
	if got := metrics.observation("http_request_duration_seconds"); got != 1 {
		t.Fatalf("duration observations = %d", got)
	}
	if got := metrics.counter("http_errors_total"); got != 0 {
		t.Fatalf("error counter = %d", got)
	}
}

func TestWriteHTTPErrorRetryAfter(t *testing.T) {
	external := NewExternal("payments", "payment_unavailable", "payment service unavailable",
		errors.New("connection refused"), http.StatusServiceUnavailable, 2*time.Second)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	WriteHTTPError(rec, req, external)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q", got)
	}
}

func TestWriteHTTPErrorDoesNotLeakCause(t *testing.T) {
	internal := NewInternal("load_user", errors.New("password=hunter2"))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	WriteHTTPError(rec, req, internal)

	body := rec.Body.String()
	if strings.Contains(body, "hunter2") {
		t.Fatalf("internal cause leaked: %s", body)
	}
	if !strings.Contains(body, "internal server error") {
		t.Fatalf("missing safe message: %s", body)
	}
}

func TestCanceledStatus(t *testing.T) {
	err := New(KindCanceled, "canceled", "request canceled")
	if got := HTTPStatus(err); got != 499 {
		t.Fatalf("status = %d, want 499", got)
	}
}

func TestRequestIDMiddlewareTraceID(t *testing.T) {
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(TraceIDFrom(r.Context())))
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Trace-ID", "trace-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Body.String(); got != "trace-123" {
		t.Fatalf("trace ID = %q", got)
	}
}

func TestErrorResponseFallbackForUntypedError(t *testing.T) {
	resp := ErrorResponseFor(errors.New("boom"), "req-1")
	if resp.Code != "internal" {
		t.Fatalf("code = %q", resp.Code)
	}
	if resp.Message != "internal server error" {
		t.Fatalf("message = %q", resp.Message)
	}
}

func TestWriteHTTPErrorNil(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteHTTPError(rec, httptest.NewRequest(http.MethodGet, "/", nil), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want default 200", rec.Code)
	}
}

func TestLogErrorHidesCauseAtInfoLevel(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	err := NewInternal("load_user", errors.New("password=hunter2"))
	LogError(context.Background(), logger, err)

	output := logs.String()
	if strings.Contains(output, "hunter2") {
		t.Fatalf("internal cause leaked at info level: %s", output)
	}
	if !strings.Contains(output, `"error_kind":"internal"`) {
		t.Fatalf("missing classification: %s", output)
	}
}

func TestRouteLabel(t *testing.T) {
	cases := map[string]string{
		"/":          "/",
		"/users/123": "/users/:id",
		"/orders/550e8400-e29b-41d4-a716-446655440000": "/orders/:id",
		"/users/alice": "/users/alice",
	}
	for path, want := range cases {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if got := routeLabel(req); got != want {
			t.Errorf("routeLabel(%q) = %q, want %q", path, got, want)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/users/123", nil)
	req.Pattern = "GET /users/{id}"
	if got := routeLabel(req); got != "GET /users/{id}" {
		t.Fatalf("routeLabel with pattern = %q", got)
	}
}
