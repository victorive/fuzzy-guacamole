package q3

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

type Metrics interface {
	IncCounter(name string, labels map[string]string)
	Observe(name string, value float64, labels map[string]string)
}

type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, map[string]string)       {}
func (NoopMetrics) Observe(string, float64, map[string]string) {}

type HandlerFunc func(http.ResponseWriter, *http.Request) error

func ErrorHandler(logger *slog.Logger, metrics Metrics, next HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		var err error
		panicked := false

		func() {
			defer func() {
				if rec := recover(); rec != nil {
					panicked = true
					err = NewInternal("panic", fmt.Errorf("panic: %v", rec))
					if logger != nil {
						logger.ErrorContext(r.Context(), "panic recovered",
							"panic", rec,
							"stack", string(debug.Stack()),
							"request_id", RequestIDFrom(r.Context()),
						)
					}
				}
			}()
			err = next(w, r)
		}()

		route := routeLabel(r)
		duration := time.Since(start)
		if metrics != nil {
			metrics.Observe("http_request_duration_seconds", duration.Seconds(), map[string]string{
				"route":  route,
				"method": r.Method,
			})
		}
		if err == nil {
			return
		}

		if !panicked {
			LogError(r.Context(), logger, err,
				"route", route,
				"method", r.Method,
				"duration_ms", float64(duration.Microseconds())/1000.0,
			)
		}
		if metrics != nil {
			metrics.IncCounter("http_errors_total", map[string]string{
				"kind":  string(KindOf(err)),
				"code":  ErrorCode(err),
				"route": route,
			})
		}
		WriteHTTPError(w, r, err)
	})
}

// routeLabel keeps metric cardinality bounded. http.ServeMux sets r.Pattern;
// otherwise numeric and UUID path segments are replaced with ":id".
func routeLabel(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	segments := strings.Split(r.URL.Path, "/")
	for i, segment := range segments {
		if isIDSegment(segment) {
			segments[i] = ":id"
		}
	}
	return strings.Join(segments, "/")
}

func isIDSegment(segment string) bool {
	if segment == "" {
		return false
	}
	if len(segment) == 36 && strings.Count(segment, "-") == 4 {
		return true
	}
	for _, c := range segment {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = newRequestID()
		}
		ctx := WithRequestID(r.Context(), id)
		if traceID := r.Header.Get("X-Trace-ID"); traceID != "" {
			ctx = WithTraceID(ctx, traceID)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func LogError(ctx context.Context, logger *slog.Logger, err error, attrs ...any) {
	if logger == nil || err == nil {
		return
	}
	kind := KindOf(err)
	level := slog.LevelError
	switch kind {
	case KindValidation, KindAuthorization, KindNotFound, KindConflict:
		level = slog.LevelWarn
	case KindCanceled:
		level = slog.LevelInfo
	}

	args := []any{
		"error", PublicMessage(err),
		"error_kind", string(kind),
		"error_code", ErrorCode(err),
		"error_op", ErrorOp(err),
		"retryable", IsRetryable(err),
		"request_id", RequestIDFrom(ctx),
		"trace_id", TraceIDFrom(ctx),
	}
	// Full causes can contain secrets or PII. Keep them behind debug logging
	// and use a redacting slog.Handler in production.
	if logger.Enabled(ctx, slog.LevelDebug) {
		args = append(args, "error_detail", err.Error())
	}
	args = append(args, attrs...)
	logger.Log(ctx, level, "request failed", args...)
}
