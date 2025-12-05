package q1

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"
)

type Attr struct {
	Key   string
	Value any
}

type Span interface {
	SetAttributes(attrs ...Attr)
	RecordError(err error)
	End()
}

type Tracer interface {
	Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

type NoopTracer struct{}

func (NoopTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) SetAttributes(...Attr) {}
func (noopSpan) RecordError(error)     {}
func (noopSpan) End()                  {}

type spanIDKey struct{}

func SpanIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(spanIDKey{}).(string)
	return id
}

type SlogTracer struct {
	Logger *slog.Logger
	now    func() time.Time
	nextID atomic.Uint64
}

func NewSlogTracer(logger *slog.Logger) *SlogTracer {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogTracer{Logger: logger, now: time.Now}
}

func (t *SlogTracer) Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	id := t.nextID.Add(1)
	spanID := "span-" + strconv.FormatUint(id, 10)
	spanCtx := context.WithValue(ctx, spanIDKey{}, spanID)
	return spanCtx, &slogSpan{
		tracer: t,
		name:   name,
		id:     spanID,
		start:  t.now(),
		attrs:  append([]Attr(nil), attrs...),
	}
}

type slogSpan struct {
	tracer *SlogTracer
	name   string
	id     string
	start  time.Time
	attrs  []Attr
	err    error
}

func (s *slogSpan) SetAttributes(attrs ...Attr) {
	s.attrs = append(s.attrs, attrs...)
}

func (s *slogSpan) RecordError(err error) {
	if err != nil {
		s.err = err
	}
}

func (s *slogSpan) End() {
	args := []any{
		"span", s.name,
		"span_id", s.id,
		"duration_ms", float64(s.tracer.now().Sub(s.start).Microseconds()) / 1000.0,
	}
	for _, a := range s.attrs {
		args = append(args, a.Key, a.Value)
	}
	if s.err != nil {
		args = append(args, "error", s.err.Error())
	}
	s.tracer.Logger.Debug("trace span", args...)
}
