package q3

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Kind string

const (
	KindValidation    Kind = "validation"
	KindAuthorization Kind = "authorization"
	KindNotFound      Kind = "not_found"
	KindConflict      Kind = "conflict"
	KindExternal      Kind = "external"
	KindTimeout       Kind = "timeout"
	KindInternal      Kind = "internal"
	KindCanceled      Kind = "canceled"
)

// Error is the application's structured error. Message is safe to show to
// clients; Err is the internal cause and must not be serialized.
type Error struct {
	Kind      Kind
	Code      string
	Message   string
	Op        string
	Err       error
	Retryable bool
	Meta      map[string]string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	var b strings.Builder
	if e.Kind != "" {
		b.WriteString(string(e.Kind))
	}
	if e.Code != "" {
		if b.Len() > 0 {
			b.WriteByte('/')
		}
		b.WriteString(e.Code)
	}
	if e.Op != "" {
		b.WriteString(" [")
		b.WriteString(e.Op)
		b.WriteByte(']')
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	if b.Len() == 0 {
		return "error"
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if t.Kind != "" && e.Kind != t.Kind {
		return false
	}
	if t.Code != "" && e.Code != t.Code {
		return false
	}
	return true
}

var (
	ErrValidation    = &Error{Kind: KindValidation}
	ErrAuthorization = &Error{Kind: KindAuthorization}
	ErrNotFound      = &Error{Kind: KindNotFound}
	ErrConflict      = &Error{Kind: KindConflict}
	ErrExternal      = &Error{Kind: KindExternal}
	ErrTimeout       = &Error{Kind: KindTimeout}
	ErrInternal      = &Error{Kind: KindInternal}
	ErrCanceled      = &Error{Kind: KindCanceled}
)

type Option func(*Error)

func WithOp(op string) Option {
	return func(e *Error) { e.Op = op }
}

func WithCause(err error) Option {
	return func(e *Error) { e.Err = err }
}

func WithRetryable(v bool) Option {
	return func(e *Error) { e.Retryable = v }
}

func WithMeta(key, value string) Option {
	return func(e *Error) {
		if e.Meta == nil {
			e.Meta = make(map[string]string)
		}
		e.Meta[key] = value
	}
}

func New(kind Kind, code, message string, opts ...Option) *Error {
	e := &Error{Kind: kind, Code: code, Message: message}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ValidationError struct {
	Base   *Error
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	if e == nil || e.Base == nil {
		return "<nil>"
	}
	return e.Base.Error()
}

func (e *ValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Base
}

func NewValidation(code, message string, fields ...FieldError) *ValidationError {
	return &ValidationError{
		Base:   New(KindValidation, code, message, WithRetryable(false)),
		Fields: fields,
	}
}

type ExternalError struct {
	Base       *Error
	Dependency string
	StatusCode int
	RetryAfter time.Duration
}

func (e *ExternalError) Error() string {
	if e == nil || e.Base == nil {
		return "<nil>"
	}
	return e.Base.Error()
}

func (e *ExternalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Base
}

// NewExternal classifies a downstream failure. 4xx responses (except 408 and
// 429) and caller cancellation are not retryable; network errors, 408, 429, and
// 5xx are.
func NewExternal(dependency, code, message string, cause error, statusCode int, retryAfter time.Duration) *ExternalError {
	retryable := true
	if statusCode >= 400 && statusCode < 500 && statusCode != 408 && statusCode != 429 {
		retryable = false
	}
	if errors.Is(cause, context.Canceled) {
		retryable = false
	}
	return &ExternalError{
		Base: New(KindExternal, code, message,
			WithCause(cause),
			WithRetryable(retryable),
			WithMeta("dependency", dependency),
		),
		Dependency: dependency,
		StatusCode: statusCode,
		RetryAfter: retryAfter,
	}
}

func External(dependency string, cause error) *ExternalError {
	return NewExternal(dependency, "external_dependency", "external service unavailable", cause, 0, 0)
}

func NewAuthorization(code, message string) *Error {
	return New(KindAuthorization, code, message, WithRetryable(false))
}

func NewNotFound(code, message string) *Error {
	return New(KindNotFound, code, message, WithRetryable(false))
}

func NewConflict(code, message string) *Error {
	return New(KindConflict, code, message, WithRetryable(false))
}

func NewTimeout(op string, cause error) *Error {
	return New(KindTimeout, "timeout", "request timed out",
		WithOp(op), WithCause(cause), WithRetryable(true))
}

func NewInternal(op string, cause error) *Error {
	return New(KindInternal, "internal", "internal server error",
		WithOp(op), WithCause(cause), WithRetryable(false))
}

func Wrap(err error, op string) error {
	if err == nil {
		return nil
	}
	if op == "" {
		return err
	}
	return fmt.Errorf("%s: %w", op, err)
}

// KindOf returns the classification of err. Structured errors win over raw
// context errors so a domain External error that wraps a deadline stays
// External.
func KindOf(err error) Kind {
	if err == nil {
		return ""
	}
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Kind
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout
	case errors.Is(err, context.Canceled):
		return KindCanceled
	default:
		return KindInternal
	}
}

func IsRetryable(err error) bool {
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Retryable
	}
	return false
}

func ErrorCode(err error) string {
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Code
	}
	return ""
}

func ErrorOp(err error) string {
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Op
	}
	return ""
}

func RetryAfter(err error) time.Duration {
	var ext *ExternalError
	if errors.As(err, &ext) {
		return ext.RetryAfter
	}
	return 0
}
