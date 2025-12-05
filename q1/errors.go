package q1

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNotFound     = errors.New("cache: key not found")
	ErrCircuitOpen  = errors.New("cache: circuit breaker open")
	ErrNoBackend    = errors.New("cache: no backend available")
	ErrQueueFull    = errors.New("cache: async queue full")
	ErrClosed       = errors.New("cache: client closed")
	ErrBodyTooLarge = errors.New("cache: response body too large")
)

type ErrorKind string

const (
	KindNotFound    ErrorKind = "not_found"
	KindBackend     ErrorKind = "backend"
	KindCircuitOpen ErrorKind = "circuit_open"
	KindNoBackend   ErrorKind = "no_backend"
	KindTimeout     ErrorKind = "timeout"
	KindCanceled    ErrorKind = "canceled"
	KindQueueFull   ErrorKind = "queue_full"
	KindClosed      ErrorKind = "closed"
)

type Error struct {
	Kind    ErrorKind
	Op      string
	Backend string
	KeyHash string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := string(e.Kind)
	if e.Op != "" {
		msg += " " + e.Op
	}
	if e.Backend != "" {
		msg += " backend=" + e.Backend
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrNotFound:
		return e.Kind == KindNotFound
	case ErrCircuitOpen:
		return e.Kind == KindCircuitOpen
	case ErrNoBackend:
		return e.Kind == KindNoBackend
	case ErrQueueFull:
		return e.Kind == KindQueueFull
	case ErrClosed:
		return e.Kind == KindClosed
	}
	return false
}

func KindOf(err error) ErrorKind {
	if err == nil {
		return ""
	}
	var cacheErr *Error
	if errors.As(err, &cacheErr) {
		return cacheErr.Kind
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return KindNotFound
	case errors.Is(err, ErrCircuitOpen):
		return KindCircuitOpen
	case errors.Is(err, ErrNoBackend):
		return KindNoBackend
	case errors.Is(err, ErrQueueFull):
		return KindQueueFull
	case errors.Is(err, ErrClosed):
		return KindClosed
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout
	case errors.Is(err, context.Canceled):
		return KindCanceled
	default:
		return KindBackend
	}
}

type BackendError struct {
	Backend    string
	Op         string
	StatusCode int
	Err        error
}

func (e *BackendError) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := fmt.Sprintf("cache backend %s %s", e.Backend, e.Op)
	if e.StatusCode != 0 {
		msg += fmt.Sprintf(" status=%d", e.StatusCode)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *BackendError) Unwrap() error { return e.Err }

func (e *BackendError) Retryable() bool {
	if e == nil {
		return false
	}
	if errors.Is(e.Err, context.Canceled) {
		return false
	}
	if e.StatusCode == 0 {
		return true // network, DNS, or connection failure
	}
	if e.StatusCode == 408 || e.StatusCode == 429 {
		return true
	}
	return e.StatusCode >= 500
}

func contextError(op, keyHash string, err error) *Error {
	kind := KindBackend
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		kind = KindTimeout
	case errors.Is(err, context.Canceled):
		kind = KindCanceled
	}
	return &Error{Kind: kind, Op: op, KeyHash: keyHash, Err: err}
}
