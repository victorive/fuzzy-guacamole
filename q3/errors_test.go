package q3

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestValidationErrorIsAsAndMapping(t *testing.T) {
	validation := NewValidation("invalid_email", "email is invalid", FieldError{
		Field:   "email",
		Code:    "invalid_format",
		Message: "must contain @",
	})
	wrapped := Wrap(validation, "create_user")

	if !errors.Is(wrapped, ErrValidation) {
		t.Fatal("errors.Is(wrapped, ErrValidation) = false")
	}
	if errors.Is(wrapped, ErrAuthorization) {
		t.Fatal("validation matched authorization")
	}

	var validationErr *ValidationError
	if !errors.As(wrapped, &validationErr) {
		t.Fatal("errors.As did not find ValidationError")
	}
	if len(validationErr.Fields) != 1 || validationErr.Fields[0].Field != "email" {
		t.Fatalf("fields = %+v", validationErr.Fields)
	}

	var appErr *Error
	if !errors.As(wrapped, &appErr) {
		t.Fatal("errors.As did not find Error")
	}
	if appErr.Kind != KindValidation {
		t.Fatalf("kind = %s", appErr.Kind)
	}

	if got := KindOf(wrapped); got != KindValidation {
		t.Fatalf("KindOf = %s", got)
	}
	if IsRetryable(wrapped) {
		t.Fatal("validation should not be retryable")
	}
	if got := HTTPStatus(wrapped); got != http.StatusBadRequest {
		t.Fatalf("status = %d", got)
	}
	if got := PublicMessage(wrapped); got != "email is invalid" {
		t.Fatalf("message = %q", got)
	}
}

func TestExternalError(t *testing.T) {
	cause := errors.New("connection refused")
	external := NewExternal("payments", "payment_unavailable", "payment service unavailable", cause, http.StatusServiceUnavailable, 2*time.Second)

	if !errors.Is(external, ErrExternal) {
		t.Fatal("errors.Is(external, ErrExternal) = false")
	}
	if !errors.Is(external, cause) {
		t.Fatal("wrapped cause is not reachable")
	}
	if got := KindOf(external); got != KindExternal {
		t.Fatalf("KindOf = %s", got)
	}
	if !IsRetryable(external) {
		t.Fatal("5xx external error should be retryable")
	}
	if got := HTTPStatus(external); got != http.StatusBadGateway {
		t.Fatalf("status = %d", got)
	}
	if got := RetryAfter(external); got != 2*time.Second {
		t.Fatalf("retry after = %v", got)
	}

	var extErr *ExternalError
	if !errors.As(external, &extErr) {
		t.Fatal("errors.As did not find ExternalError")
	}
	if extErr.Dependency != "payments" || extErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("external details = %+v", extErr)
	}
}

func TestExternalNonRetryableCases(t *testing.T) {
	clientError := NewExternal("inventory", "bad_request", "bad request", nil, http.StatusBadRequest, 0)
	if IsRetryable(clientError) {
		t.Fatal("4xx external error should not be retryable")
	}

	canceled := NewExternal("inventory", "canceled", "canceled", context.Canceled, 0, 0)
	if IsRetryable(canceled) {
		t.Fatal("canceled external error should not be retryable")
	}
}

func TestTimeoutAndInternal(t *testing.T) {
	timeout := NewTimeout("fetch_user", context.DeadlineExceeded)
	if got := KindOf(timeout); got != KindTimeout {
		t.Fatalf("kind = %s", got)
	}
	if !IsRetryable(timeout) {
		t.Fatal("timeout should be retryable")
	}
	if got := HTTPStatus(timeout); got != http.StatusGatewayTimeout {
		t.Fatalf("status = %d", got)
	}
	if !errors.Is(timeout, context.DeadlineExceeded) {
		t.Fatal("timeout cause is not reachable")
	}

	internal := NewInternal("load_user", errors.New("database password leaked"))
	if got := KindOf(internal); got != KindInternal {
		t.Fatalf("kind = %s", got)
	}
	if IsRetryable(internal) {
		t.Fatal("internal should not be retryable")
	}
	if got := HTTPStatus(internal); got != http.StatusInternalServerError {
		t.Fatalf("status = %d", got)
	}
	if got := PublicMessage(internal); got != "internal server error" {
		t.Fatalf("message = %q", got)
	}
}

func TestAuthorizationNotFoundConflict(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		kind   Kind
		status int
	}{
		{"authorization", NewAuthorization("forbidden", "forbidden"), KindAuthorization, http.StatusForbidden},
		{"not_found", NewNotFound("missing", "not found"), KindNotFound, http.StatusNotFound},
		{"conflict", NewConflict("conflict", "conflict"), KindConflict, http.StatusConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := KindOf(tc.err); got != tc.kind {
				t.Fatalf("kind = %s, want %s", got, tc.kind)
			}
			if got := HTTPStatus(tc.err); got != tc.status {
				t.Fatalf("status = %d, want %d", got, tc.status)
			}
			if IsRetryable(tc.err) {
				t.Fatal("should not be retryable")
			}
		})
	}
}

func TestErrorCodeMatching(t *testing.T) {
	err := NewValidation("invalid_email", "bad email")
	if !errors.Is(err, &Error{Kind: KindValidation, Code: "invalid_email"}) {
		t.Fatal("expected code match")
	}
	if errors.Is(err, &Error{Kind: KindValidation, Code: "other"}) {
		t.Fatal("unexpected code match")
	}
}

func TestJoinedErrors(t *testing.T) {
	validation := NewValidation("invalid_email", "bad email")
	external := NewExternal("payments", "payment_unavailable", "down", errors.New("boom"), 503, 0)
	joined := errors.Join(validation, external)

	if !errors.Is(joined, ErrValidation) {
		t.Fatal("joined error lost validation")
	}
	if !errors.Is(joined, ErrExternal) {
		t.Fatal("joined error lost external")
	}
	var validationErr *ValidationError
	if !errors.As(joined, &validationErr) {
		t.Fatal("joined error lost validation details")
	}
}

func TestNilHandling(t *testing.T) {
	if got := Wrap(nil, "op"); got != nil {
		t.Fatalf("Wrap(nil) = %v", got)
	}
	if got := KindOf(nil); got != "" {
		t.Fatalf("KindOf(nil) = %q", got)
	}
	if IsRetryable(nil) {
		t.Fatal("nil should not be retryable")
	}
	if got := ErrorCode(nil); got != "" {
		t.Fatalf("ErrorCode(nil) = %q", got)
	}
}
