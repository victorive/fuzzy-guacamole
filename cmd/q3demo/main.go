package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"

	"test/q3"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := q3.WithRequestID(context.Background(), "demo-request-1")

	validation := q3.Wrap(q3.NewValidation("invalid_email", "email is invalid", q3.FieldError{
		Field:   "email",
		Code:    "invalid_format",
		Message: "must contain @",
	}), "create_user")

	external := q3.NewExternal("payments", "payment_unavailable", "payment service unavailable",
		errors.New("connection refused"), http.StatusServiceUnavailable, 0)

	internal := q3.NewInternal("load_user", errors.New("database password=hunter2"))

	for _, err := range []error{validation, external, internal} {
		fmt.Printf("kind=%s code=%s status=%d retryable=%v message=%q\n",
			q3.KindOf(err), q3.ErrorCode(err), q3.HTTPStatus(err), q3.IsRetryable(err), q3.PublicMessage(err))
		q3.LogError(ctx, logger, err, "demo", true)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/users", nil)
	req = req.WithContext(ctx)
	q3.WriteHTTPError(rec, req, validation)
	fmt.Printf("HTTP response: %d %s\n", rec.Code, rec.Body.String())
}
