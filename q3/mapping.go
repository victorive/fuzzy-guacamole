package q3

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

type ErrorResponse struct {
	Code      string       `json:"code"`
	Message   string       `json:"message"`
	RequestID string       `json:"request_id,omitempty"`
	Fields    []FieldError `json:"fields,omitempty"`
}

func HTTPStatus(err error) int {
	switch KindOf(err) {
	case KindValidation:
		return http.StatusBadRequest
	case KindAuthorization:
		return http.StatusForbidden
	case KindNotFound:
		return http.StatusNotFound
	case KindConflict:
		return http.StatusConflict
	case KindExternal:
		// Do not forward dependency statuses: an upstream 429 or 503 would
		// otherwise look like this service's own rate limit or outage.
		return http.StatusBadGateway
	case KindTimeout:
		return http.StatusGatewayTimeout
	case KindCanceled:
		// 499 is widely used for "client closed request" even though it is not
		// in the RFC.
		return 499
	case KindInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

func PublicMessage(err error) string {
	if err == nil {
		return ""
	}
	var appErr *Error
	if errors.As(err, &appErr) && appErr.Message != "" {
		return appErr.Message
	}
	switch KindOf(err) {
	case KindValidation:
		return "invalid request"
	case KindAuthorization:
		return "forbidden"
	case KindNotFound:
		return "not found"
	case KindConflict:
		return "conflict"
	case KindExternal:
		return "external service unavailable"
	case KindTimeout:
		return "request timed out"
	case KindCanceled:
		return "request canceled"
	default:
		return "internal server error"
	}
}

func ErrorResponseFor(err error, requestID string) ErrorResponse {
	kind := KindOf(err)
	code := ErrorCode(err)
	if code == "" {
		code = string(kind)
	}
	resp := ErrorResponse{
		Code:      code,
		Message:   PublicMessage(err),
		RequestID: requestID,
	}
	var validation *ValidationError
	if errors.As(err, &validation) {
		resp.Fields = append([]FieldError(nil), validation.Fields...)
	}
	return resp
}

func WriteHTTPError(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	status := HTTPStatus(err)
	if retryAfter := RetryAfter(err); retryAfter > 0 {
		seconds := int(retryAfter / time.Second)
		if retryAfter%time.Second != 0 {
			seconds++
		}
		if seconds < 1 {
			seconds = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponseFor(err, RequestIDFrom(r.Context())))
}
