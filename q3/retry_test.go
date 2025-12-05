package q3

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryPolicyRetriesTransient(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
	}
	attempts := 0
	err := policy.Do(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return NewExternal("payments", "unavailable", "down", errors.New("boom"), 503, 0)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryPolicyDoesNotRetryValidation(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond}
	attempts := 0
	err := policy.Do(context.Background(), func(context.Context) error {
		attempts++
		return NewValidation("invalid", "invalid")
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRetryPolicyStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: 10 * time.Millisecond}
	attempts := 0
	err := policy.Do(ctx, func(context.Context) error {
		attempts++
		cancel()
		return NewExternal("payments", "unavailable", "down", errors.New("boom"), 503, 0)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRetryPolicyMaxAttempts(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}
	attempts := 0
	err := policy.Do(context.Background(), func(context.Context) error {
		attempts++
		return NewExternal("payments", "unavailable", "down", errors.New("boom"), 503, 0)
	})
	if !errors.Is(err, ErrExternal) {
		t.Fatalf("error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryPolicyHonorsRetryAfterButCapsIt(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
	}.withDefaults()

	errWithHint := NewExternal("payments", "rate_limited", "slow down", errors.New("429"), 429, time.Hour)
	if got := policy.delay(1, errWithHint); got != policy.MaxDelay {
		t.Fatalf("delay = %v, want cap %v", got, policy.MaxDelay)
	}
}

func TestRetryPolicyNilArguments(t *testing.T) {
	var policy RetryPolicy
	if err := policy.Do(nil, func(context.Context) error { return nil }); err == nil {
		t.Fatal("expected nil-context error")
	}
	if err := policy.Do(context.Background(), nil); err == nil {
		t.Fatal("expected nil-function error")
	}
}

func TestRetryPolicyDefaults(t *testing.T) {
	got := (RetryPolicy{}).withDefaults()
	if got.MaxAttempts != 3 || got.BaseDelay != 100*time.Millisecond || got.MaxDelay != 2*time.Second {
		t.Fatalf("defaults = %+v", got)
	}
}
