package q3

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      float64
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 100 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 2 * time.Second
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter > 1 {
		p.Jitter = 1
	}
	return p
}

func (p RetryPolicy) Do(ctx context.Context, fn func(context.Context) error) error {
	if ctx == nil {
		return errors.New("q3: nil context")
	}
	if fn == nil {
		return errors.New("q3: nil retry function")
	}
	p = p.withDefaults()

	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsRetryable(err) {
			return err
		}
		if attempt == p.MaxAttempts {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		timer := time.NewTimer(p.delay(attempt, err))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (p RetryPolicy) delay(attempt int, err error) time.Duration {
	if retryAfter := RetryAfter(err); retryAfter > 0 {
		if retryAfter > p.MaxDelay {
			return p.MaxDelay
		}
		return retryAfter
	}

	delay := float64(p.BaseDelay) * math.Pow(2, float64(attempt-1))
	if delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}
	if p.Jitter > 0 {
		delay *= 1 + (rand.Float64()*2-1)*p.Jitter
	}
	if delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}
	if delay < 0 {
		return 0
	}
	return time.Duration(delay)
}
