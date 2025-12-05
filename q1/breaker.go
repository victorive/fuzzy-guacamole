package q1

import (
	"sync"
	"time"
)

type BreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	OpenTimeout      time.Duration
	HalfOpenMax      int
}

func defaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 1,
		OpenTimeout:      10 * time.Second,
		HalfOpenMax:      1,
	}
}

func (c BreakerConfig) withDefaults() BreakerConfig {
	d := defaultBreakerConfig()
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = d.FailureThreshold
	}
	if c.SuccessThreshold <= 0 {
		c.SuccessThreshold = d.SuccessThreshold
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = d.OpenTimeout
	}
	if c.HalfOpenMax <= 0 {
		c.HalfOpenMax = d.HalfOpenMax
	}
	return c
}

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerHalfOpen
	breakerOpen
)

func (s breakerState) String() string {
	switch s {
	case breakerClosed:
		return "closed"
	case breakerHalfOpen:
		return "half_open"
	case breakerOpen:
		return "open"
	default:
		return "unknown"
	}
}

func (s breakerState) metricValue() float64 {
	switch s {
	case breakerClosed:
		return 0
	case breakerHalfOpen:
		return 1
	case breakerOpen:
		return 2
	default:
		return -1
	}
}

type breaker struct {
	mu       sync.Mutex
	name     string
	cfg      BreakerConfig
	now      func() time.Time
	state    breakerState
	failures int
	success  int
	openedAt time.Time
	inFlight int
	onChange func(name string, state breakerState)
}

func newBreaker(name string, cfg BreakerConfig, now func() time.Time, onChange func(string, breakerState)) *breaker {
	if now == nil {
		now = time.Now
	}
	return &breaker{
		name:     name,
		cfg:      cfg.withDefaults(),
		now:      now,
		state:    breakerClosed,
		onChange: onChange,
	}
}

func (b *breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if b.now().Sub(b.openedAt) >= b.cfg.OpenTimeout {
			b.setStateLocked(breakerHalfOpen)
			b.inFlight = 1
			return true
		}
		return false
	case breakerHalfOpen:
		if b.inFlight < b.cfg.HalfOpenMax {
			b.inFlight++
			return true
		}
		return false
	default:
		return false
	}
}

// Record feeds the result of an allowed request back into the breaker. A miss
// (ErrNotFound) is a successful interaction with the backend and should be
// recorded as success by the caller.
func (b *breaker) Record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerClosed:
		if success {
			b.failures = 0
			return
		}
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			b.setStateLocked(breakerOpen)
		}
	case breakerHalfOpen:
		if b.inFlight > 0 {
			b.inFlight--
		}
		if success {
			b.success++
			if b.success >= b.cfg.SuccessThreshold {
				b.setStateLocked(breakerClosed)
			}
		} else {
			b.setStateLocked(breakerOpen)
		}
	case breakerOpen:
		// A result from a request that started before the breaker opened.
		// Ignore it so stale failures cannot extend the open window.
	}
}

func (b *breaker) State() breakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *breaker) setStateLocked(s breakerState) {
	if b.state == s {
		return
	}
	b.state = s
	b.failures = 0
	b.success = 0
	b.inFlight = 0
	if s == breakerOpen {
		b.openedAt = b.now()
	}
	if b.onChange != nil {
		b.onChange(b.name, s)
	}
}
