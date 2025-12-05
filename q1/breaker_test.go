package q1

import (
	"testing"
	"time"
)

type fakeClock struct {
	t time.Time
}

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func TestBreakerOpensHalfOpensAndCloses(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	var states []breakerState
	b := newBreaker("node-a", BreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 1,
		OpenTimeout:      time.Second,
		HalfOpenMax:      1,
	}, clock.Now, func(_ string, state breakerState) {
		states = append(states, state)
	})

	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("request %d should be allowed while closed", i)
		}
		b.Record(false)
	}
	if got := b.State(); got != breakerOpen {
		t.Fatalf("state = %v, want open", got)
	}
	if b.Allow() {
		t.Fatal("open breaker should fail fast")
	}

	clock.Advance(2 * time.Second)
	if !b.Allow() {
		t.Fatal("half-open breaker should allow one probe")
	}
	if b.Allow() {
		t.Fatal("half-open breaker should limit concurrent probes")
	}
	b.Record(true)
	if got := b.State(); got != breakerClosed {
		t.Fatalf("state = %v, want closed after successful probe", got)
	}

	want := []breakerState{breakerOpen, breakerHalfOpen, breakerClosed}
	if len(states) != len(want) {
		t.Fatalf("transitions = %v, want %v", states, want)
	}
	for i := range want {
		if states[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", states, want)
		}
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newBreaker("node-a", BreakerConfig{
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
		HalfOpenMax:      1,
	}, clock.Now, nil)

	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
	b.Record(false)
	if b.State() != breakerOpen {
		t.Fatal("expected open")
	}

	clock.Advance(time.Second)
	if !b.Allow() {
		t.Fatal("expected half-open probe")
	}
	b.Record(false)
	if b.State() != breakerOpen {
		t.Fatal("failed probe should reopen breaker")
	}
}

func TestBreakerSuccessResetsClosedFailures(t *testing.T) {
	b := newBreaker("node-a", BreakerConfig{FailureThreshold: 3}, time.Now, nil)

	b.Record(false)
	b.Record(false)
	b.Record(true)
	b.Record(false)
	b.Record(false)
	if b.State() != breakerClosed {
		t.Fatal("success should reset the consecutive failure count")
	}
	b.Record(false)
	if b.State() != breakerOpen {
		t.Fatal("third consecutive failure should open")
	}
}

func TestBreakerSuccessThreshold(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newBreaker("node-a", BreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 2,
		OpenTimeout:      time.Second,
		HalfOpenMax:      2,
	}, clock.Now, nil)

	b.Record(false)
	clock.Advance(time.Second)
	if !b.Allow() || !b.Allow() {
		t.Fatal("expected two half-open probes")
	}
	b.Record(true)
	if b.State() != breakerHalfOpen {
		t.Fatal("one success is not enough to close")
	}
	b.Record(true)
	if b.State() != breakerClosed {
		t.Fatal("two successes should close")
	}
}
