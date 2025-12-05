package q1

import (
	"context"
	"fmt"
	"sync"
)

type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
	val  []byte
	err  error
}

func newFlightGroup() *flightGroup {
	return &flightGroup{calls: make(map[string]*flightCall)}
}

// Do runs fn once for a key while concurrent callers wait. Waiters respect
// their own context, so one caller cancelling does not cancel the leader.
func (g *flightGroup) Do(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	g.mu.Lock()
	if call, ok := g.calls[key]; ok {
		g.mu.Unlock()
		select {
		case <-call.done:
			return call.val, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	call := &flightCall{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	val, err := g.run(fn)
	call.val = val
	call.err = err
	close(call.done)

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return val, err
}

func (g *flightGroup) run(fn func() ([]byte, error)) (val []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			val = nil
			err = fmt.Errorf("q1: panic in singleflight: %v", r)
		}
	}()
	return fn()
}
