// Package state provides a minimal, concurrency-safe finite state machine.
//
// A Machine is parameterized by a state type S and a context type C. The
// context is created once with the machine and passed (by pointer) to every
// handler, so it can accumulate and share data across states.
//
// Actions are plain values of any type; the action's type identifies which
// handler runs. Handlers are registered per (state, action type) with On and
// applied with Do. An action only advances the machine if a handler is
// registered for it in the current state and that handler returns no error.
//
// A state may own background work: Enter registers an activity that runs in
// its own goroutine while the machine stays in that state, with a context
// that is canceled when the machine leaves it. Activities report outcomes
// back through Do; if the machine has moved on, the stale action is rejected
// like any other invalid action.
//
// All methods are safe for concurrent use by multiple goroutines.
package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrInvalid is wrapped by the error Do returns when no handler is registered
// for the action's type in the machine's current state.
var ErrInvalid = errors.New("action not valid in current state")

// Machine is a finite state machine over states S carrying a context C.
type Machine[S comparable, C any] struct {
	mu       sync.Mutex
	cond     sync.Cond
	state    S
	ctx      C
	handlers map[S]map[any]any
	enters   map[S]func(context.Context)
	cancel   context.CancelFunc // stops the current state's activity, if any
}

// New returns a Machine in state initial holding ctx as its context.
func New[S comparable, C any](initial S, ctx C) *Machine[S, C] {
	m := &Machine[S, C]{
		state:    initial,
		ctx:      ctx,
		handlers: make(map[S]map[any]any),
	}
	m.cond.L = &m.mu
	return m
}

// On registers fn to handle actions of type A while the machine is in state
// from. fn returns the next state; if it returns an error the action is
// rejected and the state is unchanged. Registering the same (from, A) pair
// again replaces the previous handler.
func (m *Machine[S, C]) On[A any](from S, fn func(*C, A) (S, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hs := m.handlers[from]
	if hs == nil {
		hs = make(map[any]any)
		m.handlers[from] = hs
	}
	hs[key[A]()] = fn
}

// Enter registers fn as the entry activity of state s. Each time the machine
// transitions into s from a different state, fn runs in its own goroutine
// with a context that is canceled when the machine next leaves s. A handler
// returning the state it is in is an internal transition: the activity is
// neither canceled nor restarted, so it can keep running while actions
// mutate the context (progress updates, coalesced input, ...).
//
// fn typically does work and reports the outcome with Do. The cancellation
// happens under the machine's lock, before the next state's handlers can
// run, so an activity that has been left behind either sees its context
// done or has its action rejected — never both accepted. fn must return
// once its context is done, or the goroutine leaks. Registering Enter for
// the same state again replaces the activity used by future entries.
//
// Being constructed in a state is not a transition, so the initial state's
// activity does not run at New; start machines in a quiescent state and
// kick them off with an action. To bind another resource to the state —
// including a child Machine — use context.AfterFunc:
//
//	m.Enter(Running, func(ctx context.Context) {
//		child := newWorker()
//		context.AfterFunc(ctx, func() { child.Do(halt{}) })
//		...
//	})
func (m *Machine[S, C]) Enter(s S, fn func(ctx context.Context)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.enters == nil {
		m.enters = make(map[S]func(context.Context))
	}
	m.enters[s] = fn
}

// Do applies action a. If no handler is registered for A in the current
// state, Do reports ErrInvalid. If the handler fails, its error is returned
// and the state is unchanged. Otherwise the machine advances to the returned
// state and blocked Wait calls are re-evaluated. If the state changed, the
// old state's entry activity (if running) is canceled and the new state's
// (if registered) is started. Do returns the state the machine is in
// afterwards.
func (m *Machine[S, C]) Do[A any](a A) (S, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.handlers[m.state][key[A]()]
	if !ok {
		return m.state, fmt.Errorf("%w: %T in state %v", ErrInvalid, a, m.state)
	}
	next, err := h.(func(*C, A) (S, error))(&m.ctx, a)
	if err != nil {
		return m.state, err
	}
	if next != m.state {
		if m.cancel != nil {
			m.cancel()
			m.cancel = nil
		}
		if fn, ok := m.enters[next]; ok {
			var ctx context.Context
			ctx, m.cancel = context.WithCancel(context.Background())
			go fn(ctx)
		}
	}
	m.state = next
	m.cond.Broadcast()
	return next, nil
}

// State returns the current state.
func (m *Machine[S, C]) State() S {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Wait blocks until cond returns true. cond runs with the machine locked, so
// it may freely read the state and context (or copy values out of them); it
// must not call other Machine methods. cond is evaluated immediately and then
// again after every successful Do, so a cond that always returns true acts as
// a synchronized read of the context.
func (m *Machine[S, C]) Wait(cond func(S, *C) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for !cond(m.state, &m.ctx) {
		m.cond.Wait()
	}
}

// key returns a comparable identity for type A without using reflect: the
// boxed nil *A compares equal only to another boxed nil *A.
func key[A any]() any { return (*A)(nil) }
