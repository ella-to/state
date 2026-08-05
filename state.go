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
// # Completing
//
// Final declares the states that complete a machine. Entering one cancels the
// machine's activities, stops its children, closes Done and rejects further
// actions with ErrStopped; Result then reports where it came to rest. Stop
// does the same without a transition. The difference matters to a parent:
// completing is reported upward, being stopped is not.
//
// # Children
//
// A child machine is just another Machine, held by pointer. Invoke binds one
// to a state: it is built from the parent's context on entry, stopped on exit,
// and its completion is a transition of the parent. Spawn binds one to the
// machine itself, for a dynamic number of children that outlive transitions.
// Stopping a machine stops its children, and theirs, all the way down.
//
// # Concurrency
//
// All methods are safe for concurrent use by multiple goroutines. Handlers
// (and the done callbacks of Invoke and Spawn) run with the machine locked,
// and locks are only ever taken from a parent toward its children. So from
// inside a handler:
//
//   - Send to any machine — up, down or sideways. It queues and returns.
//   - Do or Stop your own children, but never a parent or a sibling.
//   - Never Wait.
//
// Activities hold no lock and may call anything.
package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrInvalid is wrapped by the error Do returns when no handler is registered
// for the action's type in the machine's current state.
var ErrInvalid = errors.New("action not valid in current state")

// ErrStopped is wrapped by the error Do returns once the machine has completed
// or been stopped, and returned by Wait if that happens before its condition
// holds.
var ErrStopped = errors.New("machine stopped")

// actor is the type-erased view a parent has of its children. Its methods
// never mention S or C, so every *Machine[S, C] satisfies it — which is what
// lets one machine hold children of unrelated types without reflect.
type actor interface {
	launch()
	halt()
	Done() <-chan struct{}
}

// Machine is a finite state machine over states S carrying a context C.
type Machine[S comparable, C any] struct {
	mu    sync.Mutex
	cond  sync.Cond
	state S
	ctx   C

	handlers map[S]map[any]any
	enters   map[S][]func(context.Context)
	invokes  map[S][]func(*C) actor
	finals   map[S]bool

	cancel  context.CancelFunc // stops the current state's activities
	kids    []actor            // children invoked by the current state
	visit   uint64             // identifies the current stay in a state
	started bool
	stopped bool
	done    chan struct{}

	// Mailbox: deliveries that must not take a lock on their target — child
	// reports and Send — queue here, applied in order by one drain goroutine.
	pmu      sync.Mutex
	pending  []func()
	draining bool

	// Spawned children, guarded separately from the state so that Spawn is
	// safe to call from inside a handler.
	smu  sync.Mutex
	dead bool
	subs []actor
}

// New returns a Machine in state initial holding ctx as its context. Nothing
// runs until Start.
func New[S comparable, C any](initial S, ctx C) *Machine[S, C] {
	m := &Machine[S, C]{
		state:    initial,
		ctx:      ctx,
		handlers: make(map[S]map[any]any),
		done:     make(chan struct{}),
	}
	m.cond.L = &m.mu
	return m
}

// On registers fn to handle actions of type A while the machine is in state
// from. fn returns the next state; if it returns an error the action is
// rejected and the state is unchanged. Registering the same (from, A) pair
// again replaces the previous handler. Handlers registered in a final state
// never run.
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

// Final declares states that complete the machine. Entering one cancels the
// current activities, stops every child, closes Done and stops the machine for
// good: later actions are rejected with ErrStopped and Result reports the final
// state and context. A machine with no final states runs until Stop.
func (m *Machine[S, C]) Final(states ...S) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.finals == nil {
		m.finals = make(map[S]bool)
	}
	for _, s := range states {
		m.finals[s] = true
	}
}

// Enter registers fn as an entry activity of state s. Each time the machine
// transitions into s from a different state, fn runs in its own goroutine
// with a context that is canceled when the machine next leaves s. Activities
// accumulate: every activity registered for s runs on entry, alongside the
// children Invoke binds to it.
//
// A handler returning the state it is in is an internal transition: activities
// are neither canceled nor restarted, so they can keep running while actions
// mutate the context (progress updates, coalesced input, ...).
//
// fn typically does work and reports the outcome with Do. The cancellation
// happens under the machine's lock, before the next state's handlers can run,
// so an activity that has been left behind either sees its context done or has
// its action rejected — never both accepted. fn must return once its context
// is done, or the goroutine leaks.
//
// The initial state's activities run at Start. A final state's never run.
func (m *Machine[S, C]) Enter(s S, fn func(ctx context.Context)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.enters == nil {
		m.enters = make(map[S][]func(context.Context))
	}
	m.enters[s] = append(m.enters[s], fn)
}

// After applies a once, d after the machine enters s, unless it left s first.
// It is Enter with a timer: a state that advances itself after a dwell.
func (m *Machine[S, C]) After[A any](s S, d time.Duration, a A) {
	m.Enter(s, func(ctx context.Context) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
			m.Do(a)
		}
	})
}

// Invoke binds a child machine to state at.
//
// On entering at, start builds the child from the parent's context — this is
// how a parent hands state down — and the child is started. Leaving at stops
// the child, whatever the reason.
//
// If the child completes during that same stay in at, done runs like a
// handler: it receives the parent's context along with the child's final state
// and a copy of its context, and the state it returns is the parent's next
// state. A child that completes after the parent has moved on, or that is
// stopped rather than completing, reports nothing.
//
// start runs with the parent locked, so it may stash the child in the parent's
// context to send to it later. Registering Invoke more than once for the same
// state gives that state several children, each with its own done.
func (m *Machine[S, C]) Invoke[CS comparable, CC any](
	at S,
	start func(*C) *Machine[CS, CC],
	done func(*C, CS, CC) (S, error),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.invokes == nil {
		m.invokes = make(map[S][]func(*C) actor)
	}
	m.invokes[at] = append(m.invokes[at], func(c *C) actor {
		child := start(c)
		visit := m.visit // the stay in at that this child belongs to
		child.launch()
		go m.report(child, visit, true, done)
		return child
	})
}

// Spawn attaches child to the machine itself rather than to one of its states:
// it survives transitions and is stopped when the parent stops. done runs when
// the child completes, whatever state the parent is in then, and works like
// Invoke's. Spawn takes no lock on the parent's state, so it is safe to call
// from a handler — which is where a dynamic number of children usually comes
// from.
func (m *Machine[S, C]) Spawn[CS comparable, CC any](
	child *Machine[CS, CC],
	done func(*C, CS, CC) (S, error),
) {
	m.smu.Lock()
	if m.dead {
		m.smu.Unlock()
		child.halt() // the parent is already gone
		return
	}
	m.subs = append(m.subs, child)
	m.smu.Unlock()

	child.launch()
	go m.report(child, 0, false, done)
}

// Start runs the initial state's activities and children. Wiring happens
// before Start and nothing runs until it is called; Start on a machine that
// has already started or stopped does nothing. A machine whose initial state
// owns no activities, children or timers never needs it.
func (m *Machine[S, C]) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startLocked()
}

// Do applies action a. If no handler is registered for A in the current state,
// Do reports ErrInvalid; if the machine has stopped, ErrStopped. If the
// handler fails, its error is returned and the state is unchanged. Otherwise
// the machine advances to the returned state and blocked Wait calls are
// re-evaluated. If the state changed, the old state's activities and invoked
// children are canceled and the new state's are started. Do returns the state
// the machine is in afterwards.
//
// Do runs the handler on the calling goroutine with the machine locked. Never
// call it from a handler; see Send.
func (m *Machine[S, C]) Do[A any](a A) (S, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return m.state, fmt.Errorf("%w: %T in state %v", ErrStopped, a, m.state)
	}
	h, ok := m.handlers[m.state][key[A]()]
	if !ok {
		return m.state, fmt.Errorf("%w: %T in state %v", ErrInvalid, a, m.state)
	}
	next, err := h.(func(*C, A) (S, error))(&m.ctx, a)
	if err != nil {
		return m.state, err
	}
	m.goTo(next)
	return next, nil
}

// Send queues a and returns immediately, taking no lock on its target. It is
// how machines talk to each other from inside handlers — a child reporting
// progress to its parent, a parent nudging a child — where Do would take a
// second lock and could deadlock. Queued actions are applied in order, on a
// goroutine of the target's own, and are rejected like any other action if
// they no longer fit the state they arrive in.
func (m *Machine[S, C]) Send[A any](a A) {
	m.post(func() { m.Do(a) })
}

// Stop halts the machine without a transition: activities are canceled,
// children are stopped recursively, Done closes and later actions are rejected
// with ErrStopped. Result reports the state it was in. A stopped machine
// reports nothing to its parent — only completing does that.
func (m *Machine[S, C]) Stop() { m.halt() }

// State returns the current state.
func (m *Machine[S, C]) State() S {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Read projects the state and context under the machine's lock and returns the
// result: a synchronized read that does not pretend to wait for anything. fn
// must not call other Machine methods.
func (m *Machine[S, C]) Read[R any](fn func(S, *C) R) R {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fn(m.state, &m.ctx)
}

// Wait blocks until cond returns true, or reports ErrStopped if the machine
// completes or is stopped first, so a waiter is never stranded. cond runs with
// the machine locked, so it may freely read the state and context (or copy
// values out of them); it must not call other Machine methods. cond is
// evaluated immediately and then again after every successful Do, so a cond
// that always returns true acts as a synchronized read — though Read says that
// more plainly.
func (m *Machine[S, C]) Wait(cond func(S, *C) bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for !cond(m.state, &m.ctx) {
		if m.stopped {
			return ErrStopped
		}
		m.cond.Wait()
	}
	return nil
}

// Done returns a channel that is closed once the machine has completed
// (entered a final state) or been stopped.
func (m *Machine[S, C]) Done() <-chan struct{} { return m.done }

// Result reports the state the machine came to rest in and a copy of its
// context. It is meaningful once Done is closed; before that it is a snapshot.
func (m *Machine[S, C]) Result() (S, C) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, m.ctx
}

// ---- internals ------------------------------------------------------------

// launch is Start under the actor view.
func (m *Machine[S, C]) launch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startLocked()
}

func (m *Machine[S, C]) startLocked() {
	if m.started || m.stopped {
		return
	}
	m.started = true
	if m.finals[m.state] {
		m.finish()
		return
	}
	m.enter()
}

// halt is Stop under the actor view.
func (m *Machine[S, C]) halt() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.stopped {
		m.finish()
	}
}

// goTo advances the machine, recycling activities and children if the state
// actually changed. Caller holds mu.
func (m *Machine[S, C]) goTo(next S) {
	if next != m.state {
		m.leave()
		m.state = next
		m.visit++
		if m.finals[next] {
			m.finish()
			return
		}
		m.enter()
	}
	m.cond.Broadcast()
}

// enter starts the current state's activities and invoked children. Caller
// holds mu.
func (m *Machine[S, C]) enter() {
	acts, invs := m.enters[m.state], m.invokes[m.state]
	if len(acts) == 0 && len(invs) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	for _, fn := range acts {
		go fn(ctx)
	}
	for _, invoke := range invs {
		m.kids = append(m.kids, invoke(&m.ctx))
	}
}

// leave cancels the current state's activities and stops its children. Caller
// holds mu.
func (m *Machine[S, C]) leave() {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	for _, k := range m.kids {
		k.halt()
	}
	m.kids = nil
}

// finish halts the machine for good. Caller holds mu.
func (m *Machine[S, C]) finish() {
	m.stopped = true
	m.leave()

	m.smu.Lock()
	m.dead = true
	subs := m.subs
	m.subs = nil
	m.smu.Unlock()
	for _, s := range subs {
		s.halt()
	}

	close(m.done)
	m.cond.Broadcast()
}

// report waits for a child to settle and hands its result to done. A child
// that was stopped rather than completing reports nothing, and a scoped report
// is dropped unless the parent is still in the stay that started the child.
// report runs on its own goroutine holding no lock, and delivers through the
// mailbox, so a child never takes a lock on its parent.
func (m *Machine[S, C]) report[CS comparable, CC any](
	child *Machine[CS, CC],
	visit uint64,
	scoped bool,
	done func(*C, CS, CC) (S, error),
) {
	<-child.Done()
	cs, cc, completed := child.settled()
	if !completed {
		return
	}
	m.post(func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.stopped || (scoped && m.visit != visit) {
			return
		}
		next, err := done(&m.ctx, cs, cc)
		if err != nil {
			return
		}
		m.goTo(next)
	})
}

// settled reports where the machine came to rest and whether it got there by
// completing rather than by being stopped.
func (m *Machine[S, C]) settled() (S, C, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, m.ctx, m.finals[m.state]
}

// post queues f on the mailbox. It never blocks and never takes m.mu, so it is
// safe from inside any handler; one goroutine drains the queue in order.
func (m *Machine[S, C]) post(f func()) {
	m.pmu.Lock()
	defer m.pmu.Unlock()
	m.pending = append(m.pending, f)
	if !m.draining {
		m.draining = true
		go m.drain()
	}
}

func (m *Machine[S, C]) drain() {
	for {
		m.pmu.Lock()
		if len(m.pending) == 0 {
			m.draining = false
			m.pmu.Unlock()
			return
		}
		f := m.pending[0]
		m.pending = m.pending[1:]
		m.pmu.Unlock()
		f()
	}
}

// key returns a comparable identity for type A without using reflect: the
// boxed nil *A compares equal only to another boxed nil *A.
func key[A any]() any { return (*A)(nil) }
