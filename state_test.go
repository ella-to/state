package state_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"ella.to/state"
)

type counter struct{ total int }

type (
	add  struct{ n int }
	stop struct{}
)

func newCounter() *state.Machine[string, counter] {
	m := state.New("running", counter{})
	m.On("running", func(c *counter, a add) (string, error) {
		if a.n < 0 {
			return "", errors.New("negative")
		}
		c.total += a.n
		return "running", nil
	})
	m.On("running", func(c *counter, _ stop) (string, error) {
		return "stopped", nil
	})
	return m
}

func TestTransitions(t *testing.T) {
	m := newCounter()

	if s, err := m.Do(add{5}); err != nil || s != "running" {
		t.Fatalf("Do(add{5}) = %q, %v", s, err)
	}
	if s, err := m.Do(stop{}); err != nil || s != "stopped" {
		t.Fatalf("Do(stop{}) = %q, %v", s, err)
	}

	var total int
	m.Wait(func(_ string, c *counter) bool { total = c.total; return true })
	if total != 5 {
		t.Fatalf("total = %d, want 5", total)
	}
}

func TestInvalidAction(t *testing.T) {
	m := newCounter()
	m.Do(stop{})

	// add is not registered in "stopped".
	if _, err := m.Do(add{1}); !errors.Is(err, state.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if s := m.State(); s != "stopped" {
		t.Fatalf("state = %q, want stopped", s)
	}
}

func TestHandlerErrorKeepsState(t *testing.T) {
	m := newCounter()
	if _, err := m.Do(add{-1}); err == nil {
		t.Fatal("expected error")
	}
	if s := m.State(); s != "running" {
		t.Fatalf("state = %q, want running", s)
	}
}

func TestConcurrentDo(t *testing.T) {
	m := newCounter()

	const goroutines, each = 100, 100
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				m.Do(add{1})
			}
		}()
	}
	wg.Wait()

	var total int
	m.Wait(func(_ string, c *counter) bool { total = c.total; return true })
	if total != goroutines*each {
		t.Fatalf("total = %d, want %d", total, goroutines*each)
	}
}

func BenchmarkDo(b *testing.B) {
	m := newCounter()
	b.ReportAllocs()
	for b.Loop() {
		m.Do(add{1})
	}
}

type (
	start  struct{}
	finish struct{}
)

// newWorkflow returns an idle -> working -> done machine with no activities.
func newWorkflow() *state.Machine[string, counter] {
	m := state.New("idle", counter{})
	m.On("idle", func(*counter, start) (string, error) { return "working", nil })
	m.On("working", func(c *counter, a add) (string, error) {
		c.total += a.n
		return "working", nil
	})
	m.On("working", func(*counter, finish) (string, error) { return "done", nil })
	m.On("working", func(*counter, stop) (string, error) { return "stopped", nil })
	return m
}

func TestEnterRunsActivity(t *testing.T) {
	m := newWorkflow()
	m.Enter("working", func(context.Context) {
		m.Do(add{7})
		m.Do(finish{})
	})

	m.Do(start{})
	m.Wait(func(s string, _ *counter) bool { return s == "done" })

	var total int
	m.Wait(func(_ string, c *counter) bool { total = c.total; return true })
	if total != 7 {
		t.Fatalf("total = %d, want 7", total)
	}
}

func TestEnterCancelsOnLeave(t *testing.T) {
	m := newWorkflow()
	entered := make(chan context.Context, 1)
	m.Enter("working", func(ctx context.Context) { entered <- ctx })

	m.Do(start{})
	ctx := <-entered
	if ctx.Err() != nil {
		t.Fatal("context canceled before leaving the state")
	}

	m.Do(stop{})
	// Cancellation happens inside Do, so it is visible once Do returns.
	if ctx.Err() == nil {
		t.Fatal("context not canceled after leaving the state")
	}
}

func TestEnterSelfTransitionIsInternal(t *testing.T) {
	m := newWorkflow()
	entered := make(chan context.Context, 1)
	m.Enter("working", func(ctx context.Context) { entered <- ctx })

	m.Do(start{})
	ctx := <-entered

	// add keeps the machine in "working": the activity must survive.
	if _, err := m.Do(add{1}); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("self-transition canceled the activity")
	}
	select {
	case <-entered:
		t.Fatal("self-transition restarted the activity")
	default:
	}

	m.Do(stop{})
	if ctx.Err() == nil {
		t.Fatal("context not canceled after leaving the state")
	}
}

func TestEnterStaleActionRejected(t *testing.T) {
	m := newWorkflow()
	release := make(chan struct{})
	result := make(chan error, 1)
	m.Enter("working", func(context.Context) {
		<-release // hold the result until the machine has moved on
		_, err := m.Do(finish{})
		result <- err
	})

	m.Do(start{})
	m.Do(stop{}) // leave "working" before the activity reports
	close(release)

	if err := <-result; !errors.Is(err, state.ErrInvalid) {
		t.Fatalf("stale Do err = %v, want ErrInvalid", err)
	}
	if s := m.State(); s != "stopped" {
		t.Fatalf("state = %q, want stopped", s)
	}
}

func TestChildMachineStopsWithParent(t *testing.T) {
	parent := newWorkflow()
	childStopped := make(chan string, 1)

	parent.Enter("working", func(ctx context.Context) {
		child := newWorkflow()
		context.AfterFunc(ctx, func() {
			s, _ := child.Do(stop{})
			childStopped <- s
		})
		child.Do(start{})
	})

	parent.Do(start{})
	parent.Do(stop{}) // parent leaves "working" -> child must be halted

	if s := <-childStopped; s != "stopped" {
		t.Fatalf("child state = %q, want stopped", s)
	}
}

func TestWaitWakesOnChange(t *testing.T) {
	m := newCounter()

	done := make(chan struct{})
	go func() {
		m.Wait(func(s string, _ *counter) bool { return s == "stopped" })
		close(done)
	}()

	m.Do(add{1}) // wakes the waiter, cond still false
	m.Do(stop{})
	<-done
}
