package state_test

import (
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
