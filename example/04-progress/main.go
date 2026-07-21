// A download with live observers: every Wait notification pattern.
//
// What this example shows:
//   - self-transitions that mutate the context without changing state
//     (each chunk keeps the machine in Downloading)
//   - Wait re-evaluates after every successful Do, so waiters observe
//     context changes, not just state changes
//   - several waiters on one machine, each with a different condition:
//     1. a progress logger that wakes only when the received count changes
//        (change detection by comparing against the last value it saw)
//     2. a milestone waiter blocking on a context value (>= 50%)
//     3. main blocking on the terminal state
//   - copying values out of the context through variables captured by the
//     condition, which runs under the machine's lock
package main

import (
	"fmt"
	"sync"
	"time"

	"ella.to/state"
)

type Phase int

const (
	Downloading Phase = iota
	Done
)

type Chunk struct{ Bytes int }

type Transfer struct {
	Received int
	Total    int
}

func main() {
	m := state.New(Downloading, Transfer{Total: 100})

	m.On(Downloading, func(t *Transfer, a Chunk) (Phase, error) {
		t.Received += a.Bytes
		if t.Received >= t.Total {
			return Done, nil
		}
		return Downloading, nil // self-transition: progress, same state
	})

	var wg sync.WaitGroup

	// Observer 1: log every change in progress. The condition returns false
	// while nothing changed, so the goroutine sleeps between chunks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		last := -1
		for {
			var received, total int
			done := false
			m.Wait(func(p Phase, t *Transfer) bool {
				if p == Done {
					done = true
					return true
				}
				if t.Received == last {
					return false // no news; keep waiting
				}
				received, total = t.Received, t.Total
				return true
			})
			if done {
				fmt.Println("logger: download complete")
				return
			}
			last = received
			fmt.Printf("logger: %d/%d bytes\n", received, total)
		}
	}()

	// Observer 2: block once until a context value crosses a threshold.
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.Wait(func(p Phase, t *Transfer) bool {
			return p == Done || t.Received*2 >= t.Total
		})
		fmt.Println("milestone: halfway there")
	}()

	// Producer: feed chunks until the machine reports Done.
	for _, n := range []int{10, 15, 30, 25, 20} {
		time.Sleep(20 * time.Millisecond)
		if next, err := m.Do(Chunk{Bytes: n}); err != nil {
			panic(err)
		} else if next == Done {
			break
		}
	}

	// Main is a waiter too: block on the terminal state.
	m.Wait(func(p Phase, _ *Transfer) bool { return p == Done })
	wg.Wait()
	fmt.Println("main: all observers finished")
}
