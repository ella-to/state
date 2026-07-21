// Racing goroutines: exactly-once claim, first Do wins.
//
// What this example shows:
//   - actions are serialized: concurrent Do calls run their handlers one at
//     a time, each seeing the state left by the previous one
//   - the first-wins pattern: five goroutines race to claim a job; the
//     winning Claim moves Open -> Claimed, and every later Claim finds no
//     handler in Claimed and gets state.ErrInvalid
//   - using Do's error to learn whether this goroutine's action was the one
//     that advanced the machine — no extra locks or atomics needed
//   - run it with -race: the workers share the machine and nothing else
package main

import (
	"errors"
	"fmt"
	"sync"

	"ella.to/state"
)

type Phase int

const (
	Open Phase = iota
	Claimed
)

type Claim struct{ Worker int }

type Ticket struct {
	Owner int
}

func main() {
	m := state.New(Open, Ticket{Owner: -1})

	m.On(Open, func(t *Ticket, a Claim) (Phase, error) {
		t.Owner = a.Worker
		return Claimed, nil
	})
	// Claimed has no handlers: it is terminal, so later claims are invalid.

	var wg sync.WaitGroup
	for id := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Do(Claim{Worker: id}); err != nil {
				if !errors.Is(err, state.ErrInvalid) {
					panic(err)
				}
				fmt.Printf("worker %d: too late\n", id)
				return
			}
			fmt.Printf("worker %d: claimed it!\n", id)
		}()
	}
	wg.Wait()

	var owner int
	m.Wait(func(_ Phase, t *Ticket) bool { owner = t.Owner; return true })
	fmt.Printf("final owner: worker %d\n", owner)
}
