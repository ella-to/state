// Timers as entry activities: states that advance themselves after a dwell.
//
// What this example shows:
//   - the timeout pattern: Enter starts a timer, the timer Do-es an action;
//     a state can therefore time itself out with no scheduler anywhere
//   - one activity factory (dwell) reused across several states, each with
//     its own duration
//   - interrupting a pending timer: Do(emergency{}) leaves the state, which
//     cancels the timer's context — the tick it would have sent never fires
//   - the second safety net: even a tick that somehow slipped through would
//     be rejected in Flashing with state.ErrInvalid, because Flashing has no
//     tick handler; the machine is protected twice
//   - the quiescent-start idiom: activities only run on transitions, so the
//     machine boots in Off and is switched on with an action
/*

     ●  power{}         tick after 600ms
     │   ┌───────────────────────────────────┐
     ▼   ▼                                   │
   ┌───────┐  tick after   ┌───────┐  tick after   ┌────────┐
   │  Red  │──────────────▶│ Green │──────────────▶│ Yellow │
   └───┬───┘  600ms        └───┬───┘  400ms        └───┬────┘
       │                       │                       │
       │ emergency{}           │ emergency{}           │ emergency{}
       │                       ▼                       │
       │                ┌──────────────┐               │
       └───────────────▶│   Flashing   │◀──────────────┘
                        └──────────────┘
                          (terminal — the pending tick is canceled,
                           and would be rejected anyway)

*/
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/state"
)

type Light int

const (
	Off Light = iota
	Red
	Green
	Yellow
	Flashing
)

func (l Light) String() string {
	return [...]string{"off", "red", "green", "yellow", "flashing"}[l]
}

type (
	power     struct{}
	tick      struct{}
	emergency struct{}
)

// Clock is the machine's context.
type Clock struct {
	Ticks int
}

func main() {
	m := state.New(Off, Clock{})

	m.On(Off, func(*Clock, power) (Light, error) { return Red, nil })

	advance := func(to Light) func(*Clock, tick) (Light, error) {
		return func(c *Clock, _ tick) (Light, error) {
			c.Ticks++
			return to, nil
		}
	}
	m.On(Red, advance(Green))
	m.On(Green, advance(Yellow))
	m.On(Yellow, advance(Red))

	toFlashing := func(*Clock, emergency) (Light, error) { return Flashing, nil }
	m.On(Red, toFlashing)
	m.On(Green, toFlashing)
	m.On(Yellow, toFlashing)

	// dwell returns an entry activity that ticks the machine after d —
	// unless the state is left first, which cancels ctx and the timer with
	// it.
	dwell := func(d time.Duration) func(context.Context) {
		return func(ctx context.Context) {
			select {
			case <-time.After(d):
				m.Do(tick{})
			case <-ctx.Done(): // left the state some other way; stand down
			}
		}
	}
	m.Enter(Red, dwell(600*time.Millisecond))
	m.Enter(Green, dwell(400*time.Millisecond))
	m.Enter(Yellow, dwell(200*time.Millisecond))

	start := time.Now()
	m.Do(power{}) // Off -> Red starts the first timer

	// An emergency vehicle arrives mid-dwell: the pending Red timer is
	// canceled and the light goes straight to Flashing.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		m.Do(emergency{})
	}()

	// Watch the light change, 04-progress style: remember the last state
	// seen and wake when it differs.
	last := Off
	for last != Flashing {
		var ticks int
		m.Wait(func(l Light, c *Clock) bool {
			if l == last {
				return false
			}
			last, ticks = l, c.Ticks
			return true
		})
		fmt.Printf("%5s  light is %v (ticks=%d)\n",
			time.Since(start).Round(100*time.Millisecond), last, ticks)
	}
}
