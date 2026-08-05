// Timers: states that advance themselves after a dwell.
//
// What this example shows:
//   - After(state, d, action): the timeout pattern in one line. A state can
//     time itself out with no scheduler and no goroutine of yours anywhere
//   - the same action reused across several states, each with its own
//     duration — After is per state, so the durations differ freely
//   - interrupting a pending timer: Do(emergency{}) leaves the state, which
//     cancels the timer — the tick it would have sent never fires
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

	// After is the dwell: entering the state arms a timer that Do-es the
	// action, and leaving the state cancels it. Written out by hand it is
	// Enter plus a select on ctx.Done and time.After — see 07-async for that
	// shape.
	m.After(Red, 600*time.Millisecond, tick{})
	m.After(Green, 400*time.Millisecond, tick{})
	m.After(Yellow, 200*time.Millisecond, tick{})

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
