// The smallest possible machine: three states cycling on a single action.
//
// What this example shows:
//   - defining states as an enum and the context as a plain struct
//   - a payload-less action (an empty struct)
//   - Do returning the state the machine landed in
//   - reading the context afterwards with a Wait snapshot
package main

import (
	"fmt"

	"ella.to/state"
)

type Light int

const (
	Red Light = iota
	Green
	Yellow
)

func (l Light) String() string { return [...]string{"red", "green", "yellow"}[l] }

// Tick carries no data; its type alone is the event.
type Tick struct{}

// Clock is the machine's context.
type Clock struct {
	Ticks int
}

func main() {
	m := state.New(Red, Clock{})

	m.On(Red, func(c *Clock, _ Tick) (Light, error) {
		c.Ticks++
		return Green, nil
	})
	m.On(Green, func(c *Clock, _ Tick) (Light, error) {
		c.Ticks++
		return Yellow, nil
	})
	m.On(Yellow, func(c *Clock, _ Tick) (Light, error) {
		c.Ticks++
		return Red, nil
	})

	for range 6 {
		light, err := m.Do(Tick{})
		if err != nil {
			panic(err)
		}
		fmt.Println("light is now", light)
	}

	// A Wait whose condition returns true immediately is a synchronized read.
	var ticks int
	m.Wait(func(_ Light, c *Clock) bool { ticks = c.Ticks; return true })
	fmt.Println("total ticks:", ticks)
}
