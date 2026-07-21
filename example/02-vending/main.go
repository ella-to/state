// A vending machine: actions with payloads and every way a machine says no.
//
// What this example shows:
//   - string states (S can be any comparable type)
//   - actions carrying payloads (coins, item names)
//   - the two rejection paths, and that both leave the machine untouched:
//     1. structurally invalid: no handler for the action in the current
//        state -> Do returns an error wrapping state.ErrInvalid
//     2. semantically invalid: the handler returns an error (unknown item,
//        not enough credit, out of stock)
//   - the same action type registered in two states with different handlers
//     (Insert works in "idle" and in "paid")
//   - a self-transition: inserting more coins keeps the machine in "paid"
//     while the context accumulates credit
package main

import (
	"errors"
	"fmt"

	"ella.to/state"
)

const (
	idle = "idle"
	paid = "paid"
)

type Insert struct{ Cents int }
type Select struct{ Item string }
type Refund struct{}

type Vending struct {
	Credit int
	Price  map[string]int
	Stock  map[string]int
}

func main() {
	m := state.New(idle, Vending{
		Price: map[string]int{"cola": 150, "chips": 100},
		Stock: map[string]int{"cola": 1, "chips": 0},
	})

	insert := func(v *Vending, a Insert) (string, error) {
		if a.Cents <= 0 {
			return "", fmt.Errorf("bad coin: %d", a.Cents)
		}
		v.Credit += a.Cents
		return paid, nil
	}
	m.On(idle, insert) // first coin: idle -> paid
	m.On(paid, insert) // more coins: paid -> paid (self-transition)

	m.On(paid, func(v *Vending, a Select) (string, error) {
		price, ok := v.Price[a.Item]
		if !ok {
			return "", fmt.Errorf("unknown item %q", a.Item)
		}
		if v.Stock[a.Item] == 0 {
			return "", fmt.Errorf("%s is out of stock", a.Item)
		}
		if v.Credit < price {
			return "", fmt.Errorf("%s costs %d, credit is %d", a.Item, price, v.Credit)
		}
		v.Stock[a.Item]--
		fmt.Printf("dispensing %s, returning %d change\n", a.Item, v.Credit-price)
		v.Credit = 0
		return idle, nil
	})

	m.On(paid, func(v *Vending, _ Refund) (string, error) {
		fmt.Printf("refunding %d\n", v.Credit)
		v.Credit = 0
		return idle, nil
	})

	// Structurally invalid: Select has no handler in "idle".
	_, err := m.Do(Select{"cola"})
	fmt.Printf("select before paying: %v (ErrInvalid: %t)\n", err, errors.Is(err, state.ErrInvalid))

	// Semantically invalid: handlers reject, state stays "paid" each time.
	m.Do(Insert{100})
	for _, a := range []Select{{"beer"}, {"chips"}, {"cola"}} {
		if _, err := m.Do(a); err != nil {
			fmt.Println("rejected:", err)
		}
	}

	// Top up with a second coin (self-transition) and buy.
	m.Do(Insert{50})
	if _, err := m.Do(Select{"cola"}); err != nil {
		panic(err)
	}

	fmt.Println("machine is back to:", m.State())
}
