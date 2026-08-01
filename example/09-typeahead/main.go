// Search-as-you-type: one activity serving many queries, stale results
// dropped, in-flight fetches canceled.
//
// What this example shows:
//   - a long-lived activity: Enter(Searching) starts a searcher that serves
//     every query typed during one stay in the state, not just the first
//   - coalescing via Wait: keypresses while a fetch is in flight are
//     internal transitions that only mutate the context; when the searcher
//     loops back to Wait it sees the latest query and fetches that,
//     skipping the ones in between
//   - handler-level stale checks: results carry the query they answer, and
//     the handler rejects them if the user has typed since — the machine's
//     lock makes the comparison race-free
//   - cancellation as a transition, again: escape{} leaves Searching, which
//     cancels the activity's context and with it the in-flight fetch
//   - the activity watching its own context inside a Wait condition, so a
//     replaced searcher retires instead of double-fetching
/*

        ●
        │                     keypress{} (internal: update Query;
        ▼                       the searcher coalesces)
   ┌────────┐  keypress{}   ┌───────────┐◀──┐
   │  Idle  │──────────────▶│ Searching │───┘
   └────────┘               └─┬───────┬─┘ Enter: go searcher(ctx)
        ▲                     │       │
        │       results{q,..} │       │      results stale
        │      q == Query     │       │ ┌──▶ (q != Query): rejected,
        │                     ▼       └─┘    searcher fetches again
        │ escape{}       ┌─────────┐
        ├────────────────│ Showing │───┐
        │                └─────────┘   │ keypress{}: re-enter
        │                     ▲        │ Searching, fresh searcher
        └─────────────────────┼────────┘
          (leaving Searching  │
           cancels the fetch) │

*/
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/state"
)

type Status int

const (
	Idle Status = iota
	Searching
	Showing
)

func (s Status) String() string {
	return [...]string{"idle", "searching", "showing"}[s]
}

// Box is the machine's context: what the user typed and what is on screen.
type Box struct {
	Query   string   // live text in the search box
	Results []string // suggestions currently shown
	For     string   // the query Results answer
}

type (
	keypress struct{ text string }
	results  struct {
		q     string // the query these items answer
		items []string
	}
	escape struct{}
)

// search simulates a backend call that honors cancellation.
func search(ctx context.Context, q string) ([]string, error) {
	fmt.Printf("        > fetching %q\n", q)
	select {
	case <-time.After(120 * time.Millisecond):
		return []string{q + "-lang", q + "-docs"}, nil
	case <-ctx.Done():
		fmt.Printf("        > fetch %q canceled\n", q)
		return nil, ctx.Err()
	}
}

func main() {
	m := state.New(Idle, Box{})

	// Typing does the same thing everywhere: record the text and be in
	// Searching. From Searching itself that is an internal transition — the
	// running searcher is kept, it just sees a newer Query.
	onKey := func(b *Box, a keypress) (Status, error) {
		b.Query = a.text
		return Searching, nil
	}
	m.On(Idle, onKey)
	m.On(Searching, onKey)
	m.On(Showing, onKey)

	m.On(Searching, func(b *Box, a results) (Status, error) {
		if a.q != b.Query {
			// The user typed while this fetch was in flight. Reject it;
			// the searcher will fetch the newer query instead.
			fmt.Printf("        > dropped stale %q (query is now %q)\n", a.q, b.Query)
			return 0, fmt.Errorf("stale results for %q", a.q)
		}
		b.Results, b.For = a.items, a.q
		return Showing, nil
	})

	toIdle := func(b *Box, _ escape) (Status, error) {
		b.Query, b.Results, b.For = "", nil, ""
		return Idle, nil
	}
	m.On(Searching, toIdle)
	m.On(Showing, toIdle)

	// The searcher: serves queries for as long as the machine stays in
	// Searching. Wait both coalesces input (only the latest unserved query
	// matters) and tells the searcher when to retire.
	m.Enter(Searching, func(ctx context.Context) {
		served := "" // last query fetched during this stay
		for {
			var q string
			gone := false
			m.Wait(func(s Status, b *Box) bool {
				if ctx.Err() != nil || s != Searching {
					gone = true // left the state, or a fresh searcher took over
					return true
				}
				q = b.Query
				return q != served
			})
			if gone {
				return
			}
			served = q
			items, err := search(ctx, q)
			if err != nil {
				return // canceled mid-fetch; the state has moved on
			}
			// If this lands stale it is rejected and the loop tries again.
			m.Do(results{q: q, items: items})
		}
	})

	show := func(label string) {
		var b Box
		m.Wait(func(s Status, box *Box) bool {
			if s != Showing {
				return false
			}
			b = *box
			return true
		})
		fmt.Printf("%s: %v for %q\n", label, b.Results, b.For)
	}

	// The user types "gop" one letter at a time, faster than the backend
	// answers. Only "g" and "gop" are ever fetched: "go" is coalesced away,
	// and "g"'s answer arrives stale and is dropped.
	fmt.Println("typing g, go, gop ...")
	for _, text := range []string{"g", "go", "gop"} {
		m.Do(keypress{text})
		time.Sleep(30 * time.Millisecond)
	}
	show("shown")

	// More typing re-enters Searching from Showing: a fresh searcher runs.
	fmt.Println("typing gopher ...")
	m.Do(keypress{"gopher"})
	show("shown")

	// Typing and then hitting escape mid-fetch: leaving Searching cancels
	// the in-flight backend call.
	fmt.Println("typing gophers, then escape ...")
	m.Do(keypress{"gophers"})
	time.Sleep(30 * time.Millisecond)
	m.Do(escape{})
	time.Sleep(30 * time.Millisecond) // let the cancellation log print
	fmt.Printf("state: %v\n", m.State())
}
