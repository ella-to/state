// Async work owned by a state: Enter runs the fetch, transitions cancel it.
//
// What this example shows:
//   - Enter(Loading, fn): when the machine transitions into Loading it
//     starts fn in a goroutine, and when it leaves Loading it cancels fn's
//     context — the state owns the work, so there is no wrapper type, no
//     CancelFunc to store, no extra public API
//   - cancellation as a transition: Do(cancel{}) is all it takes; leaving
//     Loading cancels the activity as a side effect
//   - retries: the attempt loop lives inside the activity, while the fail
//     handler decides retry vs give up (compare 03-retry)
//   - internal transitions: Do(fail) with attempts left returns Loading
//     from Loading, which does NOT cancel or restart the activity — it
//     keeps looping
//   - stale results resolving themselves: a result reported after the
//     machine moved on is rejected with state.ErrInvalid, so a canceled
//     download can never flip the machine to Succeeded
//   - re-entry: Failed -> Loading is a real state change, so a fresh
//     activity runs (manual "try again")
/*

        ●  m := newDownload(fetch, attempts)
        │
        ▼
   ┌────────┐  Do(start{})  ┌───────────┐ Enter: go fetch(ctx)
   │  Idle  │──────────────▶│  Loading  │─┐
   └────────┘               └─┬───┬───┬─┘◀┘ Do(fail{err}), attempts left
                              │   │   │     (internal: activity keeps looping)
               Do(succeed{d}) │   │   │ Do(cancel{}) — leaving Loading
                              │   │   │ cancels the activity's context
                              ▼   │   ▼
                    ┌───────────┐ │ ┌──────────┐
                    │ Succeeded │ │ │ Canceled │
                    └───────────┘ │ └──────────┘
                     (terminal)   │  (terminal)
                                  │ Do(fail{err}), attempts exhausted
                                  ▼
                             ┌────────┐  Do(retry{})
                             │ Failed │────────────▶ Loading again
                             └────────┘  (re-entry: a fresh activity runs)

*/
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ella.to/state"
)

// Status is the observable state of a download.
type Status int

const (
	Idle Status = iota
	Loading
	Succeeded
	Failed
	Canceled
)

func (s Status) String() string {
	return [...]string{"idle", "loading", "succeeded", "failed", "canceled"}[s]
}

// Download is the machine's context: it counts attempts and keeps the outcome.
type Download struct {
	Attempts    int
	MaxAttempts int
	Data        string
	Err         error
}

type (
	start   struct{}
	succeed struct{ data string }
	fail    struct{ err error }
	cancel  struct{}
	retry   struct{}
)

// newDownload wires a machine around fetch. fetch must return early when its
// context is done; the machine cancels that context whenever it leaves
// Loading.
func newDownload(fetch func(context.Context) (string, error), attempts int) *state.Machine[Status, Download] {
	m := state.New(Idle, Download{MaxAttempts: attempts})

	m.On(Idle, func(d *Download, _ start) (Status, error) { return Loading, nil })

	m.On(Loading, func(d *Download, a succeed) (Status, error) {
		d.Data = a.data
		return Succeeded, nil
	})
	// fail stays in Loading (internal — the activity keeps its loop) until
	// attempts run out, then gives up.
	m.On(Loading, func(d *Download, a fail) (Status, error) {
		d.Attempts++
		d.Err = a.err
		if d.Attempts >= d.MaxAttempts {
			return Failed, nil
		}
		return Loading, nil
	})
	m.On(Loading, func(d *Download, _ cancel) (Status, error) {
		d.Err = context.Canceled
		return Canceled, nil
	})

	// Manual "try again": re-entering Loading starts a fresh activity.
	m.On(Failed, func(d *Download, _ retry) (Status, error) {
		d.Attempts = 0
		return Loading, nil
	})

	// The activity: one goroutine per stay in Loading. It reports outcomes
	// with plain Do calls; the machine decides what they mean.
	m.Enter(Loading, func(ctx context.Context) {
		for {
			data, err := fetch(ctx)
			if err == nil {
				// If cancel{} won the race the machine is already in
				// Canceled and this Do is rejected with ErrInvalid: the
				// stale result is simply dropped.
				m.Do(succeed{data})
				return
			}
			if next, _ := m.Do(fail{err}); next != Loading {
				return // Failed, or Canceled beat us to it
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond): // pause between attempts
			}
		}
	})

	return m
}

// result blocks until the download settles, then returns the data
// (Succeeded) or the error (Failed or Canceled).
func result(m *state.Machine[Status, Download]) (string, error) {
	var (
		data string
		err  error
	)
	m.Wait(func(s Status, d *Download) bool {
		switch s {
		case Succeeded:
			data = d.Data
		case Failed, Canceled:
			err = d.Err
		default:
			return false
		}
		return true
	})
	return data, err
}

func main() {
	// A flaky server: fails twice, then delivers. The fail handler retries
	// inside the same activity until it succeeds.
	calls := 0
	flaky := newDownload(func(context.Context) (string, error) {
		calls++
		if calls < 3 {
			return "", fmt.Errorf("call %d: connection reset", calls)
		}
		return "report.pdf", nil
	}, 4)

	flaky.Do(start{})
	data, err := result(flaky)
	fmt.Printf("flaky:  status=%v data=%q err=%v\n", flaky.State(), data, err)

	// A slow download, canceled mid-flight. Do(cancel{}) leaves Loading,
	// which cancels the activity's context — no CancelFunc anywhere.
	finished := make(chan struct{})
	slow := newDownload(func(ctx context.Context) (string, error) {
		defer close(finished)
		select {
		case <-time.After(time.Second):
			return "huge.iso", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, 1)

	slow.Do(start{})
	slow.Do(cancel{})
	data, err = result(slow)
	fmt.Printf("slow:   status=%v data=%q err=%v\n", slow.State(), data, err)
	<-finished // the fetch really did stop early, released by its context

	// A dead server, then a fixed one: attempts run out (Failed), the user
	// clicks "try again", and re-entering Loading runs a fresh activity.
	up := false
	dead := newDownload(func(context.Context) (string, error) {
		if !up {
			return "", errors.New("503 service unavailable")
		}
		return "backup.tar", nil
	}, 2)

	dead.Do(start{})
	data, err = result(dead)
	fmt.Printf("dead:   status=%v data=%q err=%v\n", dead.State(), data, err)

	up = true // the server comes back...
	dead.Do(retry{})
	data, err = result(dead)
	fmt.Printf("retry:  status=%v data=%q err=%v\n", dead.State(), data, err)
}
