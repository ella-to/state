// A job that retries on failure: error-driven flow and terminal states.
//
// What this example shows:
//   - handlers written as methods on the context and registered as method
//     expressions: m.On(Pending, (*Job).start)
//   - one action (Fail) fanning out to different next states based on the
//     context (retry while attempts remain, otherwise give up)
//   - actions carrying an error as payload
//   - terminal states are simply states with no handlers: once the machine
//     reaches Succeeded or Failed, every action gets state.ErrInvalid
/*

                ●  m := state.New(Pending, Job{MaxAttempts: 5})
                │
                ▼
          ┌───────────┐
   ┌─────▶│  Pending  │
   │      └─────┬─────┘
   │            │  Do(Start{})                 m
   │            │  attempts++
   │            ▼
   │      ┌───────────┐
   │      │  Running  │
   │      └─────┬─────┘
   │            │
   │    ┌───────┴───────────────────┐
   │    │ Do(Succeed{output})       │ Do(Fail{err})
   │    │ m.On(Running,             │ m.On(Running, (*Job).fail)
   │    │      (*Job).succeed)      │ saves LastErr, then decides:
   │    ▼                           ▼
   │ ┌───────────┐         ┌──────────────────────┐  yes   ┌──────────┐
   │ │ Succeeded │         │ attempts >= MaxAtt.? │ ─────▶ │  Failed  │
   │ └───────────┘         └──────────┬───────────┘        └──────────┘
   │  (terminal)                      │ no                 (terminal)
   │                                  │
   └──────────────────────────────────┘

*/
package main

import (
	"errors"
	"fmt"

	"ella.to/state"
)

type Phase int

const (
	Pending Phase = iota
	Running
	Succeeded
	Failed
)

func (p Phase) String() string {
	return [...]string{"pending", "running", "succeeded", "failed"}[p]
}

type Start struct{}
type Succeed struct{ Output string }
type Fail struct{ Err error }

type Job struct {
	Attempts    int
	MaxAttempts int
	LastErr     error
	Output      string
}

func (j *Job) start(_ Start) (Phase, error) {
	j.Attempts++
	return Running, nil
}

func (j *Job) succeed(a Succeed) (Phase, error) {
	j.Output = a.Output
	return Succeeded, nil
}

// fail retries by transitioning back to Pending until attempts run out.
func (j *Job) fail(a Fail) (Phase, error) {
	j.LastErr = a.Err
	if j.Attempts >= j.MaxAttempts {
		return Failed, nil
	}
	return Pending, nil
}

func main() {
	m := state.New(Pending, Job{MaxAttempts: 5})
	m.On(Pending, (*Job).start)
	m.On(Running, (*Job).succeed)
	m.On(Running, (*Job).fail)

	// Drive the job: the "work" fails twice, then succeeds.
	for attempt := 1; ; attempt++ {
		if _, err := m.Do(Start{}); err != nil {
			panic(err)
		}

		var next Phase
		if attempt < 3 {
			next, _ = m.Do(Fail{Err: fmt.Errorf("attempt %d: connection reset", attempt)})
		} else {
			next, _ = m.Do(Succeed{Output: "report.pdf"})
		}
		fmt.Printf("attempt %d -> %v\n", attempt, next)

		if next == Succeeded || next == Failed {
			break
		}
	}

	// Succeeded is terminal: nothing is registered there.
	_, err := m.Do(Start{})
	fmt.Printf("restart after success: %v (ErrInvalid: %t)\n", err, errors.Is(err, state.ErrInvalid))

	var job Job
	m.Wait(func(_ Phase, j *Job) bool { job = *j; return true })
	fmt.Printf("attempts: %d, last error: %v, output: %s\n", job.Attempts, job.LastErr, job.Output)
}
