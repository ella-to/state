// A three-step pipeline driven by child machines: each parent step hands its
// payload to a fresh child machine, and the child completing is what advances
// the parent to the next step.
//
// What this example shows:
//   - Invoke: one call binds a child to a state. start builds the child from
//     the parent's context (state going down), done turns the child's result
//     into the parent's next state (result coming up). No watcher goroutine,
//     no report action, no staleness tag — the machine handles all three
//   - Final and completion: the worker declares Complete final, so entering it
//     ends the worker and hands its context to the parent's done
//   - the stop cascade: canceling the pipeline leaves the step, which stops
//     that step's child, which cancels its activity mid-tick
//   - stopped is not completed: the abandoned child reports nothing, so the
//     parent needs no bookkeeping state to know what to ignore
//   - Start, Done and Result: wire the machine, start it, block on it, read
//     what it came to rest as
/*

   Pipeline                              Worker (one per step)
        ● Start()                             ● (started by Invoke)
        ▼                                     ▼
   ┌───────┐  Invoke: newWorker(payload)  ┌─────────┐  Enter: tick every 60ms
   │ Step1 │────────────────────────────▶ │ Working │─┐  (internal transition)
   └───┬───┘  done(payload = out)         └────┬────┘◀┘
       │      ◀───────────────────────────     │
       ▼                                       ▼
   ┌───────┐                             ┌──────────┐
   │ Step2 │────────────────────────────▶│ Complete │ final: Done closes,
   └───┬───┘  ◀───────────────────────────└──────────┘ Result is the payload
       │
       ▼
   ┌───────┐
   │ Step3 │────────────────────────────▶ (same again)
   └───┬───┘  ◀───────────────────────────
       │
       ▼                Do(cancel{}) in any step ─▶ ┌──────────┐
   ┌──────────┐                                     │ Canceled │ final
   │ Finished │ final                               └────┬─────┘
   └──────────┘                                          │ leaving the step
                                                         ▼ stops its child

*/
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/state"
)

// ---- Worker: the child machine, one per step -------------------------------

type Stage int

const (
	Working Stage = iota
	Complete
)

func (s Stage) String() string { return [...]string{"working", "complete"}[s] }

// Work is the worker's context. Input is what the parent handed down; Output is
// what the parent reads back out.
type Work struct {
	Label  string
	Input  string
	Output string
	Unit   int // units of work finished
	Units  int // units needed
}

type tick struct{}

// newWorker returns a child machine that turns input into an output over units
// ticks. Invoke starts it; nothing runs before that.
func newWorker(label, input string, units int) *state.Machine[Stage, Work] {
	w := state.New(Working, Work{Label: label, Input: input, Units: units})
	w.Final(Complete)

	w.On(Working, func(k *Work, _ tick) (Stage, error) {
		k.Unit++
		fmt.Printf("    %s: %d/%d\n", k.Label, k.Unit, k.Units)
		if k.Unit == k.Units {
			k.Output = k.Input + "." + k.Label
			return Complete, nil
		}
		return Working, nil // internal: the ticker keeps running
	})

	// The crunch. Entering Complete cancels this context and Do starts
	// reporting ErrStopped, so either exit ends the loop.
	w.Enter(Working, func(ctx context.Context) {
		t := time.NewTicker(60 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := w.Do(tick{}); err != nil {
					return
				}
			}
		}
	})

	return w
}

// ---- Pipeline: the parent, one child machine per step ----------------------

type Step int

const (
	Step1 Step = iota
	Step2
	Step3
	Finished
	Canceled
)

func (s Step) String() string {
	return [...]string{"step1", "step2", "step3", "finished", "canceled"}[s]
}

// Doc is the parent's context. Payload flows down into each child and back out
// of it, one step at a time.
type Doc struct {
	Payload string
	Trail   []string
}

type cancel struct{}

// plan describes one step: where it runs, where it goes next, what its child is
// called and how much work that child has to do.
var plan = []struct {
	at    Step
	next  Step
	label string
	units int
}{
	{Step1, Step2, "fetched", 2},
	{Step2, Step3, "transformed", 3},
	{Step3, Finished, "published", 2},
}

func newPipeline(payload string) *state.Machine[Step, Doc] {
	p := state.New(Step1, Doc{Payload: payload})
	p.Final(Finished, Canceled)

	for _, s := range plan {
		p.Invoke(s.at,
			// Down: the child is built from the parent's context, under the
			// parent's lock.
			func(d *Doc) *state.Machine[Stage, Work] {
				fmt.Printf("  pipeline: %v starts %s(%q)\n", s.at, s.label, d.Payload)
				return newWorker(s.label, d.Payload, s.units)
			},
			// Up: the child completing *is* the transition. A child that is
			// stopped instead — by the cancel below — never gets here.
			func(d *Doc, _ Stage, w Work) (Step, error) {
				d.Payload = w.Output
				d.Trail = append(d.Trail, fmt.Sprintf("%v=%s", s.at, w.Output))
				fmt.Printf("  pipeline: %v done, payload %q\n", s.at, d.Payload)
				return s.next, nil
			})

		p.On(s.at, func(*Doc, cancel) (Step, error) { return Canceled, nil })
	}

	return p
}

func main() {
	fmt.Println("full run:")
	p := newPipeline("report")
	p.Start()
	<-p.Done()
	s, d := p.Result()
	fmt.Printf("pipeline %v: %q\n  %v\n\n", s, d.Payload, d.Trail)

	// Canceled while step2's child is mid-work: leaving the step stops the
	// child, and the payload stays whatever step1 produced.
	fmt.Println("canceled during step2:")
	q := newPipeline("memo")
	q.Start()
	time.Sleep(200 * time.Millisecond)
	q.Do(cancel{})
	<-q.Done()
	s, d = q.Result()
	fmt.Printf("pipeline %v: %q\n  %v\n", s, d.Payload, d.Trail)
}
