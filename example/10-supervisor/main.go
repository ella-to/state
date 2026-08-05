// A supervision tree: a pool machine spawns one worker machine per task,
// workers report progress up while they run, and stopping the pool halts every
// worker — recursively, and without any bookkeeping.
//
// What this example shows:
//   - Spawn: children attached to the machine rather than to a state, so their
//     number is decided at runtime. Spawn is safe from inside a handler, which
//     is where a dynamic count usually comes from
//   - talking upward with Send: a worker's own handler tells the pool about
//     progress. Do would take a second lock and could deadlock; Send queues and
//     returns, so this is the safe direction-agnostic call
//   - no actor-ref type: a worker holds *state.Machine[Pool, Report] and the
//     pool holds *state.Machine[Phase, Task]. Both are concrete types, so the
//     whole tree stays typed with no registry and no interface
//   - the cascade: entering Stopped halts the pool, which halts its children,
//     which cancels their activities mid-step. Nothing is two-levels-only
//   - stopped is not completed, so a halted worker reports nothing and the pool
//     needs no state to remember which reports to ignore
//   - a harmless double-stop: halting a worker that already delivered does
//     nothing, so the supervisor never has to check first
/*

   Pool                                  Worker (one per task)
        ● Start()                             ● (started by Spawn)
        │ Do(boot{})                          ▼
        ▼      Spawn: one per task      ┌─────────┐  Enter: one step per 80ms
   ┌─────────┐───────────────────────▶  │ Working │─┐  (internal transition)
   │ Running │  Send(progress{}) ◀───── └────┬────┘◀┘
   └──┬───┬──┘  done(Delivered) ◀────        │
      │   │                                  ▼
      │   │ every worker delivered     ┌───────────┐
      │   ▼                            │ Delivered │ final
      │ ┌──────────┐                   └───────────┘
      │ │ Finished │ final
      │ └──────────┘
      │ Do(shutdown{})
      ▼
   ┌─────────┐ final — halting the pool halts every worker still going,
   │ Stopped │ and each of those would halt its own children in turn
   └─────────┘

*/
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/state"
)

// ---- Worker: one small machine per task -----------------------------------

type Phase int

const (
	Working Phase = iota
	Delivered
)

func (p Phase) String() string { return [...]string{"working", "delivered"}[p] }

// Task is the worker's context. Pool is the parent, held as a plain typed
// pointer — that is all an "actor ref" needs to be here.
type Task struct {
	ID    int
	Step  int // completed so far
	Steps int // total needed
	Pool  *state.Machine[Pool, Report]
}

type step struct{}

func newWorker(id, steps int, pool *state.Machine[Pool, Report]) *state.Machine[Phase, Task] {
	w := state.New(Working, Task{ID: id, Steps: steps, Pool: pool})
	w.Final(Delivered)

	w.On(Working, func(t *Task, _ step) (Phase, error) {
		t.Step++
		// Upward, from inside a handler: queued, never blocks, cannot deadlock.
		t.Pool.Send(progress{id: t.ID, step: t.Step, steps: t.Steps})
		if t.Step == t.Steps {
			return Delivered, nil
		}
		return Working, nil // internal: the crunch keeps going
	})

	// The crunch: one step per tick until the handler says Delivered, or the
	// worker is halted and its context dies mid-step.
	w.Enter(Working, func(ctx context.Context) {
		t := time.NewTicker(80 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := w.Do(step{}); err != nil {
					return
				}
			}
		}
	})

	return w
}

// ---- Pool: a machine that spawns and outlives its workers ------------------

type Pool int

const (
	Idle Pool = iota
	Running
	Finished
	Stopped
)

func (p Pool) String() string {
	return [...]string{"idle", "running", "finished", "stopped"}[p]
}

// Report is the pool's context.
type Report struct {
	Total     int
	Delivered int
	Kids      []*state.Machine[Phase, Task]
}

type (
	boot     struct{ steps []int }
	progress struct{ id, step, steps int }
	shutdown struct{}
)

func newPool() *state.Machine[Pool, Report] {
	m := state.New(Idle, Report{})
	m.Final(Finished, Stopped)

	// Spawning from a handler: the number of children is whatever boot says.
	m.On(Idle, func(r *Report, b boot) (Pool, error) {
		r.Total = len(b.steps)
		for i, n := range b.steps {
			w := newWorker(i+1, n, m)
			r.Kids = append(r.Kids, w) // keep the handles to send to later
			m.Spawn(w, func(r *Report, _ Phase, t Task) (Pool, error) {
				r.Delivered++
				fmt.Printf("  pool: worker %d delivered (%d/%d)\n", t.ID, r.Delivered, r.Total)
				if r.Delivered == r.Total {
					return Finished, nil
				}
				return Running, nil
			})
		}
		return Running, nil
	})

	m.On(Running, func(_ *Report, a progress) (Pool, error) {
		fmt.Printf("    worker %d: %d/%d\n", a.id, a.step, a.steps)
		return Running, nil // internal: nothing to recycle
	})

	m.On(Running, func(*Report, shutdown) (Pool, error) { return Stopped, nil })

	return m
}

func main() {
	// A quick batch: every worker delivers, so the pool finishes by itself.
	fmt.Println("batch of 3 quick tasks:")
	quick := newPool()
	quick.Start()
	quick.Do(boot{steps: []int{1, 2, 2}})
	<-quick.Done()
	s, r := quick.Result()
	fmt.Printf("pool %v: %d/%d delivered\n\n", s, r.Delivered, r.Total)

	// A mixed batch, shut down mid-run: worker 1 delivers in time, workers 2
	// and 3 are halted mid-task by the cascade and report nothing.
	fmt.Println("batch of 3 tasks, shutdown after 250ms:")
	mixed := newPool()
	mixed.Start()
	mixed.Do(boot{steps: []int{2, 6, 8}})
	time.Sleep(250 * time.Millisecond)
	mixed.Do(shutdown{})
	<-mixed.Done()
	s, r = mixed.Result()

	halted := 0
	for _, w := range r.Kids {
		if p, _ := w.Result(); p != Delivered {
			halted++
		}
	}
	fmt.Printf("pool %v: %d/%d delivered, %d halted\n", s, r.Delivered, r.Total, halted)
}
