// A supervision tree: a parent machine whose Running state owns child
// machines; stopping the parent halts every child, recursively.
//
// What this example shows:
//   - child machines as resources of a parent state: workers are created in
//     Enter(Running) and bound to its context with context.AfterFunc, so
//     leaving Running — for any reason — halts them all
//   - the cascade: parent leaves Running -> AfterFunc halts each worker ->
//     each worker leaves Working -> its own activity's context is canceled
//     mid-step. A worker that spawned grandchildren the same way would keep
//     the cascade going; nothing about it is two-levels-only
//   - "stopping a machine" is not a special operation, just a transition to
//     a state with no way out — the FSM model stays pure
//   - children reporting up: the parent watches each worker with Wait in a
//     goroutine and applies settled{} to itself — and Stopped keeps a
//     settled handler (a bookkeeping self-transition) so halt reports
//     arriving after the shutdown are still counted
//   - a harmless double-stop: halting a worker that already delivered is
//     rejected with state.ErrInvalid (Delivered has no halt handler), so the
//     supervisor never needs to check first
/*

   Supervisor                              Worker (one per task)
        ●                                       ●
        │ Do(boot{})                            │ Do(begin{})
        ▼                                       ▼
   ┌─────────┐  Enter: for each task       ┌─────────┐  Enter: crunch, one
   │ Running │    spawn a Worker,          │ Working │─┐  Do(step{}) per step
   └──┬───┬──┘    AfterFunc(ctx, halt),    └──┬───┬──┘◀┘  (internal)
      │   │       watch it with Wait          │   │
      │   │                                   │   │ Do(halt{}) from the
      │   │ Do(settled{})                     │   │ parent's AfterFunc
      │   │ from every watcher                ▼   ▼
      │   │                        ┌───────────┐ ┌────────┐
      │   ▼                        │ Delivered │ │ Halted │
      │ ┌──────────┐               └───────────┘ └────────┘
      │ │ Finished │  all delivered
      │ └──────────┘
      │ Do(shutdown{})   — leaving Running cancels its context,
      ▼                    which halts every worker still going
   ┌─────────┐
   │ Stopped │
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
	Queued Phase = iota
	Working
	Delivered
	Halted
)

func (p Phase) String() string {
	return [...]string{"queued", "working", "delivered", "halted"}[p]
}

// Task is the worker's context.
type Task struct {
	ID    int
	Step  int // completed so far
	Steps int // total needed
}

type (
	begin struct{}
	step  struct{}
	halt  struct{}
)

func newWorker(id, steps int) *state.Machine[Phase, Task] {
	w := state.New(Queued, Task{ID: id, Steps: steps})

	w.On(Queued, func(*Task, begin) (Phase, error) { return Working, nil })
	w.On(Working, func(t *Task, _ step) (Phase, error) {
		t.Step++
		if t.Step == t.Steps {
			return Delivered, nil
		}
		return Working, nil // internal: the crunch keeps going
	})
	w.On(Queued, func(*Task, halt) (Phase, error) { return Halted, nil })
	w.On(Working, func(*Task, halt) (Phase, error) { return Halted, nil })
	// No halt in Delivered or Halted: a late halt is rejected, harmlessly.

	// The crunch: one tick per step until the step handler says Delivered,
	// or the context dies because the worker was halted.
	w.Enter(Working, func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return // halted mid-task
			case <-time.After(80 * time.Millisecond):
			}
			if next, _ := w.Do(step{}); next != Working {
				return
			}
		}
	})

	return w
}

// ---- Supervisor: a machine whose Running state owns the workers ------------

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

// Report is the supervisor's context.
type Report struct {
	Total     int
	Delivered int
	Halted    int
}

type (
	boot    struct{}
	settled struct {
		id    int
		phase Phase // Delivered or Halted
	}
	shutdown struct{}
)

// newPool supervises one worker per entry of steps. The workers live exactly
// as long as the pool stays in Running.
func newPool(steps []int) *state.Machine[Pool, Report] {
	m := state.New(Idle, Report{Total: len(steps)})

	m.On(Idle, func(*Report, boot) (Pool, error) { return Running, nil })

	// While running, only deliveries can settle a worker (halts are what
	// leaving Running causes). The last one finishes the pool.
	m.On(Running, func(r *Report, a settled) (Pool, error) {
		r.Delivered++
		fmt.Printf("  pool: worker %d delivered (%d/%d)\n", a.id, r.Delivered, r.Total)
		if r.Delivered == r.Total {
			return Finished, nil
		}
		return Running, nil
	})
	m.On(Running, func(*Report, shutdown) (Pool, error) { return Stopped, nil })

	// After shutdown the halt reports (and any delivery that beat its halt)
	// still arrive: Stopped keeps a settled handler purely for bookkeeping,
	// self-transitioning until every worker is accounted for.
	m.On(Stopped, func(r *Report, a settled) (Pool, error) {
		if a.phase == Delivered {
			r.Delivered++
		} else {
			r.Halted++
		}
		fmt.Printf("  pool: worker %d %v\n", a.id, a.phase)
		return Stopped, nil
	})

	m.Enter(Running, func(ctx context.Context) {
		for i, n := range steps {
			w := newWorker(i+1, n)

			// The binding: when Running's context dies, halt the worker.
			// Its own activity then loses its context, and so on down.
			context.AfterFunc(ctx, func() { w.Do(halt{}) })

			// The watcher: report up when this worker settles.
			go func() {
				var last Phase
				w.Wait(func(p Phase, _ *Task) bool {
					last = p
					return p == Delivered || p == Halted
				})
				m.Do(settled{id: i + 1, phase: last})
			}()

			w.Do(begin{})
		}
	})

	return m
}

// settle blocks until the pool is done AND every worker is accounted for.
func settle(m *state.Machine[Pool, Report]) (Pool, Report) {
	var r Report
	var p Pool
	m.Wait(func(s Pool, rep *Report) bool {
		if s == Finished || (s == Stopped && rep.Delivered+rep.Halted == rep.Total) {
			p, r = s, *rep
			return true
		}
		return false
	})
	return p, r
}

func main() {
	// A quick batch: every worker delivers, the pool finishes by itself.
	fmt.Println("batch of 3 quick tasks:")
	quick := newPool([]int{1, 2, 2})
	quick.Do(boot{})
	p, r := settle(quick)
	fmt.Printf("pool %v: %d/%d delivered\n\n", p, r.Delivered, r.Total)

	// A mixed batch, shut down mid-run: worker 1 delivers in time, workers
	// 2 and 3 are halted mid-task by the cascade.
	fmt.Println("batch of 3 tasks, shutdown after 250ms:")
	mixed := newPool([]int{2, 6, 8})
	mixed.Do(boot{})
	time.Sleep(250 * time.Millisecond)
	mixed.Do(shutdown{})
	p, r = settle(mixed)
	fmt.Printf("pool %v: %d/%d delivered\n", p, r.Delivered, r.Total)
}
