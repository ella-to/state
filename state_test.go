package state_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"ella.to/state"
)

type counter struct{ total int }

type (
	add  struct{ n int }
	stop struct{}
)

func newCounter() *state.Machine[string, counter] {
	m := state.New("running", counter{})
	m.On("running", func(c *counter, a add) (string, error) {
		if a.n < 0 {
			return "", errors.New("negative")
		}
		c.total += a.n
		return "running", nil
	})
	m.On("running", func(c *counter, _ stop) (string, error) {
		return "stopped", nil
	})
	return m
}

func TestTransitions(t *testing.T) {
	m := newCounter()

	if s, err := m.Do(add{5}); err != nil || s != "running" {
		t.Fatalf("Do(add{5}) = %q, %v", s, err)
	}
	if s, err := m.Do(stop{}); err != nil || s != "stopped" {
		t.Fatalf("Do(stop{}) = %q, %v", s, err)
	}

	var total int
	m.Wait(func(_ string, c *counter) bool { total = c.total; return true })
	if total != 5 {
		t.Fatalf("total = %d, want 5", total)
	}
}

func TestInvalidAction(t *testing.T) {
	m := newCounter()
	m.Do(stop{})

	// add is not registered in "stopped".
	if _, err := m.Do(add{1}); !errors.Is(err, state.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if s := m.State(); s != "stopped" {
		t.Fatalf("state = %q, want stopped", s)
	}
}

func TestHandlerErrorKeepsState(t *testing.T) {
	m := newCounter()
	if _, err := m.Do(add{-1}); err == nil {
		t.Fatal("expected error")
	}
	if s := m.State(); s != "running" {
		t.Fatalf("state = %q, want running", s)
	}
}

func TestConcurrentDo(t *testing.T) {
	m := newCounter()

	const goroutines, each = 100, 100
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				m.Do(add{1})
			}
		}()
	}
	wg.Wait()

	var total int
	m.Wait(func(_ string, c *counter) bool { total = c.total; return true })
	if total != goroutines*each {
		t.Fatalf("total = %d, want %d", total, goroutines*each)
	}
}

func BenchmarkDo(b *testing.B) {
	m := newCounter()
	b.ReportAllocs()
	for b.Loop() {
		m.Do(add{1})
	}
}

type (
	start  struct{}
	finish struct{}
)

// newWorkflow returns an idle -> working -> done machine with no activities.
func newWorkflow() *state.Machine[string, counter] {
	m := state.New("idle", counter{})
	m.On("idle", func(*counter, start) (string, error) { return "working", nil })
	m.On("working", func(c *counter, a add) (string, error) {
		c.total += a.n
		return "working", nil
	})
	m.On("working", func(*counter, finish) (string, error) { return "done", nil })
	m.On("working", func(*counter, stop) (string, error) { return "stopped", nil })
	return m
}

func TestEnterRunsActivity(t *testing.T) {
	m := newWorkflow()
	m.Enter("working", func(context.Context) {
		m.Do(add{7})
		m.Do(finish{})
	})

	m.Do(start{})
	m.Wait(func(s string, _ *counter) bool { return s == "done" })

	var total int
	m.Wait(func(_ string, c *counter) bool { total = c.total; return true })
	if total != 7 {
		t.Fatalf("total = %d, want 7", total)
	}
}

func TestEnterCancelsOnLeave(t *testing.T) {
	m := newWorkflow()
	entered := make(chan context.Context, 1)
	m.Enter("working", func(ctx context.Context) { entered <- ctx })

	m.Do(start{})
	ctx := <-entered
	if ctx.Err() != nil {
		t.Fatal("context canceled before leaving the state")
	}

	m.Do(stop{})
	// Cancellation happens inside Do, so it is visible once Do returns.
	if ctx.Err() == nil {
		t.Fatal("context not canceled after leaving the state")
	}
}

func TestEnterSelfTransitionIsInternal(t *testing.T) {
	m := newWorkflow()
	entered := make(chan context.Context, 1)
	m.Enter("working", func(ctx context.Context) { entered <- ctx })

	m.Do(start{})
	ctx := <-entered

	// add keeps the machine in "working": the activity must survive.
	if _, err := m.Do(add{1}); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("self-transition canceled the activity")
	}
	select {
	case <-entered:
		t.Fatal("self-transition restarted the activity")
	default:
	}

	m.Do(stop{})
	if ctx.Err() == nil {
		t.Fatal("context not canceled after leaving the state")
	}
}

func TestEnterStaleActionRejected(t *testing.T) {
	m := newWorkflow()
	release := make(chan struct{})
	result := make(chan error, 1)
	m.Enter("working", func(context.Context) {
		<-release // hold the result until the machine has moved on
		_, err := m.Do(finish{})
		result <- err
	})

	m.Do(start{})
	m.Do(stop{}) // leave "working" before the activity reports
	close(release)

	if err := <-result; !errors.Is(err, state.ErrInvalid) {
		t.Fatalf("stale Do err = %v, want ErrInvalid", err)
	}
	if s := m.State(); s != "stopped" {
		t.Fatalf("state = %q, want stopped", s)
	}
}

func TestChildMachineStopsWithParent(t *testing.T) {
	parent := newWorkflow()
	childStopped := make(chan string, 1)

	parent.Enter("working", func(ctx context.Context) {
		child := newWorkflow()
		context.AfterFunc(ctx, func() {
			s, _ := child.Do(stop{})
			childStopped <- s
		})
		child.Do(start{})
	})

	parent.Do(start{})
	parent.Do(stop{}) // parent leaves "working" -> child must be halted

	if s := <-childStopped; s != "stopped" {
		t.Fatalf("child state = %q, want stopped", s)
	}
}

func TestWaitWakesOnChange(t *testing.T) {
	m := newCounter()

	done := make(chan struct{})
	go func() {
		m.Wait(func(s string, _ *counter) bool { return s == "stopped" })
		close(done)
	}()

	m.Do(add{1}) // wakes the waiter, cond still false
	m.Do(stop{})
	<-done
}

// ---- completing, children, sends ------------------------------------------

// job is the context of a child machine: it turns In into Out over Want ticks,
// optionally reporting each tick upward.
type job struct {
	In   string
	Out  string
	N    int
	Want int
	Up   func(int)
}

type (
	tick struct{}
	quit struct{}
)

// newTicker returns a "work" -> "done" child. It only completes on its own if
// Want ticks arrive; quit stops it short of completing.
func newTicker(in string, want int, up func(int)) *state.Machine[string, job] {
	c := state.New("work", job{In: in, Want: want, Up: up})
	c.Final("done")
	c.On("work", func(j *job, _ tick) (string, error) {
		j.N++
		if j.Up != nil {
			j.Up(j.N) // from inside a handler: must not block
		}
		if j.N == j.Want {
			j.Out = j.In + "!"
			return "done", nil
		}
		return "work", nil
	})
	c.On("work", func(*job, quit) (string, error) { return "gone", nil })
	return c
}

type doc struct {
	Payload string
	Trail   []string
	Bumps   int
}

func TestFinalCompletes(t *testing.T) {
	c := newTicker("x", 1, nil)
	c.Start()

	select {
	case <-c.Done():
		t.Fatal("Done closed before completing")
	default:
	}

	c.Do(tick{})
	select {
	case <-c.Done():
	default:
		t.Fatal("Done not closed after entering a final state")
	}

	s, j := c.Result()
	if s != "done" || j.Out != "x!" {
		t.Fatalf("Result() = %q, %+v", s, j)
	}
	if _, err := c.Do(tick{}); !errors.Is(err, state.ErrStopped) {
		t.Fatalf("Do after completing = %v, want ErrStopped", err)
	}
}

func TestStopClosesDoneWithoutCompleting(t *testing.T) {
	c := newTicker("x", 5, nil)
	c.Start()
	c.Stop()

	select {
	case <-c.Done():
	default:
		t.Fatal("Done not closed after Stop")
	}
	if s, _ := c.Result(); s != "work" {
		t.Fatalf("Result() state = %q, want work (Stop is not a transition)", s)
	}
	if _, err := c.Do(tick{}); !errors.Is(err, state.ErrStopped) {
		t.Fatalf("Do after Stop = %v, want ErrStopped", err)
	}
}

func TestStartRunsInitialActivity(t *testing.T) {
	c := newTicker("x", 1, nil)
	ran := make(chan struct{})
	c.Enter("work", func(context.Context) { close(ran) })

	select {
	case <-ran:
		t.Fatal("initial activity ran before Start")
	default:
	}

	c.Start()
	<-ran
}

func TestWaitReportsStopped(t *testing.T) {
	c := newTicker("x", 5, nil)
	c.Start()

	errs := make(chan error, 1)
	go func() {
		errs <- c.Wait(func(s string, _ *job) bool { return s == "never" })
	}()

	c.Stop()
	if err := <-errs; !errors.Is(err, state.ErrStopped) {
		t.Fatalf("Wait err = %v, want ErrStopped", err)
	}
}

func TestAfterAdvancesState(t *testing.T) {
	c := newTicker("x", 1, nil)
	c.After("work", time.Millisecond, tick{})
	c.Start()

	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("After did not fire")
	}
	if s, j := c.Result(); s != "done" || j.Out != "x!" {
		t.Fatalf("Result() = %q, %+v", s, j)
	}
}

func TestAfterCanceledByLeaving(t *testing.T) {
	c := newTicker("x", 1, nil)
	c.After("work", 50*time.Millisecond, tick{})
	c.Start()
	c.Do(quit{}) // leaves "work" before the timer fires

	time.Sleep(120 * time.Millisecond)
	if s := c.State(); s != "gone" {
		t.Fatalf("state = %q, want gone", s)
	}
}

// newPipeline is the whole point: three states, a child per state, the child's
// completion advancing the parent, with the payload handed down each time.
func newPipeline(payload string) *state.Machine[string, doc] {
	p := state.New("s1", doc{Payload: payload})
	p.Final("end", "killed")

	for _, step := range []struct{ at, next string }{
		{"s1", "s2"}, {"s2", "s3"}, {"s3", "end"},
	} {
		p.Invoke(step.at,
			func(d *doc) *state.Machine[string, job] {
				c := newTicker(d.Payload, 1, nil)
				c.After("work", time.Millisecond, tick{}) // the child completes itself
				return c
			},
			func(d *doc, _ string, j job) (string, error) {
				d.Payload = j.Out
				d.Trail = append(d.Trail, step.at)
				return step.next, nil
			})
		p.On(step.at, func(*doc, quit) (string, error) { return "killed", nil })
	}

	// A self-transition in the parent while a child is running: it must not
	// disturb the child.
	for _, at := range []string{"s1", "s2", "s3"} {
		p.On(at, func(d *doc, _ bump) (string, error) {
			d.Bumps++
			return at, nil // internal
		})
	}
	return p
}

type bump struct{}

func TestInvokeAdvancesParent(t *testing.T) {
	p := newPipeline("a")
	p.Start()
	p.Do(bump{}) // a parent self-transition mid-step

	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("pipeline stalled in %q", p.State())
	}

	s, d := p.Result()
	if s != "end" {
		t.Fatalf("state = %q, want end", s)
	}
	if d.Payload != "a!!!" {
		t.Fatalf("payload = %q, want a!!! (one ! per step)", d.Payload)
	}
	if len(d.Trail) != 3 {
		t.Fatalf("trail = %v, want three steps", d.Trail)
	}
}

func TestInvokeStopsChildAndDropsStaleReport(t *testing.T) {
	var child *state.Machine[string, job]

	p := state.New("s1", doc{Payload: "a"})
	p.Final("killed")
	p.Invoke("s1",
		func(d *doc) *state.Machine[string, job] {
			child = newTicker(d.Payload, 1, nil)
			return child
		},
		func(*doc, string, job) (string, error) { return "wrong", nil })
	p.On("s1", func(*doc, quit) (string, error) { return "killed", nil })
	p.Start()

	if child == nil {
		t.Fatal("child not built on entry")
	}
	p.Do(quit{}) // leaving s1 must stop the child

	select {
	case <-child.Done():
	default:
		t.Fatal("child not stopped when the parent left the state")
	}
	if s, _ := child.Result(); s != "work" {
		t.Fatalf("child state = %q, want work (stopped, not completed)", s)
	}
	if s := p.State(); s != "killed" {
		t.Fatalf("parent state = %q, want killed", s)
	}
}

func TestStaleReportDropped(t *testing.T) {
	p := state.New("s1", doc{})
	p.Final("killed")
	p.Invoke("s1",
		func(*doc) *state.Machine[string, job] {
			c := newTicker("a", 1, nil)
			c.After("work", 30*time.Millisecond, tick{}) // completes after we leave
			return c
		},
		func(*doc, string, job) (string, error) { return "wrong", nil })
	p.On("s1", func(*doc, quit) (string, error) { return "s2", nil })
	p.On("s2", func(*doc, quit) (string, error) { return "killed", nil })
	p.Start()

	p.Do(quit{}) // leave s1 while the child is still working
	time.Sleep(120 * time.Millisecond)

	if s := p.State(); s != "s2" {
		t.Fatalf("stale report moved the parent to %q", s)
	}
}

func TestInvokeRebuildsPerVisit(t *testing.T) {
	built := 0
	var kid *state.Machine[string, job] // the child of the current visit
	p := state.New("s1", doc{})
	p.Final("end")
	p.Invoke("s1",
		func(*doc) *state.Machine[string, job] {
			built++
			kid = newTicker("a", 1, nil) // completes on its first tick
			return kid
		},
		func(*doc, string, job) (string, error) { return "s2", nil })
	p.On("s1", func(d *doc, _ bump) (string, error) { d.Bumps++; return "s1", nil }) // self
	p.On("s2", func(*doc, bump) (string, error) { return "s1", nil })                // re-entry
	p.Start()

	if built != 1 {
		t.Fatalf("built = %d after Start, want 1", built)
	}
	if _, err := p.Do(bump{}); err != nil {
		t.Fatal(err)
	}
	if built != 1 {
		t.Fatalf("built = %d after a self-transition, want 1", built)
	}
	select {
	case <-kid.Done():
		t.Fatal("self-transition stopped the child")
	default:
	}

	kid.Do(tick{}) // the child completes: the parent moves to s2
	p.Wait(func(s string, _ *doc) bool { return s == "s2" })
	p.Do(bump{}) // back into s1
	if built != 2 {
		t.Fatalf("built = %d after re-entry, want 2", built)
	}
	p.Stop()
}

func TestSpawnDynamicChildren(t *testing.T) {
	const n = 5
	p := state.New("running", doc{})
	p.Final("end")
	p.On("running", func(d *doc, _ start) (string, error) {
		for range n {
			c := newTicker("a", 1, nil)
			c.After("work", time.Millisecond, tick{})
			// Spawn from inside a handler: dynamic count, parent-scoped.
			p.Spawn(c, func(d *doc, _ string, j job) (string, error) {
				d.Bumps++
				if d.Bumps == n {
					return "end", nil
				}
				return "running", nil
			})
		}
		return "running", nil
	})

	p.Start()
	p.Do(start{})

	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d of %d children reported", p.Read(func(_ string, d *doc) int { return d.Bumps }), n)
	}
	if _, d := p.Result(); d.Bumps != n {
		t.Fatalf("Bumps = %d, want %d", d.Bumps, n)
	}
}

func TestStopCascadesToGrandchildren(t *testing.T) {
	var kid, grandkid *state.Machine[string, job]

	p := state.New("s1", doc{})
	p.Invoke("s1",
		func(*doc) *state.Machine[string, job] {
			kid = newTicker("a", 99, nil)
			kid.Invoke("work",
				func(*job) *state.Machine[string, job] {
					grandkid = newTicker("b", 99, nil)
					return grandkid
				},
				func(*job, string, job) (string, error) { return "done", nil })
			return kid
		},
		func(*doc, string, job) (string, error) { return "end", nil })
	p.Start()

	if kid == nil || grandkid == nil {
		t.Fatal("tree not built")
	}
	p.Stop()

	for name, m := range map[string]*state.Machine[string, job]{"child": kid, "grandchild": grandkid} {
		select {
		case <-m.Done():
		case <-time.After(2 * time.Second):
			t.Fatalf("%s not stopped by the parent", name)
		}
	}
}

// TestCrossTalk hammers both directions at once: a parent handler reaching down
// into every child while the children report up from inside their own handlers.
// Done with Do both ways this is an ABBA deadlock; with Send it must always
// terminate.
func TestCrossTalk(t *testing.T) {
	for i := range 200 {
		p := state.New("running", doc{})
		p.Final("end")
		p.On("running", func(d *doc, _ bump) (string, error) { d.Bumps++; return "running", nil })

		var kids []*state.Machine[string, job]
		for range 4 {
			kids = append(kids, newTicker("a", 1_000_000, func(int) {}))
		}

		// A handler that reaches down the tree while holding the parent's lock.
		p.On("running", func(*doc, stop) (string, error) {
			for _, c := range kids {
				c.Send(quit{}) // queued, never blocks
				c.Stop()       // parent -> child: locks only go this way
			}
			return "end", nil
		})

		for _, c := range kids {
			c.Read(func(_ string, j *job) any {
				j.Up = func(int) { p.Send(bump{}) } // child handler -> parent
				return nil
			})
			p.Spawn(c, func(*doc, string, job) (string, error) { return "running", nil })
			go func() {
				for range 50 {
					if _, err := c.Do(tick{}); err != nil {
						return
					}
				}
			}()
		}

		p.Start()
		// Let the children get going, so the reach-down really does overlap
		// with reports coming up.
		p.Wait(func(_ string, d *doc) bool { return d.Bumps >= 20 })
		p.Do(stop{})

		for _, m := range append([]*state.Machine[string, job]{}, kids...) {
			select {
			case <-m.Done():
			case <-time.After(5 * time.Second):
				t.Fatalf("iteration %d: child not stopped", i)
			}
		}
		select {
		case <-p.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: parent deadlocked", i)
		}
	}
}
