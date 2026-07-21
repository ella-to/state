# state

A minimal, concurrency-safe finite state machine for Go, built on **generic
methods** (Go 1.27+). No `reflect`, no code generation, no `any` assertions in
your code — 15 ns and 0 allocations per action.

The entire API is five names:

| Name | What it does |
|---|---|
| `state.New(initial, ctx)` | Create a machine with an initial state and a context value |
| `m.On(from, handler)` | Register an action handler for a state |
| `m.Do(action)` | Apply an action; advances the state if valid |
| `m.State()` | Read the current state |
| `m.Wait(cond)` | Block until a condition over state + context holds |

## Requirements

Go 1.27 or newer (currently `go1.27rc1`). The `go` directive in `go.mod`
handles this automatically — running any `go` command with Go 1.25+ installed
downloads the right toolchain.

## Core concepts

A machine is `Machine[S, C]`, parameterized by two types **you** define:

- **State (`S`)** — any comparable type. Usually a small enum:

  ```go
  type Phase int

  const (
      Idle Phase = iota
      Running
      Done
  )
  ```

- **Context (`C`)** — a struct holding all the data your machine works on.
  It is created once, lives inside the machine, and is passed by pointer to
  every handler, so changes made in one state are visible in the next:

  ```go
  type Job struct {
      Attempts int
      Result   string
  }
  ```

- **Actions** — plain structs (or any type) carrying an event's payload. The
  action's *type* determines which handler runs; its fields are the payload:

  ```go
  type Start struct{ Input string }
  type Finish struct{ Output string }
  type Fail struct{ Reason error }
  ```

There is no registration step for states or actions — they exist the moment
you mention them in `On`.

## Creating a machine

```go
m := state.New(Idle, Job{})
```

`m` has type `*state.Machine[Phase, Job]` (inferred). The machine is now in
`Idle` and owns a `Job` value as its context.

## Moving between states

Wire up transitions with `On(from, handler)`. A handler receives the context
pointer and the typed action, and returns the **next state**:

```go
m.On(Idle, func(j *Job, a Start) (Phase, error) {
    j.Attempts++
    j.Result = process(a.Input)
    return Running, nil
})

m.On(Running, func(j *Job, a Finish) (Phase, error) {
    j.Result = a.Output
    return Done, nil
})

m.On(Running, func(j *Job, a Fail) (Phase, error) {
    if j.Attempts >= 3 {
        return Done, nil
    }
    return Idle, nil // go back and retry
})
```

Then apply actions with `Do`:

```go
next, err := m.Do(Start{Input: "hello"}) // Idle -> Running
```

`Do` returns the state the machine is in afterwards. Everything is fully
typed: `On` and `Do` are generic methods, so the action type is inferred and
checked at compile time — passing a `Finish` to a handler expecting `Start`
doesn't compile.

If the handler is getting long, write it as a method on your context and
register the method expression:

```go
func (j *Job) start(a Start) (Phase, error) { ... }

m.On(Idle, (*Job).start)
```

### When an action is rejected

An action only advances the machine if it is **valid**. There are two ways it
can be rejected, and in both cases the state and context are left untouched:

1. **No handler for this (state, action type) pair.** `Do` returns an error
   wrapping `state.ErrInvalid`:

   ```go
   _, err := m.Do(Finish{}) // machine is in Idle; Finish only handled in Running
   errors.Is(err, state.ErrInvalid) // true
   ```

2. **The handler itself says no.** Return an error from the handler to reject
   an action that is structurally allowed but semantically wrong right now
   (wrong player's turn, insufficient funds, ...):

   ```go
   m.On(Running, func(j *Job, a Finish) (Phase, error) {
       if a.Output == "" {
           return 0, errors.New("empty output") // state stays Running
       }
       return Done, nil
   })
   ```

A handler may also return the *same* state it is in — the action then mutates
the context without changing state (e.g. each card played in a trick keeps the
game in `Playing` until the trick completes).

## Getting notified about state changes

`Wait(cond)` blocks the calling goroutine until `cond` returns true. The
condition is checked immediately and then re-checked after **every successful
`Do`**, so it observes each state change and context mutation:

```go
// Block until the machine reaches Done.
m.Wait(func(p Phase, j *Job) bool { return p == Done })
```

The condition runs while the machine is locked, so it can safely read the
state and context — and copy values out through captured variables:

```go
var result string
m.Wait(func(p Phase, j *Job) bool {
    if p != Done {
        return false
    }
    result = j.Result // safe: we hold the machine's lock
    return true
})
```

Any number of goroutines can `Wait` at the same time; every successful `Do`
wakes all of them to re-evaluate. This is how you build turn-based flows: each
participant waits for a condition that says "it's my turn", acts, and loops.

Two rules for `cond`:

- **Don't call machine methods inside it** (`Do`, `State`, `Wait`, `On`) —
  the machine is already locked and this will deadlock.
- **Keep it fast** — it runs on the hot path of every `Do`.

## Reading the state and context

- `m.State()` returns the current state — handy for logs and assertions.
- There is deliberately no `Context()` getter: returning a pointer would let
  callers mutate shared data outside the lock. To read the context, use
  `Wait` with a condition that returns true immediately:

  ```go
  var attempts int
  m.Wait(func(_ Phase, j *Job) bool { attempts = j.Attempts; return true })
  ```

  This acquires the lock, runs your reader, and returns — a synchronized
  snapshot in one call.

## Concurrency model

All methods are safe for concurrent use from any number of goroutines:

- One mutex guards the state, the context, and the handler table. Handlers
  and `Wait` conditions always run under it, so they never race — you never
  need your own locking around context fields.
- Actions are serialized: two concurrent `Do` calls execute their handlers
  one after the other, each seeing the state left by the previous one. An
  action that becomes invalid because another goroutine got there first is
  simply rejected with an error.
- Register handlers up front, before sharing the machine, and treat the
  wiring as fixed. `On` is lock-protected so late registration won't race,
  but a transition table that changes mid-flight is hard to reason about.

## Examples

The [`example`](example) directory is a guided tour, ordered simple to
complex. Each program's doc comment lists exactly what it demonstrates; run
any of them with `go run ./example/<name>`.

| Example | Patterns and edge cases it shows |
|---|---|
| [`01-trafficlight`](example/01-trafficlight/main.go) | The smallest machine: an enum state, a payload-less action cycling three states, and a `Wait` snapshot read of the context. |
| [`02-vending`](example/02-vending/main.go) | String states, actions with payloads, both rejection paths (`ErrInvalid` vs handler errors), the same action registered in two states, and a self-transition accumulating credit. |
| [`03-retry`](example/03-retry/main.go) | Handlers as methods on the context registered as method expressions, one action fanning out to different next states based on context (retry vs give up), errors as payloads, and terminal states as "no handlers registered". |
| [`04-progress`](example/04-progress/main.go) | Every notification pattern: self-transitions waking waiters on context changes, a change-detecting logger, a threshold waiter on a context value, and main blocking on the terminal state — three concurrent observers on one machine. |
| [`05-once`](example/05-once/main.go) | Racing goroutines: serialized actions, the first-wins claim pattern, and using `Do`'s error to learn whether you were the goroutine that advanced the machine. |
| [`06-cardgame`](example/06-cardgame/main.go) | The full picture: a 4-player trick-taking game where each player goroutine `Wait`s for its turn (choosing a card inside the condition, under the lock) and advances the game with `Do`. |

## Performance

`go test -bench Do`:

```
BenchmarkDo-16    153469406    15.51 ns/op    0 B/op    0 allocs/op
```

A `Do` is a mutex lock, two map lookups keyed by state and action type, one
type assertion, and your handler. Action dispatch never touches `reflect` —
type identity comes from the generic type parameter itself.
