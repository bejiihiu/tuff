# Soak and fault-injection tests

`tuff-server/src/test/java/kz/bejiihiu/tuff/threads/soak/` drives the real region scheduler through its
public seams and injects the failures the rollout plan names. The tests are ordinary JUnit tests, run by
the normal server test task, and are tagged `soak`; the server test task already runs every class ending
in `TestSuite`, and `TuffSoakTestSuite` selects this package.

```bash
./gradlew :tuff-server:test --tests "kz.bejiihiu.tuff.threads.soak.*"
```

Each class has a 150 second timeout and every wait is bounded by a deadline, never by a fixed sleep. No
test starts a server: they instantiate the scheduler the server uses and give it a test thread factory
and, when needed, a supervisor source.

## What is injected

| Failure | How it is real |
| --- | --- |
| supervisor death | `KairosServices` carries a supervisor source that returns a thread which exits without running a pass |
| spawn failure | the thread factory throws `OutOfMemoryError` on the allocation after the floor, exactly where a native thread allocation would fail |
| halt during spawn | the factory blocks inside `newThread`; the test halts while the allocation is in flight, then releases it |
| halt during retire | the fund is grown, then shrunk by a fast keep-alive; the test halts on the first observed retire |
| repeated halt | four threads call `halt()` fifty times each while a tick is looping |
| re-arm of the gate | the first fund is halted and joined, the gate is disarmed, a fresh fund is armed and started |
| churn storm | four threads issue four hundred schedule/cancel pairs each against a live fund |
| halt during tick | a tick burns for 300 ms while the halt happens |
| profiler on | every tick body reads the tick thread's CPU time on entry and exit |

## Invariants asserted

The assertions use observations, not timing assumptions:

- **no thread starts after halt** - the factory counts every call to `newThread` and every body entry; the
  test records the instant after `halt()` returned, and a tick records whether its execution started
  after that instant. Both counts must stay zero.
- **every created thread is dead before join returns** - `join` must return `true`, `getAliveThreads()`
  must be empty, and the factory's own list of created threads must contain no live thread.
- **no tick runs twice** - every tick counts its executions; a tick with `n` reschedules must show
  exactly `n + 1` executions, and a cancelled tick must show zero.
- **no permit leak** - `queued + awaiting + executing` from the snapshot is compared with the number of
  ticks the test still expects to be scheduled. The deterministic version schedules five ticks, cancels
  the four queued ones and then the awaiting one, and expects five, one and zero permits. The churn storm
  is fully concurrent: four threads schedule and cancel four hundred far-dated ticks each, so a cancel
  that frees a permit hands the replacement through the grant transition while the other threads are
  still scheduling. All one thousand six hundred cancels must succeed, none of the cancelled ticks may
  execute, and the permit count must be zero afterwards.
- **progress after a supervisor failure** - the fund is frozen at its floor when its control thread
  dies; the test then schedules a tick that reschedules five times and requires all six executions.
- **profiler pass does not change execution** - the same exactly-once check runs with CPU measurement
  switched on, and every execution must report a positive CPU time.

## Why the tests use the public surface

The scheduler's own unit tests live in its package and may use package-private seams. These tests are
deliberately outside it: they are the layer that treats the scheduler as a black box, so a refactor
inside the scheduler cannot quietly delete the evidence that its shutdown and accounting promises still
hold. Everything they need is public: the constructor takes a `ThreadFactory` and a deadline reader,
`KairosGate.arm` takes the tuning and the runtime services, and `KairosSnapshot` exposes the permit
counters and the supervisor state.

## Scope

These tests cover lifecycle and accounting, not performance. They say nothing about throughput, latency
or elasticity; that is the job of the benchmark stand in this directory. A run of the soak suite takes a
few minutes on a developer machine and is deterministic in its assertions, so it can run on every change
that touches the scheduler.
