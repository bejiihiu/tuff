# Idle-safe work

A region tick thread spends most of its life parked. It holds a permit, waits for the next deadline,
runs a short body and parks again. This page describes the one supported way to give that thread a
little housekeeping while it waits, and the constraints that keep the idea from turning into "run
arbitrary code on a tick thread".

Status: the contract is stable; the Kairos side is **experimental and off by default**.

## What qualifies

Work is idle-safe only if all of the following hold:

- **short and bounded** - it finishes inside the slice the caller passes, and returns when the
  cancellation flag turns false;
- **non-blocking** - it does not wait for a lock held across the slice, does not `join`, does not
  `sleep`, and does not call a synchronous API that may wait for another thread;
- **no I/O** - no file, socket, database or log write;
- **no world state** - it does not read or write blocks, entities, chunks or a region's task queue.
  The region that owns the thread keeps that state to itself;
- **no ordering requirement** that another thread could break.

`kz.bejiihiu.tuff.threads.idle.IdleWork` is that unit and `IdleWorkSource` is where it comes from.
There is no other entry point: a `Runnable`, a future, a scheduled job or a compute task cannot be
drained, because the runtime only ever calls `IdleWork.run(BooleanSupplier)`. Compute work is never
eligible - not "discouraged", not "filtered by a flag", but absent from the type.

`IdleWork.run` receives a `canContinue` answer that combines the slice deadline and the "a tick arrived"
signal. A unit that can run for more than a few microseconds must call it between pieces of work and
return as soon as it answers `false`. Nothing interrupts the thread on the unit's behalf. Returning
early is normal; a source that wants continuation keeps its state and offers the next step on the next
poll.

## How a tick thread runs it

Two places, and only two:

| Place | When | What may run |
| --- | --- | --- |
| Waiting for a deadline, holding the region's permit | The runner has already been granted the tick and is parked until `scheduledStart` | That region's own intermediate tasks, through the handle's `hasTasks()` / `runTasks(BooleanSupplier)` path, one pass per wait |
| Parked with no permit | The runner has nothing scheduled at all | One `IdleWork` from the configured source per slice |

The mid-tick pass is Folia's own drain, not a private queue: the region handle re-acquires itself
inside `runTasks`, the region's world state stays untouched by anyone else because this runner still
holds its permit, and a region that was already dropped is skipped before either call. One pass per
wait means a region cannot turn idle time into a second tick loop.

A job from the source runs only while the runner has no permit and no due tick. The moment a tick is
granted to that runner, its state changes; the cancellation flag turns false and the job stops at its
next check. The scheduler never holds a lock while a job runs.

## Guards

Every guard is a wall-clock bound, and every one of them exists because a slice can overrun:

| Knob | Default | Effect |
| --- | --- | --- |
| `enabled` | `false` | master switch; off means neither source is touched |
| `mid-tick` | `false` | allow the intermediate task drain |
| `utmi-jobs` | `false` | allow jobs from an `IdleWorkSource` |
| `slice-millis` | `1` | most wall clock one slice may take |
| `max-added-lag-millis` | `5` | lateness a tick may suffer before the path disables itself |
| `max-share-percent` | `25` | most of a one second window idle work may spend |

Self-protection is the last two. The scheduler measures the lateness of a body that followed idle work
(and only that: a body that starts long after the last slice is late for its own reasons) and turns the
whole idle path off when it exceeds `max-added-lag-millis`. It re-arms after five seconds without
another violation, which is what "stable again" means here. The share budget is a separate cap on work
per second; when it is spent, the source is not even polled.

A tick always wins. The deadline is checked before the slice in the mid-tick pass, and a grant changes
the runner state that the job's cancellation flag reads, so a due tick is never delayed by a
well-behaved unit. Units that ignore the flag are the reason the lag guard exists.

## The producer shipped in this build

The thread layer already refreshes its metrics once per second on `Tuff Threads Sampler`
(`ThreadsSampler`). That refresh has three parts, and only one of them is idle-safe:

| Part | Where it runs | Why |
| --- | --- | --- |
| The metrics pass | may run on an idle tick thread | one bounded walk over the registered threads, one probe per pool and one CPU read per thread; it takes no pool lock and does no I/O |
| The unregistered thread scan | refresher thread only | documented as a diagnostic, never a tick and never the sampling pass |
| The region walk | refresher thread only | it takes the regioniser read lock, which a tick thread must not do |

`MetricsIdleSource` offers the pass only, at most once every five seconds, and forces it: the public
snapshot call is throttled to one pass per interval and the refresher keeps that interval filled, so a
second driver asking politely would always read the cache and move nothing. The forced pass is the
internal sampler behind the public view. The refresher keeps running its one-second loop as the
fallback, so metrics cannot starve when every runner is busy.

The five second gap is not caution for its own sake. A pass taken from a tick thread answers the region
part from the refresher's last list, because the regioniser lock must not be taken there. Four passes
out of five stay on the refresher and do the walk, so the region view is at most about one interval
behind even while idle work takes its share.

## What it costs and what it buys

The path is off by default and costs nothing when off: no source is polled, the handle is not asked
about tasks and the counters stay at zero. When on, an idle server moves one metrics pass per five
seconds off the refresher thread and drains a region's queued intermediate tasks before its next tick
instead of at its start. The benefit is small on a calm server, which is exactly where it is safe; it
must not be turned on to make a loaded server faster - there the fund is busy and the path correctly
does nothing.

`/tuff scheduler` reports the whole picture in one line: whether the path is on, which sources are
selected, jobs and passes taken, preemptions, milliseconds spent, share of idle, the worst added lag
and, when it happened, that the self-protection disabled the path. A server that never opted in says
`idle work: off`.

## Configuration

```yaml
thread-management:
  region-execution:
    kairos:
      idle-work:
        enabled: false
        mid-tick: false
        utmi-jobs: false
        slice-millis: 1
        max-added-lag-millis: 5
        max-share-percent: 25
```

Restart only, like the rest of the `kairos` block. A value outside its range is reported at startup and
the idle path falls back to off rather than to a neighbouring value: a slice of zero milliseconds
would mean "run without a bound", which is the one thing this feature must never do by accident.

Without a source - no thread layer, or a scheduler built without one - `utmi-jobs` stays inert and the
snapshot says `no source`. The mid-tick pass does not need a source at all.

## Failure behaviour

- A job that throws `RuntimeException` or `LinkageError` is dropped; the fund keeps running.
- A job that overruns the slice adds lateness, and the lag guard disables the path.
- A source that throws while polling is treated as "nothing to offer" for that round.
- A cancelled region is never asked about tasks and never gets a job for its queue.
- Shutdown is unaffected: idle work runs on the fund's own threads, so `halt()` and `join()` see
  exactly the threads they saw before. A job in flight stops at its next cancellation check, which
  cannot block the join.
