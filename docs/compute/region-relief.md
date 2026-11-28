# Region relief

Region relief lets an overloaded region hand a piece of pure computation to the shared compute pool and
apply the result back on its own thread. It does not move the tick: the region keeps ownership of its world
state for the whole request, and the pool only computes a value from a snapshot.

The path exists so a region that is behind can shed work instead of doing all of it inside its tick. It is
**off by default**: the consumer, the service and the metrics are wired and usable, but every submission runs
inline until `async-compute.relief.enabled` is set to `true`.

## The contract

Offloadable work is described by a snapshot, a version, a pure body and its terminal callbacks
([`RegionReliefWork`](../../tuff-compute/src/main/java/kz/bejiihiu/tuff/compute/RegionReliefWork.java)):

| Member | Runs on | Rule |
| --- | --- | --- |
| `snapshot()` | region thread | Captured once, before submission. Must be immutable from then on: no live world, entity or chunk references |
| `epoch()` | any | Version of the world state the snapshot belongs to. The region defines what it means |
| `compute(snapshot)` | compute worker | Pure CPU work. No world access, no Bukkit/Paper API, no blocking IO, no locks owned by a region |
| `apply(result)` | region thread, at most once | The only place allowed to mutate world state, and only while the epoch is still current |
| `onFailure(failure)` | region thread, at most once | The computation or the applicator threw. Must not mutate world state |
| `onDiscard(reason)` | region thread, at most once | Stale, expired, cancelled or dropped. Must not mutate world state |

`onFailure` and `onDiscard` are default methods, so work that only cares about results does not have to
implement them; when both are left empty the outcome is still counted in the relief metrics.

The region half is a small port
([`RegionReliefOwner`](../../tuff-compute/src/main/java/kz/bejiihiu/tuff/compute/RegionReliefOwner.java)):
the owning `Thread`, `isCurrent(epoch)`, `isStopped()`, and `runOnOwner(Runnable)` which schedules a task onto
the region. The relief service never calls the applicator itself from a compute worker; it schedules the
applicator through that port and then verifies the thread before running it.

Ownership is absolute: an applicator that would run off the owning thread is dropped and counted, never run.
That check is what makes a scheduling mistake cost a result instead of world integrity.

## Completion contract

Every submitted request reaches exactly one of three terminal calls, always on the owning region thread,
always at most once. The only exception is an owner port that refuses the terminal task: the discard is
then delivered from the thread that observed the completion, which is still at most once.

| Call | When | Context |
| --- | --- | --- |
| `apply(result)` | The result is usable and the epoch is still current | The only terminal that may mutate world state |
| `onFailure(failure)` | The compute body threw, or `apply` itself threw | Must not mutate world state: a failure does not prove the snapshot was current |
| `onDiscard(reason)` | The result will not be applied | Must not mutate world state: the snapshot is stale or the region is going away |

The discard reasons are `STALE` (the epoch moved on), `EXPIRED` (the queued request expired and the region
was already gone, so the inline rerun could not happen), `CANCELLED` (the pool cancelled the request while
the region still existed) and `DROPPED` (the region stopped, cancelled its requests, or the result could not
be delivered to the owner thread).

A compute failure is **never** rerun inline: the same pure body would fail again and would only cost region
time. An applicator that throws is reported through `onFailure` and does not escape into the tick. Inline
fallbacks reported at submit time follow the same rule: a failure calls `onFailure` and `submit` returns
`false`.

Two cases cannot run an applicator: an owner port that runs the scheduled task off the owner thread
(the result is dropped and counted, because calling region code from the wrong thread is worse than
losing the notification), and a port that refuses the task outright. The refused task still delivers
`onDiscard(DROPPED)` exactly once, because losing the notification silently would leave a submitter
holding a resource forever; the callback runs on the thread that observed the completion (a compute
worker, or the submitting region thread when the future was already complete) instead of the owner
thread. A handler may therefore only observe, exactly as the contract says. The relief metrics count
both cases as dropped.

## Submission

```java
final RegionRelief relief = ComputeBootstrap.regionRelief().orElse(null);
if (relief == null) {
    runTheWorkHere();          // no engine at all: behave like before
    return;
}
final boolean offloaded = relief.submit(owner, work);
if (!offloaded) {
    // the work already ran inline on this thread; nothing else to do
}
```

`submit` must be called on the owning region thread and is checked. It returns `true` when the work went to the
pool and `false` when it already ran inline. It never blocks waiting for a worker and never runs the applicator
twice.

The service falls back to running the work inline, on the calling thread, when:

- relief is switched off, or the whole compute engine is disabled;
- the `region-relief` consumer is disabled or refuses the submission;
- the queue is full, globally or for the consumer;
- the region already has `max-in-flight-per-region` requests outstanding;
- an accepted request expired in the queue (`max-queue-wait-seconds`) before a worker took it, in which
  case the same work is rerun on the owner thread.

A region that already stopped, or whose owner was cancelled, is refused before anything is reserved: `submit`
returns `false`, the request is counted as `dropped` and `onDiscard(DROPPED)` runs. A result whose epoch moved
on while it was computing is counted as `stale` and reported through `onDiscard(STALE)`.

## Lifecycle

- Requests of one region are tracked together. `RegionRelief#cancel(owner)` flags every outstanding request,
  cancels the queued ones cooperatively and drops every result that arrives later. A region calls it when it
  stops; a stopped region never receives a result, and a cancelled owner is refused on every later submission
  instead of starting a fresh in-flight state.
- Running tasks are never interrupted. Cancellation is cooperative, exactly like the rest of the pool.
- The in-flight counter and the per-region slot are released by the callback that makes the final decision,
  so a cancelled or dropped request cannot leak either.

## The consumer and the reserve

Relief runs as one consumer, `region-relief`, registered by the engine at **NORMAL** priority only, and only
for `GENERAL` and `CUSTOM` task types. It cannot submit HIGH or CRITICAL work, so relief never outranks urgent
compute.

Its policy comes from `async-compute.relief.consumer.*`. An explicit `async-compute.consumers.region-relief`
section overrides the whole policy, like any other consumer id.

Tick pressure and relief would otherwise fight: a lagging region makes the pool yield cores, while that same
region is asking for help. Two rules resolve it:

1. **The reserve is not yieldable.** While relief is enabled, `reserved-cores` stay in the pool no matter what
   tick pressure decides. The pressure controller's ceiling is lowered so that the pool still runs
   `min-threads + reserved-cores` workers at maximum pressure: automatic sizing keeps one budget unit as
   headroom, so the adapter reserves that unit too. The applied budget therefore never drops below
   `min-threads + reserved-cores + 1`, which is exactly what resolves to the promised worker count.
2. **Relief never grows the pool.** The backlog of `region-relief` is excluded from the growth and autoscaling
   inputs. Relief work is work a region is trying to shed; letting it justify a bigger pool would defeat the
   point. It is still queued, still admitted and still visible in the queue gauges.

With the trigger off, `effectiveReservedCores()` is zero: a server that does not use relief gives pressure
exactly the same ceiling as before. The reserve also raises the pool's idle floor to
`min-threads + reserved-cores`, so keep-alive retirement and the idle autoscaler cannot shrink it away. A
fixed `workers.threads` count is never resized by pressure, so the reserve only matters for automatic
sizing.

## The overload trigger

The service and the consumer above are passive: they do what they are asked. The trigger is the part
that decides *when* to ask. It watches the scheduler independent tick feed and opens a relief window
for one region when that region keeps running long.

The feed is the per-tick observation the region handle emits at the end of every body, so the trigger
sees the same numbers the scheduler used: the body's real start and end, and the deadline the body was
measured against. It never measures a tick on its own, and it does not care whether EDF, work stealing
or Kairos ran the body.

### The policy

A tick body is an **overrun** when any of these holds:

- it ran for at least `overrun-millis`;
- it started at least one whole period after its deadline (`periodsAhead > 1`);
- it started at least `overrun-millis` after its deadline.

The first condition is the real body duration. The last two are the deadline miss: a body that starts a
period late has already lost the tick, even when the body itself is quick.

Every region has its own counters, so state is independent:

| Event | Effect |
| --- | --- |
| Overrun body | The overrun run grows by one, the calm run resets to zero |
| Calm body | The calm run grows by one, the overrun run resets to zero |
| Overrun run reaches `enter-overruns` and no window is open | A window opens, with a new epoch |
| Calm run reaches `exit-calm-ticks` while a window is open | The window closes and the cooldown starts |
| A window would open before `cooldown-seconds` since the last close | Suppressed, counted as cooldown |
| A region already opened `max-windows` windows inside `rate-window-seconds` | Suppressed, counted as rate |

The cooldown and the rate are checked only when the run would otherwise open a window, so a region
that keeps overrunning for a long time still opens at most the allowed number of windows.

### The unit API

The trigger owns no work. A subsystem that has offloadable work registers a
[`ReliefUnit`](../../tuff-server/src/main/java/kz/bejiihiu/tuff/threads/relief/ReliefUnit.java):

```java
final ReliefTrigger trigger = ComputeBootstrap.reliefTrigger().orElse(null);
if (trigger != null) {
    trigger.register(new PathfindingReliefUnit());
}

final class PathfindingReliefUnit implements ReliefUnit {

    @Override
    public String id() {
        return "pathfinding";
    }

    @Override
    public void onWindowOpen(final ReliefWindow window) {
        // runs on the region thread that produced the tick
        final RegionReliefOwner owner = new MyRegionOwner(window.owner(), window.regionId());
        final long epoch = window.epoch();
        relief.submit(owner, new MyPathWork(snapshot, epoch));
    }
}
```

`ReliefWindow` carries the region id, the owning thread, the window epoch and the moment the window
opened. The unit builds its own `RegionReliefOwner` because it knows its location and its scheduler; the
trigger never constructs an owner, never submits and never applies anything.

Both callbacks run on the owning region thread, inside the tick body that just ended, so a callback
must be short: capture a snapshot, submit, return. A unit that throws is isolated and counted in
`unitFailures`; the tick and the other units continue. A unit that wants to know whether a region still
has a window can call `trigger.windowEpoch(regionId)`, which returns zero when the region is calm.

**No built-in work unit ships in vanilla.** Nothing in the server is registered by default, so an
enabled trigger on a plain server opens windows that nobody listens to and only moves the counters. A
plugin or a server subsystem has to register a unit before relief does anything.

### Regions that stop ticking

Nothing in the tick feed reports a region removal, so the trigger cannot be told "this region is gone".
It bounds the state instead: a region whose last body is older than `stale-after-seconds` is forgotten
on the next sweep (at most once per second of event time), and an open window of that region is counted
as `windowsAbandoned`.

No unit callback runs for an abandoned window. There is no region thread left to run it on, and calling
a unit from a different region's thread would break the ownership rule the rest of relief is built on.
A unit that needs to cancel work when a region dies must do it from its own region or plugin lifecycle,
not from the trigger. This is the honest gap of the current seam.

### Which keys switch it on

The trigger only acts when **both** switches are on:

```yaml
async-compute:
  relief:
    enabled: true          # offloading is allowed at all
    trigger:
      enabled: true        # windows may open
```

With `relief.enabled: false` a window would only move work to the end of a tick, because every
submission would fall back to running inline. The trigger therefore stays off in that combination, and
the server log says so at start.

## Configuration

```yaml
async-compute:
  relief:
    enabled: false                   # off by default; the consumer still exists
    max-in-flight-per-region: 8
    reserved-cores: 1                # only applied while enabled
    consumer:
      enabled: true
      max-concurrent: 0.25
      fair-share-weight: 0.5
      max-queued-tasks: 64
      overflow-policy: reject
      max-queue-wait-seconds: 2
    trigger:
      enabled: false                 # off by default; no built-in unit ships anyway
      overrun-millis: 40
      enter-overruns: 3
      exit-calm-ticks: 6
      cooldown-seconds: 30
      max-windows: 4
      rate-window-seconds: 300
      stale-after-seconds: 10
```

| Key | Type | Default | Allowed | Effect |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `false` | `true`/`false` | Offload relief work at all. `false` runs every submission inline |
| `max-in-flight-per-region` | integer | `8` | `>= 1` | Accepted relief requests one region may have outstanding |
| `reserved-cores` | integer | `1` | `>= 0` | Cores the pool keeps for relief under tick pressure; only applied while `enabled` |
| `consumer.*` | policy | see above | the consumer policy keys | Policy of the `region-relief` consumer |
| `trigger.enabled` | boolean | `false` | `true`/`false` | Open relief windows at all. Off costs one flag check per tick |
| `trigger.overrun-millis` | integer | `40` | `>= 1` | Body duration, or start lag past the deadline, that counts as an overrun |
| `trigger.enter-overruns` | integer | `3` | `>= 1` | Consecutive overrun bodies that open a window |
| `trigger.exit-calm-ticks` | integer | `6` | `>= 1` | Consecutive calm bodies that close an open window |
| `trigger.cooldown-seconds` | number | `30` | `>= 0` | Time a region must stay calm after a close before the next window |
| `trigger.max-windows` | integer | `4` | `>= 1` | Windows one region may open inside one rate window |
| `trigger.rate-window-seconds` | number | `300` | `> 0` | Length of the per-region rate window |
| `trigger.stale-after-seconds` | number | `10` | `> 0` | Silence after which a region's trigger state is forgotten |

All relief keys are restart-only: the service is wired into the engine and the budget sink at start.
`/tuff compute reload` reports a change under `takes effect after a restart`.

## Metrics

The relief counters are part of `SchedulerMetrics#regionRelief()` and printed by
`/tuff compute consumers`:

```
  region relief: enabled, 0 in flight / 8 per region, reserve 1 core(s)
    12 requests, 10 accepted, 9 completed, 1 stale, 2 inline fallbacks (1 expired), 0 failed, 0 dropped, 3.40 ms estimated saved
  region relief trigger: on, 1 work unit (pathfinding), 3 region(s), 1 window(s) open
    4 windows entered, 3 exited, 0 abandoned; 27 overrun / 1800 calm bodies; 0 unit failures; last window 1.20 s ago
    suppressed 2 by cooldown, 0 by rate
    top offenders: region 5 (11 overrun, 2 windows, open), region 7 (4 overrun, 1 window)
```

| Metric | Meaning |
| --- | --- |
| `requests` | Submissions that reached the relief service |
| `accepted` | Submissions actually tracked and handed to the compute pool; a request cancelled between the in-flight reservation and the submission is counted as dropped, never as accepted |
| `completed` | Results applied on the owning region thread |
| `stale` | Results reported as `onDiscard(STALE)` because their epoch was no longer current |
| `inlineFallbacks` | Requests executed inline on the region thread |
| `expired` | Accepted requests that expired in the queue and were rerun inline |
| `failed` | Computations or applicators that threw and were reported through `onFailure` |
| `dropped` | Requests refused or results discarded because the region was gone or the apply was not on the owner thread |
| `estimatedSavedNanos` | Sum of the compute time of applied results; an estimate of region time saved |

The trigger block carries its own counters:

| Metric | Meaning |
| --- | --- |
| `windowsEntered` | Windows opened since start |
| `windowsExited` | Windows closed because the region became calm |
| `windowsAbandoned` | Open windows forgotten because the region stopped ticking |
| `overrunBodies` / `calmBodies` | Tick bodies that counted as an overrun or as calm |
| `suppressedByCooldown` / `suppressedByRate` | Window openings the hysteresis allowed but a limit blocked |
| `unitFailures` | Work unit callbacks that threw and were isolated |
| `lastWindowAgeNanos` | Age of the most recent window opening, or `-1` when none opened yet |
| `topOffenders` | Regions with the most overrun bodies, at most five |

The consumer line itself (`region-relief: enabled, 25% of the pool, ...`) carries the usual submitted,
completed, rejected, expired and timing counters of the pool.

`peakWorkers` on the `/tuff compute` line records the pool the engine actually ran with: with an automatic
worker count the pool is created empty and spawned once, at the budget the region scheduler reports, so
the startup placeholder never shows up as a peak.

## Operational notes

- Keep the trigger off unless there is a region subsystem that actually registers offloadable work. The
  service and the consumer cost nothing when nothing submits, and an enabled trigger with no unit only
  moves counters.
- Watch `inlineFallbacks` against `accepted`: a high fallback rate means the pool is too small for the relief
  share, the per-region cap is too low, or the queue wait is too short. All three are configuration, not
  corruption.
- Watch `stale`: a high stale rate means results arrive after the region moved on. Lower the work size or
  raise the queue wait only if the region can really use the older snapshot.
- Watch `suppressedByCooldown` and `suppressedByRate`: a high count means the region overruns more often than
  the policy allows windows. That is the anti-thrash guard doing its job; raise `max-windows` only if the
  unit can really absorb more work.
- `windowsAbandoned` above zero is normal after a region unloads while it was behind. It is a state cleanup,
  not an error.

## Rollback

The trigger is a restart-only feature with a single switch:

1. Set `async-compute.relief.trigger.enabled: false` and restart. Windows stop opening, the tick feed
   costs one flag check, and the registered units simply stop being called. Registered units stay
   harmless: nothing else invokes them.
2. If the offloading itself is the problem, set `async-compute.relief.enabled: false` as well. Every
   submission then runs inline again and the reserve drops to zero, which is exactly the behaviour of a
   server that never opted in.
3. Removing a unit at runtime is `trigger.unregister(unit)`; the trigger keeps counting but no longer
   calls it.

No state survives the restart: the window epoch, the counters and the cooldown are rebuilt from zero.
