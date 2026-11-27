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
always at most once:

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

Two cases cannot call anything: an owner port that runs the scheduled task off the owner thread (the result
is dropped and counted, because calling region code from the wrong thread is worse than losing the
notification), and a port that refuses the task outright. The relief metrics count both.

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
```

| Key | Type | Default | Allowed | Effect |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `false` | `true`/`false` | Offload relief work at all. `false` runs every submission inline |
| `max-in-flight-per-region` | integer | `8` | `>= 1` | Accepted relief requests one region may have outstanding |
| `reserved-cores` | integer | `1` | `>= 0` | Cores the pool keeps for relief under tick pressure; only applied while `enabled` |
| `consumer.*` | policy | see above | the consumer policy keys | Policy of the `region-relief` consumer |

All relief keys are restart-only: the service is wired into the engine and the budget sink at start.
`/tuff compute reload` reports a change under `takes effect after a restart`.

## Metrics

The relief counters are part of `SchedulerMetrics#regionRelief()` and printed by
`/tuff compute consumers`:

```
  region relief: enabled, 0 in flight / 8 per region, reserve 1 core(s)
    12 requests, 10 accepted, 9 completed, 1 stale, 2 inline fallbacks (1 expired), 0 failed, 0 dropped, 3.40 ms estimated saved
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

The consumer line itself (`region-relief: enabled, 25% of the pool, ...`) carries the usual submitted,
completed, rejected, expired and timing counters of the pool.

## Operational notes

- Keep the trigger off unless there is a region subsystem that actually registers offloadable work. The
  service and the consumer cost nothing when nothing submits.
- Watch `inlineFallbacks` against `accepted`: a high fallback rate means the pool is too small for the relief
  share, the per-region cap is too low, or the queue wait is too short. All three are configuration, not
  corruption.
- Watch `stale`: a high stale rate means results arrive after the region moved on. Lower the work size or
  raise the queue wait only if the region can really use the older snapshot.
- The metric `peakWorkers` is no longer polluted by the startup budget: with an automatic worker count the pool
  is created empty and spawned once, at the budget the region scheduler reports.
