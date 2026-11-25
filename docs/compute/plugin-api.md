# Plugin API

Everything here lives in `tuff-api` under `kz.bejiihiu.tuff.api.compute`, except the task model, which is in
`tuff-compute` under `kz.bejiihiu.tuff.compute`. Add the API artifact to the plugin build:

```kotlin
dependencies {
    compileOnly("kz.bejiihiu.tuff:tuff-api:26.3.local-SNAPSHOT")
}
```

Tuff is the server, not a plugin, so there is no `depend:` entry to add. The classes are on the server
classpath at runtime.

## Reaching the engine

```java
import kz.bejiihiu.tuff.api.compute.TuffCompute;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;

final AsyncComputeScheduler compute = TuffCompute.scheduler().orElse(null);
if (compute == null) {
    getLogger().warning("Tuff compute is unavailable; falling back to synchronous work");
}
```

| Method | Behaviour |
| --- | --- |
| `TuffCompute.scheduler()` | `Optional<AsyncComputeScheduler>`; never throws, empty when the engine is unavailable |
| `TuffCompute.require()` | The scheduler, or `IllegalStateException` with a diagnostic message |
| `TuffCompute.isAvailable()` | Cheap null check; says nothing about the scheduler accepting work |
| `TuffCompute.executor(consumerId)` | `ComputeExecutor` shortcut for `new ComputeExecutor(require(), id)` |

`isAvailable()` and a non-empty `scheduler()` do not prove that submissions will be accepted: a scheduler
can be installed while the configuration disables the engine, and it can be shutting down. Always handle
`ComputeRejectedException` and `ComputeShutdownException`.

The scheduler is process-wide and owned by the server. Plugins never call `start()`, `shutdown()` or
`TuffCompute.install()`.

## Submitting work

```java
final CompletableFuture<Route> future = compute.submit(() -> plan(snapshot));          // default consumer, GENERAL, NORMAL
final CompletableFuture<Route> urgent = compute.submit(ComputePriority.HIGH, () -> plan(snapshot));
final CompletableFuture<Route> typed = compute.submit(
    "pathfinding",
    ComputeTaskType.PATHFINDING,
    ComputePriority.NORMAL,
    () -> plan(snapshot)
);
```

All three return a `CompletableFuture` and throw on refusal. When the task id, state and cancellation flag
matter, use `enqueue`:

```java
final ComputeTaskHandle<Route> handle = compute.enqueue(
    new ComputeRequest<>("pathfinding", ComputeTaskType.PATHFINDING, ComputePriority.NORMAL, () -> plan(snapshot))
);
ComputeHandoff.toEntity(this, mob, handle, mob.getPathfinder()::moveTo);

// superseding request, mob removal, plugin disable:
// handle.cancel();
```

`ComputeRequest.of(task)` and `ComputeRequest.of(priority, task)` build requests for the default consumer
with `GENERAL`.

### What a task may do

A task body runs on a scheduler worker. It may:

- read immutable snapshots, arrays and values captured before submission;
- allocate, compute, parse, hash, sort, search;
- call thread-safe library code that does not touch the world;
- throw, including checked exceptions.

It may not read or write blocks, entities, inventories, scoreboards, chunk state or any object whose
ownership belongs to a region. It also may not block waiting for a region thread. See
[Common mistakes](#common-mistakes).

A task must return a non-null value. Returning `null` is treated as a task bug and fails the future with a
`NullPointerException`; use `Optional` when absence is meaningful.

### Priorities

| Priority | Use it for | Cost of getting it wrong |
| --- | --- | --- |
| `CRITICAL` | Work a player is actively waiting for right now | A flood of `CRITICAL` degrades scheduling into FIFO and defeats aging |
| `HIGH` | Work that gates region progress: a block decision, a spawn decision | Too much `HIGH` makes `NORMAL` behave like `LOW` |
| `NORMAL` | Ordinary compute work; the default | — |
| `LOW` | Prefetch, cache warm-up, speculative work that may be dropped | Too much `LOW` wastes worker time if the queue is contended |

Priority orders *dispatch*. It does not preempt a running task, does not reserve a worker, and does not
override consumer quotas or fair-share weights. A `LOW` task that waits long enough is aged upwards; a
`CRITICAL` task still cannot exceed its consumer's concurrency limit.

## Consumers

A consumer is a stable id with limits attached. Two ways to get one:

```java
// 1. Declare it with an explicit policy. Idempotent by id, safe across plugin reloads.
compute.registerConsumer(ComputeConsumer.of(
    "map-colour-cache",
    ConsumerPolicy.relative(0.25)          // at most a quarter of the pool, capped, not reserved
));

// 2. Just submit under the id. With allow-dynamic-registration it is registered from config/default.
compute.submit("map-colour-cache", ComputeTaskType.CUSTOM, ComputePriority.LOW, () -> rebuild(snapshot));
```

`ComputeConsumer.of(id, policy)` allows every task type up to `CRITICAL`. The four-argument `of` restricts
types and priority:

```java
compute.registerConsumer(ComputeConsumer.of(
    "map-colour-cache",
    Set.of(ComputeTaskType.CUSTOM),
    ComputePriority.LOW,
    ConsumerPolicy.relative(0.25)
));
```

Policy helpers:

| Call | Meaning |
| --- | --- |
| `ConsumerPolicy.defaults()` | Whole pool, weight 1.0, no queue bound, `REJECT`, no expiry |
| `ConsumerPolicy.relative(0.25)` | At most 25% of the current pool-wide concurrency limit, minimum 1 |
| `ConsumerPolicy.absolute(3)` | At most 3 concurrent tasks, capped by the pool limit |

Relative limits are a share of the *current* limit, so they follow adaptive resizing. They are a cap, never
a reservation: a consumer with `0.5` is not guaranteed half the workers.

`registerConsumer` replaces an existing policy with the same id, which makes plugin enable/disable cycles
safe. `unregisterConsumer(id)` cancels queued work of that consumer with `CONSUMER_DISABLED` and flags
running work; running tasks are not interrupted. `updateConsumerPolicy(id, policy)` requires the consumer
to be registered and applies the new limits from the moment it returns.

## Results, failures and refusals

| Outcome | Future completes with | Handle state |
| --- | --- | --- |
| Body returns a value | The value | `COMPLETED` |
| Body throws | The original throwable | `FAILED` |
| Cancelled before start | `ComputeCancelledException` with a reason | `CANCELLED` |
| Cancelled while running | `ComputeCancelledException` after the body returns | `CANCELLED` |
| Queue wait expired | `ComputeCancelledException` `QUEUE_EXPIRED` | `CANCELLED` |
| Evicted by `DROP_OLDEST` | `ComputeCancelledException` `EVICTED` | `CANCELLED` |

Refusals throw from the submission call, not from the future:

| Exception | Reason | What to do |
| --- | --- | --- |
| `ComputeRejectedException` | `QUEUE_FULL` | Shed, retry later, or lower the submission rate |
| | `CONSUMER_QUEUE_FULL` | The consumer's own bound; back off for that consumer |
| | `CONSUMER_DISABLED` | The config disabled this consumer |
| | `CONSUMER_UNKNOWN` | Dynamic registration is off and the id is not configured |
| | `TASK_TYPE_NOT_ALLOWED` | The consumer's allowed types do not include this one |
| | `PRIORITY_NOT_ALLOWED` | Priority outranks the consumer's maximum |
| | `SCHEDULER_DISABLED` | The engine is disabled in `config/tuff.yml` |
| `ComputeShutdownException` | — | The scheduler is not running or is shutting down; do not retry |

A rejection is a normal, recoverable outcome and the caller keeps ownership of the task: nothing was
queued, no slot was consumed, and the task will never run.

## Cancellation

```java
final ComputeTaskHandle<Route> handle = compute.enqueue(
    new ComputeRequest<>("pathfinding", ComputeTaskType.PATHFINDING, ComputePriority.NORMAL, () -> plan(snapshot))
);

// later, e.g. the mob died or the request was superseded
handle.cancel();
```

A body that runs in slices and wants to stop early has to see the flag. The handle does not exist until
`enqueue` returns, so publish it through a holder and treat "not published yet" as "keep working":

```java
final AtomicReference<ComputeTaskHandle<Route>> self = new AtomicReference<>();
final ComputeTaskHandle<Route> handle = compute.enqueue(new ComputeRequest<>(
    "pathfinding", ComputeTaskType.PATHFINDING, ComputePriority.NORMAL,
    () -> {
        for (int step = 0; step < steps; step++) {
            final ComputeTaskHandle<Route> mine = self.get();
            if (mine != null && mine.isCancellationRequested()) {
                return fallbackRoute;    // any non-null value of your own
            }
            // ... one slice of the search ...
        }
        return route;
    }
));
self.set(handle);
```

Workers are never interrupted, so `Thread.currentThread().isInterrupted()` is not a cancellation signal here;
the handle flag is the only one.

Cancelling the future is equivalent: `future.cancel(true)` routes to `handle.cancel()` and never interrupts
the worker. `cancel()` returns `false` if the task already ended; it is idempotent.

`queueWaitNanos()` and `executionNanos()` return `-1` until the corresponding phase has happened.

## Delivering results back to the server

A worker may not touch the world. `ComputeHandoff` attaches a completion callback and schedules it on the
thread that owns the target.

### Region

```java
ComputeHandoff.toRegion(this, location, future, result -> {
    block.setType(result.material(), false);      // runs on the region that owns location
});
```

`toRegion` with a `Location` captures world and chunk coordinates immediately, before the future completes,
so the callback never dereferences a mutable `Location` from a worker. There is also a `(world, chunkX,
chunkZ)` overload when you have coordinates instead of a location.

### Entity

```java
ComputeHandoff.toEntity(this, mob, future, route -> mob.getPathfinder().moveTo(route));
```

Use the entity variant for anything that acts on one entity. The entity scheduler follows it across
regions and worlds, and reports retirement when it was removed first.

### Global region

```java
ComputeHandoff.toGlobal(this, future, weather -> world.setStorm(weather.storm()));
```

Use it for work that belongs to no specific region: time, weather, global counters that must be touched on
the global thread.

### Delivery rules

- A cancelled task delivers nothing. Cancellation is a normal outcome, not an error, and is never passed to
  the failure handler.
- A failed task passes the original throwable to `onFailure`, on the target context when possible.
- A removed entity produces `HandoffException(ENTITY_RETIRED)`. That callback runs inside the entity
  scheduler's retired path, which is critical region code: it must not load chunks, remove entities or
  mutate world state, only log or release plugin-owned resources.
- A plugin disabled before delivery produces `HandoffException(PLUGIN_DISABLED)`. A task already scheduled
  and then cancelled by Bukkit because the plugin got disabled is dropped silently; the server invokes no
  callback for that case.
- A target region or world that no longer exists means the scheduled task is discarded. That is a silent
  skip by design.
- Every call delivers at most once and never joins the future. Do not hand the same future to two handoff
  calls unless applying the result twice is intended.

The overload without `onFailure` logs delivery and computation failures through the plugin logger at
`SEVERE`.

## Executor adapter

Some APIs only accept an `Executor`. `ComputeExecutor` is the bridge:

```java
final Executor executor = new ComputeExecutor(
    compute, "map-colour-cache", ComputePriority.LOW, ComputeExecutor.RejectionPolicy.CALLER_RUNS);
cache.rebuildAsync(executor);
```

| Constructor | Defaults |
| --- | --- |
| `(scheduler, consumerId)` | `NORMAL`, `GENERAL`, `CALLER_RUNS`, failures logged to the system logger |
| `(scheduler, consumerId, priority, rejectionPolicy)` | `GENERAL` |
| `(scheduler, consumerId, priority, taskType, rejectionPolicy, failureHandler)` | Everything explicit |

`execute(Runnable)` semantics:

- a normal submission runs on a worker;
- `ComputeRejectedException` is handled by the rejection policy: `CALLER_RUNS` runs the command on the
  calling thread (exceptions propagate to the caller, like `CallerRunsPolicy`), `DROP` discards it;
- `ComputeShutdownException` drops the command, because there is no safe context left to run it in;
- a task body that throws is reported to the failure handler, never to the engine;
- after `shutdown()`, `execute` throws `RejectedExecutionException`. `shutdown()` only detaches the adapter
  and never stops the shared engine.

Choose `DROP` when running on the calling thread would violate the caller's threading assumptions, for
example when the caller is a region thread and the work is only an optimisation.

## Common mistakes

### Touching the world from a task body

```java
// wrong: a worker owns no region
compute.submit(() -> {
    block.setType(Material.STONE);        // IllegalStateException, data race, or both
    return true;
});
```

Folia guards region ownership, so this usually throws — but "usually" is not a guarantee, and reading a
live object is just as wrong as writing it. Capture a snapshot on the owning thread first:

```java
final BlockData snapshot = block.getBlockData().clone();
final CompletableFuture<Boolean> changed = compute.submit(() -> analyse(snapshot));
ComputeHandoff.toRegion(this, location, changed, result -> {
    if (result) {
        block.setBlockData(snapshot, false);
    }
});
```

### `join()` or `get()` on a region thread

```java
// wrong: this region stops ticking until the task finishes
final Route route = compute.submit(() -> plan(snapshot)).join();
```

A region thread that blocks stops its region's tick loop. If the task needs that region to make progress —
directly or through another handoff — the server deadlocks. Submit, return, and use `ComputeHandoff`. If the
result is genuinely needed synchronously, run the computation on the calling thread instead of submitting
it.

### Ignoring rejection

`submit` throws when the queue is full. Dropping that exception silently loses work. Either catch
`ComputeRejectedException` and decide, or use `ComputeExecutor` with `CALLER_RUNS` so nothing is lost.

### Assuming priority preempts

Submitting `CRITICAL` does not pause a running `LOW` task, does not bypass a consumer quota and does not
reserve a worker. Keep task bodies small; a single long task occupies a slot for its whole duration.

### A consumer id per request

`"pathfinding-" + uuid` creates an unbounded number of consumer streams, each with its own queue, quota
and metrics entry. Use a stable id per subsystem and put the identity in the task payload.

### Forgetting to cancel on disable

Queued tasks and futures survive a plugin disable. Cancel handles in `onDisable`, or rely on
`ComputeHandoff` reporting `PLUGIN_DISABLED`, and never keep a callback that touches unloaded plugin state.

## When the engine is unavailable

Two independent cases:

1. `TuffCompute.scheduler()` is empty: the engine is not installed (disabled, failed to start, or not
   installed yet). Decide at `onEnable` whether the plugin can work without it.
2. The scheduler exists but rejects with `SCHEDULER_DISABLED`: `async-compute.enabled` is false, yet the
   object is installed.

Fallback pattern:

```java
private AsyncComputeScheduler compute;

@Override
public void onEnable() {
    this.compute = TuffCompute.scheduler().orElse(null);
    if (this.compute == null) {
        getLogger().warning("Tuff compute is unavailable; pathfinding runs on the region thread");
    }
}

private CompletableFuture<Route> planAsync(BlockPos from, BlockPos to) {
    if (this.compute == null) {
        return CompletableFuture.completedFuture(planSync(from, to));   // caller is on the region thread
    }
    return this.compute.submit("pathfinding", ComputeTaskType.PATHFINDING, ComputePriority.NORMAL,
        () -> planSnapshot(from, to));
}

private void submitOrRun(Runnable work) {
    if (this.compute == null) {
        work.run();                    // correct only if the caller owns the state work touches
        return;
    }
    try {
        this.compute.submit("my-plugin", ComputeTaskType.CUSTOM, ComputePriority.LOW, () -> {
            work.run();
            return Boolean.TRUE;
        });
    } catch (ComputeRejectedException | ComputeShutdownException refused) {
        work.run();
    }
}
```

Do not retry a `ComputeShutdownException`. Do not busy-retry a `QUEUE_FULL`; back off, shed, or run the work
on the caller if that is safe.

## API reference

`AsyncComputeScheduler`:

| Method | Notes |
| --- | --- |
| `submit(ComputeTask<T>)` | Default consumer, `GENERAL`, `NORMAL` |
| `submit(ComputePriority, ComputeTask<T>)` | Default consumer, `GENERAL` |
| `submit(String, ComputeTaskType, ComputePriority, ComputeTask<T>)` | Fully specified, returns the future |
| `enqueue(ComputeRequest<T>)` | Fully specified, returns the handle |
| `registerConsumer(ComputeConsumer)` | Idempotent by id |
| `unregisterConsumer(String)` | Cancels queued work of that consumer and flags running work |
| `updateConsumerPolicy(String, ConsumerPolicy)` | Consumer must exist |
| `consumer(String)` / `consumerIds()` | Introspection |
| `metrics()` | Snapshot of counters and gauges |
| `settings()` | The settings this scheduler was created with |
| `state()` / `isRunning()` | Lifecycle |

`ComputeTaskHandle<T>`: `taskId()`, `consumerId()`, `taskType()`, `priority()`, `submittedAt()`, `future()`,
`state()`, `cancel()`, `isCancellationRequested()`, `isCancelled()`, `isDone()`, `queueWaitNanos()`,
`executionNanos()`, `failure()`.
