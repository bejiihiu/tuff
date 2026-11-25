# Compute scheduler

Tuff ships one shared pool of worker threads for CPU-bound server work. Server code and plugins submit a
task, get a `CompletableFuture` back, and the scheduler decides when it runs, on which worker, and how the
pool is shared between everyone who submitted.

The workers own no region, no chunk and no entity. A task computes from a snapshot, and the result is
delivered back to the thread that owns the world state.

## Why it exists

Without a shared pool every subsystem grows its own `ExecutorService`. Each pool is sized for itself, so the
server ends up with more runnable threads than cores, and the region tick threads pay for it in context
switches. None of those pools knows about the others either, so a burst in one subsystem starves the rest,
and queues are usually unbounded.

The compute scheduler replaces that with:

- one pool, one queue, one backpressure story;
- priorities with aging, so background work still runs under load;
- per-consumer quotas and fair-share weights, so one subsystem cannot take every worker;
- a bounded queue that refuses work instead of growing the heap;
- metrics and JFR events that describe the pool as a whole.

## Modules

| Module | Package | What lives there |
| --- | --- | --- |
| `tuff-compute` | `kz.bejiihiu.tuff.compute` | Public contracts: scheduler, tasks, priorities, consumer policies, settings, metrics |
| `tuff-compute` | `kz.bejiihiu.tuff.compute.internal` | Implementation: queue, worker pool, dispatcher, adaptive controller, metrics. No compatibility promise |
| `tuff-api` | `kz.bejiihiu.tuff.api.compute` | Plugin-facing access (`TuffCompute`), Folia handoff (`ComputeHandoff`), `Executor` adapter (`ComputeExecutor`) |
| `tuff-server` | `kz.bejiihiu.tuff.compute.config` | `config/tuff.yml` loading, validation, diffing, apply hook |

`tuff-compute` has no Minecraft, Bukkit or Paper dependency. `tuff-api` is the only module that knows about
Folia schedulers, and it is published as an API artifact for plugins.

Entry points in the repository:

- [`AsyncComputeScheduler`](../../tuff-compute/src/main/java/kz/bejiihiu/tuff/compute/AsyncComputeScheduler.java) — the scheduler contract
- [`ComputeSettings`](../../tuff-compute/src/main/java/kz/bejiihiu/tuff/compute/ComputeSettings.java) — the configuration model
- [`TuffCompute`](../../tuff-api/src/main/java/kz/bejiihiu/tuff/api/compute/TuffCompute.java) — how a plugin reaches the engine
- [`config/tuff.yml`](../../tuff-server/src/main/resources/tuff/tuff.yml) — the shipped configuration template

## Scope of this build

The engine, its public API, the configuration loader and the JFR instrumentation are in this build. The
following are not:

| Not included | What it means for you |
| --- | --- |
| Built-in consumers | No server subsystem submits to the pool. The `consumers.chunk-generation`, `consumers.pathfinding` and `consumers.ai-computation` sections are **examples** of policies for ids that a plugin or a future subsystem may use; nothing runs under those ids by default |
| An admin command | There is no `/tuff` command yet. Metrics are read through the Java API (`TuffCompute.scheduler().metrics()`); JFR is recorded with the standard JDK tools |

The rest of these pages describe the subsystem as implemented. The server bootstrap owns creating,
starting and installing the engine; plugins only read it.

## Enable it

1. `config/tuff.yml` is written from the bundled template the first time the configuration is loaded.
2. Keep `async-compute.enabled: true` (the default) and adjust the sections you care about. `enabled`,
   `queue.capacity`, `scheduling.*`, `adaptive.enabled` and dynamic consumer registration are baked into a
   running pool and need a restart; everything else is adoptable at runtime when a config apply hook is
   attached.
3. From a plugin, take the scheduler in `onEnable` and submit work:

```java
final AsyncComputeScheduler compute = TuffCompute.require();
final CompletableFuture<Route> route = compute.submit(ComputePriority.NORMAL, () -> plan(snapshot));
ComputeHandoff.toEntity(this, mob, route, mob.getPathfinder()::moveTo);
```

`TuffCompute.require()` fails plugin enable with an actionable message when the engine is disabled or not
installed. Plugins with a synchronous fallback should use `TuffCompute.scheduler()` instead.

## Concepts

| Term | Meaning |
| --- | --- |
| Task | One `ComputeTask<T>`: a self-contained computation returning a non-null value |
| Consumer | A stable id that work is submitted under; carries limits, fair-share weight and a queue bound |
| Priority | `CRITICAL`, `HIGH`, `NORMAL`, `LOW`; a dispatch hint, not a reservation |
| Policy | `ConsumerPolicy`: concurrency limit, queue bound, weight, overflow policy, max queue wait |
| Handoff | Delivering a finished result onto the region, entity or global context that may touch the world |

## Pages

- [Architecture](architecture.md) — components, threading model, exactly-once invariants, scheduling,
  queue behaviour, cancellation, autoscaling, JFR.
- [Plugin API](plugin-api.md) — submit, cancel, register consumers, hand results back correctly, common
  mistakes.
- [Configuration](configuration.md) — every key in `config/tuff.yml`, defaults, validation errors, presets.
- [Operations](operations.md) — logs, metrics, JFR recordings, tuning, known limitations.
