# Compute scheduler

Tuff ships one shared scheduler for CPU-bound and blocking server work: a platform worker pool plus an
optional virtual-thread lane. Server code and plugins submit a task, get a `CompletableFuture` back, and the
scheduler decides when it runs, on which lane and worker, and how the resources are shared between everyone
who submitted.

The workers own no region, no chunk and no entity. A task computes from a snapshot, and the result is
delivered back to the thread that owns the world state.

## Why it exists

Without a shared pool every subsystem grows its own `ExecutorService`. Each pool is sized for itself, so the
server ends up with more runnable threads than cores, and the region tick threads pay for it in context
switches. None of those pools knows about the others either, so a burst in one subsystem starves the rest,
and queues are usually unbounded.

The compute scheduler replaces that with:

- one scheduler, one queue, one backpressure story, with a platform pool and a virtual lane for blocking work;
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

The server bootstrap creates, starts and installs the engine before plugins are enabled; plugins only read
it. `/tuff` reports it and controls JFR.

## Enable it

1. `config/tuff.yml` is written from the bundled template the first time the configuration is loaded.
2. Keep `async-compute.enabled: true` (the default) and adjust the sections you care about. A live reload
   applies consumer policies only; `workers.*`, `queue.*`, `scheduling.*`, `metrics.*`, `adaptive.*` and
   `shutdown-grace-seconds` need a restart. `/tuff compute reload` prints which changes were applied and
   which were deferred.
3. From a plugin, take the scheduler in `onEnable` and submit work:

```java
final AsyncComputeScheduler compute = TuffCompute.require();
final CompletableFuture<Route> route = compute.submit(ComputePriority.NORMAL, () -> plan(snapshot));
ComputeHandoff.toEntity(this, mob, route, mob.getPathfinder()::moveTo);
```

`TuffCompute.require()` fails plugin enable with an actionable message when the engine is disabled or not
installed. Plugins with a synchronous fallback should use `TuffCompute.scheduler()` instead.

## Command

`/tuff` is registered at startup and needs `tuff.command.compute` (granted to operators by default). It
works from the console and from players.

| Form | Shows |
| --- | --- |
| `/tuff`, `/tuff compute`, `/tuff status` | Engine summary: state, platform and virtual lane lines, queue, task counters, throughput, wait/execution averages, CPU, startup settings |
| `/tuff compute consumers` | Per-consumer policy, active/queued tasks, counters and latency |
| `/tuff compute reload`, `/tuff reload` | Re-reads `config/tuff.yml`, applies consumer policies, lists deferred keys and validation problems |
| `/tuff compute jfr start\|stop\|dump` | Controls a JFR recording with the `tuff.compute` events |

With `async-compute.enabled: false` the command answers that the engine is disabled and how to enable it.
See [Operations § Command](operations.md#command) for output details and JFR limits.

## Concepts

| Term | Meaning |
| --- | --- |
| Task | One `ComputeTask<T>`: a self-contained computation returning a non-null value |
| Consumer | A stable id that work is submitted under; carries limits, fair-share weight and a queue bound |
| Priority | `CRITICAL`, `HIGH`, `NORMAL`, `LOW`; a dispatch hint, not a reservation |
| Policy | `ConsumerPolicy`: concurrency limit, queue bound, weight, overflow policy, max queue wait |
| Lane | Platform worker pool or virtual-thread lane; the priority decides which one runs a task |
| Handoff | Delivering a finished result onto the region, entity or global context that may touch the world |

## Pages

- [Architecture](architecture.md) — components, threading model, exactly-once invariants, scheduling,
  queue behaviour, cancellation, autoscaling, JFR.
- [Plugin API](plugin-api.md) — submit, cancel, register consumers, hand results back correctly, common
  mistakes.
- [Configuration](configuration.md) — every key in `config/tuff.yml`, defaults, validation errors, presets.
- [Tuning](tuning.md) — server-type presets, lane and spill choices, symptom → knob, anti-patterns.
- [Operations](operations.md) — command, logs, metrics, JFR recordings, benchmarks, tuning, known limitations.
- [Benchmarks](benchmarks.md) — JMH numbers for the submission path, dispatch, strategies, cancellation,
  lifecycle and idle latency, with the harness caveats.
- [Chunk system](chunk-system.md) — why the chunk system's ordered streams stay on their own executors and
  what would break if its tasks were moved onto this queue.
