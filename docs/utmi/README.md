# Unified thread management (UTMI)

UTMI is Tuff's thread layer: one registry of every execution pool, one CPU budget ledger, one thread
factory and one metrics view for the threads the server already runs.

It is not a scheduler. Folia keeps region ticks, tick order and region ownership; the compute engine
keeps its queue, priorities and fairness. UTMI owns the accounting underneath both: which pools exist,
how many threads they have, what the CPU budget is and what happens when it runs out.

## Why it exists

A regionised server runs dozens of independent executors: region ticks, chunk workers and chunk IO,
netty event loops, the compute pool, plugin schedulers and a long tail of small service pools. Each
one is sized by a different config key, none of them knows about the others, and there is no single
place to ask "how many threads exist, who owns them and what is the machine actually doing".

Three problems follow from that:

- **No registry.** `/tps` knows regions and `/tuff compute` knows the compute engine; nothing knows
  the whole process. A new pool appears in a thread dump and nobody can say whether it is expected.
- **No shared CPU ledger.** The compute engine already subtracts region, chunk and IO threads by hand
  before sizing itself. Every new consumer would repeat that arithmetic with slightly different
  constants.
- **No per-pool accounting.** "The server has 300 threads" is not actionable; "these 12 belong to the
  chunk IO pool, 4 of them are runnable" is.

UTMI replaces that with a registry, a single budget plan, a factory that names and accounts threads,
and a snapshot that is consistent at one moment. Registration does not merge pools: a pool keeps its
owner, its queue, its ordering and its lifecycle. The layer centralises **management and visibility**,
not execution.

## What runs in this build

- The manager is assembled from `thread-management` in `config/tuff.yml` and started at the top of the
  existing compute startup hook, before the compute engine and before Folia's region scheduler is
  constructed. It is stopped at the bottom of the compute stop hook, after the compute engine is down
  and before the world save starts.
- Nine canonical pools are registered at startup: `region-execution`, `compute`, `compute-virtual`,
  `chunk-worker`, `chunk-io`, `network`, `chat`, `background`, `service`. Every registered pool has an
  entry in `/tuff threads` (unless per-pool publication is switched off), a coverage level and a budget.
- `region-execution` is observed through a probe: live thread count from Folia's region scheduler, the
  thread list from its thread group, active and idle derived from `Thread.getState()`, CPU time per
  thread from `ThreadMXBean`. With `region-execution.managed: true` the same pool is raised to L2 and
  the `RegionThreads` bridge wraps each tick body, so Folia's threads are accounted while they run
  without changing their class, name, group, daemon flag or handler.
- Every other specialised pool has a cheap L1 probe: chunk worker and IO from the Moonrise pool
  arrays, netty event loops by name, the chat executor from its `ThreadPoolExecutor` counters, and the
  background worker pool from its `ForkJoinPool` counters. None of them is resized, parked or replaced.
- The CPU ledger is computed once from the processor topology, the configured reserve and the
  deductions, and re-derived whenever the registered pool set changes. It reports itself degraded when
  the arithmetic cannot cover what the pools declared.
- A thread factory, a lease controller and the registry are implemented and covered by tests: naming,
  daemon flag, group, handler, per-pool accounting, permits, idempotent release, waking waiters on
  shutdown.
- `/tuff threads` prints the state, the pools, their coverage and degraded reasons, the unknown-thread
  count and the settings that need a restart.
- Seven `tuff.threads.*` JFR events describe pool state, thread creation and termination, saturation,
  budget changes, lease timeouts and unaccounted threads.

## What is not included yet

Saying "observe only" is not enough; this is the exact list of things that are configured or modelled
but do not execute yet, and why.

| Not included | Effect today | Why |
| --- | --- | --- |
| Region threads at L2 (bodies wrapped) | With `region-execution.managed: true` the pool is registered at L2 and each tick body goes through `ThreadFactoryProvider.wrap`, so region threads are accounted while they run. Folia still creates its own `TickThreadRunner` objects: the class, name, group, daemon flag and uncaught handler stay Folia's | L2 is the deepest honest level here. Creating the threads elsewhere needs a replacement scheduler, not a wrapper: ownership checks, `AsyncCatcher` and the shutdown contract all key off the concrete tick thread class |
| Compute at L4 | Only with `execution-pools.compute.managed: true` (or its older spelling). Off, `compute` and `compute-virtual` are registered at L1 and the engine keeps its own threads, queue and sizing | The adapter exists, but management is a separate reviewed step; the safe default is observation. With the flag on, the engine takes its workers and processor budget from the thread layer and keeps its queue |
| Queue depth for chunk, netty, chat and background pools | Snapshots report `queued = -1` for them, and `active` can be a state approximation | Moonrise has no queue-depth getter, netty keeps its internal queues private, and the chat executor's cached handoff queue is always zero by design; the counters that do exist are reported as they are |
| Region queue depth and tick-start latency | Snapshots report `queued = -1` for the region pool | `RegionTaskQueueData.hasTasks()` is a boolean and the prioritised queues have no public size; `startOvershoot` is not exposed by the tick report |
| Per-region tick breakdown | The region lines are one aggregate over all regions; there is no per-region row, and only the 5 s and 15 s windows are read | Per-region windows belong to the region handle and are updated per tick; `/tps` remains the region view, see [observability](observability.md) |
| Live reload of `thread-management` | Every key is restart-only and `/tuff reload` says so | The runtime bakes its pool set and sampling policy in at start; a live change would be a promise the manager cannot keep |
| Kairos scheduler | `scheduler: kairos` selects the elastic fund; it needs `kairos-unstable: true`, the config is `region-execution.kairos.*`, and `/tuff scheduler` reports it | Implemented and opt-in. The fund's ceiling comes from the resource controller when the layer runs, the supervisor is a `service` thread at L2, and a stale tick is warned about once per ten seconds. Under EDF or work stealing the command says honestly that it has no fund telemetry |
| One authoritative budget | The ledger is computed and reported; it sizes `compute` only when `execution-pools.compute.managed: true`. Unmanaged compute still sizes itself from `ComputeThreadBudget` | Handing capacity over is the point at which a wrong number changes thread counts, so it stays behind the compute switch instead of being always on |

`thread-management.enabled: false` is a real kill switch: no runtime is created, no pool is
registered, no metrics are sampled and no thread is started. `/tuff threads` still answers and says
where the switch is.

## Relation to the compute scheduler

The compute engine is one of the pools, not a second centre:

- `compute` (platform workers) and `compute-virtual` (virtual lane) are registered as pools with
  `PoolKind.COMPUTE` and `PoolKind.VIRTUAL`. UTMI never owns their queue, their priorities or their
  worker loop.
- By default both are registered at L1 and observed through the engine probe; the engine's own
  `SchedulerMetrics` remains the source of truth for task counters, queue depth and throughput. With
  `execution-pools.compute.managed: true` the adapter registers `compute` at L4 and `compute-virtual`
  at L2, and the engine takes its platform workers from the provider and its processor budget from
  the resource controller.
- Sizing follows the same switch, one source at a time: managed compute reads
  `ThreadResourceController`; unmanaged compute still uses `ComputeThreadBudget` (80% of the
  processors minus the live region, chunk and IO thread counts and a fixed network reserve). UTMI
  computes its own ledger with configured deductions, and no value is silently substituted for the
  other.
- The rule for the future integration is one source per number: UTMI owns capacity, compute owns
  domain metrics, and no value is summed across the two layers.

Start with [`docs/compute/`](../compute/README.md) for what the compute engine does with the CPU it is
given.

## Pages

- [Architecture](architecture.md) — components, coverage levels L0–L4, the current pool matrix, the
  region bridge, the budget model, thread safety, lifecycle and the invariants that must not be broken.
- [Configuration](configuration.md) — every key under `thread-management:`, its default, whether it is
  restart-only, and the platform limits that make some keys do less than they look.
- [Coverage](coverage.md) — the L0–L4 matrix per pool: what each level buys, where each pool stops and
  what stays outside UTMI by design.
- [Observability](observability.md) — `/tuff threads`, what a snapshot contains, `tuff.threads.*` JFR
  events, unknown threads and why server-wide tick metrics do not exist on Folia.

## Quick start

The layer is on by default and changes nothing:

```yaml
thread-management:
  enabled: true
  region-execution:
    managed: false
```

Then check what it sees:

```
/tuff threads
```

The command needs `tuff.command.compute` (granted to operators) and works from the console. There is
no separate thread-management command; `/tuff` is the single entry point.
