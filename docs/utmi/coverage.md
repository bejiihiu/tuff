# Coverage

Coverage is how deeply a pool is integrated with the thread layer, not how important it is. It is one
value per registered pool, declared in the `PoolSpec`, printed by `/tuff threads` and carried by every
snapshot and JFR pool event. This page is the honest map of where each pool is in this build, what the
printed level actually proves, and what no amount of work will move. It also says what the optional
tick policies change here, which is nothing in these levels, and where a tick number can come from.

## Levels

The five levels are cumulative: a pool at L3 also keeps everything L2 promises.

| Level | Meaning | What the operator gets |
| --- | --- | --- |
| **L0** | Outside the centre, documented | Nothing. The pool has no registry entry and is not printed |
| **L1** | Registered and observed | Pool identity, thread counts, active/idle where measurable, saturation, ownership answers |
| **L2** | Threads or their bodies go through `ThreadFactoryProvider` | Per-thread registration while the thread runs, so accounting and lifecycle visibility |
| **L3** | Capacity comes from the resource controller | Central minimum, maximum and budget pressure |
| **L4** | Execution runs through a managed lane | The manager owns the lane, the threads and the budget |

L2 has two shapes and the difference matters:

- **Created.** The manager builds the thread from a `ThreadSpec`: name, daemon flag, priority, group
  and handler are the manager's. This is `service` and, when managed, the compute platform workers.
- **Wrapped.** The owner builds the thread and only its `Runnable` is wrapped. The manager registers
  the thread when the body starts and removes it in `finally`; class, name, group, daemon flag and
  exception handler stay the owner's. This is `region-execution` (under EDF, work stealing and the
  Kairos fund alike) and the compute virtual lane.

`CoverageLevel.managesThreads()` is true for both. A wrapped pool cannot be renamed or rebuilt by the
manager, because the owner's checks key off the concrete thread class.

## Current matrix

"Today" is the default configuration. "With flag" is the single key that raises the pool, if any.

| Pool | Today | With flag | What the level means here | Ceiling | Why not higher |
| --- | --- | --- | --- | --- | --- |
| `region-execution` | L1 | L2 — `region-execution.managed: true` | The spec is L2 and every tick body is wrapped; Folia still creates, names and schedules the thread, under EDF, work stealing and the Kairos fund alike | L2 | L3 means the pool's capacity comes from the controller and the pool leases it. EDF cannot be resized at all: its arrays are fixed at construction and `setThreads` is a silent no-op, so a count change is a restart. Work stealing resizes live and the elastic Kairos fund moves its floor live, but neither makes the pool a lease: the threads are still Folia's factory and Folia's class, and the fund only reads the controller's usable cores for its ceiling. L4 would need a replacement scheduler: `TickRegionScheduler` rejects any thread that is not its `TickThreadRunner`, and `AsyncCatcher` keys off the same class |
| `compute` | L1 | L4 — `execution-pools.compute.managed: true` (or `async-compute.managed`) | The adapter registers the pool at L4; the engine builds its platform workers through the provider and reads its processor budget from the controller. The queue, priorities and fairness stay the engine's. The budget is the ledger's usable cores after load shedding and the tick-pressure reserve, and never below `min-threads` plus the relief reserve while `async-compute.relief.enabled` is on | L4 | Already the deepest level. L4 does not mean the manager orders tasks here: compute is the one pool expected at L4 because it is the one pool whose execution the manager may own the lane of |
| `compute-virtual` | L1 | L2 — same compute flag, and `lanes.virtual.enabled: true` | The lane creates its own virtual threads and the manager wraps their bodies; the lane's platform pump is the one core the spec declares. A disabled lane is registered at L1 with a zero budget instead, so it cannot claim a share | L2 | The provider refuses to create a virtual thread on purpose, and the lane owns its queue and permits. L4 would mean the manager owns the lane, which is the lane's whole reason to exist |
| `chunk-worker` | L1 | L2 claim — `execution-pools.chunk-system.managed: true` | The spec is registered at L2 with `min 1`, but no owner hands over a factory or a body, so nothing is created or wrapped | L2 today; L2 is also the honest limit | L3+ needs a different queue: ordered streams carry sub-order values, and replacing them with a plain FIFO deadlocks chunk generation. The L2 claim is blocked on Moonrise handing its factory over before static init, not on the manager |
| `chunk-io` | L1 | Same flag as `chunk-worker` | Same registration claim; `IO_POOL` keeps its own threads | L2 today | Same ordered-queue argument, plus `non-daemon` and the 60-second drain in `MoonriseCommon.haltExecutors` |
| `network` | L1 | L2 claim — `execution-pools.network.managed: true` | Same registration claim; netty keeps its loops | L2 | An event loop owns its channels: parking or sharing it stalls every connection on it, and its affinity and graceful shutdown are netty's. L3 would need sizing authority over a pool whose size is already a netty contract |
| `chat` | L1 | None | The chat decoration executor (`Async Chat Thread`) is registered and observed by a probe at L1; it keeps its own `ThreadPoolExecutor` | L2 | L2 must keep that class and those setters, because `misc.chat-threads.chat-executor-core-size/-max-size` resizes it at runtime; L3 would silently break a documented user key. The chat filter worker is a different pool: non-daemon, blocking HTTP to an external service, and only ever L2 |
| `service` | L2 for the registered pool | None; the sampler raises it | The thread-layer refresher, and the Kairos supervisor while an elastic fund runs, are created through the provider with a one-core fixed budget | L2 today; L3 for a real background pool | The other service executors (`Util`, `MCUtil`, command builder, profiles) are separate pools with their own blocking and shutdown contracts, and none is registered yet |
| `background` | L1 | None | `Util.backgroundExecutor` (the shared ForkJoinPool) is registered and observed by a probe at L1; it keeps its own threads and growth | L2, L3 candidate | A ForkJoinPool grows workers when a task blocks, so a naive cap changes throughput instead of protecting it. L4 is out of the question: it is the JDK pool the whole server and its consumers already share |

`chat` and `background` are canonical `PoolId`s (`chat`, `background`), registered at L1 and backed by
their probes; "L0" would mean no registry entry at all, which is not their state.

`region-execution` reaches the same level under all three schedulers - L1 by default, L2 with
`managed: true` - because the level describes who creates the thread and wraps its body, not what
orders the ticks. A Kairos tick thread is a `TickThreadRunner` from Folia's factory with the same
group, name pattern and handler; the fund only decides which runner takes a tick and when, and the
bridge accounts the body exactly as it does on the EDF path.

## What a printed level proves

The level is the registered spec, so a level can be ahead of the accounting. That is deliberate and
worth knowing before trusting a line:

- **L1** is backed by a probe. Every canonical pool except `service` has one: region and compute from
  their own engines, chunk worker and IO from the Moonrise pool arrays, network from the event-loop
  names, chat and background from their executor counters. `alive 0` is a measured zero - the owner
  has no live thread right now - not a missing probe.
- **L2 for `region-execution`** is backed by the bridge: patch `0016` calls `RegionThreads.wrapBody`
  in the one region thread factory, and the provider registers the thread while the body runs. The
  `/tuff threads` count is `max(registry, probe)`, so it looks identical with `managed: false`; the
  printed level, the startup log and the bridge unit test are the evidence, not the count. The same
  bridge wraps the body when the Kairos fund runs it: selecting a scheduler does not change who
  creates the thread or what the level proves.
- **L2 for `service` and `compute-virtual`** is real but narrower than the compute pool's: the
  provider creates the refresher and the virtual lane's pump, and it wraps the bodies of the lane's
  virtual threads. The level does not carry queue or permit authority with it; the lane keeps both.
- **L2 for `chunk-worker`, `chunk-io` and `network`** is a registration claim only. No code path
  creates or wraps those threads yet. Treat the printed level as the intent recorded in the spec, not
  as proof that the threads are accounted.
- **L4 for `compute`** is real: the adapter returns the provider and the controller to the engine, and
  the engine calls them. It is the only place where the budget actually sizes a pool.

**A level is not a tick measurement.** L2 accounts a body while it runs; it says nothing about whether
that body started on time. The per-tick evidence is designed to be scheduler-neutral: one record per
executed body, `tuff.scheduler.Tick`, carrying the deadline the handle was measured against, the actual
start, the whole periods the region was behind and the periods the body wrote off. That shape lets a
stand compute a real hard miss on EDF, work stealing and Kairos with the same definition, instead of
using a five second average as a proxy. Arming a recording is a measurement decision, not a coverage
level, and this page quotes no number from it; [the harness](../bench/README.md) owns the definition and
the procedure that produces those numbers.

## Optional tick policies

The region pool's level is a statement about accounting, so the optional tick policies neither raise nor
lower it. Each one changes what a tick thread does, not who owns the thread.

| Policy | Default | What it changes | What it does not change |
| --- | --- | --- | --- |
| Soft affinity and stealing | `affinity.local-queue-size: 0` (one shared queue) | A runner may keep a bounded private backlog; another runner takes a kept tick only once it is past the steal threshold | No thread, permit or queue is created for the manager. The level stays L2 and the bridge accounts the body wherever it runs |
| Catch-up | `catch-up.cap-ticks: 1`, the stock single body | A late region may run bounded extra bodies, each accounting for one period, until the cap or the time budget settles the backlog | Thread count and coverage. Every body is still one acquisition on one permit; the change is to game semantics, not to accounting |
| Permit-wait reporting | `lease.timeout-seconds: 0`, accounting only | The supervisor counts the two waits on every grant and warns, rate limited, when one exceeded the timeout | Capacity. The number proves that permits, not CPU, are why a region waits; it cannot create either |
| Per-tick and per-pass recording | off | `tuff.scheduler.Tick` describes an executed body and `tuff.scheduler.Fund` a supervisor pass | Execution. It is evidence for a stand, not a coverage level; see [what a printed level proves](#what-a-printed-level-proves) |
| Idle-safe work | `idle-work.enabled: false` | A parked runner may run one bounded unit, and a runner that holds a permit may drain that region's intermediate tasks before its deadline | The pool and its level. The work runs on a thread the fund already has; it adds no thread, no permit and no queue, and its cost is capped by the slice, the lag budget and the per-second share |

The policies share one property worth naming: none of them is a capacity claim. A level says the
manager accounts a thread; a tick policy says what that thread may do with a tick it already owns. The
fund's `tuff.scheduler.QueuedOverdue` and `tuff.scheduler.LeaseWait` warnings point the other way: they
report a shortage the fund cannot fix, and the honest answers are fewer regions or a larger budget.

## What stays outside UTMI

These are not roadmap items. Each one is outside because moving it would change semantics the server
depends on, so "L0 forever" is the correct end state, not a missing patch.

| Outside | Why it can never be managed |
| --- | --- |
| **Netty event loops** | An event loop is the owner of its channels, and blocking or parking it stalls every connection assigned to it. Its thread count, affinity and graceful shutdown are netty contracts; the server only observes them. Sizing authority (L3) over an event loop would be a network change disguised as thread bookkeeping |
| **Chunk ordered streams** | Generation and loading are sequenced by sub-order values in `AreaDependentQueue`. A different queue, or a shared lane, breaks the ordering that prevents deadlock and duplicate work; `merge` and `split` also depend on exactly-once delivery. The chunk system may be observed (L1) and, once Moonrise hands a factory over, accounted (L2), but its execution and ordering stay its own |
| **Watchdog and shutdown threads** | The watchdog must not be parked: a parked watchdog stops noticing a hung server. The region shutdown thread must be a `TickThread` of the exact shutdown class, `halt` is cooperative and `join` waits up to 60 seconds by contract. A thread layer that blocks in that window turns a healthy shutdown into the watchdog's abnormal exit. The server already has its shutdown thread, so a second hook would only race it |
| **JDK virtual-thread carriers** | The carriers belong to the JDK scheduler and are shared by the server and every plugin. The only lever is `jdk.virtualThreadScheduler.parallelism` / `maxPoolSize`, and it applies to the whole JVM. UTMI can account a virtual lane and wrap its bodies; it cannot own the carriers |
| **Console, JFR, log4j, JLine** | Library and JDK owned. Visible in a thread dump and in the unaccounted count, never scheduled by us. The console in particular blocks in `readLine`; replacing or parking it stops `/stop` from being read |

The same rule explains the invariants in
[architecture](architecture.md#what-must-not-be-touched): registration is accounting and visibility,
never a merge of queues, ordering or lifecycles.

## Reading it in the report

One line per pool, in id order:

```
    region-execution  L2  alive 2  active 0  idle 2  cpu 0%
```

- `region-execution L2` means the spec is at L2 and the bridge is bound. `alive` is Folia's live
  count, `active`/`idle` come from `Thread.getState()` and are an approximation, `cpu` is the share of
  usable cores the pool's threads consumed in the last window.
- `chunk-worker L2` with `alive 0` means the spec claims L2 while the owner has handed nothing over.
  The number is honest; the level is the claim.
- `service L2 alive 2` on an elastic Kairos fund is the refresher plus `Tuff Kairos Supervisor`, both
  created through the provider; on a fixed fund or another scheduler it is the refresher alone.
- `compute L4` appears only when the compute switch is on, and then the pool is both measured by the
  engine probe and sized by the controller. With region relief enabled the worker count stays at or
  above `min-threads + reserved-cores` whatever the `pressure:` line reports as yielded.

[`observability.md`](observability.md) explains the rest of the line and when values are `-1`.

## Critical lanes inside the coverage model

Two pools are critical by contract - `region-execution` and `network` - and coverage does not change
that. What coverage changes is how much the manager may *take* from them; the reserve and load shedding
are about how much it may take from everybody else on their behalf.

- **The critical reserve is arithmetic, not a level.** The planner subtracts the critical lanes'
  guaranteed minimum before the shared pools resolve their budgets, so a non critical pool at L3 cannot
  lease it even when its own cap is larger than the machine. A shared pool may therefore end up with
  zero permits while its spec says more; the plan reports it and `usableCores` excludes the reserve.
- **Shedding never touches a critical pool.** Only pools whose `kind` is not `REGION_EXECUTION` or
  `NETWORK` and whose coverage is at least L3 (they are the ones with permits to withhold) can lose
  capacity, and never below their declared minimum. An L1/L2 pool has no permits of its own, so it is
  not a shedding target.
- **Shedding is dormant in this build.** Its trigger is a parked lease request from a critical pool,
  and only a pool at L3 or deeper can call `acquire`. Region execution and network stop at L2 here, so
  `/tuff threads` prints the line as `load shedding on but dormant: no critical pool can lease
  capacity` instead of claiming an active protection. The critical reserve still applies and the relief
  reserve still holds its workers; the dynamic half waits for a managed critical lane.
- **The relief reserve is a second, separate reservation.** While `async-compute.relief.enabled` is on,
  the compute pool keeps `reserved-cores` for relief work. Tick pressure may not lower the budget below
  `min-threads + reserved-cores` (plus one budget unit, because automatic sizing resolves a budget of
  `N` to `N - 1` workers), and the pool's idle floor is raised by the same amount, so keep-alive and the
  idle autoscaler cannot shrink the reserve away. Relief itself is a consumer inside the compute engine
  - `region-relief`, `NORMAL` priority, `GENERAL` and `CUSTOM` tasks only - not a pool in this
  registry, so it has no coverage level. With the trigger off the reserve is zero and the budget is
  exactly what load shedding and tick pressure decided. Where a shed and the relief floor meet, the
  floor wins, because it is the number of workers the pool promised to keep.
- **The critical reserve is not a coverage promise.** `region-execution` at L2 still has Folia's
  threads and `network` at L2 still has netty's loops: the reserve protects the leases those pools may
  hold, not the threads their owners create. Nothing here moves a pool to L3/L4, and nothing here
  reshapes a queue.

## Related pages

- [Architecture](architecture.md) — levels, pool kinds, the region bridge and the invariants.
- [Configuration](configuration.md) — the `managed` keys and their restart behaviour.
- [Observability](observability.md) — the command, the snapshot and the JFR events.
- [Kairos](../scheduler/kairos.md) — the fund and the tick policies above, their defaults and limits.
- [Idle-safe work](idle-work.md) — the contract for work a tick thread may run while it waits.
- [Region relief](../compute/region-relief.md) — the relief consumer, its completion contract and the
  reserve that tick pressure cannot take away.
