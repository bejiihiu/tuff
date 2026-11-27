# Observability

Everything the thread layer knows is available through three surfaces: `/tuff threads`, the
`ResourceSnapshot` the command prints, and the `tuff.threads.*` JFR events. There is no periodic log
line and no external metrics endpoint.

## The command

```
/tuff threads
```

It needs `tuff.command.compute` (granted to operators by default) and works from the console and from
players. It is a subcommand of `/tuff`, not a separate command, and it takes no arguments.

| State | What it prints |
| --- | --- |
| Not started yet | `The Tuff thread layer has not been initialized yet.` |
| `enabled: false` | Where the switch is and what to do; no pools, no numbers |
| Startup failed | That the server runs without the layer, and to look for the `Tuff/Threads` logger |
| Running, no sample yet (`warming up (no sample yet)`) | That the refresher has not published a pass; retry in a second |
| Running | The summary below |

A running server with `region-execution.managed: true` prints something like this. The shape is fixed;
the region, degraded and settings lines below are the ones the managed smoke run produced, while
counts such as threads and unknown threads vary with the JVM and the load:

```
Tuff thread layer: running, some pools managed
  cores: 10 usable after the reserve, 1 reserved; 4 registered thread(s) (peak 4)
  reserve: 1 core(s) held for the critical lanes; load shedding off (thread-management.resources.load-shedding: false)
  sample: taken 412 ms ago
  pools: 9 registered
    background        L1  alive 3  active 1  idle 2  cpu 0%
    chat              L1  alive 0  active 0  idle 0  cpu 0%
    chunk-io          L1  alive 1  active 0  idle 1  cpu 0%
    chunk-worker      L1  alive 4  active 2  idle 2  cpu 0%
    compute           L1  alive 4  active 0  idle 3  cpu 0%
    compute-virtual   L1  alive 1  active 0  idle 0  cpu 0%
    network           L1  alive 4  active 0  idle 4  cpu 0%
    region-execution  L2  alive 2  active 0  idle 2  cpu 0%
      regions 1 / avg MSPT 0.5 ms / min TPS 20.0 / lag 0% (0 of 1)
      ticks: 5s max 1.1 ms, 15s avg 0.8 ms max 32.5 ms; entities 0 / players 0 / chunks 25
    service           L2  alive 1  active 0  idle 0  cpu 0%
  pressure: disabled (thread-management.async-compute.tick-pressure.enabled: false)
  scheduler: edf - no fund telemetry (Kairos only)
  degraded: FEATURE_DISABLED, OBSERVE_ONLY, SCHEDULER_LIMITATION
  unknown threads: 32
  settings: reserve 1 core(s), region scheduler edf, region threads unknown; changes take effect after a restart
```

With the default `region-execution.managed: false` the header says `running, all pools observed only`,
the region line says `L1`, and `SCHEDULER_LIMITATION` is absent from `degraded:`; everything else has
the same shape.

How to read it:

- **`running, all pools observed only` / `running, some pools managed`** comes from the configuration
  (`anyManagementEnabled()` derived from the `managed` flags), not from the measured coverage.
- **`cores: N usable after the reserve, R reserved`** is the snapshot's `usableCores`, which is the
  budget ledger's `usable`: `floor(processors × 0.8)` minus the deducted lanes, the effective reserve and
  the critical reserve, floored at one. `R` is the configured reserve before that floor. The raw
  subtraction and every term are in the snapshot's `budgetLedger`, the same line
  `ThreadResourceController.budgetLedger()` returns, so there is no second "usable" number to compare
  against.
- **`reserve:`** is the critical reserve and the load shedding state in one line. The reserve is the part
  of the budget the non critical pools may never lease, whatever they declare; it is already subtracted
  from `cores:`. With shedding off the line names the switch; with it on the line is the controller's own
  summary, e.g. `load shedding withholding 2 core(s) from compute while a critical pool waits; 2 core(s)
  held for the critical lanes`. `nothing withheld` means the policy is armed and no critical pool is
  waiting. A pool can hold fewer permits than its spec says for exactly these two reasons, and
  `degraded:` names them (`CRITICAL_POOL_PRESSURE`, `LOAD_SHEDDING`).
- **`sample: taken ... ago`** is the age of the cached sample the refresher published. The command never
  measures; a value of `unknown` means no sample exists yet and the summary is the `warming up` line.
- **`N registered thread(s)`** is the sum of per-pool `alive`. It includes threads a probe reports and
  the refresher itself, not only threads the engine's registry created, so it is non-zero on an idle
  observe-only server.
- **Per-pool line**: id, coverage level, `alive`, `active`, `idle` and `cpu`. Reasons are not printed
  per pool; they are summarised once in `degraded:`. `active` and `idle` print `unknown` when the value
  could not be measured. Queue depth is deliberately not printed: without a probe it is `-1`, and a
  printed `0` would look like an empty queue rather than an unmeasured one.
- **`service L2 alive 1`** is the refresher thread (`Tuff Threads Sampler`), accounted like any other
  Tuff thread. It is the only thread the layer creates at startup; with managed compute the engine's
  workers are created through the provider too. `L2` here means "created through the factory", not
  "managed region ticks".
- **`region-execution L2`** means the spec is at L2 and the bridge wraps each tick body, so the
  registry accounts the thread while the body runs. The thread is still Folia's `TickThreadRunner`,
  with Folia's name and group; `L2` is accounting, not ownership. The `alive` number is
  `max(registry, probe)`, so it looks the same with `managed: false`: the printed level and the
  startup log are the visible difference, the registration itself is proven by the unit test.
- **Region lines** appear under the pool that ticks regions, and only when the probe reported something:
  `regions N / avg MSPT X / min TPS Y / lag Z% (K of N)` on the first line, then the 15-second figure,
  the worst single tick and the entity, player and chunk totals. `unknown` replaces a value that could
  not be measured. The region count is the number of regions the pool ticks, not the number of worlds.
  On an empty server no region has loaded chunks yet, so the walk returns nothing and both lines are
  absent; that is zero regions, not a failed read. Force a region with `forceload add 0 0` (or join a
  player) to see them. A region that exists but has not ticked yet still counts, with `unknown`
  measurements.
- **`degraded:`** is the union of reasons from every pool and the manager. In the default observe-only
  configuration it always contains `OBSERVE_ONLY` and `FEATURE_DISABLED` while the manager state is
  still `RUNNING`. With `region-execution.managed: true` that pool contributes
  `SCHEDULER_LIMITATION` under EDF instead of `OBSERVE_ONLY`, while the pools still at L1 keep
  `OBSERVE_ONLY` in the union. Observe-only is a statement of coverage, not a failure, and only
  reasons such as `BUDGET_EXHAUSTED`, `SCHEDULER_LIMITATION`, `CONFIG_CONFLICT`,
  `CRITICAL_POOL_PRESSURE` or `THREAD_CREATION_FAILED` move the manager state to `DEGRADED`.
  `CRITICAL_POOL_PRESSURE` means a critical pool is at its capacity with requests parked, or its
  declared minimum only fits by overcommitting; `LOAD_SHEDDING` means the capacity of that non critical
  pool is temporarily below its budget on purpose. The second is the policy working, and it always
  travels with the first, because shedding only fires for a critical pool that is waiting.
- **`unknown threads:`** is a count, `n/a (no sample yet)` before the first pass, or
  `n/a (metrics collection is disabled)` when metrics are off. The report does not list their names;
  the snapshot does. Every thread a probe lists - region, chunk, netty, chat, background and compute -
  is already accounted, so it never lands in this count.
- **`settings:`** repeats the three values that most often explain a surprise: the reserve, the region
  scheduler and the configured region thread count (`unknown` when the config says "auto"). Every key of
  the section needs a restart.
- **`pressure:`** is the compute pool's tick pressure decision. `disabled` names the switch. With the
  feature on it prints the state (`normal` or `yielding`), the reserve against its ceiling, the budget
  against the ledger's usable cores, and the signal the decision was made from: lag share, slowest
  region TPS, worst tick and the reason (`LAG_ENTER`, `LAG_STEP_UP`, `CALM_STEP_DOWN`, `CALM_EXIT`,
  `CEILING_REACHED`). `waiting for a region reading` means the feature is on but no pass has seen a
  region summary yet, which is normal on a server with no loaded regions. The reserve is only applied
  when the compute pool is managed; an observed pool shows the same numbers, with nothing resized.
- **`scheduler:`** is the region scheduler and, under Kairos, its fund in one line: fixed or elastic,
  `live/max` with the floor and target, queued ticks, overdue ticks, the supervisor state and the fund
  status. Under EDF or `WORK_STEALING` it says `no fund telemetry (Kairos only)`: those schedulers have
  no fund, supervisor or readable wait queue, and a made-up number would be worse than the sentence.
  `/tuff scheduler` prints the same facts with the tick wait ages, the last fund decision and the
  degraded reasons, and adds the configured thread count for the schedulers that have no fund.

## What a snapshot contains

`ResourceSnapshot` is one consistent moment. Fields and sources:

| Field | Source | Notes |
| --- | --- | --- |
| `capturedAt` | monotonic clock | Wall-clock instant for display |
| `usableCores` | The budget ledger's `usable` | Minimum one; also exposed by `ThreadResourceController` |
| `reservedCores` | `resources.reserve-cores` | As configured, without the controller's floor at one |
| `shedding` | `ThreadResourceController.sheddingReport()` | Critical reserve in cores, whether the policy is on, cores withheld and the pools they came from, plus a printable summary. `SheddingReport.none()` on a hand-built snapshot |
| `shedding.criticalReserveCores` | Derived from the pool specs by the budget planner | Also a shortcut on the snapshot: `criticalReserveCores()`. The part of `ceiling` the non critical pools may not lease |
| `totalThreads`, `activeThreads`, `idleThreads`, `peakThreads` | Per-pool sums | `peak` is the highest total seen since start |
| `creationRatePerSecond`, `terminationRatePerSecond` | `ThreadMXBean.getTotalStartedThreadCount` | JVM-wide deltas over the sampling window, not per pool |
| `processCpuLoad` | `OperatingSystemMXBean` | Process-wide; `-1` when unavailable |
| `pools` | One `PoolSnapshot` per registered spec | Sorted by id; empty when `metrics.per-pool: false` |
| `pools[].regionMetrics` | The region probe | Per-pool aggregate; absent for pools that tick no regions and when the read failed |
| `threads` | Per-thread snapshots | Sorted by thread id; empty when `metrics.per-thread: false` |
| `degradedReasons` | Sampler plus manager | Union, independent of `per-pool` |
| `budgetLedger` | `ThreadResourceController.budgetLedger()` | One printable line with every ledger term; empty only on a hand-built snapshot |

`PoolSnapshot` carries the spec (id, kind, owner, purpose, min/max, coverage, budget), `alive`,
`core`, `active`, `idle`, `queued`, `capacity`, task counters, `cpuLoad`, saturation, the pool's
reasons and, for a pool that ticks regions, `regionMetrics` (see [Region metrics](#region-metrics)).

Where a value is not measurable it is `-1`, never zero:

| Value | `-1` means |
| --- | --- |
| `queued`, `capacity` | The pool has no probe, so queue depth and limit are unknown |
| `active`, `idle` | Derived from thread states is impossible (no threads and no probe) |
| `submitted`, `completed`, `rejected` | Not wired for this pool; the task counters are not connected to any owner yet, so every pool reports `-1` rather than a false zero |
| `cpuLoad` | No sampling window yet, no thread baseline, or MXBean support missing |
| `cpuTimeNanos` per thread | MXBean does not support per-thread CPU time |
| `processCpuLoad` | The OS bean returned a negative or non-finite value |
| `unregisteredThreads` in the status | Metrics are disabled |

With collection disabled (`metrics.enabled: false`, or `lifecycle.monitor-threads: false`) the snapshot
is **structural**, not empty: it still lists every registered pool and the threads the registry accounts
for, with their states, because that bookkeeping runs regardless of the switch. Everything that would
have been measured is `-1` (queue, capacity, task counters, CPU), saturation is `UNKNOWN`, the per-thread
list is empty, the rates are zero and `degradedReasons` carries `METRICS_UNAVAILABLE`. The metrics view
also reports `enabled() == false`, which is the explicit "metrics disabled" status next to the numbers.

`cpuLoad` for a pool is the share of the usable cores its threads consumed during the window:
`ΔCPU(threads) / (elapsed × usableCores)`, clamped to `[0, 1]`. A newly seen thread has no baseline
and contributes `-1`, so a fresh pool does not report a burst it did not have.

### Saturation

Saturation is derived, not measured by the pool:

- `utilisation = active / alive`
- `queuePressure = queued / capacity`, when both are known and positive; otherwise zero
- `longestWaitNanos` comes from the lease controller: the age of the oldest parked lease request for
  that pool. Zero means nothing waits
- level: `IDLE` when nothing is alive and nothing is queued, `SATURATED` at utilisation ≥ 0.95 or
  pressure ≥ 0.90, `BUSY` at utilisation ≥ 0.75 or pressure ≥ 0.50, otherwise `NORMAL`; `UNKNOWN` when
  neither active nor queued is available

For the region pool, active and idle come from `Thread.getState()` (EDF parks an idle deadline
thread), so utilisation is an approximation. There is no queue depth, so pressure stays zero and the
level describes thread occupancy only. Treat region saturation as a hint; use `/tps` for the region
verdict.

### Region metrics

The region pool also reports an aggregate of the regions it ticks. The numbers come from the scheduler's
own tick reports and from `RegionStats`; nothing is invented and nothing is sampled from a tick thread:

| Value | Source | Notes |
| --- | --- | --- |
| `regions` | Every region under every level | A region that has not ticked yet still counts as a region |
| `averageMspt5s`, `averageMspt15s` | `getTickReport5s/15s(...).timePerTickData().segmentAll().average()` | Mean over the regions that reported, in milliseconds |
| `maximumMspt5s`, `maximumMspt15s` | `.segmentAll().greatest()` | The worst single tick any one region saw, not a sum |
| `minimumTps` | `tpsData().segmentAll().average()` of the 5 s report | The slowest region |
| `laggingRegions`, `lagFraction()` | Regions whose 5 s TPS is below 18 | A proxy, not a deadline miss |
| `entities`, `players`, `chunks` | `RegionStats` | Sums over regions; acquire reads |

`lag` is a proxy for lateness. Folia does not publish how late a tick started without a patch, so "late"
is expressed as "visibly under 20 TPS": a region counts when its 5 s TPS is below 18, and a region at
exactly 18 does not.

Only the 5 s and 15 s windows are read. The 1 m, 5 m and 15 m reports copy and sort one record per tick
over the whole window - milliseconds and garbage per region, acceptable for `/tps`, wrong for a layer
that samples. Reads are throttled to one walk per second, so even the refresher cannot walk twice in a
second. A read from a region tick thread does not take the regioniser lock at all; it answers with the
last list the refresher produced. The aggregation itself is a pure function over per-region readings and
is tested without a server, including empty and unmeasured reports.

## Sampling cost

Folia executes console commands on the global region tick thread, so the command cannot take the
regioniser lock or enumerate the thread table itself. One daemon thread does that work instead:

- the refresher is named `Tuff Threads Sampler` (the factory appends `-<n>`) and is created through the
  `ThreadFactoryProvider`, so it appears in the `service` pool (raised to `L2`) and never counts as an
  unknown thread;
- it takes one pass per second and publishes an immutable sample; a pass is one walk over registered
  threads, one `observe()` per attached probe, one CPU read per thread and a handful of JVM counters;
- `/tuff threads` only copies that sample and prints its age. A command from the console or from a
  player is therefore a handful of volatile reads, not a measurement;
- before the first pass finished the command says `warming up (no sample yet)` instead of printing an
  empty table.

The expensive operations stay away from the tick threads:

- `Thread.getAllStackTraces()` is not used at all: the unaccounted-thread scan walks thread groups and
  takes no stacks. The refresher forces that scan once per pass, on the same one-second clock as the
  pools, and publishes the count with the rest of the sample; there is no separate 30-second throttle.
- Region tick reports (`5m`, `15m`) are never read by this layer: they allocate and sort one record per
  tick over the window. Region reads take the 5 s and 15 s reports only, and those reads are throttled
  to one walk per second.
- No probe runs from a tick thread. A probe call only ever happens inside the refresher's pass.

The cost is one thread walk, one probe pass and one CPU read per thread per second. On the Windows
smoke stand that measured about 33 ms of CPU per second in total for the layer; that is the price of
the one-second sample, and the reason the work belongs to a daemon thread rather than a tick.

## Unknown threads

The layer counts live threads that no pool accounts for: a walk of the thread groups, minus everything
in the registry and everything a probe reports, minus an allow-list of library-owned threads
(Gradle, the common fork-join pool, log4j, JFR, JLine and similar).

Two honest caveats:

- In observe-only mode almost every server thread is unaccounted by definition, because Folia's and
  netty's threads are not in the registry. A high count is the normal state, not a leak. The number
  becomes meaningful as pools move to L2.
- It is a count in the command and one `tuff.threads.UnknownThreads` event per finding during the
  scan, not a per-thread live metric. The scan runs once per refresher pass (one second) on the
  refresher's own thread, so the console never pays for it. A thread the refresher itself owns is
  registered under `service` and is not counted as unknown.

## JFR

Eight events, all named under `tuff.threads`:

| Event | Fields | When |
| --- | --- | --- |
| `tuff.threads.PoolState` | `poolId`, `poolKind`, `coverage`, `aliveThreads`, `coreThreads`, `activeThreads`, `idleThreads`, `queuedTasks`, `queueCapacity`, `cpuLoad`, `saturation`, `degradedReasons` | Once per pool per sampling pass, when `metrics.per-pool` is on |
| `tuff.threads.Saturation` | `poolId`, `level`, `utilisation`, `queuePressure`, `queuedTasks`, `longestWaitNanos` | On level change, and every pass while the level is `SATURATED` |
| `tuff.threads.BudgetChanged` | `poolId`, previous and current `cores`, `reservedCores`, `share` | When a pool's declared budget changes |
| `tuff.threads.LeaseTimeout` | `poolId`, `deadlineNanos`, `maxWaitNanos` | When a blocking lease request expires |
| `tuff.threads.ThreadCreated` | `threadId`, `threadName`, `poolId`, `daemon`, `priority` | When the factory creates a thread or adopts a wrapped one |
| `tuff.threads.ThreadTerminated` | `threadId`, `threadName`, `poolId`, `state`, `cpuTimeNanos` | When an accounted thread ends |
| `tuff.threads.UnknownThreads` | `threadId`, `threadName` | Per finding during the unaccounted-thread scan |
| `tuff.threads.TickPressure` | `state`, `reason`, `reserveCores`, `maxReserveCores`, `budget`, `usableCores`, `regions`, `lagFraction`, `minimumTps`, `maximumMspt` | When the compute pool's tick pressure reserve moves; never per pass |

Emission is gated on `EventType.isEnabled()` before the event object is built, so a server with no
recording pays one check per hook. There is no `thread-management` key that switches these events on
or off, and there is no `/tuff` subcommand that starts a recording for them: they appear in any
recording that has the events enabled. The `/tuff compute jfr` recording enables the `tuff.compute`
category only and will not contain them.

To capture them:

```bash
jcmd <pid> JFR.start name=tuff-threads settings=profile duration=120s filename=threads.jfr
jfr print --events tuff.threads.PoolState,tuff.threads.Saturation,tuff.threads.LeaseTimeout threads.jfr
```

The shipped `flightrecorder-config.jfc` (used by `--jfrProfile`) already records the JDK events that
complement this layer without any code of ours: `jdk.ThreadStart` / `jdk.ThreadEnd` with the creation
stack, `jdk.JavaThreadStatistics`, `jdk.ThreadCPULoad` for per-thread CPU, `jdk.JavaMonitorEnter`,
`jdk.JavaMonitorWait`, `jdk.ThreadPark` and `jdk.ThreadSleep` for contention, `jdk.ExecutionSample` for
stack profiles and `jdk.ThreadAllocationStatistics` for per-thread allocation. JFR is the second
echelon: cheap gauges live in the snapshot, contention and stack detail live in a recording.

## Tick metrics on a regionised server

This is the part operators most often get wrong, so it is stated plainly.

**There is no server-wide tick metric on Folia.** The vanilla fields `MinecraftServer.tickTimes1s…
15m` are never filled: `recordEndOfTick()` is unreachable because the server loop it belongs to is
dead, and the tick is per region. Consequences:

| Surface | State |
| --- | --- |
| `/mspt` | Not registered (disabled for region threading) |
| `/tick query` | Answers, but percentiles and busy time read zeros outside a region |
| `Bukkit.getTickTimes()` | Empty window |
| `MinecraftServer.getTPS()` | Returns the nominal 20.0 because the average window is empty |
| `minecraft.ServerTickTime` JFR event | Fed only from the dead loop, so it never appears |
| Built-in spark | Disabled in Folia builds, and the bundled spark has no region hooks; its TPS would be wrong. Only an external spark plugin works |

**The real metrics are per region**, computed every tick by the region scheduler and its scheduling
handle: five windows (5 s, 15 s, 1 m, 5 m, 15 m) of TPS, MSPT, tick CPU time, time spent in tasks
between ticks and CPU starvation, plus per-region entity, player and chunk counts. `/tps` reads them
and is the region view to use.

**This layer carries an aggregate, never a server-wide number.** The region pool's snapshot includes
`regionMetrics`: the region count, mean and worst MSPT over 5 s and 15 s, the lowest region TPS, the share
of regions below 18 TPS and the entity, player and chunk totals. That is a summary of per-region reports,
not a replacement for them: there is no per-region breakdown in the snapshot, no 1 m, 5 m or 15 m window,
and no `tuff.threads` event carries a tick value. A value the probe could not read is `unknown` (`-1`),
never zero, and a pool that ticks no regions has no region metrics at all. For one region use `/tps`; for
"is any region late right now" the region lines under `/tuff threads` answer in one look.

## During shutdown

Do not sample in the window between Folia setting `running = false` and `hasStopped = true`. The tick
counter is frozen while the watchdog is still armed, so a join, an await or a heavy scan in that
window looks exactly like a hung server: the watchdog fires, marks the exit abnormal and the process
ends with a failure code on a healthy shutdown. The layer therefore has no shutdown hook of its own,
stops the refresher with a flag and an interrupt and does not join it, and does no blocking work while
stopping. A pass that races the stop finds the sampler closed and keeps the last values. If you are
writing an integration that reads this data, obey the same rule.

## Reading a report: a short troubleshooting table

| Symptom | Likely cause | Where to look |
| --- | --- | --- |
| Most pools show `alive 0` | The pool has no live thread the probe can see: netty event loops appear on the first connection, the chat executor on first use. A zero on region, chunk or compute means the owner has not started or has halted | Expected for the lazy pools; see [architecture](architecture.md#current-matrix) |
| `region-execution alive 0` on a running server | The region scheduler does not exist yet (very early startup) or already halted (shutdown) | Lifecycle, not a leak |
| `warming up (no sample yet)` for longer than a second | The refresher thread did not start; the log has the reason under the `Tuff/Threads` logger | Look for `Tuff Threads Sampler` in a thread dump |
| `unknown threads` in the hundreds | Normal for observe-only: Folia, netty and plugin threads are not registered | The number drops as pools reach L2 |
| `degraded:` lists reasons while the layer says running | `OBSERVE_ONLY`, `FEATURE_DISABLED`, `METRICS_UNAVAILABLE` and `NUMA_UNAVAILABLE` are coverage or measurement statements | `BUDGET_EXHAUSTED`, `TOPOLOGY_UNKNOWN`, `SCHEDULER_LIMITATION`, `THREAD_CREATION_FAILED`, `POOL_UNRESPONSIVE`, `CONFIG_CONFLICT` and `CRITICAL_POOL_PRESSURE` are the reasons that move the state to `DEGRADED` |
| `queued` unknown on a pool | That owner has no queue-depth getter | Expected for region, chunk worker, chunk IO and netty; chat and background do report a depth |
| No region lines under `region-execution` | No region is loaded (empty server, no chunks, no players), the read failed this pass, or the scheduler is gone | Force one with `forceload add 0 0`; nothing is printed rather than a fake zero |
| `region-execution` prints `L2` but `unknown threads` is unchanged | Expected: `alive` is `max(registry, probe)` and the count of unaccounted threads is dominated by Folia, netty and plugins | Prove the wrap with the bridge unit test, not with the console |
| Region TPS disagrees with `/tps` | `/tuff threads` shows the slowest region and a mean over regions; `/tps` shows per-region windows, including 1 m and beyond | Different aggregates, both honest |
| No `tuff.threads.*` events in a recording | The recording does not have those events enabled, or it is the compute recording (category `tuff.compute` only) | Start a recording with the events enabled |
| A background pool holds fewer permits than its spec says | Either the critical reserve (`reserve:` line, `criticalReserveCores`) or an active load shed (`LOAD_SHEDDING`) | Compare `reserve:` with `budgetLedger`: the ledger prints `+ critical N` when the reserve is not zero, and the shed is always temporary |
| TPS looks wrong in a profiler | The profiler uses server-wide tick hooks, which Folia does not feed | Use `/tps` or a region-aware profiler |
| `pools: 0 registered` while the layer is running | `metrics.per-pool: false` (publication off), not `metrics.enabled: false` — with collection off the pools are still listed with unmeasured values | Check the `metrics` keys and the `degraded:` line (`METRICS_UNAVAILABLE` means collection is off) |

## Related pages

- [Architecture](architecture.md) — what the numbers mean and which invariants produce them.
- [Coverage](coverage.md) — what each printed level means and which levels a pool cannot reach.
- [Configuration](configuration.md) — the keys behind `settings:`, `per-pool` and `per-thread`.
- [Compute operations](../compute/operations.md) — the command, JFR and limits of the compute engine.
