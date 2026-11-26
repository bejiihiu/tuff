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
| Running, no snapshot yet | That the sampler has not completed a pass; retry in a second |
| Running | The summary below |

A running server with default settings prints something like this:

```
Tuff thread layer: running, all pools observed only
  processors: 11 usable, 1 reserved; 4 registered thread(s) (peak 4)
  pools: 7 registered
    chunk-io          L1  alive 0  active 0  idle 0  [FEATURE_DISABLED, OBSERVE_ONLY]
    chunk-worker      L1  alive 0  active 0  idle 0  [FEATURE_DISABLED, OBSERVE_ONLY]
    compute           L1  alive 0  active 0  idle 0  [FEATURE_DISABLED, OBSERVE_ONLY]
    compute-virtual   L1  alive 0  active 0  idle 0  [FEATURE_DISABLED, OBSERVE_ONLY]
    network           L1  alive 0  active 0  idle 0  [FEATURE_DISABLED, OBSERVE_ONLY]
    region-execution  L1  alive 4  active 0  idle 4  [FEATURE_DISABLED, OBSERVE_ONLY]
      regions 12 / avg MSPT 4.2 ms / min TPS 19.4 / lag 8% (1 of 12)
      ticks: 5s max 9.5 ms, 15s avg 4.4 ms max 12.0 ms; entities 8123 / players 4 / chunks 3951
    service           L1  alive 0  active 0  idle 0  [FEATURE_DISABLED, OBSERVE_ONLY]
  degraded: FEATURE_DISABLED, OBSERVE_ONLY
  unknown threads: 37
  settings: reserve 1 core(s), region scheduler edf, region threads auto; changes take effect after a restart
```

How to read it:

- **`running, all pools observed only`** comes from the configuration (`anyManagementEnabled()`
  derived from the `managed` flags), not from the measured coverage.
- **`processors: N usable, R reserved`** is the snapshot's `usableCores`, which is the budget ledger's
  `usable`: `floor(processors × 0.8)` minus the deducted lanes and the effective reserve, floored at one.
  `R` is the configured reserve before that floor. The raw subtraction and every term are in the
  snapshot's `budgetLedger`, the same line `ThreadResourceController.budgetLedger()` returns, so there is
  no second "usable" number to compare against.
- **`N registered thread(s)`** is the sum of per-pool `alive`. It includes threads a probe reports,
  not only threads the registry created, so it can be non-zero while no thread has gone through the
  factory.
- **Per-pool line**: id, coverage level, `alive`, `active`, `idle`, and the pool's degraded reasons in
  brackets. `unknown` is printed for `active` and `idle` when a value could not be measured. Queue
  depth is deliberately not printed: without a probe it is `-1`, and a printed `0` would look like an
  empty queue rather than an unmeasured one.
- **Region lines** appear under the pool that ticks regions, and only when the probe reported something:
  `regions N / avg MSPT X / min TPS Y / lag Z% (K of N)` on the first line, then the 15-second figure,
  the worst single tick and the entity, player and chunk totals. `unknown` replaces a value that could
  not be measured. The region count is the number of regions the pool ticks, not the number of worlds.
- **`degraded:`** is the union of reasons from every pool and the manager. In the default observe-only
  configuration it always contains `OBSERVE_ONLY` and `FEATURE_DISABLED` while the manager state is
  still `RUNNING`. Observe-only is a statement of coverage, not a failure, and only reasons such as
  `BUDGET_EXHAUSTED`, `SCHEDULER_LIMITATION`, `CONFIG_CONFLICT` or `THREAD_CREATION_FAILED` move the
  manager state to `DEGRADED`.
- **`unknown threads:`** is a count, or `n/a (metrics collection is disabled)` when metrics are off.
  The report does not list their names; the snapshot does.
- **`settings:`** repeats the three values that most often explain a surprise: the reserve, the region
  scheduler and the configured region thread count. Every key of the section needs a restart.

## What a snapshot contains

`ResourceSnapshot` is one consistent moment. Fields and sources:

| Field | Source | Notes |
| --- | --- | --- |
| `capturedAt` | monotonic clock | Wall-clock instant for display |
| `usableCores` | The budget ledger's `usable` | Minimum one; also exposed by `ThreadResourceController` |
| `reservedCores` | `resources.reserve-cores` | As configured, without the controller's floor at one |
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
that samples. Reads are throttled to one walk per second, so a second command in the same second gets the
previous reading. A read triggered from a region tick thread (a player running the command) does not take
the regioniser lock at all; it answers with the last reading. The aggregation itself is a pure function
over per-region readings and is tested without a server, including empty and unmeasured reports.

## Sampling cost

There is no sampling thread. A pass runs when something reads a snapshot — the command — and only if
the sampling interval (one second) has elapsed; a concurrent read gets the previous snapshot instead
of waiting. A pass is one walk over registered threads, one `observe()` per attached probe, one CPU
read per thread and a handful of JVM counters. With a few dozen threads this is tens of microseconds;
the expensive operations are deliberately elsewhere:

- `Thread.getAllStackTraces()` is not part of the live path: the unaccounted-thread scan walks thread
  groups, is throttled to once per 30 seconds and is a diagnostic, not part of the pass.
- Region tick reports (`5m`, `15m`) are never read by this layer: they allocate and sort one record per
  tick over the window. Region reads take the 5 s and 15 s reports only, and the provider throttles the
  walk to once per second.
- No probe runs from a tick thread. When a player runs the command, the region provider notices the tick
  thread and answers from its previous reading instead of taking the regioniser lock inside a tick.

## Unknown threads

The layer counts live threads that no pool accounts for: a walk of the thread groups, minus everything
in the registry and everything a probe reports, minus an allow-list of library-owned threads
(Gradle, the common fork-join pool, log4j, JFR, JLine and similar).

Two honest caveats:

- In observe-only mode almost every server thread is unaccounted by definition, because Folia's and
  netty's threads are not in the registry. A high count is the normal state, not a leak. The number
  becomes meaningful as pools move to L2.
- It is a count in the command and one `tuff.threads.UnknownThreads` event per finding during the
  scan, not a per-thread live metric. The scan is what costs; making it a metric would make the
  sampler expensive.

## JFR

Seven events, all named under `tuff.threads`:

| Event | Fields | When |
| --- | --- | --- |
| `tuff.threads.PoolState` | `poolId`, `poolKind`, `coverage`, `aliveThreads`, `coreThreads`, `activeThreads`, `idleThreads`, `queuedTasks`, `queueCapacity`, `cpuLoad`, `saturation`, `degradedReasons` | Once per pool per sampling pass, when `metrics.per-pool` is on |
| `tuff.threads.Saturation` | `poolId`, `level`, `utilisation`, `queuePressure`, `queuedTasks`, `longestWaitNanos` | On level change, and every pass while the level is `SATURATED` |
| `tuff.threads.BudgetChanged` | `poolId`, previous and current `cores`, `reservedCores`, `share` | When a pool's declared budget changes |
| `tuff.threads.LeaseTimeout` | `poolId`, `deadlineNanos`, `maxWaitNanos` | When a blocking lease request expires |
| `tuff.threads.ThreadCreated` | `threadId`, `threadName`, `poolId`, `daemon`, `priority` | When the factory creates a thread or adopts a wrapped one |
| `tuff.threads.ThreadTerminated` | `threadId`, `threadName`, `poolId`, `state`, `cpuTimeNanos` | When an accounted thread ends |
| `tuff.threads.UnknownThreads` | `threadId`, `threadName` | Per finding during the unaccounted-thread scan |

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
ends with a failure code on a healthy shutdown. The layer therefore takes no snapshot in that window,
has no shutdown hook of its own and does no blocking work while stopping. If you are writing an
integration that reads this data, obey the same rule.

## Reading a report: a short troubleshooting table

| Symptom | Likely cause | Where to look |
| --- | --- | --- |
| All pools show `alive 0` | The layer is observe-only and only the region pool has a probe | Expected in this build; see [architecture](architecture.md#current-matrix) |
| `region-execution alive 0` on a running server | The region scheduler does not exist yet (very early startup) or already halted (shutdown) | Lifecycle, not a leak |
| `unknown threads` in the hundreds | Normal for observe-only: Folia, netty and plugin threads are not registered | The number drops as pools reach L2 |
| `degraded:` lists reasons while the layer says running | `OBSERVE_ONLY` and `FEATURE_DISABLED` are coverage statements | Only `BUDGET_EXHAUSTED`, `CONFIG_CONFLICT`, `SCHEDULER_LIMITATION` and creation failures mean degraded |
| `queued` unknown on every pool | No probe is attached | Expected; queue depth needs an owner getter |
| No region lines under `region-execution` | The pool ticks no regions yet, no level is loaded, or the read failed this pass | Nothing is printed rather than a fake zero |
| Region TPS disagrees with `/tps` | `/tuff threads` shows the slowest region and a mean over regions; `/tps` shows per-region windows, including 1 m and beyond | Different aggregates, both honest |
| No `tuff.threads.*` events in a recording | The recording does not have those events enabled, or it is the compute recording (category `tuff.compute` only) | Start a recording with the events enabled |
| TPS looks wrong in a profiler | The profiler uses server-wide tick hooks, which Folia does not feed | Use `/tps` or a region-aware profiler |
| `pools: 0 registered` while the layer is running | `metrics.per-pool: false` (publication off), not `metrics.enabled: false` — with collection off the pools are still listed with unmeasured values | Check the `metrics` keys and the `degraded:` line (`METRICS_UNAVAILABLE` means collection is off) |

## Related pages

- [Architecture](architecture.md) — what the numbers mean and which invariants produce them.
- [Configuration](configuration.md) — the keys behind `settings:`, `per-pool` and `per-thread`.
- [Compute operations](../compute/operations.md) — the command, JFR and limits of the compute engine.
