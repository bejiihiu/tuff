# Configuration

The thread layer reads one section, `thread-management`, from `config/tuff.yml`. The file ships as a
template with comments; a missing file is not an error (the section is read before the compute engine
writes the template on first boot) and the observe-only defaults are used with one log line saying so.

```yaml
thread-management:
  enabled: true
  resources:
    cpu-aware: true
    reserve-cores: 1
    max-concurrent-tasks: -1
    load-shedding: false
  lifecycle:
    centralized-thread-factory: true
    monitor-threads: true
  region-execution:
    managed: false
    preserve-folia-scheduling: true
    scheduler: edf
    threads: -1
    grid-exponent: 4
    kairos-unstable: false
  async-compute:
    managed: false
    max-threads: -1
    tick-pressure:
      enabled: false
      enter-lag-fraction: 0.25
      enter-min-tps: 19.0
      exit-min-tps: 19.5
      exit-windows: 3
      cooldown-seconds: 5
      max-reserve-cores: 2
      step-cores: 1
  execution-pools:
    network:
      managed: false
      preserve-specialization: true
    chunk-system:
      managed: false
      preserve-specialization: true
    compute:
      managed: false
      preserve-specialization: true
  metrics:
    enabled: true
    per-pool: true
    per-thread: true
```

## How the section is read

- **Whole section, once, at startup.** The runtime bakes its pool set and its sampling policy in at
  `start()`. Every key of this section is restart-only and `/tuff reload` lists changed keys as "takes
  effect after a restart"; nothing here is applied to a live manager.
- **Unknown keys are reported and ignored**, one console line each. A key that cannot be read keeps
  its default and reports the reason.
- **Validation is per section.** A section whose values contradict each other (a negative reserve, a
  scheduler without its opt-in, `preserve-*: false`) is rejected as a whole and replaced by its
  defaults, with one problem line naming the section. Half a section never survives silently.
- **`-1` means "let the owner decide"**, and the owner is the server or Folia, not UTMI. Zero is only
  legal where the comment says so.

## Keys

| Key | Type | Default | Reload | What it does |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `true` | restart | Master switch. `false` creates no runtime at all: no pool registered, no probe attached, no metrics sampled, no thread started. `/tuff threads` still answers and says where the switch is |
| `resources.cpu-aware` | boolean | `true` | restart | Size from the platform's real processor count and container limits instead of a static table. Keep it on; `false` is a diagnostic for a wrong topology reading |
| `resources.reserve-cores` | integer, ≥ 0 | `1` | restart | Processors never handed to managed pools, kept for GC, JIT, the kernel and unexpected work. The budget floors the effective reserve at one core; zero is legal but risky on small machines |
| `resources.max-concurrent-tasks` | integer, `-1` or ≥ 1 | `-1` | restart | Global cap on concurrently leased tasks across pools. critical pools (`region-execution`, `network`) bypass it by policy; `-1` disables the cap and leaves only per-pool permits |
| `resources.load-shedding` | boolean | `false` | restart | Let a starving critical pool take capacity back from the non critical ones. While a critical pool has a request parked and every non critical pool is at its capacity, one core is withheld per cooldown (never below the pool's declared minimum) and given back after the critical queue has been calm. Held work is never interrupted; only new leases are refused. Off keeps the budget fixed whatever happens; the critical reserve itself does not depend on this key |
| `lifecycle.centralized-thread-factory` | boolean | `true` | restart | Policy statement: new server-owned threads should come from `ThreadFactoryProvider`. It does not move existing threads and is enforced by code review and the static guard, not by the flag alone |
| `lifecycle.monitor-threads` | boolean | `true` | restart | Sample thread states, CPU time and creation/termination rates. Combined with `metrics.enabled` with AND: either switch off leaves the layer running with a structural snapshot (registered pools and registry-known thread counts, no measured values) |
| `region-execution.managed` | boolean | `false` | restart | Registers the region pool at L2 instead of L1 and binds the `RegionThreads` bridge, so every region tick body goes through `ThreadFactoryProvider.wrap`. What changes: the pool spec (`min 1`, elastic budget), per-thread registration for the duration of each body, the printed level, and `SCHEDULER_LIMITATION` under EDF (L2 is the deepest an unresizable scheduler can promise). What stays Folia's: thread creation, class (`TickThreadRunner`), name, group, daemon flag, uncaught handler, tick order and region ownership. Restart only: the bridge is installed before the region scheduler is constructed; see [architecture](architecture.md#region-bridge-and-installation-order) |
| `region-execution.preserve-folia-scheduling` | boolean | `true` | n/a | Must stay `true`. Folia owns tick order, catch-up and region ownership. `false` is rejected at startup instead of being silently ignored |
| `region-execution.scheduler` | `edf` \| `work-stealing` \| `kairos` | `edf` | restart | Region scheduler variant the configuration describes. `kairos` additionally requires `kairos-unstable: true` |
| `region-execution.threads` | integer, `-1` or ≥ 1 | `-1` | restart | Tick thread count used by the pool spec and the budget deduction. It is **not** the number Folia runs: Folia reads `threaded-regions.threads` from `paper-global.yml` (or its own auto formula). `-1` leaves the deduction at zero |
| `region-execution.grid-exponent` | integer, 0–31 | `4` | restart | Region grid exponent, clamped to `[0, 31]` like the platform key. Modelled and validated; it does not reshape the live regioniser |
| `region-execution.kairos-unstable` | boolean | `false` | restart | Explicit opt-in for the experimental Kairos scheduler. Never enabled implicitly. Selecting `kairos` without it rejects the whole region section |
| `async-compute.managed` | boolean | `false` | restart | Older spelling of `execution-pools.compute.managed`; either key turns compute management on. With it on, the compute adapter registers the platform pool at L4 and the virtual lane at L2, and the engine takes its threads and processor budget from the thread layer. Off keeps the engine's own factory and `ComputeThreadBudget`; the pool is observed through its probe either way |
| `async-compute.max-threads` | integer, `-1` or ≥ 1 | `-1` | restart | Ceiling for the registered compute pool only. `-1` keeps the engine's own `max-threads`. The engine's queue and priorities are untouched either way |
| `async-compute.tick-pressure.enabled` | boolean | `false` | restart | Let the compute pool yield cores while regions lag. The decision is made from the region summary the sampler already publishes; on a change the compute adapter re-applies the processor budget through `applyProcessorBudget`, the same entry point the startup budget uses. Only a managed pool (`execution-pools.compute.managed` or `async-compute.managed`) can apply it; otherwise decisions are logged and shown by `/tuff threads` but nothing is resized. Off by default |
| `async-compute.tick-pressure.enter-lag-fraction` | number, `(0, 1]` | `0.25` | restart | Share of regions below 18 TPS that starts pressure; `0.25` is one lagging region out of four. Ignored while the slowest region already trips `enter-min-tps` |
| `async-compute.tick-pressure.enter-min-tps` | number, `(0, 20]` | `19.0` | restart | Slowest region TPS below which one core is yielded even when the lag share is zero |
| `async-compute.tick-pressure.exit-min-tps` | number, `(0, 20]` | `19.5` | restart | Slowest region TPS at or above which a sample counts as calm. Must not be below `enter-min-tps`; the gap is the hysteresis band where nothing moves |
| `async-compute.tick-pressure.exit-windows` | integer, ≥ 1 | `3` | restart | Consecutive calm one-second samples before a core is returned. One more step follows each cooldown, so the ramp down is as gradual as the ramp up |
| `async-compute.tick-pressure.cooldown-seconds` | integer, ≥ 1 | `5` | restart | Minimum time between two reserve changes. A sawtooth signal cannot resize the pool every second |
| `async-compute.tick-pressure.max-reserve-cores` | integer, ≥ 1 | `2` | restart | Most cores the pool may give up. The adapter lowers this further so the pool's `min-threads` still fits in the budget; a pool that cannot yield reports the ceiling as its reason |
| `async-compute.tick-pressure.step-cores` | integer, `[1, max-reserve-cores]` | `1` | restart | Cores moved per decision. One means a ramp rather than a jump |
| `execution-pools.compute.managed` | boolean | `false` | restart | The preferred spelling of the compute switch. Same effect as `async-compute.managed`, and the one `ThreadManagementSettings.computeManaged()` reads first |
| `execution-pools.network.managed` | boolean | `false` | restart | Raises the network pool's registered coverage. Netty keeps its event loops, sizing and affinity |
| `execution-pools.network.preserve-specialization` | boolean | `true` | n/a | Must stay `true`: network loops cannot be merged or reshaped without changing their semantics |
| `execution-pools.chunk-system.managed` | boolean | `false` | restart | Raises the registered coverage of `chunk-worker` and `chunk-io` from L1 to L2. The chunk system keeps its ordered streams, its queues and its threads; no factory is handed over yet, so this is a registration claim rather than accounting |
| `execution-pools.chunk-system.preserve-specialization` | boolean | `true` | n/a | Must stay `true`, for the same reason |
| `execution-pools.compute.preserve-specialization` | boolean | `true` | n/a | Must stay `true`. Compute workers are never merged with region, chunk or network threads |
| `metrics.enabled` | boolean | `true` | restart | Collect pool and thread state at all. With collection off the snapshot is structural: it lists the registered pools and the threads the registry accounts for, but queue depth, task counters, CPU, saturation and the per-thread list are unmeasured, the rates are zero and `degraded: METRICS_UNAVAILABLE` says so; `unknown threads: n/a (metrics collection is disabled)` |
| `metrics.per-pool` | boolean | `true` | restart | Publish per-pool entries, saturation and their JFR events |
| `metrics.per-thread` | boolean | `true` | restart | Publish the per-thread list. Disabling keeps pool totals but removes individual threads from the snapshot |

## What changing a key does not do

This list matters more than the table above, because several of these keys are forward-looking model
fields rather than levers that move threads today.

| You change | What actually happens |
| --- | --- |
| `region-execution.threads` | The registered pool spec and the budget deduction change. Folia's tick thread count does not: it comes from `paper-global.yml: threaded-regions.threads`, and under EDF it cannot change without a restart anyway |
| `region-execution.scheduler` | The configured and reported scheduler variant changes. It does not switch Folia's live scheduler; that happens only through the platform key and a restart, and `kairos` has no implementation yet |
| `region-execution.grid-exponent` | The model value changes. The regioniser's shift is fixed at the first initialisation |
| `execution-pools.chunk-system.managed` or `network.managed` | The pool is registered with coverage L2 instead of L1, and `/tuff threads` prints the deeper level. No thread is created or wrapped, because no owner hands its factory or its bodies over yet. The manager does not flag this as a conflict for these kinds — treat the printed level as a registration claim, not proof of accounting. `region-execution.managed` is different: that pool's bodies are really wrapped |
| `execution-pools.compute.managed` (or `async-compute.managed`) | The compute adapter registers `compute` at L4 and `compute-virtual` at L2, and the engine creates its platform workers through the provider and reads its budget from the resource controller. The engine's queue, priorities and fairness are unchanged |
| `async-compute.tick-pressure.enabled: true` without a managed compute pool | Nothing is resized. The controller still evaluates every one-second pass, logs each change with its reason and publishes the reserve for `/tuff threads`; the sink is simply not bound. This is the documented behaviour on an observe-only server, not a silent failure |
| `metrics.per-pool: false` | Pool entries disappear from the snapshot and `/tuff threads`. Degradation reasons are still collected at the top level; measurement continues |
| `lifecycle.centralized-thread-factory: false` | Nothing. The flag is a policy statement; the guard and review remain the enforcement |

## Platform limits behind the keys

These are properties of the platform, not tunables of this layer. They explain why several keys are
restart-only or do less than their name suggests.

- **Region threads cannot be resized while the scheduler is EDF** (the default). The EDF scheduler
  fixes its thread arrays at construction and its `setThreads` is a silent no-op. Only `work-stealing`
  supports a live resize, and that path recreates thread identities. A tick thread count change is a
  restart.
- **Thread priorities are close to a no-op on Linux** unless the JVM is started with
  `-XX:ThreadPriorityPolicy=1`, and even then they are a hint to the OS scheduler. `ThreadSpec.priority`
  is recorded and printed; it is not a scheduling guarantee.
- **NUMA placement exists only for `work-stealing` region threads**, only on Linux, and only with the
  platform flag `-DPaper.NumaScheduling`. There is no NUMA knob under `thread-management`; the budget
  and the factory do not set affinity at all.
- **Virtual-thread carriers belong to the JDK.** `jdk.virtualThreadScheduler.parallelism` and
  `maxPoolSize` are shared by the server and every plugin; nothing in `thread-management` can size
  them.
- **The region pool's thread names, group and daemon flag are Folia's.** No key changes them.
- **A managed flag is not a merge.** Registering a pool never joins its queue or its threads to
  another pool; specialisation is preserved by construction, which is why the `preserve-*` keys can
  only be `true`.

## Choosing values

Three configurations cover almost every server.

**Default — observe only.** Leave everything as shipped. The layer registers its pools, probes the
region pool and answers `/tuff threads`; nothing about execution changes.

**Observe a bigger server.** Same file, metrics as they are, plus the region size in the model so the
ledger's deduction matches what Folia runs:

```yaml
thread-management:
  region-execution:
    threads: 4        # keep equal to paper-global.yml threaded-regions.threads
```

The ledger now subtracts four tick threads instead of zero. That is the only effect in this
configuration: the ledger itself sizes nothing, and with compute unmanaged the engine keeps its own
budget.

**Managed region (L2).** `managed: true` registers the pool at L2 (`min 1`, elastic budget) and binds
the `RegionThreads` bridge, so each tick body goes through `ThreadFactoryProvider.wrap` while it runs:
the registry sees the thread for the duration of the body and `/tuff threads` prints `L2`. Folia
still creates the threads, names them, groups them and schedules them; the class, group, daemon flag
and uncaught handler are untouched, and the tick body's exceptions still reach Folia's handler.

```yaml
thread-management:
  region-execution:
    managed: true
    scheduler: edf
```

This is restart-only, and the order is part of the contract: the bridge is installed before Folia's
region scheduler is constructed (see
[architecture](architecture.md#region-bridge-and-installation-order)). Under EDF the snapshot then
reports `SCHEDULER_LIMITATION` next to the region pool: L2 is the deepest integration an unresizable
scheduler can promise.

The flag is only honest on a build that carries the region factory hook (`Tuff - account region tick
bodies`). A jar without it prints L2 from the spec while no body is wrapped; the flag itself cannot
detect that, because no factory calls the bridge.

## Tick pressure: let the compute pool give cores back

This is the one key group that changes a live pool without a restart of the feature itself: while a
region is late, the compute pool's processor budget is lowered, and when the regions are calm it is
raised again. It is off by default because it trades background throughput for tick headroom, and that
trade needs its own load test on the target server.

The signal comes from the thread layer's sampler, never from a tick report read on a region thread. Every
second the sampler publishes the region summary - the share of regions below 18 TPS, the slowest region's
TPS and the worst single tick - and the controller evaluates exactly that snapshot:

- **enter** when the lag share reaches `enter-lag-fraction`, or when the slowest region is below
  `enter-min-tps`;
- **grow** by `step-cores` on each cooldown while the regions are still behind, up to
  `max-reserve-cores` and never below the pool's `min-threads`;
- **stay** in the band between `enter-min-tps` and `exit-min-tps`;
- **release** one step per cooldown after `exit-windows` consecutive calm samples, where calm means no
  region below 18 TPS and the slowest region at or above `exit-min-tps`.

Every decision that moves the reserve is one log line with its reason and one `tuff.threads.TickPressure`
event; a pass that could not read the regions keeps the current reserve instead of guessing. The budget
is applied through the compute adapter and `applyProcessorBudget`, the same single writer the startup
budget uses, so there is no second sizing path. That number is `pool budget - reserve`, floored at
`min-threads`; `/tuff threads` prints it as `budget N applied of M pool core(s)` and the ledger's
`usable` next to it, because the pool may be capped by `max-threads` or by load shedding. With the pool
unmanaged the decisions are still logged and printed, but nothing changes.

The policy is read once at startup and is restart-only, like the rest of this section: the thresholds
describe the monitor that was built, and silently changing them under a running controller would make
its hysteresis and cooldown impossible to reason about.

```yaml
thread-management:
  async-compute:
    tick-pressure:
      enabled: true
```

## Critical reserve and load shedding: keep the tick and network lanes alive

Two mechanisms protect the pools the server cannot run without (`region-execution` and `network`), and
only one of them is a key.

**The reserve is always on and is arithmetic.** The budget planner subtracts the critical lanes'
guaranteed minimum before the shared pools see the remainder: the network minimum, plus any region
minimum the configured tick thread deduction did not cover, plus a critical pool's own declared
`reserved-cores` when that is larger. A compute pool whose caps add up to twenty cores therefore still
only sees what is left, and an elastic compute budget cannot resolve past it. When the reserve eats the
whole remainder the shared pools get **zero** permits and the plan says so - zero is the honest answer,
because the alternative is a permit the machine does not have. The same number is printed by
`/tuff threads` as the `reserve:` line and carried by the snapshot's `shedding.criticalReserveCores`.

The reserve is not a new sizing knob: it is derived from specs the pools already declare, so there is no
second ledger to keep in sync. Its weak spot is documented rather than hidden: netty threads are not
deducted from the ceiling yet, so a managed network pool's capacity above its minimum is best effort.

**Shedding is the temporary half and needs `resources.load-shedding: true`.** Its trigger is a
combination, not a single number: a critical pool has a live parked request **and** every non critical
managed pool is already at its capacity. A critical queue that waits on its own accounting, with idle
background pools, is normal back pressure and does not move anything.

A parked request is only possible for a pool at L3 or deeper. In this build `region-execution` and
`network` stop at L2 - Folia and netty own their threads and queues - so an enabled policy reports
itself as **dormant** in `/tuff threads` and in `snapshot().shedding().state()`: it costs nothing and
it does not claim to protect anything. The trigger becomes reachable when a critical lane is registered
at L3; the reserve above applies either way.

Once it fires, the policy is fixed (there are no keys for it, because a policy tuned per second is not a
policy):

| Step | Value | Why |
| --- | --- | --- |
| Withheld per decision | 1 core | A ramp, so an operator sees the reduction grow |
| Cooldown between decisions | 5 s | At most one core per five seconds, up and down |
| Calm evaluations before returning a core | 3 | Three admission decisions with no pressure, on top of the cooldown |
| Floor | the pool's declared minimum (or its reserve, if larger) | A guarantee is never shed |
| Order | lowest admission weight first: `service`, `compute-virtual`, `compute`, then `chunk-worker`/`chunk-io` | Cheap background work yields before work closer to the critical lanes |

The decision runs on the admission and release paths, so it needs no thread of its own: with no traffic
there is nothing to shed for, and the cooldown is measured on the injected monotonic clock. A shed is
never a revocation - permits already granted keep working to completion and are released normally; only
new leases are refused until the core comes back. Because an idle server may hold a shed until the next
request or release, the state is visible in the snapshot and the command instead of being a guess.

With the key off, the ledger, the reserve and every capacity are identical to the previous build: the
flag only adds the emergency reduction, it is not a second budget.

```yaml
thread-management:
  resources:
    load-shedding: true
```

## Validation errors you can hit

| Message | Cause | Fix |
| --- | --- | --- |
| `reserveCores must not be negative` | `resources.reserve-cores` below zero | Use `0` or more; the effective reserve is floored at one anyway |
| `threads must be -1 or at least 1` | `region-execution.threads: 0` | Use `-1` for auto or a positive count |
| `gridExponent must be in [0, 31]` | Out of range | The platform clamps the same range |
| `preserveFoliaScheduling must stay true` | Someone tried to disable Folia's ownership | Set it back; this is an architectural invariant, not a setting |
| `preserveSpecialization must stay true` | Attempt to make a specialised pool mergeable | Set it back |
| `Kairos is experimental; set kairosUnstable to true` | `scheduler: kairos` alone | Add the opt-in, and expect EDF behaviour: the scheduler is not implemented |
| `expected true or false, got '…'` | String instead of a boolean | Use literal `true` / `false` |
| `enterLagFraction must be in (0, 1]` | A share outside the range, e.g. `1.5` or `0` | Use a fraction such as `0.25` |
| `exitMinTps must not be below enterMinTps` | Inverted hysteresis band | Keep `exit-min-tps` at or above `enter-min-tps` |
| `stepCores must be in [1, max-reserve-cores]` | A step larger than the ceiling | Lower `step-cores` or raise `max-reserve-cores` |
| `expected a number, got '…'` | A TPS or share written as text that is not numeric | Use a plain number |
| `expected a whole number, got …` | Fractional or non-numeric count | Use an integer |
| `unknown key, ignored` | Typo or a key from a newer build | Remove it; the default stays in force |

## Related pages

- [Architecture](architecture.md) — how the values turn into pool specs and the budget ledger.
- [Coverage](coverage.md) — what each `managed` flag actually raises, and what it cannot.
- [Observability](observability.md) — what `/tuff threads` prints for each of these values.
- [Compute configuration](../compute/configuration.md) — the `async-compute` section, which owns the
  compute engine's own sizing.
