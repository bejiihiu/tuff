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
  execution-pools:
    network:
      managed: false
      preserve-specialization: true
    chunk-system:
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
| `lifecycle.centralized-thread-factory` | boolean | `true` | restart | Policy statement: new server-owned threads should come from `ThreadFactoryProvider`. It does not move existing threads and is enforced by code review and the static guard, not by the flag alone |
| `lifecycle.monitor-threads` | boolean | `true` | restart | Sample thread states, CPU time and creation/termination rates. Combined with `metrics.enabled` with AND: either switch off leaves the layer running with a structural snapshot (registered pools and registry-known thread counts, no measured values) |
| `region-execution.managed` | boolean | `false` | restart | Raises the registered coverage of the region pool from L1 to L2. In this build Folia still creates the threads itself, so the flag changes what the layer claims, not who builds the threads; see [architecture](architecture.md#current-matrix) |
| `region-execution.preserve-folia-scheduling` | boolean | `true` | n/a | Must stay `true`. Folia owns tick order, catch-up and region ownership. `false` is rejected at startup instead of being silently ignored |
| `region-execution.scheduler` | `edf` \| `work-stealing` \| `kairos` | `edf` | restart | Region scheduler variant the configuration describes. `kairos` additionally requires `kairos-unstable: true` |
| `region-execution.threads` | integer, `-1` or ≥ 1 | `-1` | restart | Tick thread count used by the pool spec and the budget deduction. It is **not** the number Folia runs: Folia reads `threaded-regions.threads` from `paper-global.yml` (or its own auto formula). `-1` leaves the deduction at zero |
| `region-execution.grid-exponent` | integer, 0–31 | `4` | restart | Region grid exponent, clamped to `[0, 31]` like the platform key. Modelled and validated; it does not reshape the live regioniser |
| `region-execution.kairos-unstable` | boolean | `false` | restart | Explicit opt-in for the experimental Kairos scheduler. Never enabled implicitly. Selecting `kairos` without it rejects the whole region section |
| `async-compute.managed` | boolean | `false` | restart | Lets the thread layer own the compute platform pool's capacity. Without the compute adapter this configuration cannot be honoured and is reported as a conflict |
| `async-compute.max-threads` | integer, `-1` or ≥ 1 | `-1` | restart | Ceiling for the registered compute pool only. `-1` keeps the engine's own `max-threads`. The engine's queue and sizing are untouched either way |
| `execution-pools.network.managed` | boolean | `false` | restart | Raises the network pool's registered coverage. Netty keeps its event loops, sizing and affinity |
| `execution-pools.network.preserve-specialization` | boolean | `true` | n/a | Must stay `true`: network loops cannot be merged or reshaped without changing their semantics |
| `execution-pools.chunk-system.managed` | boolean | `false` | restart | Raises the registered coverage of `chunk-worker` and `chunk-io`. The chunk system keeps its ordered streams and its queues |
| `execution-pools.chunk-system.preserve-specialization` | boolean | `true` | n/a | Must stay `true`, for the same reason |
| `metrics.enabled` | boolean | `true` | restart | Collect pool and thread state at all. With collection off the snapshot is structural: it lists the registered pools and the threads the registry accounts for, but queue depth, task counters, CPU, saturation and the per-thread list are unmeasured, the rates are zero and `degraded: METRICS_UNAVAILABLE` says so; `unknown threads: n/a (metrics collection is disabled)` |
| `metrics.per-pool` | boolean | `true` | restart | Publish per-pool entries, saturation and their JFR events |
| `metrics.per-thread` | boolean | `true` | restart | Publish the per-thread list. Disabling keeps pool totals but removes individual threads from the snapshot |

## What changing a key does not do

This list matters more than the table above, because most of these keys are forward-looking model
fields rather than levers that move threads today.

| You change | What actually happens |
| --- | --- |
| `region-execution.threads` | The registered pool spec and the budget deduction change. Folia's tick thread count does not: it comes from `paper-global.yml: threaded-regions.threads`, and under EDF it cannot change without a restart anyway |
| `region-execution.scheduler` | The configured and reported scheduler variant changes. It does not switch Folia's live scheduler; that happens only through the platform key and a restart, and `kairos` has no implementation yet |
| `region-execution.grid-exponent` | The model value changes. The regioniser's shift is fixed at the first initialisation |
| `execution-pools.chunk-system.managed` or `network.managed` | The pool is registered with coverage L2 instead of L1, and `/tuff threads` prints the deeper level. No thread is created through the factory, because no owner hands its factory over yet. The manager does not flag this as a conflict for these kinds — treat the printed level as a registration claim, not proof of management |
| `async-compute.managed` | The compute pool is still registered at L1 and the snapshot reports `CONFIG_CONFLICT`, because the adapter that would deliver the promised management is not wired |
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

The ledger now subtracts four tick threads instead of zero. It still does not size any pool.

**Experimental managed region (L2).** Only after the factory hook exists in the build you run;
today this changes the printed coverage and nothing else:

```yaml
thread-management:
  region-execution:
    managed: true
    scheduler: edf
```

Under EDF the snapshot then reports `SCHEDULER_LIMITATION` next to the region pool: L2 is the deepest
integration an unresizable scheduler can promise.

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
| `expected a whole number, got …` | Fractional or non-numeric count | Use an integer |
| `unknown key, ignored` | Typo or a key from a newer build | Remove it; the default stays in force |

## Related pages

- [Architecture](architecture.md) — how the values turn into pool specs and the budget ledger.
- [Observability](observability.md) — what `/tuff threads` prints for each of these values.
- [Compute configuration](../compute/configuration.md) — the `async-compute` section, which owns the
  compute engine's own sizing.
