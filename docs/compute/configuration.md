# Configuration

The compute scheduler reads `config/tuff.yml` from the server directory. The file is UTF-8, is created from
the bundled template on first start, and its `async-compute` section maps one-to-one onto `ComputeSettings`.

```yaml
async-compute:
  enabled: true
  workers: { ... }
  queue: { ... }
  scheduling: { ... }
  consumers: { ... }
  metrics: { ... }
  adaptive: { ... }
  shutdown-grace-seconds: 10
```

Rules that apply to the whole file:

- A missing key keeps its default. A missing section keeps that section's defaults.
- An unknown key is reported and ignored; it never fails the load.
- A value of the wrong type, or a section that is internally inconsistent, falls back to the default (for
  that key) or to the section's defaults (for the section), with a warning.
- A file that is not valid YAML keeps the previously published settings.
- Nothing in the configuration can stop the server from starting.
- Durations are plain numbers in the unit named by the key; fractions are allowed (`0.5` seconds,
  `250` milliseconds).
- `-1` means "let the scheduler choose" where the key documents it.

## Reload semantics

The loader (`TuffComputeConfig`) reads the file at startup through `load()` and can re-read it through
`reload()`. Both publish an immutable `ComputeSettings` snapshot, log every problem, and compute a diff
against the previously published snapshot, including which keys cannot be adopted by a running pool.

A reload never applies the snapshot to a running engine by itself: applying is the job of the server
bootstrap's `ComputeConfigApplyTarget`. Without that hook, a reload updates what the config layer reports
and what a *new* scheduler would be built from, while the live pool keeps the settings it started with. In
practice, restart after editing unless your build wires the hook.

Keys the diff always marks as restart-only:

| Key |
| --- |
| `async-compute.enabled` |
| `async-compute.queue.capacity` |
| `async-compute.scheduling.*` (all six keys) |
| `async-compute.consumers.allow-dynamic-registration` |
| `async-compute.adaptive.enabled` |

Everything else is live-capable in the diff: worker sizing, thread names, keep-alive, queue overflow, all
consumer policies, all metrics keys, the adaptive tuning values and the shutdown grace period.

Reload log lines:

```
Compute configuration loaded from /srv/tuff/config/tuff.yml
Compute configuration reloaded, 3 change(s): async-compute.consumers.default.max-concurrent, ...
These compute settings take effect after a restart: async-compute.scheduling.strategy
Compute configuration was not reloaded, the scheduler keeps its previous settings
Compute config: async-compute.workers.threads: expected a whole number, got 'many'; using -1
```

## `async-compute`

| Key | Type | Default | Allowed | Effect | Restart |
| --- | --- | --- | --- | --- | --- |
| `enabled` | boolean | `true` | `true`/`false` | Master switch. `false` registers consumers but starts no worker, no janitor, no adaptive timer. Submissions are refused with `SCHEDULER_DISABLED` | yes |

With `enabled: false` the scheduler still reaches `RUNNING`, so `isRunning()` is not a reliable "accepts
work" check. Catch `ComputeRejectedException`.

## `async-compute.workers`

| Key | Type | Unit | Default | Allowed | Effect | Restart |
| --- | --- | --- | --- | --- | --- | --- |
| `threads` | integer | threads | `-1` | `-1` or `>= 1` | Fixed worker count, or automatic sizing. Must not exceed `max-threads` when both are set | no |
| `min-threads` | integer | threads | `2` | `>= 1` | Workers kept alive while idle; also the floor of automatic sizing and the static autoscaler | no |
| `max-threads` | integer | threads | `-1` | `-1` or `>= min-threads` | Hard ceiling. `-1` resolves to the processor count, never below `min-threads` | no |
| `keep-alive-seconds` | number | seconds | `30` | `> 0` | How long an idle worker above the floor waits before it exits | no |
| `thread-name-prefix` | string | — | `"Tuff Compute Worker"` | non-blank | Thread name prefix, used verbatim in dumps and profilers | no |

### Automatic sizing

With `threads: -1`, the initial worker count is derived from `Runtime.availableProcessors()`:

```
tick      = cores <= 8 ? 1 : cores / 8        // assumes one tick thread per eight cores, plus global
reserve   = max(2, ceil(cores / 8))           // GC, JIT, network, watchdog
target    = cores - tick - 1 - reserve
workers   = clamp(target, min-threads, max-threads)
```

Examples with `min-threads: 2`, `max-threads: -1`:

| Cores | Automatic workers | Ceiling |
| --- | --- | --- |
| 4 | 2 (target is 0, floor wins) | 4 |
| 8 | 4 | 8 |
| 16 | 11 | 16 |
| 32 | 23 | 32 |
| 64 | 47 | 64 |

This is the sizing the pool actually uses. It does **not** subtract the live region and chunk thread counts;
reserve only approximates them. On a server with many regions or a large chunk worker pool, set `threads` or
`max-threads` explicitly — automatic sizing can otherwise hand the compute pool cores the regioniser is
already using.

When workers are automatic and adaptive concurrency is off, the pool grows on demand (one worker per
submission that finds a backlog and no idle worker, plus a janitor reconciliation every tick) and shrinks
back to `min-threads` after `keep-alive-seconds`. An explicit `threads` pins both the count and the
adaptive controller.

## `async-compute.queue`

| Key | Type | Unit | Default | Allowed | Effect | Restart |
| --- | --- | --- | --- | --- | --- | --- |
| `capacity` | integer | tasks | `8192` | `>= 1` | Hard bound on all queued (not running) tasks | yes |
| `overflow-policy` | enum | — | `reject` | `reject`, `drop-oldest` | Parsed, reported and diffed, but **not consulted by the queue**. The submitting consumer's `overflow-policy` decides for both the global and the consumer bound | no (unused) |

Capacity protects the heap from a submission burst. It does not slow the submitting thread down: at the
bound the scheduler refuses or evicts, it never blocks. See
[Architecture § Bounded queue](architecture.md#bounded-queue-and-the-three-failure-paths).

## `async-compute.scheduling`

| Key | Type | Unit | Default | Allowed | Effect | Restart |
| --- | --- | --- | --- | --- | --- | --- |
| `strategy` | enum | — | `weighted-fair` | `weighted-fair`, `priority`, `fifo` | How the next task is chosen. See below | yes |
| `work-stealing` | boolean | — | `false` | `true`/`false` | Idle workers may scan consumer streams from a rotating index instead of normal selection | yes |
| `priority-aging` | boolean | — | `true` | `true`/`false` | Waiting tasks gain effective priority levels | yes |
| `aging-interval-millis` | number | ms | `1000` | `> 0` | Wait per aging step | yes |
| `max-aging-boost` | integer | levels | `3` | `>= 1` | Maximum levels a task may gain; capped at `CRITICAL` | yes |
| `idle-wait-millis` | number | ms | `50` | `> 0` | How long an idle worker parks before checking again | yes |

| Strategy | Behaviour |
| --- | --- |
| `weighted-fair` | Highest effective priority first; ties split between consumers by `fair-share-weight` |
| `priority` | Strictly highest effective priority first. Weights are ignored |
| `fifo` | Submission order only. Priority, aging and weight are ignored. Benchmark baseline |

`work-stealing: true` is not a different queue: all tasks already live in one shared structure. It only
changes how a worker with no admissible head scans the streams. Leave it off unless a benchmark shows a
gain.

## `async-compute.consumers`

`consumers` holds the defaults, per-consumer overrides, and the dynamic registration switch.

```yaml
consumers:
  allow-dynamic-registration: true
  default:
    ...
  chunk-generation:
    ...
```

| Key | Type | Default | Allowed | Effect | Restart |
| --- | --- | --- | --- | --- | --- |
| `allow-dynamic-registration` | boolean | `true` | `true`/`false` | Submitting under an unknown consumer id registers it on the fly with `consumers.default` (or a matching override). `false` rejects such submissions with `CONSUMER_UNKNOWN` | yes |

### Consumer policy keys

Every consumer section, including `default`, accepts the same keys. A key missing from an override is
inherited from `default`.

| Key | Type | Unit | Default in `default` | Allowed | Effect |
| --- | --- | --- | --- | --- | --- |
| `enabled` | boolean | — | `true` | `true`/`false` | A disabled consumer rejects every submission with `CONSUMER_DISABLED` but keeps its metrics |
| `max-concurrent` | integer or fraction | tasks / pool share | `-1` | `-1`, `>= 1`, or `(0.0, 1.0]` | Concurrency limit. Whole number = absolute; decimal point = share of the current pool limit. `-1` = the whole pool |
| `fair-share-weight` | number | ratio | `1.0` | finite `> 0` | Dispatch weight under `weighted-fair`; `2.0` gets about twice the turns of `1.0` |
| `max-queued-tasks` | integer | tasks | `-1` | `-1` or `>= 0` | Per-consumer queue bound. `-1` = only the global capacity applies |
| `overflow-policy` | enum | — | `reject` | `reject`, `drop-oldest` | What happens when this consumer cannot queue more. Applies to both bounds |
| `max-queue-wait-seconds` | number | seconds | `0` | `>= 0` | Queued wait limit; `0` disables expiry |

`max-concurrent` parsing is literal about the decimal point: write `4` for four tasks and `0.25` for a
quarter of the pool. `4.0` is read as a fraction, fails validation, and falls back.

A share is resolved against the **current pool-wide limit**, so it follows adaptive resizing, and it is
always at least one task. It is a cap, not a reservation.

### Shipped consumer sections

The template configures three example consumers: `chunk-generation` (`0.60`), `pathfinding` (`0.30`) and
`ai-computation` (`0.20`). On start the engine registers every configured override, so a consumer with that
id gets those limits whether the work comes from a server subsystem or from a plugin using the same id.
Nothing is reserved for them; a subsystem that never submits leaves its share to everyone else.

## `async-compute.metrics`

| Key | Type | Unit | Default | Allowed | Effect | Restart |
| --- | --- | --- | --- | --- | --- | --- |
| `enabled` | boolean | — | `true` | `true`/`false` | Counters, gauges and histograms. When false, snapshots report zero counters and gauges, and no sampler thread runs | no |
| `jfr` | boolean | — | `false` | `true`/`false` | Emit `tuff.compute.*` JFR events. Independent of `enabled` | no |
| `throughput-window-seconds` | number | seconds | `10` | `> 0` | Window for `throughputPerSecond()`. Queue-wait and execution averages are lifetime averages, not windowed | no |
| `sample-cpu` | boolean | — | `true` | `true`/`false` | Sample process CPU load for `cpuUtilization()`. Requires `enabled: true` | no |
| `sample-interval-seconds` | number | seconds | `1` | `> 0` | Gauge sampling interval, and the base for the janitor tick (clamped to 10–100 ms), which sets expiry precision | no |

`sample-cpu` is the **process** CPU load, not pool utilisation: tick threads, GC and JIT are included.
`adaptive` uses the same probe for its CPU guard regardless of this key.

## `async-compute.adaptive`

| Key | Type | Default | Allowed | Effect | Restart |
| --- | --- | --- | --- | --- | --- |
| `enabled` | boolean | `false` | `true`/`false` | Let the controller move the concurrency limit. Requires `threads: -1`; an explicit worker count pins the limit | yes |
| `min-concurrency` | integer | `1` | `>= 1` | Floor, raised to at least `min-threads` | no |
| `max-concurrency` | integer | `-1` | `-1` or `>= min-concurrency` | Ceiling, capped by the worker ceiling. `-1` = worker ceiling | no |
| `sample-window-seconds` | number | `5` | `> 0` | Aggregation window; the decision interval is `max(window, 1 s)` | no |
| `min-samples` | integer | `20` | `>= 1` | Finished tasks needed in a window before the limit may move (an entirely idle pool is the exception) | no |
| `increase-step` | number | `0.1` | `(0.0, 1.0]` | Additive growth: `round(limit * step)`, clamped to 1–4 workers | no |
| `decrease-factor` | number | `0.5` | `(0.0, 1.0)` | Multiplicative shrink: `ceil(limit * (1 - factor))`, at least one slot | no |
| `target-utilization` | number | `0.9` | `(0.0, 1.0]` | Estimated utilisation required before growth | no |

The controller treats failed tasks as non-samples, never grows when process CPU is at or above 90%, and
shrinks on latency pressure or after three idle windows. Full decision logic is in
[Architecture § Autoscaling](architecture.md#autoscaling).

## `async-compute.shutdown-grace-seconds`

| Key | Type | Unit | Default | Allowed | Effect |
| --- | --- | --- | --- | --- | --- |
| `shutdown-grace-seconds` | number | seconds | `10` | `>= 0` | How long shutdown waits for running tasks before it keeps waiting in 100 ms slices. Queued work is cancelled immediately regardless |

The `ComputeSettings` record default is 30 seconds; the server file ships 10 because the world save runs
right after the pool stops. Running tasks are never interrupted, so this value does not kill anything.

## Reading validation errors

Every problem becomes one WARN line prefixed with `Compute config:` and is also available through
`TuffComputeConfig#problems()`. Shapes you will see:

| Message | Meaning | Fix |
| --- | --- | --- |
| `async-compute: section is missing, using defaults` | The section is absent or empty | Re-create it from the template |
| `async-compute.surprise: unknown key, ignored` | Typo or a key from a newer/older version | Remove it or correct the spelling |
| `async-compute.workers.threads: expected a whole number, got 'many'; using -1` | Wrong type | Write an integer |
| `async-compute.queue.overflow-policy: expected reject or drop-oldest, got 'drop'; using reject` | Unknown enum value | Use one of the documented values |
| `async-compute.consumers.default.max-concurrent: a share must be in (0.0, 1.0], got 4.0; using 1.0` | Wrote `4.0` | Write `4` for an absolute count |
| `async-compute.workers: rejected (threads must not exceed maxThreads, got 8 > 4), using defaults` | Inconsistent section | Fix the values; the whole section fell back |
| `async-compute.scheduling.aging-interval-millis: must be positive, got 0.0; using 1000` | Zero duration | Use a positive number |
| `config/tuff.yml: is not valid YAML (...), keeping the previous settings` | Parse failure | Fix the YAML; the old snapshot stays live |

A rejected section is replaced as a whole, so one bad key can reset its siblings. Read the warning, fix the
key, and reload or restart.

## Presets

Only the keys that differ from the defaults are shown.

### Small server (4–8 cores, a few plugins)

```yaml
async-compute:
  workers:
    threads: -1
    min-threads: 1
    max-threads: 2
    keep-alive-seconds: 30
  queue:
    capacity: 2048
  consumers:
    default:
      max-queued-tasks: 512
```

Automatic sizing on four cores yields a target of zero, so the floor lands on one worker; the explicit
ceiling prevents growth from taking a core the regioniser needs.

### Large server (32+ cores, chunk generation and pathfinding under load)

```yaml
async-compute:
  workers:
    threads: -1
    min-threads: 4
    max-threads: 24
    keep-alive-seconds: 60
  queue:
    capacity: 16384
  scheduling:
    strategy: weighted-fair
    priority-aging: true
    aging-interval-millis: 1000
    max-aging-boost: 3
  consumers:
    chunk-generation:
      max-concurrent: 0.50
      fair-share-weight: 2.0
    pathfinding:
      max-concurrent: 0.25
      fair-share-weight: 1.0
  metrics:
    throughput-window-seconds: 30
    jfr: true
  adaptive:
    enabled: true
    min-concurrency: 4
    max-concurrency: 24
    target-utilization: 0.85
```

`max-threads` keeps the pool below the core count, and adaptive growth is bounded by it. The weights only
matter while both consumers have queued work.

### Background-only pool (fixed size, work is disposable)

```yaml
async-compute:
  workers:
    threads: 4
    min-threads: 4
    max-threads: 4
  scheduling:
    strategy: priority
    priority-aging: true
    aging-interval-millis: 2000
    max-aging-boost: 2
  consumers:
    default:
      max-concurrent: 2
      max-queued-tasks: 256
      overflow-policy: drop-oldest
      max-queue-wait-seconds: 15
```

Every consumer inherits the policy, so stale work is replaced instead of piling up. An explicit `threads`
value disables adaptive concurrency; `max-concurrency` in the adaptive section would have no effect.
