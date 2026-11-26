# Tuning

There is no universal configuration. The compute engine shares cores with the region tick threads, the chunk
system and the IO threads, and it has two lanes with different jobs. Tune against the budget the server
actually has, not against the core count on the box.

Start from [Configuration](configuration.md) for the key reference and [Operations § Command](operations.md#command)
for the live numbers. Change one value at a time and keep a baseline.

## The two knobs that matter first

### Platform workers

The platform lane is the CPU pool. `workers.threads: -1` sizes it from the processor budget the server
computes after its own threads exist (`ComputeThreadBudget`). A fixed `threads` value is clamped by
`max-threads`, which is the budget unless you set `max-threads` explicitly.

- Leave `threads: -1` unless you have measured a problem. The budget already subtracts region, chunk, IO
  threads and a network reserve.
- `max-threads` is the hard ceiling for growth and for adaptive concurrency. An explicit value is taken as
  written, so you can deliberately oversubscribe the budget; leaving it at `-1` makes the budget the
  ceiling. Set it below the budget when tick time must be protected from compute bursts.
- `min-threads` is the idle floor. The default of `2` is why an idle server shows two workers; that is
  normal, not a leak.
- More workers than the budget do not add throughput; they add context switches for the tick threads.

### Virtual lane

The virtual lane is for blocking and IO-bound work, and as a deadlock breaker. It has no CPU parallelism:
a CPU-bound task on the lane competes with the platform workers for the same cores.

- Size `lanes.virtual.max-concurrent` to the downstream limit (database pool, HTTP connection pool, file
  handles), not to the CPU count. `64` is a ceiling for blocking work, not a target.
- Keep CPU-bound work on `NORMAL`/`HIGH` and set `normal-spill: false` if `NORMAL` is CPU-heavy.
- Use `LOW` for work that may wait and may block (prefetch, cache warm-up, webhooks). Do not use `LOW` for a
  CPU render or a structure search.
- The lane shares consumer quotas with the platform workers: a virtual task holds a consumer slot, so a
  consumer can exhaust its limit on the lane.

## Presets

Only the keys that differ from the shipped defaults are shown. Consumer ids such as `my-plugin` are
placeholders: the engine ships no built-in consumers, so replace them with the ids your plugins actually
use. An explicit `max-threads` is taken as written and is not clamped to the budget; keep it at or below the
budget unless you intend to oversubscribe. Any `lanes.*` change needs a restart; only consumer policies can
be reloaded live.

### Small VPS or home server (2–4 cores)

The budget is usually one or two processors. Protect the tick thread and let IO work use the lane.

```yaml
async-compute:
  workers:
    threads: -1
    min-threads: 1
    max-threads: 2
    keep-alive-seconds: 30
  queue:
    capacity: 1024
    overflow-policy: reject
  scheduling:
    idle-wait-millis: 10
  lanes:
    virtual:
      enabled: true
      low-priority: true
      normal-spill: true
      max-concurrent: 16
      spill-wait-millis: 100
      spill-queue-depth: 32
  consumers:
    default:
      max-queued-tasks: 256
      max-queue-wait-seconds: 30
```

### Medium SMP (8–12 cores)

Normal server load: a few plugins, some background computation, a little IO.

```yaml
async-compute:
  workers:
    threads: -1
    min-threads: 2
    max-threads: 8
    keep-alive-seconds: 30
  queue:
    capacity: 4096
    overflow-policy: reject
  scheduling:
    idle-wait-millis: 10
    aging-interval-millis: 1000
    max-aging-boost: 3
  lanes:
    virtual:
      enabled: true
      low-priority: true
      normal-spill: true
      max-concurrent: 32
      spill-wait-millis: 50
      spill-queue-depth: 64
  consumers:
    default:
      max-queued-tasks: 512
      max-queue-wait-seconds: 60
```

### Large dedicated server (16+ cores)

CPU headroom exists, but the budget still excludes region, chunk and IO threads. Keep the ceiling explicit
and let adaptive concurrency find the live limit.

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
    idle-wait-millis: 10
  lanes:
    virtual:
      enabled: true
      low-priority: true
      normal-spill: true
      max-concurrent: 128
      spill-wait-millis: 25
      spill-queue-depth: 128
  adaptive:
    enabled: true
    min-concurrency: 4
    max-concurrency: 24
    target-utilization: 0.9
  metrics:
    jfr: true
```

`max-concurrency` equal to `max-threads` keeps the controller from growing past the ceiling the
administrator chose. An explicit `max-threads` is not clamped to the budget; leave it at `-1` or set it at
or below the budget you expect. `/tuff` shows the live count and the startup settings.

### Skyblock or mini-games (many regions, short ticks)

Regions are the latency-critical part. Keep the platform pool small and predictable, and let genuinely
blocking work wait on the lane.

```yaml
async-compute:
  workers:
    threads: -1
    min-threads: 2
    max-threads: 6
    keep-alive-seconds: 30
  queue:
    capacity: 4096
  scheduling:
    idle-wait-millis: 5
    aging-interval-millis: 500
    max-aging-boost: 3
  lanes:
    virtual:
      enabled: true
      low-priority: true
      normal-spill: true
      max-concurrent: 64
      spill-wait-millis: 25
      spill-queue-depth: 32
  consumers:
    default:
      max-queued-tasks: 256
      max-queue-wait-seconds: 10
```

Short `max-queue-wait-seconds` drops stale work instead of letting it run after the player left. Keep
`overflow-policy: reject` unless the work is disposable; `DROP_OLDEST` silently cancels queued tasks of that
consumer.

### Pre-generated world vs generation on the fly

The budget is computed from allocated region, chunk and IO threads, not from how busy they are, so
pre-generating does not enlarge the compute budget by itself. It does free CPU time, which is what you can
spend on the pool.

- **Pre-generated:** the large-server preset above is a safe starting point. `max-threads` can sit at the
  budget ceiling, and `adaptive.enabled: true` is worth it.
- **Generating on the fly:** keep `max-threads` at or below the budget and leave chunk generation its
  headroom. Put speculative prefetch on `LOW` (virtual lane) and keep `normal-spill` for IO only. Use a
  shorter `max-queue-wait-seconds` (30–60) so stale generation requests do not run after the player moved.

### Heavy plugins, webhooks and database access

IO-bound work belongs on the virtual lane; CPU work must not follow it.

```yaml
async-compute:
  workers:
    threads: -1
    min-threads: 2
    max-threads: 12
  lanes:
    virtual:
      enabled: true
      low-priority: true
      normal-spill: false
      max-concurrent: 16
      spill-wait-millis: 100
      spill-queue-depth: 0
  consumers:
    my-plugin:
      max-concurrent: 8
      max-queued-tasks: 256
      overflow-policy: reject
      max-queue-wait-seconds: 30
```

Size `max-concurrent` to the database or HTTP pool, not to CPU. `spill-queue-depth: 0` disables the depth
trigger, so only age (`spill-wait-millis`) opens spill; with `normal-spill: false` the lane carries `LOW`
work only. Per-consumer quotas keep one plugin from filling the shared queue.

### Maps, rendering and structure search

These are CPU-bound. Keep them on the platform lane and leave `normal-spill` off for them, so virtual threads
do not pick the work up.

```yaml
async-compute:
  workers:
    threads: -1
    min-threads: 4
    max-threads: 32
    keep-alive-seconds: 60
  queue:
    capacity: 8192
  scheduling:
    strategy: weighted-fair
    idle-wait-millis: 10
  lanes:
    virtual:
      enabled: true
      low-priority: true
      normal-spill: false
      max-concurrent: 32
  adaptive:
    enabled: true
    min-concurrency: 4
    max-concurrency: 32
  metrics:
    jfr: true
```

Use `NORMAL` (or `HIGH` for work that gates a region) for render and search tasks. `LOW` is for work that
may wait; if it is CPU-bound, `LOW` sends it to virtual threads and makes tick jitter worse.

## Match the lane to the work

| Work | Priority | Lane | What to size |
| --- | --- | --- | --- |
| Blocking IO: HTTP, database, disk, plugin APIs that wait | `LOW` | virtual | `lanes.virtual.max-concurrent` to the downstream pool; `spill-wait-millis` low if `NORMAL` IO also needs the lane |
| CPU compute: rendering, pathfinding, structure search, serialisation | `NORMAL` (or `HIGH` when it gates a region) | platform | `workers.max-threads`/`threads` against the budget; `normal-spill: false` |
| Bursty work: login waves, world events | `NORMAL` | platform, with spill as a tail valve | `queue.capacity`, per-consumer `max-queued-tasks`, `max-queue-wait-seconds`, modest spill depth |
| Latency-critical: a player is waiting now | `CRITICAL`/`HIGH` | platform, never spill | `min-threads` high enough for the expected concurrency, low `idle-wait-millis`; do not rely on the lane |
| Disposable cache warm-up, speculative prefetch | `LOW` | virtual | `max-concurrent` and `max-queue-wait-seconds`; rejects or expiry are acceptable |

`CRITICAL` is not a capacity reservation. A flood of `CRITICAL` work degrades scheduling into FIFO; use it
only when a person is actually waiting.

## Spill thresholds

Spill is a tail valve for `NORMAL` work, opened only while the platform lane is saturated. It uses two
OR-ed triggers: the queue depth (`spill-queue-depth`) and the age of the oldest queued `NORMAL` task
(`spill-wait-millis`).

| Workload | `normal-spill` | `spill-wait-millis` | `spill-queue-depth` | Why |
| --- | --- | --- | --- | --- |
| CPU-bound `NORMAL` | `false` | — | — | Virtual threads would compete for the same cores |
| Mixed, CPU mostly | `true` | 100–250 | 0 or 64+ | Only a long tail spills; normal work stays on workers |
| Mixed, IO mostly | `true` | 10–50 | 32–64 | Blocking work escapes saturation quickly |
| Bursty, short spikes | `true` | 50–100 | 64–128 | Depth trigger handles the spike, age handles the slow tail |
| Strict CPU isolation | `false` | — | — | The lane carries `LOW` only |

`spill-queue-depth: 0` removes the depth trigger, not spill: age still opens it. Set `normal-spill: false`
to remove `NORMAL` spill entirely. `HIGH` and `CRITICAL` never spill regardless of these values.

## Symptom → what to change

| Symptom | Likely cause | First change |
| --- | --- | --- |
| Queue wait grows, tick times stay healthy | Platform pool is the bottleneck | Raise `workers.max-threads` if the budget allows; otherwise reduce offered work or per-consumer limits |
| MSPT grows together with the queue | Compute is competing with region threads | Lower `workers.max-threads`/`threads`, check that the budget is applied, and check that CPU-bound work is not on the virtual lane |
| `virtual running == limit`, `queued` grows | The lane, not the CPU, is the bottleneck | Raise `max-concurrent` only for blocking work and only if the downstream can take it; otherwise shorten `max-queue-wait-seconds` or shed |
| `spilled` climbs while `NORMAL` is CPU-bound | CPU work is being picked up by virtual threads | Set `normal-spill: false`, or add platform capacity; virtual threads add no CPU |
| Many `rejected` | Queue capacity or a per-consumer bound is exhausted | Raise `queue.capacity` carefully (heap), raise `max-queued-tasks` for that consumer, or apply backpressure at the producer |
| Many `expired` | Work waits longer than `max-queue-wait-seconds` | Raise the wait limit or reduce production; check whether the lane or a consumer quota is the bottleneck |
| `virtual` counters stay at zero on a busy server | No `LOW` work and no spill condition | Expected for CPU-only loads; check `low-priority` and the spill keys if IO work exists |
| Exactly two workers in idle | `min-threads: 2` | Normal; not a leak |
| One consumer at `active / limit` while queue grows | Its own `max-concurrent` caps it, and virtual tasks also hold those slots | Raise that consumer's limit or split the work across consumer ids |
| First task after an idle period is slow | `scheduling.idle-wait-millis` park interval | Lower it (e.g. 5–10 ms) if latency matters more than wakeups |

## Anti-patterns

- **`threads` = logical CPU count.** Region, chunk and IO threads already own part of the machine. Use the
  budget (`threads: -1`) or a value below it.
- **Huge `max-concurrent` "just in case".** Virtual threads do not add CPU. A large limit for CPU-bound work
  increases contention; a large limit for blocking work overwhelms the database or HTTP pool.
- **Using `LOW` for CPU work.** `LOW` is routed to the virtual lane and will compete with tick threads.
- **Leaving `normal-spill: true` for CPU-bound `NORMAL` work.** The spill is a tail valve for blocking work,
  not a second CPU pool.
- **`DROP_OLDEST` where losing work is not acceptable.** It cancels the oldest queued task of that consumer;
  use `REJECT` and backpressure instead.
- **Removing the bounded queue by setting an enormous capacity.** Capacity protects the heap; a queue of
  millions of tasks is an outage waiting to happen.
- **Ignoring shared quotas.** Virtual tasks hold consumer slots, so a busy lane can starve that consumer's
  platform work.
- **Expecting virtual threads in a thread dump.** Use `/tuff` and the lane metrics.

## Verifying a configuration

1. Restart after changing `workers.*`, `queue.*`, `scheduling.*`, `metrics.*`, `adaptive.*` or `lanes.*`;
   only consumer policies reload live. `/tuff compute reload` prints the deferred keys.
2. Apply representative load, not an idle server. Run `/tuff` several times and look at the lane line, queue
   depth, rejects, expired tasks, queue wait and throughput.
3. `/tuff compute consumers` to see which consumer is at its `active / limit` or queue bound.
4. Enable `metrics.jfr` and record a short window: `TaskRejected` and `TaskQueueExpired` explain failures,
   `VirtualTask` shows what landed on the lane, `AdaptiveDecision` explains why the platform limit moved.
5. Compare tick time (MSPT/TPS) with and without the load. A pool that looks busy but leaves ticks healthy
   is usually sized correctly; a pool that improves throughput while MSPT rises is too large or is running
   CPU work on the virtual lane.
6. For absolute cost expectations, see [Benchmarks](benchmarks.md). They describe the per-task wrapper and
   scaling on one machine; they are not a capacity plan for your server.
