# Tuning

This page is for operators who need to choose numbers, not for readers who want the key reference.
It covers two sections of `config/tuff.yml` - `thread-management` (the thread layer, UTMI) and
`async-compute` (the compute engine) - and the one platform key that decides how many region tick
threads exist. Every formula below is the one the code evaluates; where a number can only come from
your machine, the page says which command or log line produces it instead of guessing.

Two rules frame everything else:

- **The server has two independent budgets.** The thread layer's ledger decides how many threads the
  managed pools may hold together. The compute engine's own budget decides how many platform workers
  it may use. They share the 80% rule and nothing else, and only one of them sizes the compute pool at
  a time: with `execution-pools.compute.managed: true` the ledger wins, without it the engine's own
  arithmetic wins. See [Architecture § Budget model](architecture.md#budget-model).
- **A preset is a set of decisions, not a performance claim.** No preset here says "this is faster".
  Thread counts and budgets are arithmetic and can be checked exactly; throughput and tick time can
  only come from a measurement, and [Verifying a change](#verifying-a-change) is the procedure for one.

## The arithmetic behind every preset

### Region tick threads (Folia's number, not this layer's)

`paper-global.yml` -> `threaded-regions.threads` decides how many tick threads Folia creates. The
default is `-1`, and Folia then applies its own formula (`TickRegions#getTickThreads`):

```
t = totalCores / 2            (integer division; totalCores is what OSNuma reports)
threads = t <= 4 ? 1 : t / 4
```

| Cores | 2 | 4 | 8 | 12 | 16 | 24 | 32 | 64 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Tick threads with `-1` | 1 | 1 | 1 | 1 | 2 | 3 | 4 | 8 |

The startup log is the source of truth for the value your machine actually got:

```
Initialised EDF Folia scheduler with initial 4 target thread(s)
```

`thread-management.region-execution.threads` does **not** create or resize those threads, and it does
not set the Kairos fund's floor either. It is the number the thread layer subtracts from its ledger and
adds back when it answers the fund's ceiling; the fund's floor is the started count above. Keep the two
equal so the ledger describes the server you run. Under EDF a change to either key needs a restart:
`TickRegionScheduler#setThreads` is a silent no-op for EDF.

### The compute processor budget

With `threads: -1` the engine sizes itself from a budget, and the budget comes from exactly one source.

Without managed compute, `ComputeThreadBudget.resolve`:

```
budget = max(1, floor(cores * 0.8) - regionThreads - chunkWorkers - ioThreads - 4)
```

`regionThreads` is the live scheduler count, `chunkWorkers` and `ioThreads` are the live Moonrise pool
sizes, and the trailing `4` is `ComputeThreadBudget.NETWORK_RESERVE`, a fixed allowance for netty and
the small server pools.

With `execution-pools.compute.managed: true` (or `async-compute.managed: true`) the legacy formula is
not evaluated at all. The adapter answers with the compute pool's budget from the ledger:

```
poolBudget = max(1, usable - tick-pressure reserve)          # the ledger's usable, after load shedding
budget     = max(relief floor, poolBudget)                   # relief floor = min-threads + reserved-cores
                                                             # (+1 while threads: -1)
```

That difference matters. The ledger does **not** subtract chunk worker, chunk IO or netty threads
(it cannot read the platform configuration), so `usable` is optimistic on a server that runs chunk
threads. Managed compute can therefore hand the engine a larger budget than the legacy formula would.
Set `async-compute.workers.max-threads` to the number you actually want whenever you turn management
on; `async-compute.max-threads` is only recorded on the observed pool spec and sizes nothing.

### The worker count

Automatic sizing, `threads: -1`, evaluates
`ComputeSettings.Workers#resolveThreadCount(budget)` for every budget it is handed:

```
target  = max(min-threads, budget - 1)
ceiling = max-threads != -1 ? max-threads : max(min-threads, budget)
workers = max(1, min(ceiling, target))
```

Three consequences an operator needs before choosing numbers:

- **`min-threads` is an absolute floor, not a hint.** On a one-core budget with the shipped
  `min-threads: 2` the pool still runs two workers: `target = max(2, 0) = 2`. That is one core more
  than the budget allows, and it is the reason the small-server preset below lowers the floor to 1.
- **`max-threads` alone cannot raise an automatic count.** With `threads: -1` the count starts at
  `budget - 1`; a ceiling above the budget never binds. A ceiling in a preset is a guard, not capacity,
  and [Compute tuning](../compute/tuning.md) uses it that way.
- **A fixed count is the oversubscription knob.** `threads: N` is sized once, at startup, against the
  provisional startup budget (`max(1, floor(cores * 0.8) - 4)` while the engine owns its budget), and
  then never re-budgeted: `applyRegionBudget` sees a fixed count, logs `fixed worker count N, the
  server processor budget is not applied` and returns. The live region/chunk/IO deduction does not
  reach it; with `max-threads: -1` the startup budget is its ceiling, and an explicit `max-threads` at
  least as large as `threads` lets the pinned count through. Use a fixed count only on a machine you
  have measured.

### The thread layer's ledger

`BudgetPlanner` computes one ledger at start:

```
ceiling      = max(1, floor(availableProcessors * 0.8))
rawUsable    = ceiling - regionThreads - chunkThreads - ioThreads
                        - max(1, reserve-cores) - criticalReserve
usable       = max(1, rawUsable)       # the number /tuff threads prints on `cores:`
shared       = max(0, rawUsable)       # what the compute/service/virtual pools may divide
```

- `regionThreads` comes only from `region-execution.threads`; `-1` means zero. `chunkThreads` and
  `ioThreads` are always zero in this build, and netty is not subtracted at all.
- `criticalReserve` covers the critical lanes' guaranteed minimum that the deductions did not pay
  for: a region minimum above the configured deduction, plus the whole network minimum. It is always
  applied, whether or not `resources.load-shedding` is on.
- `usable` is floored at one so the arithmetic stays total; `shared` may be zero, and a shared pool
  then gets zero permits instead of an invented core.

With the shipped `reserve-cores: 1`, no managed critical lane and `region-execution.threads` set to
Folia's count, the printed `cores:` value is:

| Cores | 4 | 8 | 16 | 24 | 32 | 64 |
| --- | --- | --- | --- | --- | --- | --- |
| Tick threads (Folia auto) | 1 | 1 | 2 | 3 | 4 | 8 |
| `usable` with `region-execution.threads` equal to it | 1 | 4 | 9 | 15 | 20 | 42 |

This is arithmetic, not a measurement: it is `floor(cores * 0.8)` minus the two numbers in the table
minus the one-core reserve. If you leave `region-execution.threads: -1`, the second row is zero and
`usable` is higher by the tick thread count - which is exactly the optimism the configuration page
warns about.

### Threads a preset actually creates

Counts below are threads, not cores, and idle threads park. They are the direct outcome of the keys.

| Source | Threads | When |
| --- | --- | --- |
| Region tick threads | Folia's count | Always; owned by Folia, never created by UTMI |
| Compute platform workers | `min-threads` .. ceiling | `async-compute.enabled: true` |
| Compute queue janitor | 1 | Always, for expiry and pool reconciliation |
| Compute metrics sampler | 1 | `async-compute.metrics.enabled: true` (default) |
| Compute adaptive timer | 1 | `async-compute.adaptive.enabled: true` |
| Virtual lane pump | 1 | `async-compute.lanes.virtual.enabled: true` (default) |
| Thread layer sampler | 1 | `thread-management.enabled: true`; registered in `service` at L2 |
| Kairos supervisor | 1 | Kairos selected, `elastic: true`, and headroom above the floor |

`region-execution.managed: true` adds no thread: it wraps each existing tick body so the registry
accounts it while it runs. Managed compute adds no thread either; it changes where the platform
workers come from and how many the budget allows.

Utility threads outside these two subsystems - netty event loops, chunk workers and chunk IO, the
global region thread, console, watchdog, JFR - are not part of any preset here and are not counted
above.

## Presets

Only the keys a preset changes are shown; where a shipped default is repeated it is there to make the
policy readable and can be omitted. `min-threads: 1` in a preset is a decision, not a typo: it keeps the
pool inside a one-core budget.

### Small SMP or home server (2-4 cores)

Typical shape: one or two regions, a handful of players, plugins that use the compute API for small
jobs. The regioniser already owns the machine; the job here is to keep the layer from promising more
than exists.

```yaml
thread-management:
  enabled: true
  resources:
    reserve-cores: 1
  region-execution:
    managed: false
    threads: 1          # match the startup log, not the formula

async-compute:
  workers:
    threads: -1
    min-threads: 1
    max-threads: 2
  queue:
    capacity: 1024
  lanes:
    virtual:
      max-concurrent: 16
  consumers:
    default:
      max-queued-tasks: 256
      max-queue-wait-seconds: 30
```

What changes: the ledger subtracts one tick thread instead of zero; the platform pool keeps one
worker instead of two when the budget is one; the queue bound drops from 8192 to 1024; the lane keeps
16 blocking tasks instead of 64. No pool is managed, so Folia's scheduling and the engine's ownership
do not change; the sizing differences above are the whole effect.

What it costs: on four cores the compute budget is `max(1, 3 - 1 - chunk - io - 4) = 1` whatever the
chunk and IO counts are, so the pool runs one worker. Five threads belong to these two subsystems:
the worker, the janitor, the metrics sampler, the lane pump and the thread-layer sampler. Region tick
threads are Folia's one.

Verify: `/tuff threads` prints `cores: 1 usable ...`, `compute L1 alive 1`, `service L2 alive 1` and
`pressure: disabled`. The startup log prints the budget line with every term.

### Busy hub or network (16-32 cores, many players per region)

Typical shape: one lobby or hub world, hundreds of players standing close together, few regions and
therefore few tick threads doing a lot of work each. Tick headroom is the product; compute is
secondary and must yield.

```yaml
thread-management:
  resources:
    reserve-cores: 1
  region-execution:
    managed: true
    threads: 2          # the value paper-global.yml actually uses on this box
  async-compute:
    tick-pressure:
      enabled: true
      enter-lag-fraction: 0.25
      enter-min-tps: 19.0
      exit-min-tps: 19.5
      max-reserve-cores: 2
      step-cores: 1
  execution-pools:
    compute:
      managed: true

async-compute:
  workers:
    threads: -1
    min-threads: 2
    max-threads: 4      # the ledger is optimistic, so cap the pool explicitly
  lanes:
    virtual:
      normal-spill: false
```

What changes:

- `region-execution.managed: true` raises the region pool to L2: every tick body is registered while
  it runs, at no extra thread. Folia still creates, names, groups and schedules the threads, and the
  snapshot reports `SCHEDULER_LIMITATION` because EDF cannot be resized.
- `execution-pools.compute.managed: true` moves the platform pool to L4: its workers come from the
  provider and its budget from the ledger. The explicit `max-threads: 4` is the cap that keeps the
  ledger's missing chunk/IO/netty deductions from inflating the pool.
- Tick pressure lowers that budget by one core per decision while regions lag, never below
  `min-threads` (plus the relief reserve when `async-compute.relief.enabled: true`), and returns it
  after three calm samples and the five-second cooldown.

What it costs: the same threads as an unmanaged server (workers, janitor, sampler, lane pump) plus
the one thread-layer sampler. Managed region adds none. On 16 cores the ledger reports nine usable
cores with two tick threads deducted, which is why the explicit cap matters. Tick pressure is the only
part of this preset that changes a live pool, and enabling the switch itself still needs a restart.

Verify: `/tuff threads` should show `region-execution L2`, the region lines
(`regions N / avg MSPT ... / min TPS ... / lag ...%`), a `pressure:` line with `reserve X of 2 core(s)`
and `budget N applied of 4 pool core(s)`, and `degraded:` without `CONFIG_CONFLICT`. Watch the region
count: on a hub with many players per region, `regions N` can be smaller than the tick thread count,
and extra tick threads simply park. `/tuff scheduler` under EDF prints the configured count and no
fund telemetry, which is the honest answer.

### Compute-heavy redstone or farm server (32+ cores)

Typical shape: pre-generated world, large farms and redstone contraptions, plugins that offload real
CPU work. The platform pool is the product; tick time is protected by keeping CPU work off the lane.

```yaml
thread-management:
  region-execution:
    threads: 4          # Folia's count on 32 cores

async-compute:
  workers:
    threads: -1
    min-threads: 4
    max-threads: -1
  queue:
    capacity: 16384
  scheduling:
    strategy: weighted-fair
  lanes:
    virtual:
      max-concurrent: 32
      normal-spill: false
  consumers:
    default:
      max-queued-tasks: 1024
      max-queue-wait-seconds: 60
  adaptive:
    enabled: true
    min-concurrency: 4
    max-concurrency: -1
  metrics:
    jfr: true
```

What changes: the platform pool is allowed to use the whole budget (`max-threads: -1` makes the
budget the ceiling); `normal-spill: false` keeps `NORMAL` work on platform workers instead of virtual
threads; adaptive concurrency can move the limit between the floor and the worker ceiling; JFR records
the engine's events.

What it costs: on 32 cores the legacy budget is `floor(25.6) - 4 - chunk - io - 4`; with three chunk
and IO threads in total that is 14, and automatic sizing starts at `max(4, 13) = 13` workers. Add the
janitor, the metrics sampler, the lane pump, the adaptive timer and the thread-layer sampler for 18
threads created by these two subsystems, plus Folia's four tick threads. The chunk and IO counts are
whatever your server reports, not this table.

Verify: `/tuff` prints the live worker count and the requested and applied budget; `/tuff threads`
prints `compute L1 alive N` and the pool's CPU share. Use `/tuff` for queue depth, rejects and wait
time, not `/tuff threads`: the layer does not print queue depth.

### Test or lab box

Typical shape: a VM or spare machine used to reproduce a report or measure a change. Here the point
is evidence, so the preset is the shipped defaults plus the two settings that make a reading possible.

```yaml
thread-management:
  enabled: true
  resources:
    reserve-cores: 1
  region-execution:
    managed: false
    threads: 2          # equal to paper-global.yml on this box
  metrics:
    enabled: true
    per-pool: true
    per-thread: true

async-compute:
  metrics:
    jfr: true
```

What changes: nothing about execution. The layer observes, the engine keeps its own threads and
budget, and JFR records the compute events. `enabled: false` remains the kill switch for the thread
layer and is the clean way to measure what the layer itself costs: same jar, same load, one restart
apart.

What it costs: the thread-layer sampler plus the engine's defaults. With the shipped `min-threads: 2`
and a one-core budget the engine still runs two workers, because the floor is absolute; that is the
shipped behaviour and worth knowing before you change anything else.

Verify: run `forceload add 0 0` (or join) before reading the region lines, because an empty server
has no loaded region and prints none. Check `sample: taken ... ms ago` moves, then follow the
procedure below.

## Restart-only keys

The whole `thread-management` section is read once, when the layer starts; `/tuff reload` lists any
changed key as "takes effect after a restart". That includes the `managed` flags, the metrics
switches, the reserve, the Kairos block and the sampling policy.

| Key | Applied |
| --- | --- |
| `thread-management.*` | Restart only |
| `async-compute.enabled`, `workers.*`, `queue.capacity`, `scheduling.*`, `metrics.*`, `adaptive.*`, `lanes.*`, `relief.*`, `shutdown-grace-seconds`, `consumers.allow-dynamic-registration` | Restart only |
| `async-compute.consumers.<id>.*` for a consumer that is already registered on the running engine | Live through `/tuff reload` |
| `async-compute.consumers.<id>.*` for an id that has not registered yet | Restart (the policy is read from the startup snapshot) |
| `async-compute.queue.overflow-policy` | Reaches the default consumer and dynamically registered consumers on a reload; an explicit per-consumer policy wins |
| `paper-global.yml: threaded-regions.scheduler` | Restart only |
| `paper-global.yml: threaded-regions.threads` | Restart under EDF; live for work stealing and for an elastic Kairos fund (see [Kairos](../scheduler/kairos.md#configuration)) |

## Verifying a change

There are two kinds of verification, and they answer different questions.

### 1. Did the configuration take effect?

These are facts, not measurements, and they need no baseline:

- **Startup log.** `Tuff thread management enabled: 9 pool(s) registered, ...`; the per-pool L2 line
  when the region pool is managed; `Tuff compute: processor budget N of M (r region, c chunk, i IO
  threads, source legacy)` or the managed variant with the ledger; the one-line tick pressure policy
  when it is enabled.
- **`/tuff threads`.** `cores: N usable after the reserve, R reserved` is the ledger's `usable`; the
  `reserve:` line is the critical reserve and the shedding state; each pool line is the registered
  coverage and its live threads. A pool whose `alive` differs from what you expected is the first
  thing to explain, before any tick number.
- **`/tuff scheduler`.** Under Kairos it prints the fund, the queue, the supervisor and the degraded
  reasons; under EDF or work stealing it prints the configured thread count and says there is no
  fund telemetry. Both `/tuff threads` and `/tuff scheduler` need `tuff.command.compute`; the
  scheduler report also accepts `tuff.command.scheduler`.

A preset whose arithmetic does not appear here did not apply. Read the `Thread config:` and
`Compute config:` warning lines before changing anything else.

### 2. Did it make the server better?

That is a measurement, and a single run cannot answer it. Use this procedure:

1. **Freeze the inputs.** Same world copy and seed, same plugins and versions, same view and
   simulation distance, pre-generated chunks, the same player or bot load. Record the machine's
   power profile: a laptop on battery throttles and the numbers move by more than most settings.
2. **Change one thing.** One key group per run. A preset applied as a block cannot tell you which
   key did the work.
3. **Run A/A first.** The same configuration twice, back to back, establishes the noise band. If the
   two A runs disagree by more than the effect you are hoping for, the machine cannot answer the
   question that day.
4. **Warm up, then measure.** Skip the first minutes (world load, chunk generation, JIT); then take
   at least ten runs of the same duration, not one. Compare medians, not the best run.
5. **Read the region numbers, not a server-wide number.** `/tps` gives per-region windows;
   `/tuff threads` gives the aggregate under the region pool (`regions`, mean and worst MSPT over
   5 s and 15 s, slowest region TPS, lag share). The 1 m, 5 m and 15 m windows are not read by this
   layer on purpose; do not sample them per second for a tuning run.
6. **Separate the layer's own cost.** With the thread layer on and off (one restart, same jar, same
   load), the difference in thread count and CPU is the layer's cost. On the Windows smoke stand the
   one-second sample measured about 33 ms of CPU per second in total, documented on
   [Observability § Sampling cost](observability.md#sampling-cost); measure your own box rather than
   assuming that number.

Do not publish or repeat a throughput or MSPT difference that came from fewer than ten runs or from
a throttled machine. The numbers in [`docs/compute/benchmarks.md`](../compute/benchmarks.md) are JMH
runs from one developer machine and say so; they are not a capacity plan for your server.

## Limits no preset changes

- **`min-threads` wins over the budget.** It is an absolute floor in automatic sizing and in the tick
  pressure floor. A budget smaller than the floor produces an overcommit, not a smaller pool.
- **Only compute is sized by the ledger.** `region-execution`, `chunk-worker`, `chunk-io`, `network`,
  `chat` and `background` are observed; the `managed` flags for network and chunk raise the
  registered level, not the accounting.
- **The ledger is optimistic.** Chunk worker and IO threads are passed as zero and netty is not
  subtracted, so `usable` is larger than the machine's real headroom on any server with those pools.
  The compute legacy formula does subtract the live counts, which is why the two budgets disagree.
- **EDF cannot be resized.** A tick thread count change is a restart. Only work stealing and the
  experimental Kairos fund have a live path.
- **Load shedding is dormant.** Its trigger needs a critical pool at L3 or deeper; region execution
  and network stop at L2. The critical reserve is arithmetic and always applies; the shedding switch
  changes nothing in this build, and `/tuff threads` says so.
- **Tick pressure needs managed compute.** With an observed pool the decisions are logged and printed
  but nothing is resized.
- **Kairos is experimental and opt-in.** It needs `paper-global.yml: threaded-regions.scheduler:
  kairos` and `region-execution.kairos-unstable: true`; every preset on this page keeps it off. Its
  fund, ceiling and risks are on [its own page](../scheduler/kairos.md).
- **The virtual lane adds no CPU.** Its limit is a concurrency cap for blocking work; CPU-bound work
  on the lane competes with the platform workers for the same cores.
- **Region parallelism is bounded by region count.** More tick threads than regions does not tick
  more regions; the extra threads park. `/tuff threads` prints the count, and `/tps` is the per-region
  view.

## Running behind a proxy

Tuff is the backend server. Velocity or BungeeCord runs as a separate JVM and holds the player
connections; the backend only sees the connections the proxy opens to it. None of that changes the
thread model described on this page: the proxy has its own threads and its own CPU, which UTMI cannot
see, and the backend still ticks regions, runs the compute pool and accounts its own threads exactly
as it does without a proxy.

### Where the keys live in this fork

`config/paper-global.yml`, `config/spigot.yml` and `server.properties` are generated on first start;
the names below are the ones this fork reads.

| Setting | File | Key | Default |
| --- | --- | --- | --- |
| Velocity modern forwarding | `config/paper-global.yml` | `proxies.velocity.enabled` | `false` |
| Velocity forwarding secret | `config/paper-global.yml` | `proxies.velocity.secret` | `''` (empty) |
| Velocity online mode | `config/paper-global.yml` | `proxies.velocity.online-mode` | `true` |
| BungeeCord online mode | `config/paper-global.yml` | `proxies.bungee-cord.online-mode` | `true` |
| PROXY protocol | `config/paper-global.yml` | `proxies.proxy-protocol` | `false` |
| Legacy BungeeCord forwarding | `config/spigot.yml` | `settings.bungeecord` | `false` |
| Server online mode | `server.properties` | `online-mode` | `true` |
| Listen address and port | `server.properties` | `server-ip` (empty = all interfaces), `server-port` | `''`, `25565` |

The YAML shape, as generated by this fork:

```yaml
proxies:
  bungee-cord:
    online-mode: true
  proxy-protocol: false
  velocity:
    enabled: false
    online-mode: true
    secret: ''
```

`GlobalConfiguration.Proxies#isProxyOnlineMode()` reports online mode when **any** of these is true:
`server.properties: online-mode`, `settings.bungeecord` together with `bungee-cord.online-mode`, or
`proxies.velocity.enabled` together with `velocity.online-mode`. That check does not reject a
combination; it only answers the question.

The login path fixes the pairing. `ServerLoginPacketListenerImpl#handleHello` only asks Velocity for
the forwarded profile when the backend is **not** authenticating players itself: the branch tests
`server.usesAuthentication()`, and `MinecraftServer#usesAuthentication()` returns the
`server.properties: online-mode` value. With `online-mode=true` the server sends its own encryption
request and never requests the Velocity payload, so a Velocity-backed backend runs with
`server.properties: online-mode=false`; `proxies.velocity.online-mode` then describes the proxy's
authentication to the rest of the server. The proxy's own mode, its `velocity.toml` / `config.yml` and
its forwarding configuration are outside this repository and are not verified here.

### Modern forwarding versus legacy

- **Velocity modern forwarding** is the supported path. The backend enables it with
  `proxies.velocity.enabled: true` and a non-empty secret, from `proxies.velocity.secret` or the
  environment; `VelocityProxy` verifies every forwarded login payload with `HmacSHA256` over the
  shared secret and reads the player's UUID, name and profile properties from it, and it replaces the
  connection address with the one in the payload (`VelocityProxy.readAddress`). If Velocity is
  enabled and the secret is still empty, the
  server logs `Velocity is enabled, but no secret key was specified...` and disables Velocity instead
  of starting an unauthenticated forwarding path. While it is enabled, a direct connection that did
  not go through the proxy is refused with `This server requires you to connect with Velocity.`
- **Legacy BungeeCord forwarding** is `settings.bungeecord: true` in `config/spigot.yml`, with
  `proxies.bungee-cord.online-mode` describing whether that proxy authenticates. The comment in
  `VelocityProxy` states plainly that BungeeCord-style IP forwarding is not secure. Prefer modern
  forwarding; use the legacy path only when the proxy cannot do modern forwarding.
- **`proxies.proxy-protocol`** is declared with a default of `false`, and the connection API exposes
  the HAProxy address (`Player#getHAProxyAddress()`). This source tree contains no reader for that key,
  so whether setting it actually changes the reported client address is **not verified here**. If your
  plugins need the real client IP, test it on a lab box before relying on it.

### The forwarding secret

The secret is a shared credential, not a setting to paste into a ticket, a chat or a screenshot.

- Keep it in `config/paper-global.yml` under `proxies.velocity.secret`, or leave that key empty and
  provide the value through the `PAPER_VELOCITY_SECRET` environment variable. The environment variable
  wins over the file when it is set and non-empty.
- The same value must exist in the proxy's configuration. That file is not part of this repository;
  generate one random value, store it in both places and rotate both sides together.
- Restrict the file to the account that runs the server (owner read/write only on Unix, an ACL on
  Windows) and keep it out of backups, repositories and screen shares. The configuration code does not
  print the secret; do not add it to logs or startup flags, where other local users can read the
  process arguments.
- A wrong secret fails the HMAC check and the player is rejected. An empty secret disables Velocity
  with the error above. Neither failure is silent.

### Ports and firewall

The proxy is the only process that should be reachable from the internet.

- Leave the backend's `server-port` at a value you choose for the private network; the shipped
  default is `25565`, the same as a standalone server.
- Set `server-ip` to the private interface or `127.0.0.1` so the backend does not listen on every
  interface, then allow the backend port only from the proxy host. Never publish the backend port in
  DNS or forward it on the router.
- The proxy's public port is the one clients use. Its bind address, its port and its own firewall
  rules belong to the proxy configuration, not to Tuff.
- `management-server-port` (default `0`, off) opens a separate management endpoint; if you enable it,
  treat it like any other administrative port and do not expose it.

### What the proxy does and does not change

- **Separate JVM.** The proxy's threads, CPU, GC and connection limits do not appear in
  `/tuff threads`, the UTMI ledger or `/tuff scheduler`. A proxy that is out of CPU shows up as
  latency for players, not as a degraded pool on the backend.
- **Regions and ticks are unchanged.** Tuff groups the backend's loaded chunks into regions and ticks
  them exactly as before. The proxy cannot spread players inside a region: two players standing next
  to each other are still in the same region and still load the same tick thread. Player counts per
  region remain the number that matters, which is why the hub preset above is about tick headroom,
  not about compute.
- **One player is still one connection.** The backend's netty loops accept the proxy's connections and
  the player count is the same. Forwarding can carry the client address (modern forwarding does, and
  the backend applies it); without it the backend sees the proxy's address.
- **Join rate limiting moves to the proxy.** `CraftServer#getConnectionThrottle` returns `-1` when
  either `settings.bungeecord` or `proxies.velocity.enabled` is on, so `settings.connection-throttle`
  in `config/bukkit.yml` is bypassed and the proxy owns connection rate limiting.
- **Kairos is unaffected.** It schedules the backend's tick threads; the proxy has no tick loop and no
  fund.
- **The proxy adds a hop.** That is network latency, not tick time, and no `thread-management` key
  changes it.

Outside Tuff's control, and not verified in this repository: the proxy software and its version, its
forwarding mode and secret, its online-mode and compression settings, proxy-side plugins, player
limits and queues, session authentication with Mojang, DNS and TLS termination, DDoS protection, and
any cross-server transfer or queue system. Configure and measure those on the proxy itself.

## Related pages

- [Configuration](configuration.md) - every key under `thread-management`, its default and what it
  actually does.
- [Architecture](architecture.md) - the ledger, the budget model and the invariants.
- [Coverage](coverage.md) - what each printed level proves.
- [Observability](observability.md) - every line of `/tuff threads` and the JFR events.
- [Compute tuning](../compute/tuning.md) - the `async-compute` knobs in detail.
- [Compute configuration](../compute/configuration.md#automatic-sizing) - the worker sizing formula
  and the shipped compute presets.
