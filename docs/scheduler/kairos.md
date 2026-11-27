# Kairos scheduler

Status: **experimental**, opt in only. Kairos is Tuff's own implementation of the region tick
scheduler: the same earliest-deadline-first queue Folia uses, with a fund of tick threads that can
grow while regions wait and shrink back when they are calm. It is selected per server and never
turned on implicitly.

```yaml
# config/paper-global.yml
threaded-regions:
  scheduler: kairos   # env: also accepts KAIROS
  threads: 2          # the floor of the fund
```

```yaml
# config/tuff.yml
thread-management:
  region-execution:
    kairos-unstable: true   # required, Kairos refuses to start without it
    metrics:
      jfr: true             # per-tick tuff.scheduler.Tick, every scheduler
    kairos:
      elastic: true
      max-threads: 4
      keep-alive-seconds: 30
      spawn-rate-per-second: 10
      catch-up:             # off by default: 1 body, whole backlog written off
        cap-ticks: 1
        budget-millis: 50
      steal:
        threshold-millis: 3
      affinity:
        local-queue-size: 0 # off by default: one shared queue
      lease:
        timeout-seconds: 0  # accounting only, no warning
      idle-work:            # off by default: bounded housekeeping while a runner waits
        enabled: false
        mid-tick: false
        utmi-jobs: false
        slice-millis: 1
        max-added-lag-millis: 5
        max-share-percent: 25
```

## What it does

- **Order is Folia's.** The queue, the comparator (`TimeUtil.compareTimes` by `scheduledStart`) and
  the id tie break are ported one to one from `EDFSchedulerThreadPool` (leafpile 1.3.3). Catch up and
  clamping stay in the region handle, so a Kairos tick behaves like an EDF tick.
- **The floor always exists.** `threads` tick threads are created at start and are never retired.
  With `elastic: false` the fund is exactly that fixed set: no supervisor thread, no growth, no
  retire. This is the mode that was load tested first.
- **Growth is demand driven.** One daemon, `Tuff Kairos Supervisor`, wakes once a second and asks the
  scheduler how many permits the regions actually wanted since the last pass. A tick only reaches
  the wait queue when every permit is taken, so the peak of `live + queued` is a real shortfall and
  not a lucky sample of a queue that drains in microseconds. The fund adds at most
  `spawn-rate-per-second` threads per second, up to `max-threads`.
- **The supervisor is an accounted thread.** When the unified thread layer is running, the supervisor
  is created through its factory and registered under the `service` pool (L2), the same pool the
  thread refresher lives in. It therefore shows up in `/tuff threads` and is not counted as an unknown
  thread. Without the thread layer the scheduler still builds a plain daemon and says so in the
  startup log; losing the supervisor would silently freeze the fund.
- **The ceiling is one number from one place.** With `max-threads: -1` the scheduler asks the thread
  layer's resource controller for its budget: `usableCores()` plus the region threads the ledger
  already deducted for this lane. The controller has already removed the deducted lanes and the
  reserve, so nothing here subtracts the reserve a second time. Only when the thread layer is off does
  the documented `floor(cores * 0.8)` rule apply. An explicit `max-threads` always wins.
- **A late region can run its missing ticks** (`catch-up`). Off by default: the stock behaviour runs
  one body and writes off the whole backlog, so a region that missed five periods loses four ticks.
  With `cap-ticks` above 1 and a budget above 0, a region that is at least one whole period behind
  gets bodies that each account for a single period, so the missed ticks are run instead of dropped.
  After at most `cap-ticks` bodies of one burst, or when `budget-millis` is spent, the next body
  settles the remaining backlog exactly like the stock body would; the region then waits its normal
  period, so catching up is paced and never becomes a tick storm. Every body still goes through its
  own acquisition, so the one-permit-per-runner invariant, the cancellation check and the watchdog see
  exactly what they see on the stock path, and a cancelled region never gets a catch-up body.
- **An idle thread may do a little housekeeping** (`idle-work`). Off by default, and it has two
  independent sources. A runner that already holds a region's permit and is waiting for the deadline
  may drain that region's intermediate tasks through the handle's own `hasTasks()` / `runTasks` path,
  one bounded pass per wait, so the work happens between two ticks instead of at the start of the
  next one. A runner with no permit at all may take one job from the thread layer's idle-safe source
  (the only producer shipped is that layer's periodic metrics pass, offered once every five seconds
  while the refresher keeps the other passes). A tick always wins: the pass checks the deadline before
  the slice and the job's cancellation flag reads the runner state, so a granted tick stops both at
  the next check. If idle work adds more than `max-added-lag-millis` to a body it disables itself
  until the region is stable again, and `max-share-percent` caps the work per second. With
  `enabled: false` neither source is touched: no handle is asked about tasks, no source is polled.
- **A tick tends to come back to the same thread** (`affinity`). With `local-queue-size` above 0 a
  runner keeps the regions it just ticked in a bounded private queue instead of the one shared queue,
  so their thread locals and caches stay warm. The shared queue is still consulted, and an earlier
  tick there wins; the private queue never holds a permit, only ticks. A tick is kept only while every
  permit is taken, so a free permit never parks next to a private queue. Another runner may take a
  kept tick only when it is already past its deadline by `steal-threshold-millis`, and a steal moves
  the tick, never the permit. A hint dies with its runner: when a runner retires its queue goes back
  to the shared one, and the slot that a later spawn reuses starts empty.
- **A slow permit is measured and reported** (`lease`). Two waits are counted apart on every grant:
  the distance between the tick deadline and the moment a permit existed, which is the shortage
  signal, and the extra time after both the permit and the deadline existed, which is contention.
  With `timeout-seconds` above 0 the supervisor warns when a wait exceeded it, at most once per ten
  seconds, and emits `tuff.scheduler.LeaseWait`; the suppressed passes are counted and named in the
  next line. The measurement itself is a few counters on the runner thread: no tick ever waits on a
  lock or on the log.
- **A stale tick is reported.** The Folia watchdog only sees a tick that already started, so a region
  whose permit never arrives is invisible to it. The supervisor scans the wait queue - and every
  private queue - once per pass and warns when a tick's `scheduledStart` is more than two observed
  periods old: "starvation" while the fund still has room, "saturated" when it is already at its
  ceiling. The period is measured from the distance between two bodies of the same region, so a
  server that manages 12 TPS uses 166 ms instead of the fixed 100 ms that made the detector blind
  below 20 TPS; a healthy 20 TPS server keeps exactly the old number. Warnings are limited to one per
  ten seconds, suppressed passes are counted and reported in the next line, and the same moment is
  emitted as the `tuff.scheduler.QueuedOverdue` JFR event. A fixed fund has no supervisor and
  therefore no such warning; `/tuff scheduler` still counts the overdue ticks for it.
- **Shrink only ever touches a parked thread.** A runner that holds a permit (waiting for its
  deadline or executing a tick) is never retired, so `markNotTicking` always runs. A thread is
  retired when it has been idle for at least the surplus grace (1 s) while the demand is lower, or
  for `keep-alive-seconds` while the fund is above the floor. Retires are paced to at most one per
  second, which makes a sawtooth impossible.
- **A live thread is never recreated.** Growth appends a slot; shrink asks an idle runner to leave;
  a dead slot is reused by the next spawn. There is no `halt()` of a live runner, no migration and no
  `setThreadAllocation`-style rebuild. `getAliveThreads()` reflects the real fund at any moment.
- **Shutdown is unchanged.** `halt()` sets the flag and unparks the supervisor and every runner;
  `join(60s)` waits for the supervisor and for every thread that was ever created, including retired
  ones. A spawn that loses the race with `halt()` exits at its first loop check.
- **`threads` is live.** While the fund is elastic, `/tuff reload`'s `threaded-regions.threads` moves
  the floor: the supervisor grows towards it, and a lowered floor is drained one retire per second.
  While the fund is fixed the value is still restart only and logs a warning instead of silently
  doing nothing.

## Configuration

| Key | Where | Default | Meaning |
|---|---|---|---|
| `threaded-regions.scheduler` | paper-global | `EDF` | `kairos` selects this scheduler; needs the opt in |
| `threaded-regions.threads` | paper-global | `-1` | floor of the fund; `-1` keeps Folia's own formula |
| `region-execution.kairos-unstable` | tuff.yml | `false` | explicit opt in; the scheduler refuses to start without it |
| `region-execution.kairos.elastic` | tuff.yml | `false` | allow growth and retire; off is the fixed fund |
| `region-execution.kairos.max-threads` | tuff.yml | `-1` | ceiling; `-1` means the thread layer budget when it runs, otherwise `floor(cores * 0.8)` |
| `region-execution.kairos.keep-alive-seconds` | tuff.yml | `30` | how long a thread must sit idle before it may be retired |
| `region-execution.kairos.spawn-rate-per-second` | tuff.yml | `10` | how many new tick threads may be started per second |
| `region-execution.kairos.catch-up.cap-ticks` | tuff.yml | `1` | tick bodies one late region may run back to back; `1` is the stock skip, needs a budget above 0 |
| `region-execution.kairos.catch-up.budget-millis` | tuff.yml | `50` | wall clock one catch-up burst may spend; `0` disables catch-up |
| `region-execution.kairos.steal.threshold-millis` | tuff.yml | `3` | how late a kept tick must be before another runner may take it |
| `region-execution.kairos.affinity.local-queue-size` | tuff.yml | `0` | ticks a runner may keep for itself; `0` keeps the single shared queue |
| `region-execution.kairos.lease.timeout-seconds` | tuff.yml | `0` | permit wait that triggers a warning; `0` keeps the measurement only |
| `region-execution.metrics.jfr` | tuff.yml | `false` | record every executed tick body as `tuff.scheduler.Tick`, whatever scheduler runs it; off costs one volatile read per tick |
| `region-execution.kairos.metrics.jfr` | tuff.yml | `false` | record every supervisor pass as `tuff.scheduler.Fund`; the per-tick event has its own switch above |
| `region-execution.kairos.idle-work.enabled` | tuff.yml | `false` | master switch of the idle path |
| `region-execution.kairos.idle-work.mid-tick` | tuff.yml | `false` | let a waiting runner drain its region's intermediate tasks |
| `region-execution.kairos.idle-work.utmi-jobs` | tuff.yml | `false` | let a parked runner take jobs from the thread layer's idle-safe source |
| `region-execution.kairos.idle-work.slice-millis` | tuff.yml | `1` | most wall clock one idle slice may take, `1` to `100` |
| `region-execution.kairos.idle-work.max-added-lag-millis` | tuff.yml | `5` | lateness a tick may suffer from idle work before it disables itself |
| `region-execution.kairos.idle-work.max-share-percent` | tuff.yml | `25` | most of a one second window idle work may spend |

Everything except `threaded-regions.threads` is restart only. The settings are read when the region
scheduler starts; the scheduler object itself is created while `paper-global.yml` is being loaded, so
the values travel through the opt-in gate rather than the constructor. A value outside its range is
reported in the startup log and replaced by the stock behaviour instead of crashing the start.

## What the logs say

```
[Tuff/Kairos] Kairos: elastic fund min 2, max 4 (kairos.max-threads 4), keep-alive 30s, spawn rate 10/s; catch-up 3 tick(s)/50 ms, affinity queue 4 (steal 3 ms), lease timeout 100 ms
[Tuff/Kairos] Kairos: fund reached its ceiling of 4 tick thread(s) under load
[Tuff/Kairos] Kairos: fund is back to its floor of 2 tick thread(s)
[Tuff/Kairos] Kairos: lease wait above the 100 ms timeout: 3 of 812 grant(s) timed out, longest queued 240 ms, longest parked 41 ms, fund 4/4; the tick body is never blocked
```

Per thread spawn and retire decisions are `DEBUG`; only the edges (ceiling reached, floor reached,
the tick knobs at start) are `INFO`, so a normal run does not flood the log. A failing supervisor pass
is one `WARN` per minute: the fund keeps the size it has and the ticks keep running. A stale tick and
a slow permit are one `WARN` per ten seconds each, and the lines name the region, its wait, and
whether the fund is at its ceiling.

## `/tuff scheduler`

The command is a `/tuff` subcommand and needs `tuff.command.compute` (operators get it by default). It
reads counters and walks the two tick queues under the schedule lock; it never walks regions and
never waits for a tick. The shape is fixed, the numbers are whatever the server is doing:

```
Tuff scheduler: Kairos (experimental) - status: ok
  fund: min 2, live 3, target 4, max 11; elasticity on, ceiling source: utmi budget (usable cores plus the deducted region threads)
  queue: 1 queued, 2 awaiting, 0 executing; tick wait avg 40.0 ms, max 104.0 ms; overdue 1 (threshold 166 ms, oldest 180 ms)
  supervisor: running; last decision grow (2 -> 3, target 4)
  fund events: spawned 1, retired 0
  ticks: affinity hits 214, steals 3, catch-up 7 burst(s) / 12 tick(s)
  idle work: off (idle-work.enabled is false)
  leases: 812 measured, wait max queued 24 ms / parked 4 ms, 3 over the 100 ms timeout
  degraded: none
```

- `status` is `DEGRADED` as soon as `degraded` is not `none`; the reasons are `NO_HEADROOM` (the
  resolved ceiling is not above the floor, so elasticity can do nothing), `SUPERVISOR_DEAD` (the fund
  is elastic, started, and its control thread is gone) and `QUEUED_OVERDUE` (at least one tick is
  past the wait threshold right now).
- `tick wait avg / max` is the age of `scheduledStart` for ticks that are waiting. A tick whose
  deadline is still in the future contributes zero, so an idle queue and a queue of early ticks both
  read `0.0 ms`.
- `overdue` counts waiting ticks in the shared queue **and** in runner backlogs, so affinity cannot
  hide a tick from the detector. `threshold` is two observed periods with a 100 ms floor: `100 ms` on
  a healthy 20 TPS server, `166 ms` at 12 TPS.
- `ticks` is the affinity counter pair and the catch-up counters: `affinity hits` are ticks handed
  back to the runner that ran them last, `steals` are ticks another runner took because they were
  late, and `catch-up bursts / tick(s)` are the bursts started and the extra bodies they ran.
- `leases` is the permit accounting: grants measured, the worst wait before a permit existed, the
  worst wait after it existed, and the timeouts when a timeout is configured.
- `idle work` names the sources and what they did: jobs and mid-tick passes taken, preemptions, wall
  clock spent, its share of measured idle time, the worst lateness a body suffered because of it, and
  whether the self-protection turned it off. A path that is off by configuration says so and reports
  no numbers rather than zeros that look like a measurement.
- `last decision` is `grow`, `shrink` or `idle` with the before, after and target of the last pass.
- Under EDF or `WORK_STEALING` the command prints `no fund telemetry` and the configured thread count.
  Those schedulers have no fund, no supervisor and no wait queue this project can read; inventing a
  number for them would be worse than the sentence.

`/tuff threads` shows the same fund condensed into one `scheduler:` line, and under EDF or work
stealing says `no fund telemetry (Kairos only)`.

## Recording

Per-tick recording belongs to the region handle, not to a scheduler. With
`thread-management.region-execution.metrics.jfr: true` every executed body writes one
`tuff.scheduler.Tick`, whether EDF, `WORK_STEALING` or Kairos ran it. The switch is read before the
event is constructed, so off means one volatile read and a `null` check per body and nothing else. A
recording still has to enable the event, so the switch arms the code and the recording decides what is
kept.

The Kairos fund has a second switch, `thread-management.region-execution.kairos.metrics.jfr: true`,
which writes one `tuff.scheduler.Fund` per supervisor pass. The two switches are independent on
purpose: the per-tick row is not a Kairos feature, and the fund row only exists while Kairos runs.

| Event | Armed by | When | Fields |
|---|---|---|---|
| `tuff.scheduler.Tick` | `region-execution.metrics.jfr` | once per executed tick body, every scheduler | `regionId`, `tickId`, `origin`, `scheduledStartNanos`, `startNanos`, `endNanos`, `missedPeriods`, `catchUpTicks`, `skipped`, `executedNanos` |
| `tuff.scheduler.Fund` | `region-execution.kairos.metrics.jfr` | once per supervisor pass | `minThreads`, `liveThreads`, `targetThreads`, `maxThreads`, `queuedTicks`, `awaitingTicks`, `executingTicks`, `overdueTicks`, `spawned`, `retired` |

`tuff.scheduler.Tick` exists for one reason: a per-region TPS average cannot tell a missed deadline
from a slow tick. `scheduledStartNanos` is the deadline the body was actually measured against, read
before the handle advanced the schedule, so `startNanos - scheduledStartNanos` is the exact lag and
nothing has to be reconstructed. `origin` is `EDF`, `WORK_STEALING` or `KAIROS`. `missedPeriods` is the
handle's periods ahead at the body start (one is on time), `skipped` is how many of those periods the
body wrote off instead of running, and `catchUpTicks` is `1` for a body that ran to recover a period. A
stand can therefore compute a real hard miss - `lag >= period` or `missedPeriods > 1` - and a real skip
count, instead of using a five second sample as a proxy.

`tuff.scheduler.Fund` is the other half of the explanation: one row per pass with the fund's size, what
the queues held, how many ticks were already past the threshold, and the lifetime spawn and retire
counters. A hard miss next to `liveThreads == maxThreads` and `queuedTicks > 0` is a missing permit; the
same miss next to free headroom is a slow tick.


## Risks and limits

- **Oversubscription is bounded by the ledger, not by taste.** With the thread layer running, the
  ceiling is the controller's usable cores plus the region threads it already deducted for this lane;
  that number is already net of chunk, IO and the reserve, and the reserve is not removed twice. With
  the thread layer off the fallback is the documented 80% rule, which does not subtract chunk and IO,
  so `max-threads` remains the honest knob there. On a machine where the budget gives no headroom the
  fund stays at its floor and says `NO_HEADROOM`; that is a configuration fact, not a crash.
- **These are OS threads.** Every extra tick thread is a parked platform thread when idle. The fund
  exists to remove queueing, not to keep the process small.
- **Shutdown is the gate.** A retired-but-alive thread that is not joined would let a save race a
  tick, which is the one way this class can corrupt a world. Retired threads stay in the join list
  and `join` re-reads that list until it stops growing, so the guarantee holds even against a spawn
  that races with `halt()`.
- **A stale tick is detected, not fixed.** The warning proves that permits, not CPU scarcity, are the
  reason a region is not moving; it cannot create capacity that is not there. The honest fixes are
  fewer regions or a larger budget, and admission control is not in this prototype.
- **Affinity trades latency for locality.** A late tick that another runner could have taken waits for
  its owner until it is past the steal threshold, and an owner that frees up before that moment runs
  it itself. The wait is bounded by the owner's current body, and the private queue is only used while
  every permit is busy, so the fund never parks next to work it could do. Turn
  `affinity.local-queue-size` back to 0 to get the single shared queue with no such trade.
- **Catch-up changes game semantics, so it is opt in.** Running a missed tick means the world ticked
  once more than a skip would have allowed, with the same consequences for anything that counts
  ticks. That is why the cap exists, why the budget is required for it to engage at all, and why the
  default is exactly Folia's skip. Measure it on your own profile before enabling it.
- **Idle work is bounded by its own measurement, not by the author's promise.** A slice is a wall
  clock bound; a unit that ignores it is exactly what `max-added-lag-millis` exists for. The path
  disables itself when a body that followed idle work starts too late and re-arms after five seconds
  without another violation, so a bad unit costs one incident, not a permanent regression. Two limits
  remain honest: a single long intermediate task cannot be preempted inside `runTasks` (the
  work-stealing scheduler has the same property), and the added lag is attributed only when the body
  starts within 100 ms of the last slice, so a violation near that boundary may go unblamed rather
  than blaming an unrelated slow tick. Read [idle-safe work](../utmi/idle-work.md) before writing a
  producer of your own.
- **Not a faster scheduler.** Order and clamping are EDF's. Any gain must come from the elasticity
  and from the optional tick policies above; if a measurement shows none, the feature is expected to
  be removed rather than kept as a curiosity.
- **Experimental.** No soak, no full A/B and no canary yet. Do not enable it on a server whose world
  you cannot restore from a backup.
- **NUMA and affinity are not supported.** Threads are created on demand by the supervisor and
  inherit whatever the OS gives them; there is no pinning and no NUMA node allocation.

## Rollback

Set `scheduler: EDF` and restart. Kairos classes are not loaded and no Kairos thread exists on the
EDF path. If you only want the stock tick behaviour back while staying on Kairos, set
`catch-up.cap-ticks: 1`, `affinity.local-queue-size: 0` and `lease.timeout-seconds: 0` and restart:
those three defaults are the ported Folia path, verified by the ordering parity test. Idle work has
its own switch: `idle-work.enabled: false` (or a restart while it is already false) is the whole
rollback, and it leaves no job, pass or guard behind because the path is checked before the handle or
the source is touched. Nothing migrates; the world is untouched.
