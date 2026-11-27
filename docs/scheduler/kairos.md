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
    kairos:
      elastic: true
      max-threads: 4
      keep-alive-seconds: 30
      spawn-rate-per-second: 10
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
- **A stale tick is reported.** The Folia watchdog only sees a tick that already started, so a region
  whose permit never arrives is invisible to it. The supervisor scans the wait queue once per pass and
  warns when a tick's `scheduledStart` is more than two periods (100 ms) old: "starvation" while the
  fund still has room, "saturated" when it is already at its ceiling. Warnings are limited to one per
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

Everything except `threaded-regions.threads` is restart only. The settings are read when the region
scheduler starts; the scheduler object itself is created while `paper-global.yml` is being loaded, so
the values travel through the opt-in gate rather than the constructor.

## What the logs say

```
[Tuff/Kairos] Kairos: elastic fund min 2, max 4 (kairos.max-threads 4), keep-alive 30s, spawn rate 10/s
[Tuff/Kairos] Kairos: fund reached its ceiling of 4 tick thread(s) under load
[Tuff/Kairos] Kairos: fund is back to its floor of 2 tick thread(s)
```

Per thread spawn and retire decisions are `DEBUG`; only the edges (ceiling reached, floor reached)
are `INFO`, so a normal run does not flood the log. A failing supervisor pass is one `WARN` per
minute: the fund keeps the size it has and the ticks keep running. A stale tick is one `WARN` per ten
seconds, and the line names the region, its wait, and whether the fund is at its ceiling.

## `/tuff scheduler`

The command is a `/tuff` subcommand and needs `tuff.command.compute` (operators get it by default). It
reads counters and walks the two tick queues under the schedule lock; it never walks regions and
never waits for a tick. The shape is fixed, the numbers are whatever the server is doing:

```
Tuff scheduler: Kairos (experimental) - status: ok
  fund: min 2, live 3, target 4, max 11; elasticity on, ceiling source: utmi budget (usable cores plus the deducted region threads)
  queue: 1 queued, 2 awaiting, 0 executing; tick wait avg 40.0 ms, max 104.0 ms; overdue 1 (threshold 100 ms, oldest 104 ms)
  supervisor: running; last decision grow (2 -> 3, target 4)
  fund events: spawned 1, retired 0
  degraded: none
```

- `status` is `DEGRADED` as soon as `degraded` is not `none`; the reasons are `NO_HEADROOM` (the
  resolved ceiling is not above the floor, so elasticity can do nothing), `SUPERVISOR_DEAD` (the fund
  is elastic, started, and its control thread is gone) and `QUEUED_OVERDUE` (at least one tick is
  past the 100 ms wait threshold right now).
- `tick wait avg / max` is the age of `scheduledStart` for ticks that are waiting. A tick whose
  deadline is still in the future contributes zero, so an idle queue and a queue of early ticks both
  read `0.0 ms`.
- `last decision` is `grow`, `shrink` or `idle` with the before, after and target of the last pass.
- Under EDF or `WORK_STEALING` the command prints `no fund telemetry` and the configured thread count.
  Those schedulers have no fund, no supervisor and no wait queue this project can read; inventing a
  number for them would be worse than the sentence.

`/tuff threads` shows the same fund condensed into one `scheduler:` line, and under EDF or work
stealing says `no fund telemetry (Kairos only)`.


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
- **Not a faster scheduler.** Order, catch up and clamping are EDF's. Any gain must come from the
  elasticity itself; if a measurement shows none, the feature is expected to be removed rather than
  kept as a curiosity.
- **Experimental.** No soak, no full A/B and no canary yet. Do not enable it on a server whose world
  you cannot restore from a backup.
- **NUMA and affinity are not supported.** Threads are created on demand by the supervisor and
  inherit whatever the OS gives them; there is no pinning and no NUMA node allocation.

## Rollback

Set `scheduler: EDF` and restart. Kairos classes are not loaded and no Kairos thread exists on the
EDF path. Nothing migrates; the world is untouched.
