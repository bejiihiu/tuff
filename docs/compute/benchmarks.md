# Benchmarks

Real JMH runs of the compute engine on one developer machine. The module lives in
`tuff-benchmarks`, depends on `:tuff-compute` only, is not part of `build` or CI, is not published and
is not bundled into the server jar.

> **Two runs, two baselines.** The tables up to *Baseline against `ThreadPoolExecutor`* are the first
> run, made before the idle worker was signalled on submission. The *Execution lanes* section and the
> *Idle latency* table are a second run made after that fix landed. Do not compare an absolute number
> from one section with a number from the other: the machine load was different and the submission
> path itself changed. Comparisons are valid inside one section.

## Machine and toolchain

| | |
| --- | --- |
| CPU | Intel Core i7-13620H, 10 physical cores / 16 logical, max 4.9 GHz, 2.4 GHz base |
| RAM | 16 GB |
| OS | Windows 11 Pro 10.0.26100 |
| JVM | Eclipse Temurin 25.0.2+10 (HotSpot), G1, `-Xms1g -Xmx1g -XX:+AlwaysPreTouch` |
| JMH | 1.37 through `me.champeau.jmh` 0.7.4 |
| Load | pool sizes fixed per run, metrics off unless stated, no other benchmark in flight |
| Second run | 2026-11-26, lane work + audit fixpass |


JMH 1.37 runs on JDK 25 without an ASM override: the plugin resolves `asm:9.0` and produced and ran
every benchmark class in this suite, so the Java 25 class file version is not a problem for this
generator. The only JDK 25 complaint is a `sun.misc.Unsafe::objectFieldOffset` deprecation warning
printed by `jmh-core` itself.

## How to reproduce

```bash
./gradlew :tuff-benchmarks:jmh
```

The task runs the whole suite with 3 warmup and 5 measurement iterations of one second, one fork, and
writes `tuff-benchmarks/build/results/jmh/results.txt` plus `.json`. A subset:

```bash
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 ScalingBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-p workers=1,8 .*Scaling.*"
```

The lane scenarios in this report:

```bash
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 (VirtualLaneBenchmark|PlatformLaneBenchmark)"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 SpillBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 VirtualLaneCeilingBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 IdleLatencyBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 LaneSubmitCostBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 BlockingIoBaselineBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 (VirtualLaneQueuedCancelBenchmark|VirtualLaneRunningCancelBenchmark|VirtualLaneEvictBenchmark)"
```

`--rerun` is needed when the JMH arguments did not change, because Gradle would consider the task up
to date.

## Scenarios

| Class | What it measures |
| --- | --- |
| `SubmitPathBenchmark` | submission path alone: one worker is held, 20 000 tasks are submitted into a deep queue, no execution |
| `SubmitOverheadBenchmark` | submit + execute on one worker, empty task and a ~0.6 us task |
| `ScalingBenchmark` | throughput of 1/2/4/8 workers with four producers and a ~4-6 us task |
| `QueueDepthDispatchBenchmark` | serial dispatch out of a 10 000 task backlog, 4 priorities, 1/4/16 consumers |
| `SelectionStrategyBenchmark` | FIFO vs PRIORITY vs WEIGHTED_FAIR, aging on/off, 10 000 task backlog, 4 consumers |
| `FailurePathBenchmark` | submissions against a full queue with `REJECT` and with `DROP_OLDEST` |
| `QueuedCancellationBenchmark` | cancelling 2 048 queued tasks in one go |
| `RunningCancellationBenchmark` | `cancel()` on an already running task, with a finished handle as the floor |
| `LifecycleBenchmark` | create + start + shutdown + await, with and without metrics, plus a thread leak check |
| `IdleLatencyBenchmark` | round trip of one task on an idle pool at three idle-wait settings |
| `ExecutorBaselineBenchmark` | the same load on a plain `ThreadPoolExecutor` with eight threads |
| `VirtualLaneBenchmark` | LOW work on the virtual lane, IO and CPU bodies, lane limit 8/32/128 |
| `PlatformLaneBenchmark` | the same LOW work on the platform pool, 4/8/16/32 workers |
| `SpillBenchmark` | NORMAL work on a saturated pool, with and without spill to the lane |
| `VirtualLaneCeilingBenchmark` | one wave of `maxConcurrent` tasks, observed peak concurrency |
| `LaneSubmitCostBenchmark` | submission cost against a completely full lane or pool |
| `BlockingIoBaselineBenchmark` | the blocking load on the lane, a fixed pool and a raw VT executor |
| `VirtualLaneQueuedCancelBenchmark` | cancelling 2 048 LOW tasks queued behind a full lane |
| `VirtualLaneRunningCancelBenchmark` | cancelling a LOW task already running on a virtual thread |
| `VirtualLaneEvictBenchmark` | `DROP_OLDEST` eviction on a full per-consumer LOW queue |

Every throughput benchmark keeps the pool busy for the whole measured window and waits for the batch
once, either by joining the last future (FIFO makes it the last to complete) or, for the plain
executor, with `invokeAll`. Nothing in the measured path parks per task, because a parked thread is
woken by the OS and that wake-up is milliseconds on Windows.

## Submission path

One worker held by a spinning task, 20 000 submissions per invocation, queue depth 20 000
(`SubmitPathBenchmark`, 3 warmup / 5 measurement).

| Scenario | ns per accepted submit |
| --- | --- |
| validation + handle + entry + queue insert, metrics off | 219 |

The same benchmark read 0.8 us on a run where the machine was busy with another build. The
submission path is memory-latency bound: the queue insert walks a `ConcurrentSkipListMap` per
consumer and priority, so a co-tenant that evicts caches costs more than the insert itself.

## Submit and execute, one worker

Batch of 8 192 tasks per invocation, one consumer, FIFO (`SubmitOverheadBenchmark`).

| Task body | ops/s | us per task |
| --- | --- | --- |
| empty | 1 244 240 | 0.80 |
| ~0.6 us | 848 789 | 1.18 |

The empty-task figure is the floor for this API shape: submit, dispatch, handle transition, future
completion and the one join at the end of the batch. The difference between the two rows is the body
plus the slightly larger allocation footprint, not extra scheduler work.

## Scaling

Four producers, batch of 2 048 tasks each, ~4-6 us body, FIFO (`ScalingBenchmark`).

| Workers | ops/s | vs 1 worker |
| --- | --- | --- |
| 1 | 145 217 | 1.00x |
| 2 | 309 295 | 2.13x |
| 4 | 621 655 | 4.28x |
| 8 | 1 161 529 | 8.00x |

This run scaled linearly through eight workers, which matches the shape of the load: 8 workers plus 4
producers on 16 logical threads, and a body long enough that the producers are not the bottleneck.
The absolute rate is a best case; the same benchmark read 118 000 ops/s at eight workers in a run
where other builds were active on the machine (see *Noise* below).

## Dispatch out of a deep queue

One worker, 10 000 queued tasks spread over four priorities and 1/4/16 consumers, timed drain
(`QueueDepthDispatchBenchmark`, 3 warmup / 5 measurement).

| Consumers | us per task |
| --- | --- |
| 1 | 0.91 |
| 4 | 0.81 |
| 16 | 1.27 |

Selection is `O(consumers)` per poll: every poll reads the best head of each of the four priority
buckets of every consumer before picking one. With one or four consumers that cost is lost in the
per-task bookkeeping; at sixteen consumers it adds roughly 0.4 us per dispatched task, which is a
measurable share of the serial dispatch budget on a single worker.

## Fairness machinery

Same 10 000 task backlog, four consumers, one worker (`SelectionStrategyBenchmark`).

| Strategy | Aging off, us/task | Aging on, us/task |
| --- | --- | --- |
| FIFO | 0.61 | 0.74 |
| PRIORITY | 0.62 | 0.89 |
| WEIGHTED_FAIR | 0.85 | 0.96 |

Weighted fair scheduling costs about 0.2-0.35 us per task over FIFO at four consumers. Aging costs
nothing measurable: it is recomputed from the head timestamp on every poll, there is no timer and no
re-sorting of the queue. Both figures are close to the run-to-run noise band of this laptop, so treat
them as "a fraction of a microsecond", not as precise constants.

## Failure paths

One worker held, queue exactly at its bound of 256, one submission per op (`FailurePathBenchmark`).

| Policy | ops/s | us per submission |
| --- | --- | --- |
| `REJECT` | 166 439 | 6.0 |
| `DROP_OLDEST` | 145 417 | 6.9 |

A refused submission is not cheap. The engine allocates the task handle and its queue entry before it
knows the queue is full, then discards them and builds a `ComputeRejectedException` with a message;
that is the six microseconds. `DROP_OLDEST` adds the eviction callback and the cancellation of the
evicted task's future, about another microsecond. A setup that rejects a large fraction of its traffic
pays this per refused call, so capacity and overflow policy deserve the same attention as the task
body.

## Cancellation

Queued cancellation is a batch of 2 048 handles in one invocation. Running cancellation is timed
around the single `cancel()` call inside a loop that starts a fresh spinning task each time; the
finished-handle row uses the identical loop as a floor (`QueuedCancellationBenchmark`,
`RunningCancellationBenchmark`).

| Case | Cost |
| --- | --- |
| queued, per handle | 2.70 us |
| running, p50 | 2.4 us |
| running, p99 | 18 us |
| running, max | 242 us |
| finished handle (floor), p50 | 0.10 us |

Queued cancellation is the expensive one: it removes the entry from the priority queue and completes
the future with a `ComputeCancelledException`, message included. Running cancellation only sets the
flag, but `cancel()` first probes the queue and all carry slots to find out where the task is; the
several microseconds are cross-core cache misses on structures the worker touched last, not a lock.
The 0.1 us floor is what the call costs once the handle is already terminal.

## Lifecycle

Two workers, no queued work, create + start + `shutdown(Duration.ZERO)` + `awaitTermination`
(`LifecycleBenchmark`).

| Metrics | us per cycle | leaked scheduler threads |
| --- | --- | --- |
| off | 550 | 0 |
| on | 715 | 0 |

Thirteen cycles per configuration left no worker, janitor, adaptive, shutdown or metrics sampler
thread behind. Enabling metrics adds about 165 us to the cycle, which is the collector and its
sampler thread being created and stopped, not per-task work.

## Idle latency

One empty task, one worker, submit and wait for the result (`IdleLatencyBenchmark`, sample mode,
second run).

| `idle-wait` | p0 | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| 1 us | 1.8 us | 22.7 us | 43.2 us | 681 us | 9.7 ms |
| 50 us | 3.2 us | 23.4 us | 41.8 us | 408 us | 6.9 ms |
| 50 ms (default) | 1.3 us | 22.2 us | 35.8 us | 190 us | 25.8 ms |

Before the fix the same benchmark read p50 112 us, **p90 50.6 ms and p99 54 ms** at the default 50 ms
idle wait. The submission now unparks an idle worker, so the park interval no longer caps the round
trip: p90 fell from 50.6 ms to 35.8 us and all three settings land in the same noise band. The p99 in
the hundreds of microseconds and the occasional multi-millisecond maximum are OS wake-up latency on
a laptop, not a scheduler decision; the earlier sub-50 us settings show the same tail.

The fix is not free. Every accepted submission calls `wakeUp()`, which signals the concurrency gate
and unparks **every** worker. With parked workers that is O(workers) system wake-ups per submission:

| Target lane, 8 parked workers | ns per accepted submit |
| --- | --- |
| virtual lane (`LaneSubmitCostBenchmark`, limit 8) | 14.4 us |
| virtual lane (limit 32) | 14.4 us |
| platform pool | 11.0-12.2 us |

For comparison, the pre-fix submission path with a **busy** worker and no parked threads measured
219 ns (`SubmitPathBenchmark`). The gap is the wake-ups: roughly 1.4 us per parked worker, plus the
pump notify on the virtual lane. That is the price of the latency win and the reason a large pool
with a shallow queue does not scale (see *Platform pool size* below).


## Baseline against `ThreadPoolExecutor`

Eight threads, four producers, batch of 8 192, same bodies and same wait-for-all shape
(`ExecutorBaselineBenchmark`).

| Workload | Compute ops/s | TPE ops/s | Compute / TPE |
| --- | --- | --- | --- |
| empty | 1 752 157 | 6 368 264 | 0.28x |
| ~4-6 us | 1 056 588 | 1 146 464 | 0.92x |

For near-empty tasks the wrapper is the dominant cost: 0.57 us per task against 0.16 us for a plain
executor, a 3.6x difference. For a task of a few microseconds the difference falls to about 8%,
because the body is the same on both sides. The per-task price of the wrapper is therefore roughly
0.4 us: consumer validation, the priority queue insert and remove, the quota slot, the handle state
machine and a `CompletableFuture` subclass per task. That is the number to compare against the value
of priorities, quotas, fair share and metrics, not against a bare `execute(Runnable)`.

## Execution lanes

Second run, after the idle wake-up fix. The virtual lane is measured against the platform lane on the
same pool, and the platform pool is measured against itself at different sizes. The lane throughput
comparisons use four producers, a batch of 256 tasks per producer, FIFO, metrics off, and
`-wi 3 -i 5 -f 1`; `SpillBenchmark` uses 128, `BlockingIoBaselineBenchmark` 512, and
`LaneSubmitCostBenchmark` 20 000.

Two fixture details matter for reproducing these numbers:

- The worker limit is pinned with `maxThreads == threads`. With `maxThreads: -1` the pool ceiling is
  the processor budget, so a fixed `threads: 32` is silently clamped to 16 on this machine and the
  benchmark would measure the clamp instead of the setting.
- The IO body is `Thread.sleep(1 ms)`, not `LockSupport.parkNanos`. The pool unparks every worker on
  a submission, and a leftover unpark permit makes the next `parkNanos` return immediately: a park
  body does not block at all on the platform lane while it does block on a virtual thread. A timed
  park also has a floor of about 1.5 ms on this OS. The sleep measures 1.87 ms on a platform thread
  and 2.13 ms on a virtual thread, which is the block both lanes actually pay.

### LOW blocking work: throughput

8 worker pool, LOW routed to the lane, 1 ms blocking body.

| Lane limit | ops/s | peak task bodies |
| --- | --- | --- |
| 8 | 3 620 | 8 |
| 32 | 3 605 | 8 |
| 128 | 3 706 | 8 |

The same load on the platform pool, LOW not routed to a lane:

| Workers | ops/s |
| --- | --- |
| 4 | 1 911 |
| 8 | 3 957 |
| 16 | 7 667 |
| 32 | 14 460 |

The lane is flat at its 8 worker pool and matches the platform pool of 8 within 9%. Raising
`maxConcurrent` above 8 changes nothing, and the reason is the quota model, not the lane: a
consumer's concurrency limit is resolved against the pool wide limit, so one consumer can never run
more than `workers` tasks across both lanes. `maxConcurrent` is an upper bound on the lane, not a way
to exceed the pool. The platform pool scales linearly with workers for blocking work because each
worker is a real thread that can block independently.

### LOW blocking work: latency

One submit and join per op, one benchmark thread, idle lane (`VirtualLaneLatencyBenchmark`).

| Body | Limit | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| empty | 8 | 89 us | 200 us | 876 us | 26.5 ms |
| empty | 32 | 113 us | 205 us | 544 us | 25.9 ms |
| empty | 128 | 82 us | 187 us | 357 us | 25.6 ms |
| 1 ms sleep | 8 | 2.09 ms | 3.09 ms | 4.29 ms | 6.6 ms |
| 1 ms sleep | 32 | 2.07 ms | 3.11 ms | 5.15 ms | 9.0 ms |
| 1 ms sleep | 128 | 2.04 ms | 3.05 ms | 3.99 ms | 5.1 ms |

An empty task costs about 0.1-0.2 ms on the lane, against ~20 us on the platform pool: launching and
finishing a virtual thread is the extra. The 25 ms maximum is the pump's re-evaluation tick, the one
path that still does not have a direct wake-up. For a task that actually blocks, the 2 ms sleep
dominates and the limit does not matter.

### LOW CPU-bound work

`mix(4096)`, the same body as `ScalingBenchmark`, 8 worker pool.

| Lane | Limit | ops/s |
| --- | --- | --- |
| virtual | 8 | 27 641 |
| virtual | 32 | 29 564 |
| virtual | 128 | 27 912 |
| platform | 8 | 69 327 |

Virtual threads add no CPU parallelism, and here they cost 2.3x: every task pays a virtual thread
start plus the pump hand-off, and the platform workers sit idle. The lane is for blocking work; LOW
CPU work should not be routed there. The platform CPU numbers in this run are about 6x below the
first-run `ScalingBenchmark` values because the machine was slower during the second run; compare
ratios inside this section, not absolute rates across sections.

### Spill

8 workers, NORMAL 1 ms blocking work, lane limit 64, `spill-wait` 5 ms, `spill-queue-depth` 32,
four producers (`SpillBenchmark`).

| Spill | ops/s | tasks spilled | queue wait p50 | p90 | p99 |
| --- | --- | --- | --- | --- | --- |
| on | 3 900 | 1-3 per second (~0.05%) | 107 ms | 125 ms | 139-155 ms |
| off | 3 632 | 0 | 104-112 ms | 118-137 ms | 126-172 ms |

Spill fires, but only for a few tasks per second: at saturation all 8 pool slots are held by running
tasks, so the same pool wide quota that caps the lane refuses the pump's claim. The pump then carries
the entry and retries on its 25 ms tick, which is why a spilled task can wait tens of milliseconds
longer than one that stayed in the queue. Throughput is unchanged within noise. Spill as specified
cannot relieve a saturated single-consumer pool.

### Lane ceiling

One wave of exactly `maxConcurrent` tasks, 8 worker pool, 20 ms block
(`VirtualLaneCeilingBenchmark`).

| Lane limit | Tasks in the wave | Observed peak bodies | Platform threads |
| --- | --- | --- | --- |
| 8 | 8 | 8 | 12 |
| 32 | 32 | 8 | 12 |
| 128 | 128 | 8 | 12 |

The observed concurrency is `min(maxConcurrent, workers)`, and the JVM platform thread count does not
move: virtual tasks do not add OS threads, so there is no thread explosion even at limit 128. A run
with a 32 worker pool in the same session peaked at 32 for limits 32 and 128, which is the same rule.
Above the pool size, a bigger `maxConcurrent` only makes the lane's queue longer.

### Cancellation and eviction on the lane

| Case | Cost |
| --- | --- |
| queued cancellation, per handle (2 048 at once) | 1.08 us |
| running cancellation, p50 | 1.5 us |
| running cancellation, p90 | 2.2 us |
| running cancellation, p99 | 4.0 us |
| running cancellation, max | 0.88 ms |
| `DROP_OLDEST` submit + evict, full 256 queue | 189 us per op (5 303 ops/s) |

Queued and running cancellation are cheaper than on the platform (2.70 us and 2.4 us p50 in the
first run): the lane moves an entry out of the queue or flips a flag, and there is no worker to race.
Eviction is expensive, about 30x the platform `REJECT` path: every submit has to evict, cancel the
oldest future (exception construction included) and wake the pump, all against a queue that is
already full.

### Blocking baseline: lane vs fixed pool vs raw virtual threads

8 workers, 1 ms blocking body, four producers, batch 512 (`BlockingIoBaselineBenchmark`).

| Executor | ops/s |
| --- | --- |
| virtual lane, limit 64 | 4 017 |
| `ThreadPoolExecutor`, 8 threads | 3 759 |
| `Executors.newThreadPerTaskExecutor` | 356 025 |

This is the honest shape of the feature. A bare virtual thread executor turns the same blocking work
into about 90x the throughput because every blocked task simply gets another virtual thread. The lane
does not, because the per-consumer quota is the pool size: it is a bounded lane for LOW work, not an
unbounded thread-per-task escape hatch.

### Platform pool size and the wake-up cost

Fixed platform pool, no lane, four producers, batch 256 (`PlatformLaneBenchmark`).

| Workers | CPU ops/s | Blocking ops/s |
| --- | --- | --- |
| 4 | 71 159 | 1 911 |
| 8 | 69 327 | 3 957 |
| 16 | 23 172 | 7 667 |
| 32 | 5 641 | 14 460 |

Blocking work scales with workers as expected. CPU work peaks at 8 and collapses above it: 12x lower
at 32 workers. The same shape appeared in a second run with the same fixtures. The suspected cause is
the wake-all on submission: with a shallow queue the workers are parked most of the time, and each
submission unparks all of them, so the per-submission cost grows with the pool while the useful work
does not. Oversubscription of 16 logical cores by 32 workers plus 4 producers is a second candidate.
This run does not isolate the two; a quiet machine and a deep-queue variant are needed before calling
it a defect.

### When the lane helps, and when it hurts

- **Helps.** Keeping LOW blocking work off the platform workers: a blocked virtual task does not hold
  a pool slot, so NORMAL work keeps its workers. That is the isolation the lane was built for, and it
  is real even when throughput is the same. Long blocking calls (network, database, file) are the
  intended load.
- **Neutral.** Throughput for blocking LOW work at the same pool size: 3 620 against 3 957 ops/s at
  8 workers, inside noise. If the goal is more blocking throughput, raise the pool, not
  `maxConcurrent`.
- **Hurts.** CPU-bound LOW work (2.3x slower), very short blocks (a timed park on a virtual thread has
  a floor of roughly 1 ms on this JDK/OS, so a "50 us" IO task is not 50 us), and an idle pool
  (0.1-0.2 ms lane round trip against ~20 us on the platform).
- **Cannot do what it looks like it can do.** `maxConcurrent` above the worker count is inert for one
  consumer, and spill cannot relieve a saturated pool because the quota is already fully held. Both
  are quota-model decisions, not lane bugs, but an operator reading `max-concurrent: 64` will expect
  otherwise.

## Observations and suspected defects

The first five came out of the first run, before the wake-up fix; items 6-10 are from the second run.
No engine code was changed for this work.

1. **No submit-to-wakeup (fixed after the first run).** An idle worker used to be woken only by the
   expiry of `idle-wait-millis`, which capped an idle pool at p90 50.6 ms. The submission now unparks
   the pool and the same benchmark reads p90 36 us. The cost of the fix is item 6.
2. **Selection is `O(consumers)` per poll.** 16 consumers cost ~0.4 us more per dispatched task than
   one consumer at a 10 000 task backlog. Fine with the three example consumers, worth watching if
   many plugins register their own.
3. **A refused submission costs ~6 us** because the handle and entry are allocated before the queue
   refuses. A rejection-heavy configuration burns a core at ~166 000 refusals per second.
4. **Running cancellation costs microseconds, not nanoseconds**, because `cancel()` probes the queue
   and all carry slots before it checks the state. Still bounded and lock-free, but not a bare CAS.
5. **Hot-path sensitivity to co-tenants.** The same binary measured 1.16 M ops/s and 118 k ops/s at
   eight workers on different runs, while total CPU utilisation stayed below 25%. The hot path is
   memory-latency bound; on a shared host it degrades well before the CPU is saturated.
6. **The wake-up is O(parked workers) per submission.** `wakeUp()` signals the gate and unparks every
   worker, so a submission against an idle 8 worker pool costs 11-12 us instead of the 219 ns a busy
   pool costs (`LaneSubmitCostBenchmark`). A wake-one strategy would keep the latency win and remove
   the per-worker price.
7. **A single consumer cannot exceed the pool size, on either lane.** The quota controller resolves
   the consumer limit against the pool wide limit, so `maxConcurrent: 64` with 8 workers still runs 8
   LOW tasks at a time and the extra 56 slots are inert. It is why the lane's blocking throughput is
   flat across 8/32/128 and why the ceiling is `min(maxConcurrent, workers)`.
8. **Spill cannot relieve a saturated single-consumer pool.** At saturation the quota is fully held,
   so the pump's claim loses and the entry sits in the carry slot until the next 25 ms tick. Measured
   spill was 2-7 tasks per second (~0.1%) with no throughput change and a longer tail.
9. **LOW CPU work on the lane is 2.3x slower than on the platform pool** (29 564 against 69 327
   ops/s at 8 workers). Expected in kind, useful to have a number: the lane is not a general worker
   replacement.
10. **A timed park is not a usable block inside the scheduler's own workers.** `wakeUp()` leaves an
    unpark permit on every worker, and the next `LockSupport.parkNanos` in a task body returns
    immediately: a park-based "IO" task does not block on the platform lane at all. Timed parks on a
    virtual thread also have a ~1 ms floor on this OS. Benchmarks must use `Thread.sleep`, and the
    same applies to any real task body that expects a park to wait.

## Limitations

- One machine, one run per scenario, one fork. Throughput figures are best-case numbers from short
  runs on an otherwise idle laptop; the scaling series and the baseline comparison are internally
  consistent within one run.
- While another Gradle build or the Windows search indexer was active, the same throughput benchmarks
  read 3-10x lower. The serial measurements (dispatch, cancellation, failure paths, lifecycle) were
  stable across all runs; the multi-threaded throughput figures were not. Rerun on a quiet machine
  before making decisions that need absolute values. The second run was made while other work was
  active in the same workspace and engine-side edits were still landing: its CPU rates are several
  times below the first run, so only ratios inside the second run are used, and the numbers should be
  re-run once the fixpass is quiet and merged.
- The lane numbers use a 1 ms `Thread.sleep` body. Windows rounds it to 1.87 ms on a platform thread
  and 2.13 ms on a virtual thread, so "ops/s" is really "blocks of ~2 ms per second". The two lanes
  pay the same block, which is what makes the comparison fair, but no absolute rate here transfers to
  a real network call.
- `maxConcurrent` above the worker count, and spill under a single consumer, were measured but not
  isolated from the quota model. A multi-consumer run would separate the two.
- The CPU collapse above 8 workers is the least certain number in this report: two runs agree on the
  shape, but oversubscription and the wake-all are not separated, and the machine was not quiet.
- The dispatch benchmarks spin the producer on purpose and time a drain of a fixed backlog; they are
  service-time measurements, not latency distributions under a fixed arrival rate.
- The under-load queue wait (p50 5.5 ms, p99 12.3 ms at a ~8 000 task burst) drifts with machine load
  and is a burst measurement, not a steady-state arrival process.
- Not measured: Folia region threads competing with the pool, GC behaviour under a sustained server
  load, JFR overhead, and metrics-on cost on the task path (only the lifecycle case covers metrics).
- The benchmark module is a tool, not a supported artifact: it has no publication, no CI job and no
  stability promise.
