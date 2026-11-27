# Benchmarks

Real JMH runs of the compute engine on one developer machine. The module lives in
`tuff-benchmarks`, depends on `:tuff-compute` only, is not part of `build` or CI, is not published and
is not bundled into the server jar.

This page covers in-process JMH measurements only. Process-level evidence against a real server jar -
the scheduler comparison, its artifacts and its statistics - is documented in
[`docs/bench/`](../bench/README.md).

> **One run, current code.** Every table below was measured on the code that ships the wake-one
> submission signal, the consumer quota resolved against platform workers plus virtual-lane permits,
> and the deterministic lane accounting. The earlier "pre-fix" tables are gone; where a change is the
> point, the old value is quoted in the surrounding text.
>
> **The machine was power-throttled during this run.** CPU-bound throughput collapses inside a single
> fork: the first two warmup seconds read 218 k ops/s at one worker and 1.04 M at eight, the third
> warmup and every measurement second read 58 k and 114 k. A single worker loses 3.7x with no
> concurrency involved, so the ceiling is the laptop's sustained power limit, not the scheduler. The
> serial rows (submission, dispatch, failure, cancellation, lifecycle), the idle-latency
> distributions, and every blocking-`Thread.sleep` lane row are unaffected and are the usable part of
> this report. The CPU-bound rate tables are reported as measured and must not be compared with the
> earlier run's absolute values.

## Machine and toolchain

| | |
| --- | --- |
| CPU | Intel Core i7-13620H, 10 physical cores / 16 logical, max 4.9 GHz, 2.4 GHz base |
| RAM | 16 GB |
| OS | Windows 11 Pro 10.0.26100 |
| JVM | Eclipse Temurin 25.0.2+10 (HotSpot), G1, `-Xms1g -Xmx1g -XX:+AlwaysPreTouch` (`-Xmx2G` for the spill scenario) |
| JMH | 1.37 through `me.champeau.jmh` 0.7.4 |
| Load | pool sizes fixed per run, metrics off unless stated, no other benchmark in flight |
| Run | 2026-11-26, `ddead1a` |

JMH 1.37 runs on JDK 25 without an ASM override: the plugin resolves `asm:9.0` and produced and ran
every benchmark class in this suite, so the Java 25 class file version is not a problem for this
generator. The only JDK 25 complaint is a `sun.misc.Unsafe::objectFieldOffset` deprecation warning
printed by `jmh-core` itself.

The machine sits on a desk next to a browser, a chat client and the IDE. Its sustained limit is well
below the turbo limit and this session hit it: the CPU-bound rates are 3-10x below the first run on
the same box. Treat every `ops/s` figure for `mix(4096)` work as a lower bound from a hot laptop.

## How to reproduce

```bash
./gradlew :tuff-benchmarks:jmh
```

The task runs the whole suite with 3 warmup and 5 measurement iterations of one second, one fork, and
writes `tuff-benchmarks/build/results/jmh/results.txt` plus `.json`. Run it in waves instead of one
go: JMH keeps one fork per benchmark and a single stuck fork holds the Gradle task open.

```bash
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 SubmitPathBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 (SubmitOverheadBenchmark|ScalingBenchmark)"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 (QueueDepthDispatchBenchmark|SelectionStrategyBenchmark)"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 (FailurePathBenchmark|QueuedCancellationBenchmark|RunningCancellationBenchmark|LifecycleBenchmark)"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 (VirtualLaneBenchmark|PlatformLaneBenchmark|BlockingIoBaselineBenchmark)"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 IdleLatencyBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 VirtualLaneLatencyBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 (VirtualLaneCeilingBenchmark|LaneSubmitCostBenchmark)"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 SpillBenchmark"
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 (VirtualLaneQueuedCancelBenchmark|VirtualLaneRunningCancelBenchmark|VirtualLaneEvictBenchmark)"
```

`-to 120` is the JMH iteration timeout in seconds; without it a scenario that blocks forever keeps
the fork alive and the Gradle task never returns. `--rerun` is needed when the JMH arguments did not
change, because Gradle would consider the task up to date.

`UnderLoadLatencyBenchmark` is **not part of the trusted set**: both attempts in this session stalled
for more than fifteen minutes with no iteration output, before and after the harness fix. It is left
in the source tree for a quiet machine and is excluded from every table and conclusion here.

## Harness bounds

Two fixtures deliberately bound what a producer can put into memory:

- `SpillBenchmark` submits through a `Semaphore(4096)` and releases the permit when the task settles.
  Without that window the producers can fill the queue faster than a `Thread.sleep` body retires it
  across iterations; the batch latches do not bound the earlier batches. The first version of this
  scenario did exactly that and died with `OutOfMemoryError` after ten minutes, which also left the
  JMH fork alive. The window is eight times the lane's concurrency, so it never throttles the
  measured lane, and the score is a steady state, not the maximum a submitter can push into heap.
- Every lane fixture uses a bounded queue (`8192` for the saturation scenarios, `20 000 + 4096` for
  the submit-cost scenario that has to hold a full batch). A rejection is then a visible fact rather
  than an unbounded backlog.

This also means the throughput figures in the saturation scenarios are "what the lane sustains with
a bounded producer", not "the maximum this machine can absorb". A benchmark that ignores backpressure
measures heap, not the scheduler.

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
| `VirtualLaneLatencyBenchmark` | one submit and join per op on an idle lane, empty and 1 ms bodies |
| `VirtualLaneQueuedCancelBenchmark` | cancelling 2 048 LOW tasks queued behind a full lane |
| `VirtualLaneRunningCancelBenchmark` | cancelling a LOW task already running on a virtual thread |
| `VirtualLaneEvictBenchmark` | `DROP_OLDEST` eviction on a full per-consumer LOW queue |
| `UnderLoadLatencyBenchmark` | excluded this run, see above |

Every throughput benchmark keeps the pool busy for the whole measured window and waits for the batch
once, either by joining the last future (FIFO makes it the last to complete) or, for the plain
executor, with `invokeAll`. Nothing in the measured path parks per task, because a parked thread is
woken by the OS and that wake-up is milliseconds on Windows.

## Submission path

One worker held by a spinning task, 20 000 submissions per invocation, queue depth 20 000
(`SubmitPathBenchmark`, 3 warmup / 5 measurement).

| Scenario | ns per accepted submit |
| --- | --- |
| validation + handle + entry + queue insert, metrics off | 278 |

The earlier run read 219 ns; the difference is run-to-run memory latency, not a change in the path.
The submission path is memory-latency bound: the queue insert walks a `ConcurrentSkipListMap` per
consumer and priority, so a hot laptop with a busy desktop costs more than the insert itself. On a
run where another build was active the same figure read 0.8 us.

Submission against a **full** target (`LaneSubmitCostBenchmark`, 20 000 submissions per batch, the
lane held by as many blocking bodies as it has permits):

| Target | ns per accepted submit |
| --- | --- |
| virtual lane, limit 8 (8 held bodies) | 1 012 |
| virtual lane, limit 32 (32 held bodies) | 964 |
| platform pool, 8 held workers | 195 |
| platform pool, 8 held workers (repeat of the same fixture) | 207 |

Before the wake-one change the same benchmark read 14.4 us on the virtual lane and 11.0-12.2 us on
the platform pool: the old signal unparked every parked worker on every submission, roughly 1.4 us
per worker. A submission now hands one credit to one waiter under the idle monitor, so the platform
row is the submission path itself and the virtual row is one monitor notification to the pump. This
is the expected shape: nanoseconds on the platform lane, about a microsecond where a pump has to be
woken, and no growth with the pool size.

The platform rows hold 8 workers busy, so they measure a saturated pool, not a parked one. The
parked case is the idle round trip below, where the wake-up is on the critical path.

## Submit and execute, one worker

Batch of 8 192 tasks per invocation, one consumer, FIFO (`SubmitOverheadBenchmark`).

| Task body | ops/s | us per task |
| --- | --- | --- |
| empty | 414 049 | 2.42 |
| ~0.6 us | 288 698 | 3.46 |

These two rows are the ones the throttled machine distorted most: the earlier run read 1.24 M
(0.80 us) and 849 k (1.18 us), and the third warmup in this run already read 0.54 us per submission
with the same binary. The difference between the two rows is still the body plus the slightly larger
allocation footprint, not extra scheduler work.

## Scaling

Four producers, batch of 2 048 tasks each, ~4-6 us body, FIFO (`ScalingBenchmark`).

| Workers | ops/s | vs 1 worker |
| --- | --- | --- |
| 1 | 62 870 | 1.00x |
| 2 | 84 072 | 1.34x |
| 4 | 107 567 | 1.71x |
| 8 | 113 711 | 1.81x |

The series is not usable as a scaling claim in this session. Each parameter is a fresh fork and each
fork starts at full boost: the first two warmup seconds read 218 k / 353 k / 643 k / 1.04 M ops/s for
1/2/4/8 workers, and the third warmup collapses to 70 k / 134 k / 315 k / 553 k. A single worker
losing 3.7x rules out scheduler contention as the cause; the machine is hitting its sustained power
limit after roughly two seconds of load. The eight-worker boost figure is within 11% of the earlier
run's 1.16 M, so the pool has not lost capacity. Re-run this table on a quiet, cool machine before
quoting it.

## Dispatch out of a deep queue

One worker, 10 000 queued tasks spread over four priorities and 1/4/16 consumers, timed drain
(`QueueDepthDispatchBenchmark`, 3 warmup / 5 measurement).

| Consumers | us per task |
| --- | --- |
| 1 | 0.80 |
| 4 | 0.82 |
| 16 | 0.91 |

Selection is `O(consumers)` per poll: every poll reads the best head of each of the four priority
buckets of every consumer before picking one. At sixteen consumers that shows up as roughly 0.1 us
per dispatched task, inside the noise band of this laptop. The earlier run read 1.27 us at sixteen
consumers; the difference is mostly run-to-run noise on a serial benchmark, and the shape stays: many
consumers are more expensive per poll, not catastrophically so.

## Fairness machinery

Same 10 000 task backlog, four consumers, one worker (`SelectionStrategyBenchmark`).

| Strategy | Aging off, us/task | Aging on, us/task |
| --- | --- | --- |
| FIFO | 0.61 | 0.52 |
| PRIORITY | 0.74 | 0.70 |
| WEIGHTED_FAIR | 0.80 | 0.63 |

Weighted fair scheduling costs about 0.1-0.2 us per task over FIFO at four consumers. Aging costs
nothing measurable: it is recomputed from the head timestamp on every poll, there is no timer and no
re-sorting of the queue; the aging-on rows land below the aging-off rows simply because these are
sub-microsecond serial measurements with error bars of the same order. Treat all six as
"a fraction of a microsecond", not as precise constants.

## Failure paths

One worker held, queue exactly at its bound of 256, one submission per op (`FailurePathBenchmark`).

| Policy | ops/s | us per submission |
| --- | --- | --- |
| `REJECT` | 203 766 | 4.9 |
| `DROP_OLDEST` | 131 597 | 7.6 |

A refused submission is not cheap. The engine allocates the task handle and its queue entry before it
knows the queue is full, then discards them and builds a `ComputeRejectedException` with a message;
that is the five microseconds. `DROP_OLDEST` adds the eviction callback and the cancellation of the
evicted task's future, about another 2.7 us. A setup that rejects a large fraction of its traffic pays
this per refused call, so capacity and overflow policy deserve the same attention as the task body.

## Cancellation

Queued cancellation is a batch of 2 048 handles in one invocation. Running cancellation is timed
around the single `cancel()` call inside a loop that starts a fresh spinning task each time; the
finished-handle row uses the identical loop as a floor (`QueuedCancellationBenchmark`,
`RunningCancellationBenchmark`).

| Case | Cost |
| --- | --- |
| queued, per handle | 1.53 us |
| running, p50 | ~3.0 us |
| running, p99 | ~20 us |
| running, max | 228 us |
| finished handle (floor), per call | 0.15 us |

Queued cancellation is the expensive one: it removes the entry from the priority queue and completes
the future with a `ComputeCancelledException`, message included. Running cancellation only sets the
flag, but `cancel()` first probes the queue and all carry slots to find out where the task is; the
microseconds are cross-core cache misses on structures the worker touched last, not a lock. The
per-iteration sampler medians move between 1.8 us and 4.3 us as the sample set grows, which is why the
table quotes the aggregate. The 0.15 us floor is what the call costs once the handle is already
terminal.

## Lifecycle

Two workers, no queued work, create + start + `shutdown(Duration.ZERO)` + `awaitTermination`
(`LifecycleBenchmark`).

| Metrics | us per cycle | leaked scheduler threads |
| --- | --- | --- |
| off | 551 | 0 |
| on | 699 | 0 |

Thirteen cycles per configuration left no worker, janitor, adaptive, shutdown or metrics sampler
thread behind. Enabling metrics adds about 150 us to the cycle, which is the collector and its
sampler thread being created and stopped, not per-task work.

## Idle latency

One empty task, one worker, submit and wait for the result (`IdleLatencyBenchmark`, sample mode).

| `idle-wait` | p0 | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| 1 us | 3.9 us | 19.2 us | 35.4 us | 228 us | 7.7 ms |
| 50 us | 3.8 us | 18.9 us | 32.5 us | 226 us | 26.1 ms |
| 50 ms (default) | 3.8 us | 18.1 us | 34.8 us | 211 us | 12.9 ms |

The ~200 ns body (row set from `roundTripShort`) lands about 6 us higher: p50 25-27 us, p90 35-37 us,
p99 254-297 us.

A submission signals an idle worker, so the park interval no longer caps the round trip: all three
settings land in the same band and an empty task on an idle pool costs tens of microseconds. Before
that fix the default setting read p50 112 us and p90 50.6 ms, because the worker slept out the whole
`idle-wait`. The signal is wake-one — a credit plus a notification to a single waiter under the idle
monitor — and the p99 in the hundreds of microseconds and the multi-millisecond maximum are OS
wake-up latency on a laptop, not a scheduler decision; the shorter settings show the same tail.

That round trip is the whole path, not the submission: the accepted-submit cost against a saturated
pool is 195-207 ns (see *Submission path*), and the 20-30 us here is the worker wake-up, the empty
body, future completion and the joining thread.

## Baseline against `ThreadPoolExecutor`

Eight threads, four producers, batch of 8 192, same bodies and same wait-for-all shape
(`ExecutorBaselineBenchmark`).

| Workload | Compute ops/s | TPE ops/s | Compute / TPE |
| --- | --- | --- | --- |
| empty | 1 752 157 | 6 368 264 | 0.28x |
| ~4-6 us | 1 056 588 | 1 146 464 | 0.92x |

These rows are from the earlier run and were not re-measured: the throttled machine cannot produce a
meaningful comparison. For near-empty tasks the wrapper is the dominant cost: 0.57 us per task against
0.16 us for a plain executor, a 3.6x difference. For a task of a few microseconds the difference falls
to about 8%, because the body is the same on both sides. The per-task price of the wrapper is therefore
roughly 0.4 us: consumer validation, the priority queue insert and remove, the quota slot, the handle
state machine and a `CompletableFuture` subclass per task. That is the number to compare against the
value of priorities, quotas, fair share and metrics, not against a bare `execute(Runnable)`.

## Execution lanes

The virtual lane is measured against the platform lane on the same pool, and the platform pool is
measured against itself at different sizes. The lane throughput comparisons use four producers, a
batch of 256 tasks per producer, FIFO, metrics off, and `-wi 3 -i 5 -f 1`; `SpillBenchmark` uses 128,
`BlockingIoBaselineBenchmark` 512, and `LaneSubmitCostBenchmark` 20 000.

Two fixture details matter for reproducing these numbers:

- The worker limit is pinned with `maxThreads == threads`. With `maxThreads: -1` the pool ceiling is
  the processor budget, so a fixed `threads: 32` is silently clamped to 16 on this machine and the
  benchmark would measure the clamp instead of the setting.
- The IO body is `Thread.sleep(1 ms)`. Windows rounds it to 1.87 ms on a platform thread and 2.13 ms
  on a virtual thread, so "ops/s" is really "blocks of ~2 ms per second". Both lanes pay the same
  block, which is what makes the comparison fair. A timed `LockSupport.parkNanos` is not a substitute:
  it has a floor of about 1.5 ms on this OS and used to be corrupted by leftover unpark permits; the
  pool now uses a monitor wait, so a park in a task body does block again, but `Thread.sleep` remains
  the comparable body.
- All lanes run under the same per-consumer quota. Its relative form resolves against platform
  workers plus lane permits, so one consumer can hold every lane slot; `absolute(N)` is exactly `N`.

### LOW blocking work: throughput

8 worker pool, LOW routed to the lane, 1 ms blocking body.

| Lane limit | ops/s | peak task bodies |
| --- | --- | --- |
| 8 | 3 404 | 8 |
| 32 | 13 030 | 32 |
| 128 | 44 976 | 128 |

This is the change the quota fix was for. The earlier run was flat at 3 6xx ops/s for all three
limits and peaked at 8 bodies, because a consumer's limit was capped by the platform pool. A relative
limit now resolves against workers plus lane permits, so the lane fills to `maxConcurrent` and the
blocking rate scales with it (1 : 3.8 : 13.2 across 8/32/128). The last row is about three quarters of
the theoretical `128 / 2.13 ms` (~60 k), which is the four producers and the pump hand-off, not the
lane.

The same load on the platform pool, LOW not routed to a lane:

| Workers | ops/s |
| --- | --- |
| 4 | 1 808 |
| 8 | 3 730 |
| 16 | 7 553 |
| 32 | 15 740 |

The platform pool scales linearly with workers for blocking work because each worker is a real thread
that can block independently. The lane at 8 bodies matches 8 workers (3 404 against 3 730, 9%);
above that it keeps scaling within its permit budget instead of stopping at the worker count.

### LOW blocking work: latency

One submit and join per op, one benchmark thread, idle lane (`VirtualLaneLatencyBenchmark`).

| Body | Limit | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| empty | 8 | 63 us | 108 us | 547 us | 24.9 ms |
| empty | 32 | 62 us | 100 us | 552 us | 25.8 ms |
| empty | 128 | 64 us | 112 us | 592 us | 27.3 ms |
| 1 ms sleep | 8 | 2.13 ms | 3.14 ms | 5.40 ms | 27.5 ms |
| 1 ms sleep | 32 | 2.19 ms | 3.39 ms | 7.52 ms | 28.5 ms |
| 1 ms sleep | 128 | 2.16 ms | 3.28 ms | 7.09 ms | 26.3 ms |

An empty task costs about 0.06-0.1 ms on the lane, against ~20 us on the platform pool: launching and
finishing a virtual thread is the extra. The ~25 ms maximum is the pump's re-evaluation tick, the one
path that still does not have a direct wake-up. For a task that actually blocks, the 2 ms sleep
dominates and the limit does not matter. The empty p50 is below the earlier run's 82-113 us; one run
each, so read the difference as the same order rather than as an improvement.

### LOW CPU-bound work

`mix(4096)`, the same body as `ScalingBenchmark`, 8 worker pool.

| Lane | Limit | ops/s |
| --- | --- | --- |
| virtual | 8 | 57 091 |
| virtual | 32 | 76 159 |
| virtual | 128 | 74 997 |
| platform | 8 | 112 877 |

Virtual threads add no CPU parallelism, and here they cost about 2.0x at equal concurrency: every task
pays a virtual thread start plus the pump hand-off, and the platform workers sit idle. That is the
same degradation class as the earlier run's 2.3x, as expected: the quota fix changes how many slots a
consumer gets, not what a virtual thread costs. The absolute rates are the throttled ones from this
session (see the banner); only the 2.0x ratio is meaningful.

The lane is for blocking work; LOW CPU work should not be routed there. Raising the limit above the
core count does not add throughput (32 and 128 land at the same ~75 k), it only adds virtual threads
competing for the same cores.

### Spill

8 workers, NORMAL 1 ms blocking work, lane limit 64, `spill-wait` 5 ms, `spill-queue-depth` 32,
four producers, bounded queue of 8 192 (`SpillBenchmark`).

| Spill | ops/s | tasks spilled per iteration | queue wait p50 | p90 | p99 |
| --- | --- | --- | --- | --- | --- |
| on | 26 672 | 21 000-27 000 (~85%) | 12.2 ms | 16-18 ms | 25-35 ms |
| off | 3 590 | 0 | 105-113 ms | 121-137 ms | 132-288 ms |

This is the second place where the quota fix changed the answer. Spill used to fire for 1-3 tasks per
second because the pump's quota claim lost against a pool-wide cap; now a saturated single-consumer
pool spills nearly everything, the lane runs NORMAL work at its own 64-way concurrency, and a
blocking backlog goes 7.4x faster with a queue wait an order of magnitude shorter. The shape is
unchanged in one respect: spill only helps because the tasks block. A CPU-bound `NORMAL` backlog still
gains nothing from virtual threads and should keep `normal-spill: false`.

The number is a steady state under a bounded producer (see *Harness bounds*). Without the in-flight
window the harness outran the lane, grew the queue across iterations and died with `OutOfMemoryError`.

### Lane ceiling

One wave of exactly `maxConcurrent` tasks, 8 worker pool, 20 ms block
(`VirtualLaneCeilingBenchmark`).

| Lane limit | Tasks in the wave | Observed peak bodies | JVM platform threads |
| --- | --- | --- | --- |
| 8 | 8 | 8 | 12 |
| 32 | 32 | 32 | 12 |
| 128 | 128 | 128 | 12 |

The wave takes 23-24.5 ms at every limit, i.e. one 20 ms block. The lane ceiling is `maxConcurrent`,
bounded by the consumer's resolved limit (workers plus lane permits for a relative share, or
`absolute(N)`), and the JVM platform thread count does not move: virtual tasks add no OS threads, so a
high limit does not create thread explosion. The earlier run peaked at 8 bodies at every limit because
the consumer quota was capped by the pool; that cap is gone.

### Cancellation and eviction on the lane

| Case | Cost |
| --- | --- |
| queued cancellation, per handle (2 048 at once) | 1.61 us |
| running cancellation, p50 | 0.9 us |
| running cancellation, p90 | 1.9 us |
| running cancellation, p99 | 3.0 us |
| running cancellation, max | 1.24 ms |
| `DROP_OLDEST` submit + evict, full 256 queue | 56.2 us per op (17 776 ops/s) |

Queued and running cancellation are cheaper than on the platform (1.53 us and ~3.0 us p50): the lane
moves an entry out of the queue or flips a flag, and there is no worker to race. Eviction is still the
expensive path — every submit has to evict, cancel the oldest future and wake the pump against a queue
that is already full — but it improved from 189 us to 56 us per op, because the submission signal is
now a single notify instead of a wake of every parked worker.

### Blocking baseline: lane vs fixed pool vs raw virtual threads

8 workers, 1 ms blocking body, four producers, batch 512 (`BlockingIoBaselineBenchmark`).

| Executor | ops/s |
| --- | --- |
| virtual lane, limit 64 | 25 088 |
| `ThreadPoolExecutor`, 8 threads | 4 020 |
| `Executors.newThreadPerTaskExecutor` | 310 402 |

The earlier run's lane row measured 4 017 ops/s and looked "neutral" against the fixed pool, but that
row was running 8 bodies because the quota capped it at the worker count. With the quota counting lane
permits, the limit-64 row really runs 64 bodies and lands at 6.2x the fixed pool. That is the honest
shape of the feature: a bounded lane is a capacity multiplier for blocking work up to
`maxConcurrent`, while a bare virtual thread executor is still about 12x faster again because every
blocked task simply gets another virtual thread. The lane remains bounded by `maxConcurrent` and the
consumer quota; it is not an unbounded thread-per-task escape hatch.

### Platform pool size

Fixed platform pool, no lane, four producers, batch 256 (`PlatformLaneBenchmark`).

| Workers | CPU ops/s | Blocking ops/s |
| --- | --- | --- |
| 4 | 113 573 | 1 808 |
| 8 | 112 877 | 3 730 |
| 16 | 110 522 | 7 553 |
| 32 | 105 451 | 15 740 |

Blocking work scales with workers as expected. CPU work is flat here, and the flatness is the
throttled machine rather than a scheduler property: the same fork reads 113 k at four workers and
105 k at thirty-two, where a linear pool should read eight times more at the top. The earlier run's
collapse at 16 and 32 workers (23 k and 5.6 k) did **not** reproduce, which is consistent with the
wake-all theory — but with the CPU pinned at its power limit this run cannot prove it. Re-measure the
CPU column on a quiet machine before treating the 32-worker number as healthy.

### When the lane helps, and when it hurts

- **Helps.** Keeping LOW blocking work off the platform workers: a blocked virtual task does not hold
  a pool slot, so NORMAL work keeps its workers. That is the isolation the lane was built for. With
  the quota counting lane permits it is also a throughput multiplier for blocking LOW work
  (`maxConcurrent` bodies instead of `workers`), and spilling a saturated `NORMAL` blocking backlog
  now gives 7.4x.
- **Neutral.** Throughput at *equal concurrency*: the lane at 8 bodies matches the 8-worker pool
  (3 404 against 3 730 ops/s). The lane is not a free multiplier; it is a multiplier up to
  `maxConcurrent` and only for work that blocks.
- **Hurts.** CPU-bound `LOW` work (2.0x slower at equal concurrency), very short blocks (a timed park
  on a virtual thread has a floor of roughly 1 ms on this JDK/OS, so a "50 us" IO task is not 50 us),
  and an idle lane (0.06-0.1 ms round trip against ~20 us on the platform).
- **Cannot do what it looks like it can do.** `maxConcurrent` is a hard cap on virtual bodies, and a
  relative consumer limit is resolved against workers plus those permits, so a value above the lane
  cap or above the consumer's resolved limit is inert. Spill is a capacity valve for *blocking* `NORMAL`
  work; for CPU-bound `NORMAL` work it is still pure overhead.

## Observations and suspected defects

1. **Submission no longer pays per parked worker.** A submission against a full pool costs 195-207 ns
   on the platform lane against 11-12 us when `wakeUp()` unparked every worker; on the virtual lane
   it is about 1 us (one notify to the pump) against 14.4 us. Wake-one removed the last scaling
   penalty on the submit path.
2. **Submission on an empty pool costs what a wake-up costs.** An empty task on an idle single-worker
   pool is p50 18-19 us / p90 32-35 us / p99 211-228 us end to end, and the three idle waits (1 us,
   50 us, 50 ms) are indistinguishable. Before the signal existed the default setting read p90 50.6 ms.
3. **A consumer's lane limit now takes effect.** Lane throughput scales 1 : 3.8 : 13.2 across limits
   8/32/128, the observed body count equals the limit, and the full-lane submit cost is flat across
   limits. The old flat rows were the pool-wide quota, not the lane.
4. **Spill is a throughput lever for blocking work, not a tail valve.** 26 672 against 3 590 ops/s at
   limit 64, with ~85% of NORMAL tasks spilled. Queue wait drops from ~110 ms to ~12 ms. This is new
   behaviour from the quota fix and it changes the operational advice: spill is capped by
   `maxConcurrent` and the downstream system, and it must stay off for CPU-bound `NORMAL` work.
5. **Selection is `O(consumers)` per poll.** 16 consumers cost ~0.1 us more per dispatched task than
   one consumer at a 10 000 task backlog. Fine with the three example consumers, worth watching if
   many plugins register their own.
6. **A refused submission costs ~5 us** because the handle and entry are allocated before the queue
   refuses; `DROP_OLDEST` costs ~7.6 us. A rejection-heavy configuration burns a core at ~204 000
   refusals per second.
7. **Running cancellation costs microseconds, not nanoseconds**, because `cancel()` probes the queue
   and all carry slots before it checks the state. Still bounded and lock-free, but not a bare CAS.
8. **Eviction on the lane is expensive but improved 3.4x**: 56 us per submit-and-evict, because every
   submission cancels a future, counts a cancellation and wakes the pump.
9. **CPU-bound work on the virtual lane stays ~2x slower than the platform pool.** Expected in kind;
   the quota fix does not change the price of a virtual thread. Do not route CPU work to the lane.
10. **CPU-bound throughput is unmeasurable on this machine right now.** See the banner: the first two
    warmup seconds are 3-9x faster than every measurement second, a single worker collapses 3.7x, and
    the 8-worker rate is flat from four workers up. This is the laptop's sustained power limit. It is
    not evidence about the engine, and the serial rows are stable, so the report keeps them and drops
    the scaling claims.

## Limitations

- One machine, one run per scenario, one fork. The serial measurements (submission, dispatch,
  cancellation, failure paths, lifecycle) and the lane rows built on `Thread.sleep` are stable across
  runs; the CPU-bound throughput rows are not, and this session's are 3-10x below the first run on
  the same box.
- The machine was power-throttled for the whole run. The evidence is inside a single fork: warmup
  seconds 1-2 read 218 k / 1.04 M ops/s at 1 / 8 workers and warmup 3 plus all five measurement
  seconds read 58-72 k / 108-119 k. A single worker cannot be limited by scheduler contention, so the
  cause is the sustained power limit. The CPU tables are kept for shape only.
- `UnderLoadLatencyBenchmark` is excluded: two attempts stalled for over fifteen minutes with no
  output. It is still in the tree for a quiet machine.
- The lane and spill numbers use a 1 ms `Thread.sleep` body. Windows rounds it to 1.87 ms on a
  platform thread and 2.13 ms on a virtual thread, so "ops/s" is really "blocks of ~2 ms per second".
  No absolute rate here transfers to a real network call.
- `SpillBenchmark` and every lane throughput scenario run under a bounded in-flight window and a
  bounded queue. The figures are steady-state rates, not the maximum a producer can push into memory.
  An unbounded producer measures heap, and the first version of this scenario proved it by dying with
  `OutOfMemoryError`.
- `LaneSubmitCostBenchmark` holds every lane permit, which is only possible because the quota no
  longer caps a consumer at the worker count. The rows are not comparable with the earlier run's
  "full lane" rows, which held `min(maxConcurrent, workers)` bodies.
- The dispatch and cancellation benchmarks spin the producer on purpose and time a fixed workload;
  they are service-time measurements, not latency distributions under a fixed arrival rate.
- Not measured: Folia region threads competing with the pool, GC behaviour under a sustained server
  load, JFR overhead, and metrics-on cost on the task path (only the lifecycle case covers metrics).
- The benchmark module is a tool, not a supported artifact: it has no publication, no CI job and no
  stability promise.
