# Benchmarks

Real JMH runs of the compute engine on one developer machine. The module lives in
`tuff-benchmarks`, depends on `:tuff-compute` only, is not part of `build` or CI, is not published and
is not bundled into the server jar.

## Machine and toolchain

| | |
| --- | --- |
| CPU | Intel Core i7-13620H, 10 physical cores / 16 logical, max 4.9 GHz, 2.4 GHz base |
| RAM | 16 GB |
| OS | Windows 11 Pro 10.0.26100 |
| JVM | Eclipse Temurin 25.0.2+10 (HotSpot), G1, `-Xms1g -Xmx1g -XX:+AlwaysPreTouch` |
| JMH | 1.37 through `me.champeau.jmh` 0.7.4 |
| Load | pool sizes fixed per run, metrics off unless stated, no other benchmark in flight |

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

One task, one worker, wait for the result (`IdleLatencyBenchmark`, sample mode).

| `idle-wait` | p0 | p50 | mean | p90 | p99 |
| --- | --- | --- | --- | --- | --- |
| 1 us | 4.1 us | 19.2 us | 136 us | 51.8 us | 2.06 ms |
| 50 us | 2.9 us | 18.7 us | 74 us | 23.5 us | 1.97 ms |
| 50 ms (default) | 17.8 us | 112 us | 12.0 ms | 50.6 ms | 54.0 ms |

This is the most important operational number in the report. A submission does **not** wake a parked
worker: the worker parks for `scheduling.idle-wait-millis` between polls and only notices new work
when the park expires. With the default 50 ms, a single task on an idle pool sees a p50 around
0.1 ms when the submission happens to land near the end of a park, but p90 is 50.6 ms and p99 is
54 ms because most submissions land right after the worker parked. Below roughly 50 us the wait stops
mattering and the round trip settles at p50 ~19 us, p0 ~4 us, which is the thread handoff itself.

If a subsystem needs low latency on an occasionally idle pool, lower `scheduling.idle-wait-millis`.
The engine-side fix would be to unpark an idle worker when a task is offered; that is a code change,
not a configuration one, and it is out of scope for this report.

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

## Observations and suspected defects

No engine code was changed for this work. The following came out of the measurements and should be
checked by the owners:

1. **No submit-to-wakeup.** An idle worker is woken only by the expiry of `idle-wait-millis`; a
   submission does not unpark it. The default 50 ms therefore becomes a latency cap on an idle pool
   (p90 50.6 ms above). This affects any consumer that submits a task and waits for the result.
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

## Limitations

- One machine, one run per scenario, one fork. Throughput figures are best-case numbers from short
  runs on an otherwise idle laptop; the scaling series and the baseline comparison are internally
  consistent within one run.
- While another Gradle build or the Windows search indexer was active, the same throughput benchmarks
  read 3-10x lower. The serial measurements (dispatch, cancellation, failure paths, lifecycle) were
  stable across all runs; the multi-threaded throughput figures were not. Rerun on a quiet machine
  before making decisions that need absolute values.
- The dispatch benchmarks spin the producer on purpose and time a drain of a fixed backlog; they are
  service-time measurements, not latency distributions under a fixed arrival rate.
- The under-load queue wait (p50 5.5 ms, p99 12.3 ms at a ~8 000 task burst) drifts with machine load
  and is a burst measurement, not a steady-state arrival process.
- Not measured: Folia region threads competing with the pool, GC behaviour under a sustained server
  load, JFR overhead, and metrics-on cost on the task path (only the lifecycle case covers metrics).
- The benchmark module is a tool, not a supported artifact: it has no publication, no CI job and no
  stability promise.
