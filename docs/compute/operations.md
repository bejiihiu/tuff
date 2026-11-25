# Operations

## What is logged

The compute module logs through the server logger; the configuration layer uses the `Tuff/ComputeConfig`
logger. There is no periodic "pool status" log line — use the metrics API or JFR for that.

| Logger | Level | Message | Meaning |
| --- | --- | --- | --- |
| `Tuff/ComputeConfig` | INFO | `Created the default compute configuration at …` | First start; the template was written |
| `Tuff/ComputeConfig` | INFO | `Compute configuration loaded from …` | Startup read succeeded |
| `Tuff/ComputeConfig` | INFO | `Compute configuration reloaded, nothing changed` | Reload with an identical effective tree |
| `Tuff/ComputeConfig` | INFO | `Compute configuration reloaded, N change(s): …` | Reload changed N dotted keys |
| `Tuff/ComputeConfig` | WARN | `Compute config: <key>: <detail>` | One validation problem; the key or section fell back |
| `Tuff/ComputeConfig` | WARN | `These compute settings take effect after a restart: …` | Changed keys the live pool cannot adopt |
| `Tuff/ComputeConfig` | WARN | `Compute configuration was not reloaded, the scheduler keeps its previous settings` | The file could not be parsed at all |
| `Tuff/ComputeConfig` | ERROR | `Failed to apply the compute configuration to the scheduler` | The apply hook threw; the snapshot stays published |
| `Tuff/Compute` | INFO | `Tuff compute enabled: N worker(s) (processor budget B), queue capacity C, scheduling S` | Engine created and installed before plugins |
| `Tuff/Compute` | INFO | `Tuff compute is disabled in <file> (async-compute.enabled: false)` | No engine, no threads; the command reports it |
| `Tuff/Compute` | INFO | `Tuff compute: processor budget B of C (R region, K chunk, I IO threads)` | Real budget applied after region startup |
| `Tuff/Compute` | INFO | `Tuff compute: fixed worker count N, the server processor budget of B is not applied` | Explicit `workers.threads`; the budget is ignored |
| `Tuff/Compute` | INFO | `Tuff compute: applied new consumer policies to …` | Live reload changed registered consumers |
| `Tuff/Compute` | INFO | `Tuff compute stopped in N ms` | Graceful stop completed before the world save |
| `Tuff/Compute` | ERROR | `Could not initialise/failed to start the Tuff compute engine; the server runs without it` | Bootstrap failure; status becomes `FAILED` |
| `Tuff/Compute` | ERROR | `The Tuff compute engine did not terminate within N ms; …` | Stop timeout; worker stacks are dumped, daemon threads end with the process |
| `Tuff/Compute` | WARN | `Could not read the server thread counts; the compute pool keeps its startup size` | Budget not applied after region startup |
| `Tuff/Compute` | WARN | `Could not apply the processor budget to the compute engine` | `applyProcessorBudget` failed; the pool keeps its size |
| `Tuff/Compute` | INFO | `Stopped the compute JFR recording on shutdown, wrote …` | An active recording was written during stop |
| `ComputeEngineImpl` | INFO | `compute processor budget changed from X to Y (target workers N, ceiling M)` | The platform applied a new CPU budget at runtime |
| `ComputeEngineImpl` | WARN | `compute scheduler terminated with N task(s) still running after the grace period; …` | Running bodies outlived the grace period; they are not interrupted and complete after termination |
| `DynamicWorkerPool` | ERROR | `could not create/start a worker thread` | Thread creation failed; the pool is short a worker |
| `DynamicWorkerPool` | WARN | `worker N escaped executeNext (attempt N)` | A loop bug escaped; three in a row retire the worker |
| `DynamicWorkerPool` | WARN | `workerStopped failed for worker N` | Cleanup failure while a worker exited |
| `QueueJanitor` | WARN | `compute janitor tick failed` | One housekeeping tick threw; the cadence continues |
| `ComputeEngineImpl` | WARN | `adaptive evaluation failed` | The controller threw; the limit holds |
| `ComputeEngineImpl` | ERROR | `compute janitor died` / `compute shutdown failed` | A housekeeping thread died unexpectedly |

Task failures are **not** logged. They go to the caller's future and are counted in `failedTasks`; a stack
trace in the log for every failed computation would be unusable under load. If you need them, attach a
logging failure handler in the plugin or record `tuff.compute.TaskFailed`.

## Command

`/tuff` is registered in the Paper command map at startup, survives `/reload`, and works from the console
and from players. It requires `tuff.command.compute`, granted to operators by default. Tab completion covers
the subcommands.

| Invocation | Action |
| --- | --- |
| `/tuff`, `/tuff compute`, `/tuff status` | Summary |
| `/tuff compute consumers` | Per-consumer breakdown |
| `/tuff compute reload`, `/tuff reload` | Re-read `config/tuff.yml` |
| `/tuff compute jfr start\|stop\|dump` | Flight Recorder control |

The summary prints the scheduler state, live/busy/idle workers, concurrency limit and peak, queue depth and
capacity, submitted/completed/failed/rejected/cancelled/expired counters, throughput, queue-wait and
execution averages with their longest values, the process CPU estimate, and the startup worker/queue/
scheduling settings. It reads the public `SchedulerMetrics` snapshot, which has no percentiles, so the
command shows averages and longest values only.

`consumers` prints, for every registered consumer, its policy (enabled, concurrency limit, weight, queue
bound, overflow policy, max wait), active/limit and queued/bound, the same counters, and wait/execution
averages.

`reload` sends `Reloading the compute configuration...` and then runs on the global tick thread. It reports
the number of changes, the consumer policies applied now, the keys that take effect after a restart and
every validation problem from `problems()`.

When the engine is not running, every subcommand answers with the reason instead of an error. For
`async-compute.enabled: false` it prints `The Tuff compute engine is disabled in config/tuff.yml (async-compute.enabled: false).`
followed by `Set it to true and restart the server to enable the shared worker pool.` A startup failure
points at the `Tuff/Compute` log; very early in startup the state is "not initialized yet".

## Metrics

Read a point-in-time snapshot with `/tuff` or programmatically with `scheduler.metrics()`. Counters are
monotonic since the scheduler started; gauges describe the moment of capture; durations are averages over
the scheduler's lifetime. There is no metrics exporter: the command prints text and the Java API returns an
immutable snapshot. If the engine is not running, neither is available.

| Metric | Meaning | Watch for |
| --- | --- | --- |
| `submittedTasks` | Accepted submissions, including later cancelled ones | Growth rate = offered work |
| `completedTasks` / `failedTasks` | Finished successfully / with a throwable | A rising failure share means a task bug, not pool trouble |
| `rejectedTasks` | Refused submissions, never queued | Any sustained increase means backpressure is real |
| `cancelledTasks` | Cancelled after acceptance (any reason) | Spikes after deployment restarts are expected |
| `expiredTasks` | Cancelled because `max-queue-wait` elapsed | Non-zero means the consumer cannot keep up with its own production |
| `queuedTasks` / `queueCapacity` | Current depth / bound | Depth pinned at capacity is the clearest overload signal |
| `totalWorkers` | Live workers | Should follow load when sizing is automatic |
| `activeWorkers` | Workers inside a task | Compare with `concurrencyLimit`, not with `totalWorkers` |
| `idleWorkers` | Live workers without a permit | Zero while the queue is non-empty means the pool is saturated |
| `peakWorkers` | Highest live count since start | Shows whether the ceiling was ever reached |
| `concurrencyLimit` | Pool-wide permit count | Below `totalWorkers` means adaptive is throttling |
| `throughputPerSecond` | Finished tasks per second over `throughput-window-seconds` | Flat throughput while the queue grows is saturation, not demand |
| `cpuUtilization` | Process CPU load, `NaN` when unavailable | Includes tick threads, GC and JIT; never read it as pool utilisation |
| `averageQueueWait` / `longestQueueWait` | Wait before execution | `longest` catching the window in which a task was starved |
| `averageExecutionTime` / `longestExecutionTime` | Body duration | `longest` shows a task that holds a slot for too long |
| `consumers()` | Per-consumer counters, limits and active/queued counts | Find the subsystem that fills the queue |

Per-consumer snapshots expose `activeTasks`, `queuedTasks`, `concurrencyLimit`, `maxQueuedTasks`, the same
counter set and the same wait/execution statistics. A consumer whose `activeTasks` sits at its
`concurrencyLimit` while its `queuedTasks` grows is limited by its own policy, not by the pool.

### Reading pool state

| State | Signature | What to do |
| --- | --- | --- |
| Healthy under load | Queue near zero or draining, `activeWorkers <= concurrencyLimit`, `averageQueueWait` well below `averageExecutionTime`, no rejects | Nothing |
| Too small | Queue grows over minutes, `averageQueueWait` rises, `activeWorkers` equals `concurrencyLimit`, `idleWorkers` zero, throughput flat | Raise `max-threads`/`threads`, raise consumer limits, or reduce offered work. Check `peakWorkers` against `max-threads` |
| Overloaded | `queuedTasks` at capacity, `rejectedTasks` and/or `expiredTasks` climbing, wait far above execution, throughput not rising | Shed work (drop `LOW`, lower per-consumer bounds), shrink `max-queue-wait`, or add CPU. Raising capacity only buys memory pressure |
| Throttled | `concurrencyLimit < totalWorkers`, low `cpuUtilization`, queue present, wait healthy | Adaptive shrank and has not grown back: check `tuff.compute.AdaptiveDecision` reasons, `target-utilization`, CPU headroom |
| Wasting workers | `totalWorkers` high, `activeWorkers` near zero, queue empty for long periods | `min-threads` too high, keep-alive too long, or a fixed `threads` value larger than needed |
| One task hogging | `activeWorkers == limit`, queue present, `longestExecutionTime` orders of magnitude above the average | Split the task; there is no preemption and no watchdog |
| Queue bound hit per consumer | One consumer at `maxQueuedTasks`, others fine | The consumer's own limit is the bottleneck; raise it or fix its production rate |

A snapshot can be up to one `sample-interval-seconds` old for the queue and worker gauges; counters derived
from lifecycle events are current.

### Thread dumps

Workers are named `<thread-name-prefix>-N`. A dump therefore shows exactly how many workers exist and which
one is inside user code. `jcmd <pid> Thread.print` is enough; look for the prefix configured in
`async-compute.workers.thread-name-prefix`.

The janitor and adaptive timer appear as `…-janitor` and `…-adaptive`. If the pool is saturated and every
worker sits in the same plugin frame, the problem is the task body, not the queue.

## JFR

The scheduler emits `tuff.compute.*` events only with `async-compute.metrics.jfr: true`; with `jfr: false`
the hooks are no-ops even while a recording runs. Changing the key needs a restart.

`/tuff compute jfr start` creates one named recording (`tuff-compute`) from the JVM `default` configuration
with the `tuff.compute` category enabled. The recording is bounded to 30 minutes and 64 MB. It refuses when
the engine is not running, when Flight Recorder is unavailable, when `metrics.jfr` is false (the file would be
empty), or when another Tuff recording is already active.

`dump` writes a copy of the running recording without stopping it, `stop` stops it and writes the file, and
an active recording is written during server shutdown if nobody stopped it. Files go to
`debug/tuff-compute-<UTC timestamp>.jfr` under the server working directory; the command prints the absolute
path and size.

The same data can be recorded with the JDK tools, as long as `metrics.jfr` is enabled:

```bash
jcmd <pid> JFR.start name=tuff-compute settings=profile duration=120s filename=compute.jfr
jfr summary compute.jfr
jfr print --events tuff.compute.TaskFailed,tuff.compute.TaskRejected compute.jfr
```

On Windows use a normal path such as `C:\temp\compute.jfr`.

| Event | Fields | When it fires |
| --- | --- | --- |
| `tuff.compute.TaskSubmitted` | `taskId`, `consumerId`, `taskType`, `priority`, `queueDepth` | Accepted submission |
| `tuff.compute.TaskStarted` | `taskId`, `consumerId`, `taskType`, `priority`, `queueWaitNanos`, `activeTasks` | A worker starts the body; `activeTasks` is that consumer's active count |
| `tuff.compute.TaskCompleted` | `taskId`, `consumerId`, `taskType`, `priority`, `executionNanos` | Successful finish |
| `tuff.compute.TaskFailed` | same plus `failureType`, `failureMessage` | Finish with a throwable |
| `tuff.compute.TaskCancelled` | `taskId`, `consumerId`, `reason` | Cancellation other than queue expiry |
| `tuff.compute.TaskQueueExpired` | `taskId`, `consumerId` | Queue wait limit reached |
| `tuff.compute.TaskRejected` | `consumerId`, `reason` | Refused submission |
| `tuff.compute.QueueDepthSample` | `queuedTasks`, `capacity`, `fillRatio` | Every gauge sample |
| `tuff.compute.WorkerPoolState` | `totalWorkers`, `activeWorkers`, `idleWorkers`, `concurrencyLimit` | Every gauge sample |
| `tuff.compute.WorkerPoolResized` | `totalWorkers`, `concurrencyLimit` | When the sampled worker count changes |
| `tuff.compute.AdaptiveDecision` | `currentLimit`, `desiredLimit`, `minLimit`, `maxLimit`, `samples`, `averageQueueWaitNanos`, `averageExecutionNanos`, `p99QueueWaitNanos`, `utilization`, `reason` | Each adaptive evaluation that passed the cooldown checks |

`AdaptiveDecision.reason` is the fastest way to answer "why is the pool not growing": `HOLD_CPU_BUDGET`,
`HOLD_STABLE`, `AT_MAXIMUM`, `EXPLICIT_WORKER_COUNT`, `INSUFFICIENT_SAMPLES`, `SHRINK_WAIT`, `SHRINK_IDLE`
and the rest name the exact branch.

There is no built-in per-task sampling or throttling on top of JFR's own settings. A profiling preset with
a low threshold will produce a lot of events on a busy server; use a custom recording with `threshold` if
size matters.

## Reading benchmark numbers

The JMH suite and its full tables are in [Benchmarks](benchmarks.md). Use the numbers for shape and
regression tracking, not as server SLAs:

- The wrapper costs roughly **0.4 µs per task** over a bare `execute(Runnable)`: 0.57 µs against 0.16 µs for
  an empty task, but only about 8% on a 4–6 µs body. Priorities, quotas, fair share and metrics are what that
  buys.
- Submit-only is ~219 ns; submit + execute of an empty task is ~0.8 µs. Both are memory-latency bound and
  degrade when another build shares the machine.
- Throughput scaled almost linearly from 1 to 8 workers on the test laptop with a 4–6 µs body. That says the
  pool does not serialise, not that eight workers fit your CPU budget.
- Dispatch out of a deep queue costs ~0.8–1.3 µs per task; 16 consumers add ~0.4 µs because selection is
  `O(consumers)` per poll.
- `WEIGHTED_FAIR` costs ~0.2–0.35 µs per task over FIFO at four consumers; aging is not measurable.
- A refused submission costs ~6 µs (the handle and entry are allocated before the refusal), and a queued
  cancellation ~2.7 µs. Rejection-heavy or cancel-heavy designs pay this per call.
- Idle latency is dominated by `scheduling.idle-wait-millis`: with the 50 ms default the p90 for one task on
  an idle pool is ~50 ms. Lower it if a subsystem waits for results from an occasionally idle pool.

The suite ran on one machine, one fork, one run per scenario; multi-threaded figures moved by 3–10x when
another build was active. Re-run on a quiet machine before using absolute values.

## Tuning playbook

| Symptom | First knob | Then | Caveat |
| --- | --- | --- | --- |
| Queue grows, workers busy | `workers.max-threads` or `workers.threads` | `adaptive.enabled: true` with a matching `max-concurrency` | More workers than the processor budget only adds context switches |
| Queue grows, workers idle | Consumer `max-concurrent` | `fair-share-weight` | A share is a cap, not a reservation; check which consumer is at its limit |
| Background work starves | `scheduling.priority-aging`, `aging-interval-millis` | `max-aging-boost` | Aging lifts priority, it does not preempt running work |
| One consumer monopolises turns | `weighted-fair` + weights | Per-consumer `max-concurrent` | Weights only order dispatch while both queues have work |
| Rejections under bursts | `consumers.*.max-queued-tasks`, `queue.capacity` | Client-side shedding, `drop-oldest` for disposable work | Capacity is memory, not throughput; restart required to change it |
| Stale work runs too late | `max-queue-wait-seconds` | `drop-oldest` for the consumer | Expiry granularity is the janitor tick, 10–100 ms |
| First task after an idle period is slow | `scheduling.idle-wait-millis` | Consumer-side batching | This is the worker park interval; lowering it shortens the wait but polls more often |
| Latency spikes at constant load | `adaptive.enabled` | `target-utilization`, `decrease-factor` | Adaptive needs `threads: -1` and only moves once per window |
| Pool shrinks too aggressively | `adaptive.decrease-factor` | `min-concurrency` | The floor is `max(min-concurrency, min-threads)` |
| Need per-task detail | `metrics.jfr: true` | A JFR recording around the incident | JFR adds allocation only while events are enabled |

Change one thing at a time. Most "the pool is slow" reports are either a task body holding a slot for
seconds or a consumer policy capping itself.

## Known limitations

These are deliberate, with the technical reason:

- **No virtual threads.** They add no parallelism for CPU-bound work, hide workers from thread dumps, and do
  not provide the stable worker index the dispatcher uses. The pool rejects them.
- **No automatic retries.** A failed task is reported to the caller and counted. Retrying is a policy
  decision that belongs to the subsystem, which knows whether the work is idempotent.
- **No preemption or per-task timeout.** A running task holds its slot until it returns. Interrupting
  arbitrary user code is unsafe, and a watchdog that kills threads would corrupt plugin state.
- **Cooperative cancellation only.** Queued work is removed immediately; running work is flagged and its
  result discarded. Consumer removal is the exception: running tasks finish and deliver their result, only
  not-started work is cancelled. A body that never checks the flag runs to completion.
- **Queue capacity needs a restart.** The queue allocates its bound at construction; growing it live would
  require a different structure with the same ordering guarantees.
- **Live reload is narrow.** Only the policies of consumers that are already registered change on a running
  engine. `workers.*`, `queue.*`, `scheduling.*`, `metrics.*`, `adaptive.*` and `shutdown-grace-seconds`
  need a restart, and an override for a consumer that has not registered yet is only read at startup.
- **`queue.overflow-policy` is a default, not an override.** It applies to the `default` consumer and to
  consumers registered on the fly; `consumers.<id>.overflow-policy` and an explicit `registerConsumer`
  policy take precedence for their ids.
- **No built-in consumers.** No server subsystem submits to the pool in this build. The three consumer
  sections in the template are example policies; a plugin must submit under its own id.
- **No metrics exporter.** `/tuff` prints text and `SchedulerMetrics` returns an immutable snapshot;
  there is no Prometheus/OpenMetrics endpoint. Pushing the numbers anywhere is the job of the consumer of
  that API.
- **No percentiles in the public snapshot.** `SchedulerMetrics` exposes averages and longest values, and the
  command prints those. Percentiles are not part of the public API and cannot be shown by `/tuff`; the
  adaptive controller uses them internally for its window.
- **The CPU figure is process-wide.** There is no cheap, honest pool-only CPU measurement on the JVM; a
  made-up number would be worse than `NaN`.
- **Queue and worker gauges are sampled.** They can be one sample interval stale. Counters derived from
  lifecycle events are current.
- **Latency averages are lifetime averages.** `throughputPerSecond` uses the configured window; the wait and
  execution means do not. Restarting resets both.
- **Adaptive concurrency is off by default and needs automatic worker sizing.** With an explicit `threads`
  value the controller holds and reports `EXPLICIT_WORKER_COUNT`.
- **Failed tasks are not adaptive samples.** Their timing says nothing about capacity.
- **Weights are dispatch order, not CPU shares.** There is no bandwidth reservation and no guarantee of a
  minimum share.
- **Work stealing is a stream scan.** All work already lives in one shared queue, so "stealing" only changes
  how an idle worker picks the next consumer to look at.
- **Per-poll cost grows with the consumer count.** Selection is `O(consumers × log(entries per consumer))`.
  Use a stable consumer id per subsystem, never one per request.
- **Automatic sizing trusts the platform budget.** The engine only applies `Workers.resolveThreadCount`;
  the quality of the number depends on the region, chunk and IO thread counts the server reports to
  `ComputeThreadBudget`, and on the bootstrap calling `applyProcessorBudget(int)`. A direct engine
  construction falls back to `Runtime.availableProcessors()`.
- **Shutdown does not wait for running work forever.** Queued work is cancelled immediately. Running tasks
  get the grace period; when it expires the scheduler terminates and the tasks keep running, with their
  futures completing later.
- **No state survives a restart.** Counters, queues and in-flight work are process-local by design.
