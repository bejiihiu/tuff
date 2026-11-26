package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cost of one accepted submission while the target lane is completely full.
 *
 * <p>The lane is held by blocking tasks, then every op enqueues one more task: nothing executes, so
 * the score is validation, handle and entry allocation, the queue insert and the submission signal
 * itself. On the virtual lane that signal is a monitor notify on the pump; on the platform pool it
 * is the single idle-worker wake-up the submission path issues, not a wake of the whole pool.
 *
 * <p>The queue is emptied by cancelling the batch in an iteration teardown, which is not measured.
 * One op is one accepted submission.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class LaneSubmitCostBenchmark {

    /** Tasks submitted per invocation. */
    static final int BATCH = 20_000;

    /** Pool size, also the per-consumer quota ceiling. */
    static final int WORKERS = VirtualLaneBenchmark.WORKERS;

    /** Lane the task is placed on. */
    @Param({"VIRTUAL", "PLATFORM"})
    private Lane lane;

    /** Virtual lane hard limit; ignored by the platform variant. */
    @Param({"8", "32"})
    private int maxConcurrent;

    private AsyncComputeScheduler scheduler;
    private CountDownLatch gate;
    private final CompletableFuture<?>[] submitted = new CompletableFuture<?>[BATCH];
    private int blockers;

    /**
     * Starts the scheduler and fills the target lane with gated tasks.
     *
     * @throws InterruptedException if waiting for a blocker to start is interrupted
     */
    @Setup(Level.Trial)
    public void setUp() throws InterruptedException {
        this.gate = new CountDownLatch(1);
        // hold every lane permit, or every worker for the platform variant: the quota now resolves
        // against workers plus lane permits, so one consumer can fill the whole lane. Holding only
        // min(maxConcurrent, WORKERS) would leave the extra permits free and the "full lane" row
        // would actually measure a draining one.
        this.blockers = this.lane == Lane.VIRTUAL ? this.maxConcurrent : WORKERS;
        this.scheduler = ComputeSchedulers.create(this.lane == Lane.VIRTUAL
            ? LaneFixtures.virtual(WORKERS, BATCH + 4096, this.maxConcurrent, false, Duration.ZERO, 0)
            : LaneFixtures.platform(WORKERS, BATCH + 4096));
        this.scheduler.start();
        final ComputeTask<Boolean> block = () -> {
            this.gate.await();
            return Boolean.TRUE;
        };
        final ComputePriority priority = this.lane == Lane.VIRTUAL ? ComputePriority.LOW : ComputePriority.NORMAL;
        for (int index = 0; index < this.blockers; index++) {
            final ComputeTaskHandle<Boolean> handle = this.scheduler.enqueue(new ComputeRequest<>(
                Fixtures.CONSUMER, ComputeTaskType.GENERAL, priority, block
            ));
            Fixtures.awaitState(handle, ComputeTaskHandle.State.RUNNING);
        }
    }

    /**
     * Releases the blockers and stops the scheduler.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        this.gate.countDown();
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Drops whatever the iteration left queued so the next one starts from the same state.
     */
    @TearDown(Level.Iteration)
    public void drain() {
        for (final CompletableFuture<?> future : this.submitted) {
            if (future != null) {
                future.cancel(false);
            }
        }
        Fixtures.await(() -> {
            for (final CompletableFuture<?> future : this.submitted) {
                if (future != null && !future.isDone()) {
                    return false;
                }
            }
            return true;
        }, "the submitted batch to leave the queue");
    }

    /**
     * Submits the whole batch while the lane is held.
     *
     * @param blackhole consumes the number of accepted submissions
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void enqueueOnly(final Blackhole blackhole) {
        int accepted = 0;
        for (int index = 0; index < BATCH; index++) {
            this.submitted[index] = this.scheduler.enqueue(new ComputeRequest<>(
                Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.LOW, LaneFixtures.NOOP
            )).future();
            accepted++;
        }
        blackhole.consume(accepted);
    }

    /**
     * Lane under test.
     */
    public enum Lane {

        /** LOW work on virtual threads. */
        VIRTUAL,

        /** LOW work on the platform pool. */
        PLATFORM
    }
}
