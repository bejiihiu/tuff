package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
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
 * What a submission costs when the queue is already full.
 *
 * <p>A full queue is held full by a blocked worker, then every op submits one more task. With
 * {@code REJECT} that op pays the handle allocation, the queue refusal and the exception the caller
 * sees; with {@code DROP_OLDEST} it pays the eviction of the oldest task of the same consumer plus
 * the cancellation of that task's future.
 *
 * <p>One op is one submission that meets a full queue.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class FailurePathBenchmark {

    /** Queue capacity and number of pre-filled tasks. */
    static final int CAPACITY = 256;

    /** Overflow policy under test. */
    @Param({"REJECT", "DROP_OLDEST"})
    private OverflowPolicy overflow;

    private AsyncComputeScheduler scheduler;
    private CountDownLatch gate;
    private ComputeTaskHandle<Boolean> blocker;

    /**
     * Starts a one worker pool, holds the worker and fills the queue exactly to its bound.
     */
    @Setup
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.settings(
            1,
            CAPACITY,
            this.overflow,
            ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            Duration.ofMillis(1L),
            Duration.ofMillis(1L),
            false
        ));
        this.scheduler.start();
        this.gate = new CountDownLatch(1);
        this.blocker = Fixtures.blockWorker(this.scheduler, this.gate);
        for (int index = 0; index < CAPACITY; index++) {
            this.scheduler.submit(Fixtures.CONSUMER, kz.bejiihiu.tuff.compute.ComputeTaskType.GENERAL,
                ComputePriority.NORMAL, Fixtures.NOOP);
        }
    }

    /**
     * Releases the held worker and stops the scheduler.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown
    public void tearDown() throws InterruptedException {
        this.gate.countDown();
        this.blocker.future().join();
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Submits a task against a full queue and consumes whichever outcome the policy produces.
     *
     * @param blackhole consumes the accepted future or the rejection reason
     */
    @Benchmark
    public void submitAgainstFullQueue(final Blackhole blackhole) {
        try {
            final CompletableFuture<Boolean> accepted = this.scheduler.submit(
                Fixtures.CONSUMER,
                kz.bejiihiu.tuff.compute.ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                Fixtures.NOOP
            );
            blackhole.consume(accepted);
        } catch (final ComputeRejectedException rejected) {
            blackhole.consume(rejected.reason());
        }
    }
}
