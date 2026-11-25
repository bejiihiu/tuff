package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cost of cancelling a task that has not started yet.
 *
 * <p>The worker is held, a batch of tasks is queued and then every handle is cancelled. A queued
 * cancellation removes the entry from the priority queue and completes the future with a
 * {@code ComputeCancelledException}, so the score includes the exception and its message.
 *
 * <p>One op is one {@code cancel()} that actually removed something.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class QueuedCancellationBenchmark {

    /** Number of queued tasks cancelled per invocation. */
    static final int BATCH = 2048;

    private AsyncComputeScheduler scheduler;
    private CountDownLatch gate;
    private ComputeTaskHandle<Boolean> blocker;
    private ComputeTaskHandle<?>[] queued;

    /**
     * Starts a one worker pool with metrics off.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.settings(
            1,
            65536,
            OverflowPolicy.REJECT,
            ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            Duration.ofMillis(1L),
            Duration.ofNanos(1000L),
            false
        ));
        this.scheduler.start();
    }

    /**
     * Holds the worker and queues a fresh batch.
     */
    @Setup(Level.Iteration)
    public void fill() {
        this.gate = new CountDownLatch(1);
        this.blocker = Fixtures.blockWorker(this.scheduler, this.gate);
        this.queued = new ComputeTaskHandle<?>[BATCH];
        for (int index = 0; index < BATCH; index++) {
            this.queued[index] = this.scheduler.enqueue(new ComputeRequest<>(
                Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.NORMAL, Fixtures.NOOP));
        }
    }

    /**
     * Frees the worker again so the next iteration can hold it.
     */
    @TearDown(Level.Iteration)
    public void release() {
        this.gate.countDown();
        Fixtures.settle(this.blocker.future());
    }

    /**
     * Stops the scheduler.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Cancels the whole batch through the handles.
     *
     * @param blackhole consumes the number of effective cancellations
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void cancelQueued(final Blackhole blackhole) {
        int cancelled = 0;
        for (final ComputeTaskHandle<?> handle : this.queued) {
            if (handle.cancel()) {
                cancelled++;
            }
        }
        blackhole.consume(cancelled);
    }
}
