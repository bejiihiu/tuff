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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cost of cancelling LOW tasks that are queued behind a full virtual lane.
 *
 * <p>The lane is held by eight gated tasks, so a batch of 2 048 submissions stays queued. The
 * measured call is {@link ComputeTaskHandle#cancel()} on each handle; the submission happens in a
 * per invocation setup and the queue is verified empty afterwards, both outside the score. One op
 * is one queued cancellation.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class VirtualLaneQueuedCancelBenchmark {

    /** Tasks queued and cancelled per invocation. */
    static final int BATCH = 2_048;

    /** Virtual lane limit; also the number of gated tasks that fill it. */
    static final int MAX_CONCURRENT = 8;

    private AsyncComputeScheduler scheduler;
    private CountDownLatch gate;
    private ComputeTaskHandle<?>[] queued;

    /**
     * Starts a 32 worker pool with an eight slot virtual lane and fills the lane.
     *
     * @throws InterruptedException if waiting for a holder to start is interrupted
     */
    @Setup(Level.Trial)
    public void setUp() throws InterruptedException {
        this.scheduler = ComputeSchedulers.create(LaneFixtures.virtual(
            VirtualLaneBenchmark.WORKERS, 65536, MAX_CONCURRENT, false, Duration.ZERO, 0
        ));
        this.scheduler.start();
        this.gate = new CountDownLatch(1);
        final ComputeTask<Boolean> hold = () -> {
            this.gate.await();
            return Boolean.TRUE;
        };
        for (int index = 0; index < MAX_CONCURRENT; index++) {
            final ComputeTaskHandle<Boolean> handle = this.scheduler.enqueue(new ComputeRequest<>(
                Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.LOW, hold
            ));
            Fixtures.awaitState(handle, ComputeTaskHandle.State.RUNNING);
        }
        this.queued = new ComputeTaskHandle<?>[BATCH];
    }

    /**
     * Releases the holders and stops the scheduler.
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
     * Fills the queue with the batch that the benchmark will cancel.
     */
    @Setup(Level.Invocation)
    public void queueBatch() {
        for (int index = 0; index < BATCH; index++) {
            this.queued[index] = this.scheduler.enqueue(new ComputeRequest<>(
                Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.LOW, LaneFixtures.NOOP
            ));
        }
    }

    /**
     * Cancels the batch.
     *
     * @param blackhole consumes the number of handles that accepted the cancellation
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

    /**
     * Waits until every cancelled handle is terminal and the queue is empty.
     */
    @TearDown(Level.Invocation)
    public void verifyEmpty() {
        for (final ComputeTaskHandle<?> handle : this.queued) {
            final CompletableFuture<?> future = handle.future();
            if (!future.isDone()) {
                future.cancel(false);
            }
        }
        Fixtures.await(() -> {
            for (final ComputeTaskHandle<?> handle : this.queued) {
                if (!handle.future().isDone()) {
                    return false;
                }
            }
            return true;
        }, "the cancelled batch to become terminal");
    }
}
