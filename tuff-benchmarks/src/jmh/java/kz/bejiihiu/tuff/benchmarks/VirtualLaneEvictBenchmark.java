package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cost of a LOW submission that evicts the oldest queued LOW task on a full virtual lane.
 *
 * <p>The lane is held by eight gated tasks and the consumer queue is bound to
 * {@value #QUEUE_BOUND} with {@code DROP_OLDEST}. Every measured op accepts one task and cancels the
 * oldest queued one, so it pays the queue insert plus the eviction and the cancelled future. This is
 * the lane equivalent of the platform failure path benchmark.
 *
 * <p>One op is one accepted submission that evicts a queued task.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class VirtualLaneEvictBenchmark {

    /** Per consumer queued task bound. */
    static final int QUEUE_BOUND = 256;

    /** Virtual lane limit and the number of gated holders. */
    static final int MAX_CONCURRENT = 8;

    private AsyncComputeScheduler scheduler;
    private CountDownLatch gate;

    /**
     * Starts a pool with the lane held and the consumer queue filled to its bound.
     *
     * @throws InterruptedException if waiting for a holder to start is interrupted
     */
    @Setup(Level.Trial)
    public void setUp() throws InterruptedException {
        final ConsumerPolicy bounded = new ConsumerPolicy(
            true,
            ConsumerPolicy.ConcurrencyLimit.automatic(),
            QUEUE_BOUND,
            1.0d,
            OverflowPolicy.DROP_OLDEST,
            Duration.ZERO
        );
        this.scheduler = ComputeSchedulers.create(LaneFixtures.settings(
            VirtualLaneBenchmark.WORKERS,
            65536,
            OverflowPolicy.DROP_OLDEST,
            new ComputeSettings.Lanes(new ComputeSettings.Lanes.Virtual(
                true, true, false, MAX_CONCURRENT, Duration.ZERO, 0
            )),
            Map.of(Fixtures.CONSUMER, bounded),
            Duration.ofMillis(1L),
            false
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
        for (int index = 0; index < QUEUE_BOUND; index++) {
            this.scheduler.submit(Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.LOW, LaneFixtures.NOOP);
        }
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
     * Submits one LOW task against a full consumer queue.
     *
     * @param blackhole consumes the accepted future
     */
    @Benchmark
    public void submitAndEvict(final Blackhole blackhole) {
        final CompletableFuture<Boolean> accepted = this.scheduler.submit(
            Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.LOW, LaneFixtures.NOOP
        );
        blackhole.consume(accepted);
    }
}
