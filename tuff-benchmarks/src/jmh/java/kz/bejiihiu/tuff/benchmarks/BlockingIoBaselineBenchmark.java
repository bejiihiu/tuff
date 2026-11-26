package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
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
 * The same 1 ms blocking load on three executors: the virtual lane, a fixed platform pool and a
 * bare virtual thread executor.
 *
 * <p>Eight platform workers is the deployed shape for this feature: the lane exists to keep LOW
 * blocking work off the platform workers, not to add CPU. The bare executor has no quota, so it
 * shows what one thread per blocking task would do and therefore what the lane leaves on the table.
 *
 * <p>One op is one submitted and executed task.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(4)
public class BlockingIoBaselineBenchmark {

    /** Platform threads or compute workers. */
    static final int WORKERS = 8;

    /** Tasks per producer per invocation. */
    static final int BATCH = 512;

    /** Virtual lane limit; the quota still caps a single consumer at {@value #WORKERS}. */
    static final int MAX_CONCURRENT = 64;

    /** Executor under test. */
    @Param({"VT_LANE", "TPE", "VT_EXECUTOR"})
    private Implementation implementation;

    private AsyncComputeScheduler scheduler;
    private ThreadPoolExecutor platform;
    private ExecutorService virtual;
    private final AtomicInteger sequence = new AtomicInteger();

    /**
     * Builds the selected executor.
     */
    @Setup(Level.Trial)
    public void setUp() {
        switch (this.implementation) {
            case VT_LANE -> {
                this.scheduler = ComputeSchedulers.create(LaneFixtures.virtual(
                    WORKERS, 65536, MAX_CONCURRENT, false, Duration.ZERO, 0
                ));
                this.scheduler.start();
            }
            case TPE -> {
                final ThreadFactory factory = runnable -> {
                    final Thread thread = new Thread(runnable, "jmh-tpe-io-" + this.sequence.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                };
                this.platform = new ThreadPoolExecutor(
                    WORKERS, WORKERS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), factory
                );
                this.platform.prestartAllCoreThreads();
            }
            case VT_EXECUTOR -> {
                final ThreadFactory factory = runnable -> {
                    final Thread thread = Thread.ofVirtual()
                        .name("jmh-vt-io-" + this.sequence.incrementAndGet())
                        .unstarted(runnable);
                    return thread;
                };
                this.virtual = Executors.newThreadPerTaskExecutor(factory);
            }
        }
    }

    /**
     * Stops the selected executor.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        if (this.scheduler != null) {
            this.scheduler.shutdown(Duration.ZERO);
            this.scheduler.awaitTermination(Duration.ofSeconds(10L));
        }
        if (this.platform != null) {
            this.platform.shutdownNow();
            this.platform.awaitTermination(10L, TimeUnit.SECONDS);
        }
        if (this.virtual != null) {
            this.virtual.shutdownNow();
            this.virtual.awaitTermination(10L, TimeUnit.SECONDS);
        }
    }

    /**
     * Submits a batch of 1 ms blocking tasks and waits for all of them.
     *
     * @param blackhole consumes a marker
     * @throws InterruptedException if the wait for the batch is interrupted
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void ioThroughput(final Blackhole blackhole) throws InterruptedException {
        final CountDownLatch done = new CountDownLatch(BATCH);
        final Runnable body = () -> {
            try {
                Thread.sleep(java.time.Duration.ofNanos(LaneFixtures.IO_BLOCK_NANOS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        };
        int submitted = 0;
        for (int index = 0; index < BATCH; index++) {
            switch (this.implementation) {
                case VT_LANE -> this.scheduler.submit(ComputePriority.LOW, () -> {
                    body.run();
                    return Boolean.TRUE;
                });
                case TPE -> this.platform.execute(body);
                case VT_EXECUTOR -> this.virtual.execute(body);
            }
            submitted++;
        }
        done.await();
        blackhole.consume(submitted);
    }

    /**
     * Executor under test.
     */
    public enum Implementation {

        /** LOW work on the virtual lane of a compute scheduler. */
        VT_LANE,

        /** A fixed {@link ThreadPoolExecutor} with eight threads. */
        TPE,

        /** A bare virtual thread executor with no quota. */
        VT_EXECUTOR
    }
}
