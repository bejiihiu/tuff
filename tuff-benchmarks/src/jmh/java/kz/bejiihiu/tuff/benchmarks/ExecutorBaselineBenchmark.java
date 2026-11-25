package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
 * The price of the compute wrapper against a plain JDK executor on the same load.
 *
 * <p>{@code ThreadPoolExecutor} with eight platform threads and an unbounded linked queue is the
 * baseline: same producers, same batch size, same task bodies, same wait-for-all shape. The compute
 * side joins the last future (FIFO makes it the last to finish); the baseline uses
 * {@link ThreadPoolExecutor#invokeAll}, because a plain executor does not promise completion order.
 * The difference is what the scheduler adds on top: consumer validation, the priority queue, quotas,
 * handle bookkeeping and its own future subclass.
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
public class ExecutorBaselineBenchmark {

    /** Worker threads in both executors. */
    static final int THREADS = 8;

    /** Tasks submitted per producer per invocation. */
    static final int BATCH = 8192;

    /** The executor under test. */
    @Param({"COMPUTE", "TPE"})
    private Implementation implementation;

    /** The task body, identical for both executors. */
    @Param({"NOOP", "CPU"})
    private Workload workload;

    private AsyncComputeScheduler scheduler;
    private ThreadPoolExecutor executor;
    private ThreadLocal<CompletableFuture<?>[]> computeBatches;
    private ThreadLocal<Callable<Object>[]> callables;

    /**
     * Builds the selected executor and the task body for the selected workload.
     */
    @Setup
    public void setUp() {
        final boolean cpu = this.workload == Workload.CPU;
        if (this.implementation == Implementation.COMPUTE) {
            this.scheduler = ComputeSchedulers.create(Fixtures.saturated(THREADS, 65536));
            this.scheduler.start();
            this.computeBatches = ThreadLocal.withInitial(() -> new CompletableFuture<?>[BATCH]);
        } else {
            final AtomicInteger counter = new AtomicInteger();
            final ThreadFactory factory = runnable -> {
                final Thread thread = new Thread(runnable, "jmh-tpe-" + counter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            };
            final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
            this.executor = new ThreadPoolExecutor(THREADS, THREADS, 0L, TimeUnit.MILLISECONDS, queue, factory);
            this.executor.prestartAllCoreThreads();
            this.callables = ThreadLocal.withInitial(() -> {
                @SuppressWarnings("unchecked")
                final Callable<Object>[] tasks = new Callable[BATCH];
                for (int index = 0; index < BATCH; index++) {
                    tasks[index] = cpu ? () -> Fixtures.mix(4096) : () -> Boolean.TRUE;
                }
                return tasks;
            });
        }
    }

    /**
     * Shuts the selected executor down.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown
    public void tearDown() throws InterruptedException {
        if (this.scheduler != null) {
            this.scheduler.shutdown(Duration.ZERO);
            this.scheduler.awaitTermination(Duration.ofSeconds(10L));
        }
        if (this.executor != null) {
            this.executor.shutdownNow();
            this.executor.awaitTermination(10L, TimeUnit.SECONDS);
        }
    }

    /**
     * Submits a batch and waits until the whole batch is done.
     *
     * @param blackhole consumes a marker
     * @throws Exception if the baseline executor fails
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void batch(final Blackhole blackhole) throws Exception {
        if (this.implementation == Implementation.COMPUTE) {
            final ComputeTask<?> task = this.workload == Workload.CPU ? Bodies.CPU : Bodies.NOOP;
            final CompletableFuture<?>[] futures = this.computeBatches.get();
            for (int index = 0; index < BATCH; index++) {
                futures[index] = this.scheduler.submit(task);
            }
            blackhole.consume(futures[BATCH - 1].join());
        } else {
            final Callable<Object>[] tasks = this.callables.get();
            final List<Callable<Object>> submitted = new ArrayList<>(BATCH);
            for (int index = 0; index < BATCH; index++) {
                submitted.add(tasks[index]);
            }
            final List<java.util.concurrent.Future<Object>> futures = this.executor.invokeAll(submitted);
            blackhole.consume(futures.get(BATCH - 1).get());
        }
    }

    /**
     * Executor under test.
     */
    public enum Implementation {

        /** The Tuff compute scheduler. */
        COMPUTE,

        /** A plain fixed {@link ThreadPoolExecutor}. */
        TPE
    }

    /**
     * Task body under test.
     */
    public enum Workload {

        /** Empty body. */
        NOOP,

        /** About 4-6 us of dependent integer work. */
        CPU
    }
}
