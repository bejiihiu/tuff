package kz.bejiihiu.tuff.threads.observe;

import java.time.Duration;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * With compute management off, the engine keeps its own threads, its own size and its own budget.
 *
 * <p>The thread layer registers the compute pools at {@link CoverageLevel#L1}, claims a zero core
 * budget for them and observes whatever the engine reports. It is never the source of a worker thread
 * or a processor budget. The two compute ports ({@link ComputeThreadFactoryPort},
 * {@link ComputeBudgetPort}) exist so a future managed mode can plug in without touching the engine,
 * so the parity that has to hold today is: the port overload filled with the legacy implementations
 * produces the same worker names and the same pool size as the plain factory, even while a thread layer
 * runs next to it.
 *
 * <p>The pool is counted in the live thread table, not through {@code metrics()}: those gauges are
 * sampled on an interval and are a lagging view, so comparing two fresh engines through them is a race.
 * Sizing is fixed, because an automatic pool shrinks itself when the queue is empty and then the count
 * depends on the janitor rather than on anything this test is about.
 */
@Timeout(180)
class ObserveOnlyComputeParityTest {

    /**
     * Fixed prefix, so a leaked worker from an earlier run is easy to spot in a dump.
     */
    private static final String PREFIX = "Tuff Compute Parity";

    private static final int FIXED_WORKERS = 3;

    @Test
    void legacyPortsProduceTheSameEngineAsThePlainFactory() throws Exception {
        Assumptions.assumeTrue(
            Runtime.getRuntime().availableProcessors() >= FIXED_WORKERS,
            "a host with fewer processors than the fixed count cannot run every worker"
        );
        final ComputeSettings settings = fixedSettings();
        final int budget = Math.max(FIXED_WORKERS, Runtime.getRuntime().availableProcessors());

        final ThreadManagerRuntime runtime = new DefaultThreadManagerFactory()
            .create(ThreadManagementSettings.defaults());
        runtime.start();
        try {
            final UnifiedThreadManager manager = runtime.manager();
            final PoolSpec compute = manager.pools().spec(PoolId.COMPUTE).orElseThrow();
            assertEquals(CoverageLevel.L1, compute.coverage(), "observe-only registers compute at L1");
            assertEquals(0, compute.minThreads(), "an observed pool claims no floor");
            assertEquals(0, compute.budget().cores(), "an observed pool claims no processor budget");
            assertFalse(manager.settings().anyManagementEnabled(), "observe-only manages nothing by default");

            final Reading plain = measure(settings, budget, PortKind.PLAIN);
            final Reading ports = measure(settings, budget, PortKind.LEGACY_PORTS);

            assertEquals(PREFIX, family(plain.workerName()), "the plain factory names workers <prefix>-<n>");
            assertEquals(PREFIX, family(ports.workerName()), "the port factory keeps the same name format");
            assertEquals(plain.workers(), ports.workers(), "the pool size must not move");
            assertEquals(FIXED_WORKERS, ports.workers().size(), "a fixed count is the whole pool");
            assertTrue(ports.workers().contains(ports.workerName()), "the task ran on a pool worker");
        } finally {
            runtime.shutdown(Duration.ZERO);
            assertTrue(runtime.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    /**
     * Runs one engine and records what an operator would see.
     *
     * @param settings configuration of the engine
     * @param budget   processor budget handed to the engine
     * @param kind     which factory overload to use
     * @return the reading
     * @throws Exception if the task does not finish inside the bound
     */
    private static Reading measure(
        final ComputeSettings settings,
        final int budget,
        final PortKind kind
    ) throws Exception {
        final AsyncComputeScheduler engine = switch (kind) {
            case PLAIN -> ComputeSchedulers.create(settings, ComputeClock.system(), budget);
            case LEGACY_PORTS -> ComputeSchedulers.create(
                settings,
                ComputeClock.system(),
                ComputeBudgetPort.legacy(),
                ComputeThreadFactoryPort.legacy()
            );
        };
        engine.start();
        try {
            final String worker = engine.submit(() -> Thread.currentThread().getName()).get(20L, TimeUnit.SECONDS);
            return new Reading(worker, workerThreads());
        } finally {
            engine.shutdown();
            assertTrue(engine.awaitTermination(Duration.ofSeconds(10)), "the engine must stop inside the bound");
        }
    }

    /**
     * Names of the live worker threads of the compute pool.
     *
     * <p>A worker is {@code <prefix>-<n>}. The service threads the engine also owns are excluded by the
     * name: {@code <prefix>-janitor}, {@code <prefix>-adaptive}, {@code <prefix>-shutdown},
     * {@code <prefix>-completion-N} and {@code <prefix>-virtual-N}.
     *
     * @return the sorted thread names, possibly empty
     */
    private static Set<String> workerThreads() {
        final SortedSet<String> found = new TreeSet<>();
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            final String name = thread.getName();
            if (name.startsWith(PREFIX + '-') && PREFIX.equals(family(name))) {
                found.add(name);
            }
        }
        return found;
    }

    private static ComputeSettings fixedSettings() {
        return new ComputeSettings(
            true,
            new ComputeSettings.Workers(FIXED_WORKERS, FIXED_WORKERS, FIXED_WORKERS, Duration.ofSeconds(30), PREFIX),
            ComputeSettings.QueueSettings.defaults(),
            ComputeSettings.Scheduling.defaults(),
            ComputeSettings.Consumers.defaults(),
            ComputeSettings.Metrics.defaults(),
            ComputeSettings.Adaptive.defaults(),
            ComputeSettings.Lanes.withoutVirtual(),
            Duration.ofSeconds(5)
        );
    }

    /**
     * Strips the creation counter from a thread name.
     *
     * @param name thread name
     * @return the name without a trailing {@code -<number>}
     */
    private static String family(final String name) {
        final int dash = name.lastIndexOf('-');
        if (dash <= 0) {
            return name;
        }
        final String suffix = name.substring(dash + 1);
        for (int index = 0; index < suffix.length(); index++) {
            if (!Character.isDigit(suffix.charAt(index))) {
                return name;
            }
        }
        return name.substring(0, dash);
    }

    /**
     * Which factory overload a reading came from.
     */
    private enum PortKind {

        /**
         * The plain factory with an explicit budget.
         */
        PLAIN,

        /**
         * The port overload filled with the legacy implementations.
         */
        LEGACY_PORTS
    }

    /**
     * What one engine run looked like from the outside.
     *
     * @param workerName name of the thread that ran the task
     * @param workers    live worker threads of the pool
     */
    private record Reading(String workerName, Set<String> workers) {
    }
}
