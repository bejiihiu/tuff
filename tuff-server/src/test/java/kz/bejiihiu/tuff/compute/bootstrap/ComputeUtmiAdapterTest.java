package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.config.ComputeThreadBudget;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ComputeUtmiAdapterTest {

    private ThreadManagerRuntime runtime;
    private AsyncComputeScheduler engine;

    @AfterEach
    void stopEverything() {
        if (this.engine != null) {
            this.engine.shutdown(Duration.ZERO);
            try {
                this.engine.awaitTermination(Duration.ofSeconds(5L));
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
            this.engine = null;
        }
        if (this.runtime != null) {
            this.runtime.shutdown(Duration.ZERO);
            this.runtime = null;
        }
    }

    @Test
    void anObserveOnlyLayerLeavesTheEngineOnItsOwnThreads() {
        final ThreadManagerRuntime started = this.start(false);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        assertFalse(adapter.isManaged());
        final PoolSpec compute = started.manager().pools().spec(PoolId.COMPUTE).orElseThrow();
        assertEquals(CoverageLevel.L1, compute.coverage(), "without the flag the pool stays observed");

        final String prefix = settings.workers().threadNamePrefix();
        final Thread thread = adapter.platform(prefix, true).newThread(() -> { });
        assertEquals(prefix + "-1", thread.getName(), "observation mode keeps the legacy name");
        assertTrue(thread.isDaemon());
        assertTrue(started.manager().poolOf(thread).isEmpty(), "the legacy factory does not register threads");
        startAndJoin(thread);
    }

    @Test
    void aManagedLayerCreatesTheWorkersAndKeepsTheLegacyNames() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        assertTrue(adapter.isManaged());

        final PoolSpec compute = started.manager().pools().spec(PoolId.COMPUTE).orElseThrow();
        assertEquals(CoverageLevel.L4, compute.coverage());
        assertEquals(settings.workers().minThreads(), compute.minThreads());
        if (started.manager().resources().usableCores() > settings.workers().minThreads()) {
            assertFalse(
                started.manager().degraded(),
                () -> "a managed compute pool must not degrade the layer: " + started.manager().degradedReasons()
            );
        }

        final String prefix = settings.workers().threadNamePrefix();
        final Thread legacy = ComputeThreadFactoryPort.legacy().platform(prefix, true).newThread(() -> { });
        final Thread managed = adapter.platform(prefix, true).newThread(() -> { });

        assertEquals(legacy.getName(), managed.getName(), "the operational thread name must not change");
        assertEquals(prefix + "-1", managed.getName());
        assertEquals(Optional.of(PoolId.COMPUTE), started.manager().poolOf(managed), "the layer accounts the worker");
        startAndJoin(legacy);
        startAndJoin(managed);
        assertTrue(started.manager().poolOf(managed).isEmpty(), "a finished worker is unregistered");
    }

    @Test
    void aManagedThreadKeepsItsExactNameAndIsAccounted() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        final String name = settings.workers().threadNamePrefix() + "-janitor";
        final CountDownLatch inside = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread thread = adapter.named(name, true).newThread(() -> {
            inside.countDown();
            await(release);
        });
        thread.start();
        assertTrue(await(inside), "the named thread must start");
        assertEquals(name, thread.getName(), "a service thread keeps its exact name");
        assertEquals(Optional.of(PoolId.COMPUTE), started.manager().poolOf(thread));
        release.countDown();
        join(thread);
    }

    @Test
    void theVirtualLaneIsAccountedThroughTheLayer() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        final PoolSpec virtual = started.manager().pools().spec(PoolId.COMPUTE_VIRTUAL).orElseThrow();
        assertEquals(PoolKind.VIRTUAL, virtual.kind());
        assertEquals(CoverageLevel.L2, virtual.coverage(), "the lane's threads are accounted, its lane stays the engine's");
        assertEquals(settings.lanes().virtual().maxConcurrent(), virtual.maxThreads());

        final String name = settings.workers().threadNamePrefix() + "-virtual";
        final CountDownLatch inside = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread thread = adapter.virtual(name).newThread(() -> {
            inside.countDown();
            await(release);
        });
        assertTrue(thread.isVirtual());
        assertEquals(name + "-1", thread.getName(), "virtual threads keep the legacy numbering");
        thread.start();
        assertTrue(await(inside), "the virtual thread must start");
        assertEquals(Optional.of(PoolId.COMPUTE_VIRTUAL), started.manager().poolOf(thread));
        release.countDown();
        join(thread);
    }

    @Test
    void theBudgetIsTheControllersNumberWithoutASecondSubtraction() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, ComputeSettings.defaults());

        final int usable = started.manager().resources().usableCores();
        final int expected = started.manager().resources().budget(PoolId.COMPUTE).orElseThrow().resolveCores(usable);

        assertEquals(expected, adapter.processors());
        assertEquals("utmi", adapter.source());
        // the compute pool and the virtual lane's pump share the remainder; nothing else is subtracted
        assertEquals(Math.max(1, usable - 1), expected);
        assertTrue(
            expected >= ComputeThreadBudget.resolve(Runtime.getRuntime().availableProcessors()),
            "the managed budget must not be lower than the legacy one it replaces"
        );
    }

    @Test
    void aRefusedProviderFallsBackToTheLegacyFactory() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        assertTrue(adapter.isManaged());

        // a stopped provider refuses both the factory and the wrapper; the engine must survive that
        started.shutdown(Duration.ZERO);
        this.runtime = null;

        final String prefix = settings.workers().threadNamePrefix();
        final Thread thread = adapter.platform(prefix, true).newThread(() -> { });
        assertEquals(prefix + "-1", thread.getName(), "the fallback keeps the legacy naming scheme");
        startAndJoin(thread);
    }

    @Test
    void theProbeReportsTheEnginesOwnNumbers() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        this.engine = ComputeSchedulers.create(settings, ComputeClock.system(), adapter, adapter);
        this.engine.start();
        adapter.bind(this.engine);

        this.awaitMinimumWorkers();
        final SchedulerMetrics metrics = this.engine.metrics();
        final PoolObservation reading = adapter.platformProbe().observe();

        assertEquals(metrics.totalWorkers(), reading.aliveThreads());
        assertEquals(metrics.activeWorkers(), reading.activeThreads());
        assertEquals(metrics.idleWorkers(), reading.idleThreads());
        assertEquals(metrics.activeWorkers() + metrics.idleWorkers(), reading.aliveThreads(),
            "busy and idle must come from the worker bookkeeping, not from thread states");
        assertEquals(metrics.queuedTasks(), reading.queuedTasks());
        assertEquals(metrics.queueCapacity(), reading.queueCapacity());
        assertTrue(reading.aliveThreads() >= 1);
        assertFalse(reading.threads().isEmpty(), "the probe must list the workers it counted");
        for (final Thread thread : reading.threads()) {
            // workers and the engine's own metrics sampler are the platform pool; the lane threads are
            // filtered out by the scan. Anything else is a foreign thread and a real bug
            final String name = thread.getName();
            assertTrue(
                name.startsWith(settings.workers().threadNamePrefix())
                    || name.startsWith(ComputeSettings.METRICS_THREAD_PREFIX),
                () -> "a foreign thread leaked into the compute pool listing: " + name
            );
        }
    }

    private ThreadManagerRuntime start(final boolean managed) {
        this.runtime = new DefaultThreadManagerFactory().create(settings(managed));
        this.runtime.start();
        return this.runtime;
    }

    private void awaitMinimumWorkers() {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (System.nanoTime() < deadline) {
            if (this.engine.metrics().totalWorkers() >= this.engine.settings().workers().minThreads()) {
                return;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fail("the engine did not spawn its minimum workers in time");
    }

    private static ThreadManagementSettings settings(final boolean computeManaged) {
        return new ThreadManagementSettings(
            true,
            ThreadManagementSettings.Resources.defaults(),
            ThreadManagementSettings.Lifecycle.defaults(),
            ThreadManagementSettings.RegionExecution.defaults(),
            ThreadManagementSettings.AsyncCompute.defaults(),
            new ThreadManagementSettings.Pools(
                ThreadManagementSettings.PoolSettings.defaults(),
                ThreadManagementSettings.PoolSettings.defaults(),
                new ThreadManagementSettings.PoolSettings(computeManaged, true)
            ),
            ThreadManagementSettings.Metrics.defaults()
        );
    }

    private static void startAndJoin(final Thread thread) {
        thread.start();
        join(thread);
    }

    private static void join(final Thread thread) {
        try {
            thread.join(5_000L);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean await(final CountDownLatch latch) {
        try {
            return latch.await(5L, TimeUnit.SECONDS);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
