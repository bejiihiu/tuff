package kz.bejiihiu.tuff.threads.internal.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.impl.controller.ResourceControllerImpl;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceMetricsImpl;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers assembly, lifecycle and the honest reporting of coverage and degradation.
 */
class ThreadManagerRuntimeImplTest {

    private static final Thread.UncaughtExceptionHandler HANDLER = (thread, error) -> { };

    private final RuntimeFixtures.FakeClock clock = new RuntimeFixtures.FakeClock();
    private final RuntimeFixtures.FixedTopology topology = new RuntimeFixtures.FixedTopology(8);
    private final RuntimeFixtures.RecordingSink sink = new RuntimeFixtures.RecordingSink();
    private final List<String> stopSteps = new ArrayList<>();
    private final DefaultThreadManagerFactory factory =
        new DefaultThreadManagerFactory(this.clock, this.topology, this.sink, this.stopSteps::add);

    @Test
    void startsFromDefaultsWithTheCanonicalPoolSet() {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();

        assertEquals(UnifiedThreadManager.State.NEW, manager.state());
        assertTrue(manager.pools().ids().isEmpty());
        assertEquals(ThreadManagementSettings.defaults(), manager.settings());

        runtime.start();

        assertEquals(UnifiedThreadManager.State.RUNNING, manager.state());
        assertEquals(
            Set.of(
                PoolId.REGION_EXECUTION,
                PoolId.COMPUTE,
                PoolId.COMPUTE_VIRTUAL,
                PoolId.CHUNK_WORKER,
                PoolId.CHUNK_IO,
                PoolId.NETWORK,
                PoolId.SERVICE
            ),
            manager.pools().ids()
        );
        for (final PoolSpec spec : manager.pools().specs()) {
            assertEquals(CoverageLevel.L1, spec.coverage(), spec.id().value());
        }
        assertEquals(0, manager.pools().spec(PoolId.REGION_EXECUTION).orElseThrow().minThreads());
        assertNotNull(manager.resources());
        assertNotNull(manager.threadFactory());
        assertNotNull(manager.metrics());
        assertTrue(manager.resources().usableCores() >= 1);

        final ResourceSnapshot snapshot = this.sample(manager);
        assertEquals(7, snapshot.pools().size());
        assertEquals(0, snapshot.totalThreads());

        runtime.shutdown(Duration.ZERO);
    }

    @Test
    void startIsIdempotentAndCreatesNoThread() {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();

        runtime.start();
        runtime.start();

        assertEquals(UnifiedThreadManager.State.RUNNING, manager.state());
        assertEquals(7, manager.pools().ids().size());
        assertEquals(0, this.sample(manager).totalThreads());
        assertEquals(0, this.sink.created().size());
        assertEquals(0, this.sink.terminated().size());

        runtime.shutdown(Duration.ZERO);
    }

    @Test
    void stopIsIdempotentAndBlocksEveryMutation() throws InterruptedException {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();
        runtime.start();

        runtime.shutdown(Duration.ofSeconds(1));
        assertEquals(UnifiedThreadManager.State.STOPPED, manager.state());

        runtime.shutdown(Duration.ofSeconds(1));
        assertTrue(runtime.awaitTermination(Duration.ZERO));
        assertThrows(IllegalStateException.class, runtime::start);
        assertThrows(
            IllegalStateException.class,
            () -> manager.resources().acquire(
                LeaseRequest.immediate(PoolId.COMPUTE, System.nanoTime() + 1_000_000_000L)
            )
        );
        assertThrows(
            IllegalStateException.class,
            () -> manager.threadFactory().newThread(
                ThreadSpec.of(PoolId.COMPUTE, "after-stop", HANDLER),
                () -> { }
            )
        );
        assertThrows(
            IllegalStateException.class,
            () -> manager.pools().register(PoolSpec.observeOnly(PoolId.SERVICE, PoolKind.SERVICE, "test", "late"))
        );
        assertTrue(manager.degradedReasons().contains(DegradedReason.NOT_RUNNING));
    }

    @Test
    void stopRunsTheComponentsInTheDocumentedOrder() {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        runtime.start();

        runtime.shutdown(Duration.ZERO);

        assertEquals(
            List.of("pools", "factory", "registry", "probes", "sampler", "controller"),
            this.stopSteps
        );
        runtime.shutdown(Duration.ZERO);
        assertEquals(6, this.stopSteps.size());
    }

    @Test
    void coverageReasonsReachTheSnapshotWithoutDegrading() {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();
        runtime.start();

        final ResourceSnapshot snapshot = manager.snapshot();
        assertTrue(snapshot.degradedReasons().contains(DegradedReason.OBSERVE_ONLY));
        assertTrue(snapshot.degradedReasons().contains(DegradedReason.FEATURE_DISABLED));
        assertTrue(
            snapshot.pool(PoolId.COMPUTE).orElseThrow().degradedReasons()
                .contains(DegradedReason.OBSERVE_ONLY)
        );
        assertEquals(UnifiedThreadManager.State.RUNNING, manager.state());
        assertFalse(manager.degraded());

        runtime.shutdown(Duration.ZERO);
    }

    @Test
    void managementTheRuntimeCannotDeliverIsDegraded() {
        final ThreadManagementSettings computeConfigured = new ThreadManagementSettings(
            true,
            ThreadManagementSettings.Resources.defaults(),
            ThreadManagementSettings.Lifecycle.defaults(),
            ThreadManagementSettings.RegionExecution.defaults(),
            new ThreadManagementSettings.AsyncCompute(true, -1),
            ThreadManagementSettings.Pools.defaults(),
            ThreadManagementSettings.Metrics.defaults()
        );
        final ThreadManagerRuntime computeRuntime = this.factory.create(computeConfigured);
        computeRuntime.start();
        final UnifiedThreadManager computeManager = computeRuntime.manager();

        assertEquals(UnifiedThreadManager.State.DEGRADED, computeManager.state());
        assertTrue(computeManager.degradedReasons().contains(DegradedReason.CONFIG_CONFLICT));
        assertTrue(computeManager.snapshot().degradedReasons().contains(DegradedReason.CONFIG_CONFLICT));
        computeRuntime.shutdown(Duration.ZERO);

        final ThreadManagementSettings regionConfigured = new ThreadManagementSettings(
            true,
            ThreadManagementSettings.Resources.defaults(),
            ThreadManagementSettings.Lifecycle.defaults(),
            new ThreadManagementSettings.RegionExecution(
                true,
                true,
                ThreadManagementSettings.RegionScheduler.EDF,
                -1,
                4,
                false
            ),
            ThreadManagementSettings.AsyncCompute.defaults(),
            ThreadManagementSettings.Pools.defaults(),
            ThreadManagementSettings.Metrics.defaults()
        );
        final ThreadManagerRuntime regionRuntime = this.factory.create(regionConfigured);
        regionRuntime.start();
        final UnifiedThreadManager regionManager = regionRuntime.manager();

        final PoolSpec region = regionManager.pools().spec(PoolId.REGION_EXECUTION).orElseThrow();
        assertEquals(CoverageLevel.L2, region.coverage());
        assertEquals(1, region.minThreads());
        assertEquals(UnifiedThreadManager.State.DEGRADED, regionManager.state());
        assertTrue(regionManager.degradedReasons().contains(DegradedReason.SCHEDULER_LIMITATION));
        assertTrue(
            regionManager.snapshot().degradedReasons().contains(DegradedReason.SCHEDULER_LIMITATION)
        );
        assertFalse(
            regionManager.snapshot().pool(PoolId.REGION_EXECUTION).orElseThrow().degradedReasons()
                .contains(DegradedReason.OBSERVE_ONLY)
        );
        regionRuntime.shutdown(Duration.ZERO);
    }

    @Test
    void leaseRunsEndToEndThroughTheManager() {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();
        runtime.start();

        final PoolId pool = PoolId.of("test-lease");
        manager.pools().register(new PoolSpec(
            pool,
            PoolKind.COMPUTE,
            "test",
            "lease test pool",
            1,
            1,
            CoverageLevel.L3,
            ResourceBudget.fixed(1, 0)
        ));

        assertEquals(1, manager.resources().budget(pool).orElseThrow().cores());

        final Lease first = manager.resources().acquire(
            LeaseRequest.immediate(pool, System.nanoTime() + 1_000_000_000L)
        );
        assertFalse(first.isReleased());
        assertThrows(
            LeaseTimeoutException.class,
            () -> manager.resources().acquire(
                LeaseRequest.immediate(pool, System.nanoTime() + 1_000_000_000L)
            )
        );
        first.release();
        first.release();

        final Lease second = manager.resources().acquire(
            LeaseRequest.immediate(pool, System.nanoTime() + 1_000_000_000L)
        );
        second.release();
        assertTrue(second.isReleased());

        runtime.shutdown(Duration.ZERO);
    }

    @Test
    void leaseWaitIsVisibleThroughTheWiredController() throws InterruptedException {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();
        runtime.start();

        final PoolId pool = PoolId.of("test-wait");
        manager.pools().register(new PoolSpec(
            pool,
            PoolKind.COMPUTE,
            "test",
            "lease wait pool",
            1,
            1,
            CoverageLevel.L3,
            ResourceBudget.fixed(1, 0)
        ));
        final Lease held = manager.resources().acquire(
            LeaseRequest.immediate(pool, System.nanoTime() + 60_000_000_000L)
        );

        final AtomicReference<Lease> granted = new AtomicReference<>();
        final CountDownLatch acquired = new CountDownLatch(1);
        final Thread waiter = new Thread(() -> {
            granted.set(manager.resources().acquire(new LeaseRequest(
                pool,
                System.nanoTime() + 60_000_000_000L,
                Duration.ofSeconds(30)
            )));
            acquired.countDown();
        }, "utmi-lease-waiter");
        waiter.setDaemon(true);
        waiter.start();

        final ResourceControllerImpl controller = (ResourceControllerImpl) manager.resources();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (controller.waiting(pool) == 0 && System.nanoTime() < deadline) {
            Thread.sleep(1L);
        }
        assertEquals(1, controller.waiting(pool));

        this.clock.advance(Duration.ofMillis(500));
        final ResourceSnapshot snapshot = ((ResourceMetricsImpl) manager.metrics()).sampler().sampleNow();
        assertEquals(500_000_000L, snapshot.pool(pool).orElseThrow().saturation().longestWaitNanos());

        held.release();
        assertTrue(acquired.await(5, TimeUnit.SECONDS));
        assertNotNull(granted.get());
        granted.get().release();
        waiter.join(5_000L);

        runtime.shutdown(Duration.ZERO);
    }

    @Test
    void createdThreadsAreAccountedAndLeaveNothingBehind() throws InterruptedException {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();
        runtime.start();

        final PoolId pool = PoolId.of("test-accounted");
        manager.pools().register(new PoolSpec(
            pool,
            PoolKind.COMPUTE,
            "test",
            "accounted pool",
            1,
            -1,
            CoverageLevel.L2,
            ResourceBudget.elastic(1.0d, 0)
        ));
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread thread = manager.threadFactory().newThread(
            ThreadSpec.of(pool, "utmi-accounted-thread", HANDLER),
            () -> {
                started.countDown();
                await(release);
            }
        );
        thread.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));

        assertEquals(Optional.of(pool), manager.poolOf(thread));
        final ResourceSnapshot running = this.sample(manager);
        assertEquals(1, running.totalThreads());
        assertEquals(pool, running.threads().getFirst().pool());

        release.countDown();
        thread.join(5_000L);
        assertFalse(thread.isAlive());
        assertTrue(manager.poolOf(thread).isEmpty());
        assertEquals(0, this.sample(manager).totalThreads());

        runtime.shutdown(Duration.ZERO);
        assertFalse(threadExists("utmi-accounted-thread"));
    }

    @Test
    void factoryRefusesPoolsItMayNotCreateThreadsFor() {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();
        runtime.start();

        final PoolId unregistered = PoolId.of("never-registered");
        assertThrows(
            IllegalArgumentException.class,
            () -> manager.pools().register(new PoolSpec(
                unregistered,
                PoolKind.COMPUTE,
                "test",
                "l0 pool",
                0,
                -1,
                CoverageLevel.L0,
                ResourceBudget.defaults()
            ))
        );
        assertThrows(
            IllegalStateException.class,
            () -> manager.threadFactory().newThread(
                ThreadSpec.of(unregistered, "l0-thread", HANDLER),
                () -> { }
            )
        );
        assertThrows(
            IllegalStateException.class,
            () -> manager.threadFactory().newThread(
                ThreadSpec.of(PoolId.COMPUTE, "l1-thread", HANDLER),
                () -> { }
            )
        );
        assertThrows(
            IllegalStateException.class,
            () -> manager.threadFactory().wrap(
                ThreadSpec.of(PoolId.REGION_EXECUTION, "l1-wrap", HANDLER),
                () -> { }
            )
        );

        final PoolId virtual = PoolId.of("test-virtual");
        manager.pools().register(new PoolSpec(
            virtual,
            PoolKind.VIRTUAL,
            "test",
            "virtual lane",
            1,
            -1,
            CoverageLevel.L2,
            ResourceBudget.elastic(1.0d, 0)
        ));
        assertThrows(
            IllegalStateException.class,
            () -> manager.threadFactory().newThread(ThreadSpec.of(virtual, "vt-thread", HANDLER), () -> { })
        );

        runtime.shutdown(Duration.ZERO);
    }

    @Test
    void stoppedManagerKeepsItsLastKnownSnapshot() throws InterruptedException {
        final ThreadManagerRuntime runtime = this.factory.create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();
        runtime.start();

        final PoolId pool = PoolId.of("test-last");
        manager.pools().register(new PoolSpec(
            pool,
            PoolKind.COMPUTE,
            "test",
            "last values pool",
            1,
            -1,
            CoverageLevel.L2,
            ResourceBudget.elastic(1.0d, 0)
        ));
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread thread = manager.threadFactory().newThread(
            ThreadSpec.of(pool, "utmi-last-thread", HANDLER),
            () -> {
                started.countDown();
                await(release);
            }
        );
        thread.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        assertEquals(1, this.sample(manager).totalThreads());

        runtime.shutdown(Duration.ofSeconds(1));

        assertEquals(UnifiedThreadManager.State.STOPPED, manager.state());
        final ResourceSnapshot last = manager.snapshot();
        assertEquals(1, last.totalThreads());
        assertTrue(last.degradedReasons().contains(DegradedReason.NOT_RUNNING));

        release.countDown();
        thread.join(5_000L);
    }

    private ResourceSnapshot sample(final UnifiedThreadManager manager) {
        return ((ResourceMetricsImpl) manager.metrics()).sampler().sampleNow();
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean threadExists(final String namePrefix) {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        final Thread[] buffer = new Thread[root.activeCount() + 16];
        final int count = root.enumerate(buffer, true);
        for (int index = 0; index < count; index++) {
            if (buffer[index].getName().startsWith(namePrefix)) {
                return true;
            }
        }
        return false;
    }
}
