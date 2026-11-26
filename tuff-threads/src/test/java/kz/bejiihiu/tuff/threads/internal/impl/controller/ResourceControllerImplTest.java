package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetDeductions;
import kz.bejiihiu.tuff.threads.internal.impl.budget.FakeTopology;
import kz.bejiihiu.tuff.threads.internal.impl.registry.ThreadRegistryImpl;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;
import kz.bejiihiu.tuff.threads.internal.testing.FakePoolRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the permit ledger through the public controller contract.
 */
class ResourceControllerImplTest {

    private static final PoolId COMPUTE = PoolId.COMPUTE;
    private static final PoolId CHUNK = PoolId.CHUNK_WORKER;
    private static final PoolId SERVICE = PoolId.SERVICE;
    private static final PoolId NETWORK = PoolId.NETWORK;

    private FakeClock clock;
    private CountingSink sink;
    private ThreadRegistry threads;

    @BeforeEach
    void setUp() {
        this.clock = new FakeClock();
        this.sink = new CountingSink();
        this.threads = new ThreadRegistryImpl(this.sink);
    }

    @Test
    void acquireGrantsAndReleaseIsIdempotent() {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(2, 2)));

        final Lease lease = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));

        assertEquals(1, controller.inUse(COMPUTE));
        assertFalse(lease.isReleased());
        lease.release();
        lease.release();
        controller.release(lease);
        assertTrue(lease.isReleased());
        assertEquals(0, controller.inUse(COMPUTE));
    }

    @Test
    void capacityIsNeverExceeded() {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(2, 2)));

        final Lease first = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        final Lease second = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));

        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))
        );
        assertEquals(1, this.sink.timeouts());
        assertEquals(2, controller.inUse(COMPUTE));
        first.release();
        second.release();
        assertEquals(0, controller.inUse(COMPUTE));
    }

    @Test
    void blockingWaiterTakesTheReleasedPermit() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(1, 1)));
        final Lease held = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        final AtomicReference<Lease> granted = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread waiter = this.waiter(controller, COMPUTE, granted, failure);

        waiter.start();
        awaitWaiting(controller, COMPUTE, 1);
        held.release();
        waiter.join(2_000);

        assertFalse(waiter.isAlive(), "the waiter was not woken by the release");
        assertNull(failure.get(), () -> "waiter failed: " + failure.get());
        assertNotNull(granted.get());
        assertEquals(1, controller.inUse(COMPUTE));
        granted.get().release();
        assertEquals(0, controller.inUse(COMPUTE));
    }

    @Test
    void shutdownWakesWaitersInsteadOfHanging() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(1, 1)));
        final Lease held = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        final AtomicReference<Lease> granted = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread waiter = this.waiter(controller, COMPUTE, granted, failure);

        waiter.start();
        awaitWaiting(controller, COMPUTE, 1);
        controller.close();
        waiter.join(2_000);

        assertFalse(waiter.isAlive(), "a shutdown must not leave a waiter parked");
        assertInstanceOf(IllegalStateException.class, failure.get());
        assertNull(granted.get());
        assertEquals(1, controller.inUse(COMPUTE), "held permits stay valid during shutdown");
        held.release();
        assertEquals(0, controller.inUse(COMPUTE));
        assertThrows(
            IllegalStateException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))
        );
        assertTrue(controller.isClosed());
    }

    @Test
    void regionThreadCannotWaitForALease() {
        final ThreadRegistryImpl regionThreads = new ThreadRegistryImpl(this.sink);
        regionThreads.register(Thread.currentThread(), PoolId.REGION_EXECUTION);
        this.threads = regionThreads;
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(2, 2)));

        final LeaseRequest blocking = new LeaseRequest(COMPUTE, this.deadline(), Duration.ofMillis(50));

        assertThrows(IllegalStateException.class, () -> controller.acquire(blocking));
        try (Lease _ = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))) {
            assertEquals(1, controller.inUse(COMPUTE));
        }
        assertEquals(0, controller.inUse(COMPUTE));
    }

    @Test
    void aRegisteredNonRegionThreadWithAFoliaLikeNameMayBlock() throws Exception {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(1, 2)));

        final AtomicReference<Lease> granted = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread caller = Thread.ofPlatform()
            .name("Folia Region Scheduler Thread #7")
            .daemon()
            .unstarted(() -> {
                try {
                    granted.set(controller.acquire(
                        new LeaseRequest(COMPUTE, this.deadline(), Duration.ofMillis(50))
                    ));
                } catch (final Throwable throwable) {
                    failure.set(throwable);
                }
            });
        this.threads.register(caller, COMPUTE);
        caller.start();
        caller.join(2_000);

        assertNull(failure.get(), () -> "a registered compute thread was mistaken for a tick thread: " + failure.get());
        assertNotNull(granted.get());
        granted.get().release();
    }

    @Test
    void poolRegisteredAfterTheControllerCanLease() {
        final FakePoolRegistry registry = new FakePoolRegistry();
        final ResourceControllerImpl controller = new ResourceControllerImpl(
            new FakeTopology(16),
            registry,
            this.threads,
            new ThreadManagementSettings.Resources(true, 1, -1),
            BudgetDeductions.none(),
            this.clock,
            this.sink
        );

        assertTrue(controller.budget(COMPUTE).isEmpty(), "nothing is registered yet");
        registry.register(this.compute(2, 2));

        try (Lease _ = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))) {
            assertEquals(1, controller.inUse(COMPUTE));
        }
        assertEquals(2, controller.budget(COMPUTE).orElseThrow().cores());
        assertEquals(2, controller.plan().pool(COMPUTE).orElseThrow().capacity());
    }

    @Test
    void replacingTheSpecByIdUpdatesTheLedger() {
        final FakePoolRegistry registry = new FakePoolRegistry();
        registry.register(PoolSpec.observeOnly(COMPUTE, PoolKind.COMPUTE, "async-compute", "background tasks"));
        final ResourceControllerImpl controller = new ResourceControllerImpl(
            new FakeTopology(16),
            registry,
            this.threads,
            new ThreadManagementSettings.Resources(true, 1, -1),
            BudgetDeductions.none(),
            this.clock,
            this.sink
        );

        assertThrows(
            IllegalStateException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline())),
            "L1 observe-only cannot lease"
        );

        // the documented L1 -> L3 path: same id, same kind, new coverage and budget
        registry.register(this.compute(2, 4));

        final Lease lease = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        assertEquals(1, controller.inUse(COMPUTE));
        assertEquals(4, controller.budget(COMPUTE).orElseThrow().cores(), "the ledger must follow the new spec");
        assertEquals(4, controller.plan().pool(COMPUTE).orElseThrow().capacity());
        lease.release();
        assertEquals(0, controller.inUse(COMPUTE));
    }

    @Test
    void aThreadHoldingAPermitCannotWaitForAnother() {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(1, 1)));

        final Lease first = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        final IllegalStateException rejected = assertThrows(
            IllegalStateException.class,
            () -> controller.acquire(new LeaseRequest(COMPUTE, this.deadline(), Duration.ofSeconds(1)))
        );
        assertTrue(rejected.getMessage().contains("already holds a lease"), rejected.getMessage());
        assertEquals(1, controller.inUse(COMPUTE));

        // a non blocking request is still allowed while holding a permit; it just fails fast here
        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))
        );
        first.release();
        assertEquals(0, controller.inUse(COMPUTE));
    }

    @Test
    void criticalPoolIgnoresTheGlobalBackgroundCap() {
        final ResourceControllerImpl controller = this.controller(
            16,
            1,
            List.of(
                this.compute(1, 2),
                new PoolSpec(
                    NETWORK, PoolKind.NETWORK, "netty", "event loops", 2, -1, CoverageLevel.L3,
                    ResourceBudget.fixed(4, 0)
                )
            )
        );

        final Lease compute = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline())),
            "the global cap must stop a second background lease"
        );

        final Lease network = controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()));
        assertEquals(1, controller.inUse(NETWORK), "network must never be starved by the background cap");

        network.release();
        compute.release();
        assertEquals(0, controller.inUse(COMPUTE));
        assertEquals(0, controller.inUse(NETWORK));
    }

    @Test
    void criticalPoolKeepsItsOwnPermits() {
        final ResourceControllerImpl controller = this.controller(
            16,
            -1,
            List.of(
                this.compute(1, 2),
                new PoolSpec(
                    NETWORK, PoolKind.NETWORK, "netty", "event loops", 2, -1, CoverageLevel.L3,
                    ResourceBudget.fixed(2, 0)
                )
            )
        );

        final Lease first = controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()));
        final Lease second = controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()));
        assertEquals(2, controller.inUse(NETWORK));
        assertEquals(0, controller.inUse(COMPUTE));

        final Lease compute = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        assertEquals(1, controller.inUse(COMPUTE));
        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()))
        );

        compute.release();
        first.release();
        second.release();
    }

    @Test
    void heavierWaiterIsServedFirst() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(
            16,
            1,
            List.of(
                new PoolSpec(SERVICE, PoolKind.SERVICE, "test", "gate", 1, -1, CoverageLevel.L3,
                    ResourceBudget.fixed(1, 0)),
                this.compute(1, 1),
                new PoolSpec(CHUNK, PoolKind.CHUNK, "moonrise", "chunks", 1, -1, CoverageLevel.L3,
                    ResourceBudget.fixed(1, 0))
            )
        );
        final Lease gate = controller.acquire(LeaseRequest.immediate(SERVICE, this.deadline()));

        final AtomicReference<Lease> lightweight = new AtomicReference<>();
        final AtomicReference<Lease> heavyweight = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread low = this.waiter(controller, COMPUTE, lightweight, failure);
        low.start();
        awaitWaiting(controller, COMPUTE, 1);
        final Thread high = this.waiter(controller, CHUNK, heavyweight, failure);
        high.start();
        awaitWaiting(controller, CHUNK, 1);

        gate.release();
        high.join(2_000);

        assertNull(failure.get(), () -> "waiter failed: " + failure.get());
        assertNotNull(heavyweight.get(), "the heavier waiter must get the freed permit");
        assertNull(lightweight.get(), "the lighter waiter must keep waiting");
        assertEquals(1, controller.waiting(COMPUTE));

        heavyweight.get().release();
        low.join(2_000);

        assertNotNull(lightweight.get(), "the remaining waiter must be served after the next release");
        lightweight.get().release();
        assertEquals(0, controller.inUse(COMPUTE));
        assertEquals(0, controller.inUse(CHUNK));
    }

    @Test
    void saturationTracksPressure() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(1, 1)));

        final SaturationReport idle = controller.saturation(COMPUTE);
        assertEquals(SaturationReport.Level.IDLE, idle.level());
        assertEquals(0.0d, idle.utilisation());
        assertEquals(SaturationReport.Level.UNKNOWN, controller.saturation(PoolId.of("ghost")).level());

        final Lease held = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        final SaturationReport busy = controller.saturation(COMPUTE);
        assertEquals(1.0d, busy.utilisation());
        assertEquals(SaturationReport.Level.BUSY, busy.level());
        assertEquals(0.0d, busy.queuePressure());

        final Thread waiter = this.waiter(controller, COMPUTE, new AtomicReference<>(), new AtomicReference<>());
        waiter.start();
        awaitWaiting(controller, COMPUTE, 1);

        final SaturationReport saturated = controller.saturation(COMPUTE);
        assertEquals(SaturationReport.Level.SATURATED, saturated.level());
        assertTrue(saturated.saturated());
        assertEquals(1.0d, saturated.queuePressure());
        assertEquals(1L, saturated.queuedTasks());

        controller.close();
        waiter.join(2_000);
        held.release();
    }

    @Test
    void unknownAndObservedOnlyPoolsCannotLease() {
        final FakePoolRegistry registry = new FakePoolRegistry();
        registry.register(PoolSpec.observeOnly(CHUNK, PoolKind.CHUNK, "moonrise", "chunks"));
        final ResourceControllerImpl controller = new ResourceControllerImpl(
            new FakeTopology(16),
            registry,
            this.threads,
            new ThreadManagementSettings.Resources(true, 1, -1),
            BudgetDeductions.none(),
            this.clock,
            this.sink
        );

        assertThrows(
            IllegalStateException.class,
            () -> controller.acquire(LeaseRequest.immediate(CHUNK, this.deadline()))
        );
        assertThrows(
            IllegalStateException.class,
            () -> controller.acquire(LeaseRequest.immediate(PoolId.of("ghost"), this.deadline()))
        );
        assertTrue(controller.budget(PoolId.of("ghost")).isEmpty());
        assertEquals(SaturationReport.Level.UNKNOWN, controller.saturation(PoolId.of("ghost")).level());
        assertTrue(controller.budget(CHUNK).isPresent(), "observation still reports the declared budget");
    }

    @Test
    void foreignLeasesAreRejected() {
        final ResourceControllerImpl owner = this.controller(16, -1, List.of(this.compute(1, 2)));
        final ResourceControllerImpl other = this.controller(16, -1, List.of(this.compute(1, 2)));
        final Lease lease = owner.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));

        assertThrows(IllegalArgumentException.class, () -> other.release(lease));
        assertThrows(NullPointerException.class, () -> owner.release(null));

        lease.release();
        assertEquals(0, owner.inUse(COMPUTE));
    }

    @Test
    void budgetAndCapacityComeFromTheSinglePlan() {
        final FakeTopology topology = new FakeTopology(16);
        final FakePoolRegistry registry = new FakePoolRegistry();
        registry.register(this.compute(4, 4));
        final ResourceControllerImpl controller = new ResourceControllerImpl(
            topology,
            registry,
            this.threads,
            new ThreadManagementSettings.Resources(true, 1, -1),
            BudgetDeductions.none(),
            this.clock,
            this.sink
        );

        assertEquals(11, controller.usableCores());
        assertEquals(4, controller.budget(COMPUTE).orElseThrow().cores());
        assertTrue(controller.plan().ledger().contains("cores 16"));

        topology.setProcessors(64);

        assertEquals(11, controller.usableCores(), "the ledger must not be recomputed");
        assertEquals(4, controller.budget(COMPUTE).orElseThrow().cores());
        assertEquals(11, controller.totalBudget().resolveCores(controller.usableCores()));
    }

    @Test
    void ledgerIsPrintableThroughThePublicContract() {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(4, 4)));

        final String ledger = controller.budgetLedger();

        assertEquals(controller.plan().ledger(), ledger, "one ledger, one text");
        assertTrue(ledger.contains("cores 16"), ledger);
        assertTrue(ledger.contains("ceiling 12"), ledger);
        assertTrue(ledger.contains("region 0"), ledger);
        assertTrue(ledger.contains("chunk 0"), ledger);
        assertTrue(ledger.contains("io 0"), ledger);
        assertTrue(ledger.contains("reserve 1"), ledger);
        assertTrue(ledger.contains("usable 11"), ledger);
    }

    @Test
    void concurrentAcquireReleaseKeepsTheLedgerBalanced() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(32, -1, List.of(this.compute(1, 2)));
        final int workers = 6;
        final int iterations = 100;
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>(workers);
        for (int worker = 0; worker < workers; worker++) {
            final Thread thread = new Thread(() -> {
                try {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    for (int iteration = 0; iteration < iterations; iteration++) {
                        final Lease lease = controller.acquire(
                            new LeaseRequest(COMPUTE, this.deadline(), Duration.ofSeconds(5))
                        );
                        final int active = concurrent.incrementAndGet();
                        peak.accumulateAndGet(active, Math::max);
                        concurrent.decrementAndGet();
                        lease.release();
                    }
                } catch (final Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }, "test-worker-" + worker);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
        start.countDown();
        for (final Thread thread : threads) {
            thread.join(10_000);
            assertFalse(thread.isAlive(), "a worker did not finish");
        }

        assertNull(failure.get(), () -> "unexpected failure: " + failure.get());
        assertTrue(peak.get() <= 2, "capacity was exceeded: peak " + peak.get());
        assertEquals(0, controller.inUse(COMPUTE));
        assertEquals(0, controller.waiting(COMPUTE));
    }

    @Test
    void leaseIdsAreMonotonic() {
        final ResourceControllerImpl controller = this.controller(16, -1, List.of(this.compute(2, 2)));

        final Lease first = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
        final Lease second = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));

        assertTrue(second.uid() > first.uid());
        assertEquals(this.clock.nanoTime(), first.grantedNanos());
        assertEquals(this.deadline(), first.deadlineNanos());
        first.release();
        second.release();
    }

    private ResourceControllerImpl controller(
        final int processors,
        final int maxConcurrentTasks,
        final List<PoolSpec> specs
    ) {
        final ExecutionPoolRegistry registry = this.registry(specs);
        return new ResourceControllerImpl(
            new FakeTopology(processors),
            registry,
            this.threads,
            new ThreadManagementSettings.Resources(true, 1, maxConcurrentTasks),
            BudgetDeductions.none(),
            this.clock,
            this.sink
        );
    }

    private ExecutionPoolRegistry registry(final List<PoolSpec> specs) {
        final FakePoolRegistry registry = new FakePoolRegistry();
        for (final PoolSpec spec : specs) {
            registry.register(spec);
        }
        return registry;
    }

    private PoolSpec compute(final int minThreads, final int cores) {
        return new PoolSpec(
            COMPUTE,
            PoolKind.COMPUTE,
            "async-compute",
            "background tasks",
            minThreads,
            -1,
            CoverageLevel.L3,
            ResourceBudget.fixed(cores, 0)
        );
    }

    private Thread waiter(
        final ResourceControllerImpl controller,
        final PoolId pool,
        final AtomicReference<Lease> granted,
        final AtomicReference<Throwable> failure
    ) {
        final Thread thread = new Thread(() -> {
            try {
                granted.set(controller.acquire(new LeaseRequest(pool, this.deadline(), Duration.ofSeconds(5))));
            } catch (final Throwable throwable) {
                failure.set(throwable);
            }
        }, "test-waiter-" + pool);
        thread.setDaemon(true);
        return thread;
    }

    private long deadline() {
        return this.clock.nanoTime() + Duration.ofSeconds(10).toNanos();
    }

    private static void awaitWaiting(
        final ResourceControllerImpl controller,
        final PoolId pool,
        final int expected
    ) throws InterruptedException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (controller.waiting(pool) == expected) {
                return;
            }
            Thread.sleep(1L);
        }
        assertEquals(expected, controller.waiting(pool), "waiter never parked in pool '" + pool + "'");
    }
}
