package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import kz.bejiihiu.tuff.threads.SheddingReport;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the load shedding policy: when, how much, how fast and how it stops.
 *
 * <p>The scenario in every test is the same one the policy exists for: a critical pool is at its capacity
 * with a request parked, and every background pool is at its capacity too. With the flag off nothing may
 * change; with it on the controller withholds a core from the background pools, never below their minimum,
 * and gives it back after the critical queue has been calm for a few evaluations and a cooldown.
 */
class LoadSheddingTest {

    private static final PoolId COMPUTE = PoolId.COMPUTE;
    private static final PoolId NETWORK = PoolId.NETWORK;
    private static final long COOLDOWN_NANOS = Duration.ofSeconds(5).toNanos();

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
    void withTheFlagOffPressureChangesNothing() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(false, this.compute(1, 2), this.network(1));
        final Lease network = controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()));
        final List<Lease> compute = this.hold(controller, COMPUTE, 2);
        final Thread waiter = this.waiter(controller, NETWORK, Duration.ofMinutes(1));
        waiter.start();
        awaitWaiting(controller, NETWORK, 1);

        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline())),
            "the pool is at its capacity; the flag must not hand it more"
        );

        final SheddingReport report = controller.sheddingReport();
        assertFalse(report.enabled());
        assertFalse(report.active());
        assertEquals(0, report.shedCores());
        assertTrue(report.pools().isEmpty());
        assertEquals(2, controller.budget(COMPUTE).orElseThrow().cores(), "the budget is untouched");
        assertEquals(List.of(), controller.degradedReasons(COMPUTE));

        this.cleanUp(controller, waiter, network, compute);
    }

    @Test
    void pressureWithholdsACoreFromTheBackgroundPool() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(true, this.compute(1, 2), this.network(1));
        final Lease network = controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()));
        final List<Lease> compute = this.hold(controller, COMPUTE, 2);
        final Thread waiter = this.waiter(controller, NETWORK, Duration.ofMinutes(1));
        waiter.start();
        awaitWaiting(controller, NETWORK, 1);

        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))
        );

        final SheddingReport report = controller.sheddingReport();
        assertTrue(report.enabled());
        assertTrue(report.active());
        assertEquals(1, report.shedCores());
        assertEquals(List.of(COMPUTE), report.pools());
        assertTrue(report.summary().contains("1 core(s)"), report.summary());
        assertTrue(report.summary().contains("compute"), report.summary());
        assertEquals(1, controller.budget(COMPUTE).orElseThrow().cores(), "two permits, one withheld");
        assertTrue(controller.degradedReasons(COMPUTE).contains(DegradedReason.LOAD_SHEDDING));
        assertTrue(controller.degradedReasons(NETWORK).contains(DegradedReason.CRITICAL_POOL_PRESSURE));

        // a shed is not a revoked permit: the two leases in flight keep working
        assertEquals(2, controller.inUse(COMPUTE));
        compute.get(0).release();
        assertEquals(1, controller.inUse(COMPUTE));
        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline())),
            "the freed permit must not be re-granted while the shed is in force"
        );

        this.cleanUp(controller, waiter, network, compute.subList(1, compute.size()));
    }

    @Test
    void theWithheldCoreComesBackAfterTheQueueCalms() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(true, this.compute(1, 2), this.network(1));
        final Lease network = controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()));
        final List<Lease> compute = this.hold(controller, COMPUTE, 2);
        final Thread waiter = this.waiter(controller, NETWORK, Duration.ofMinutes(1));
        waiter.start();
        awaitWaiting(controller, NETWORK, 1);
        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))
        );
        assertEquals(1, controller.sheddingReport().shedCores());

        compute.get(0).release();
        network.release();
        waiter.join(2_000);
        assertFalse(waiter.isAlive(), "the parked critical request must be served by the release");

        assertEquals(1, controller.sheddingReport().shedCores(),
            "one calm evaluation is not enough to give the core back");
        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline())),
            "the second calm evaluation is still inside the hysteresis window"
        );
        assertEquals(1, controller.sheddingReport().shedCores());

        this.clock.advance(COOLDOWN_NANOS);
        final Lease restored = controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));

        assertEquals(0, controller.sheddingReport().shedCores());
        assertFalse(controller.sheddingReport().active());
        assertEquals(2, controller.budget(COMPUTE).orElseThrow().cores());
        assertEquals(2, controller.inUse(COMPUTE));
        assertFalse(controller.degradedReasons(COMPUTE).contains(DegradedReason.LOAD_SHEDDING));
        restored.release();
        compute.get(1).release();
        assertEquals(0, controller.inUse(COMPUTE));
    }

    @Test
    void theShedStopsAtTheDeclaredMinimum() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(true, this.compute(1, 3), this.network(1));
        final Lease network = controller.acquire(LeaseRequest.immediate(NETWORK, this.deadline()));
        final List<Lease> compute = this.hold(controller, COMPUTE, 3);
        final Thread waiter = this.waiter(controller, NETWORK, Duration.ofMinutes(1));
        waiter.start();
        awaitWaiting(controller, NETWORK, 1);

        // first decision: one core
        assertThrows(LeaseTimeoutException.class, () -> this.trigger(controller));
        assertEquals(1, controller.sheddingReport().shedCores());

        // the cooldown forbids a second step in the same window
        assertThrows(LeaseTimeoutException.class, () -> this.trigger(controller));
        assertEquals(1, controller.sheddingReport().shedCores());

        // after the cooldown the second core may go
        this.clock.advance(COOLDOWN_NANOS);
        assertThrows(LeaseTimeoutException.class, () -> this.trigger(controller));
        assertEquals(2, controller.sheddingReport().shedCores());
        assertEquals(1, controller.budget(COMPUTE).orElseThrow().cores());

        // and that is the floor: the declared minimum is never shed
        this.clock.advance(COOLDOWN_NANOS);
        assertThrows(LeaseTimeoutException.class, () -> this.trigger(controller));
        assertEquals(2, controller.sheddingReport().shedCores(),
            "a third core would put the pool below its declared minimum");
        assertEquals(1, controller.budget(COMPUTE).orElseThrow().cores());

        this.cleanUp(controller, waiter, network, compute);
    }

    @Test
    void theCriticalReserveIsTheSameWithSheddingOnAndOff() {
        final ResourceControllerImpl shedding = this.controller(true, this.compute(1, 2), this.network(2));
        final ResourceControllerImpl plain = this.controller(false, this.compute(1, 2), this.network(2));

        // no pressure: the switch is a policy for emergencies, not a second budget
        assertEquals(plain.criticalReserve(), shedding.criticalReserve());
        assertEquals(plain.usableCores(), shedding.usableCores());
        assertEquals(plain.budget(COMPUTE), shedding.budget(COMPUTE));
        assertEquals(plain.budgetLedger(), shedding.budgetLedger());
        assertEquals(0, shedding.sheddingReport().shedCores());
        assertFalse(plain.sheddingReport().enabled());
        assertTrue(shedding.sheddingReport().enabled());
    }

    @Test
    void concurrentTrafficUnderSheddingDoesNotLeakPermits() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(true, this.compute(1, 4), this.network(1));
        final int workers = 6;
        final int iterations = 120;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>(workers);
        for (int worker = 0; worker < workers; worker++) {
            final PoolId pool = worker % 3 == 0 ? NETWORK : COMPUTE;
            final Thread thread = new Thread(() -> {
                try {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    for (int iteration = 0; iteration < iterations; iteration++) {
                        try (Lease _ = controller.acquire(LeaseRequest.immediate(pool, this.deadline()))) {
                            Thread.onSpinWait();
                        } catch (LeaseTimeoutException _) {
                            // refused capacity is the policy working, not a failure
                        }
                    }
                } catch (final Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }, "shedding-worker-" + worker);
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
        assertEquals(0, controller.inUse(COMPUTE));
        assertEquals(0, controller.inUse(NETWORK));
        assertEquals(0, controller.waiting(COMPUTE));
        assertEquals(0, controller.waiting(NETWORK));
        final SheddingReport report = controller.sheddingReport();
        assertNotNull(report);
        assertTrue(report.shedCores() >= 0);
        assertTrue(controller.budget(COMPUTE).orElseThrow().cores() >= 1,
            "the floor survives the storm");
    }

    @Test
    void anEnabledPolicyWithoutACriticalLaneThatCanLeaseIsDormant() {
        final PoolSpec networkBelowCapacityCoverage = new PoolSpec(
            NETWORK,
            PoolKind.NETWORK,
            "netty",
            "event loops",
            1,
            -1,
            CoverageLevel.L2,
            ResourceBudget.fixed(1, 0)
        );
        final ResourceControllerImpl controller = this.controller(
            true, this.compute(1, 2), networkBelowCapacityCoverage
        );
        final List<Lease> compute = this.hold(controller, COMPUTE, 2);

        // saturate the shared pool with admission decisions; a critical pool that cannot lease never
        // parks a request, so the trigger has nothing to see
        for (int attempt = 0; attempt < 10; attempt++) {
            assertThrows(
                LeaseTimeoutException.class,
                () -> controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()))
            );
            this.clock.advance(COOLDOWN_NANOS);
        }

        final SheddingReport report = controller.sheddingReport();
        assertTrue(report.enabled(), "the flag is on");
        assertEquals(SheddingReport.State.DORMANT, report.state(),
            "region and network are at most L2, so nothing can ever be shed");
        assertFalse(report.active());
        assertEquals(0, report.shedCores());
        assertEquals(2, controller.budget(COMPUTE).orElseThrow().cores(), "a dormant policy changes no budget");
        assertTrue(report.summary().contains("dormant"), report.summary());

        for (final Lease lease : compute) {
            lease.release();
        }
    }

    private void trigger(final ResourceControllerImpl controller) {
        controller.acquire(LeaseRequest.immediate(COMPUTE, this.deadline()));
    }

    private List<Lease> hold(final ResourceControllerImpl controller, final PoolId pool, final int permits) {
        final List<Lease> leases = new ArrayList<>(permits);
        for (int permit = 0; permit < permits; permit++) {
            leases.add(controller.acquire(LeaseRequest.immediate(pool, this.deadline())));
        }
        return leases;
    }

    private void cleanUp(
        final ResourceControllerImpl controller,
        final Thread waiter,
        final Lease network,
        final List<Lease> compute
    ) throws InterruptedException {
        controller.close();
        waiter.join(2_000);
        assertFalse(waiter.isAlive(), "a shutdown must not leave a waiter parked");
        network.release();
        for (final Lease lease : compute) {
            lease.release();
        }
    }

    private ResourceControllerImpl controller(final boolean loadShedding, final PoolSpec... specs) {
        final FakePoolRegistry registry = new FakePoolRegistry();
        for (final PoolSpec spec : specs) {
            registry.register(spec);
        }
        return new ResourceControllerImpl(
            new FakeTopology(16),
            registry,
            this.threads,
            new ThreadManagementSettings.Resources(true, 1, -1, loadShedding),
            BudgetDeductions.none(),
            this.clock,
            this.sink
        );
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

    private PoolSpec network(final int cores) {
        return new PoolSpec(
            NETWORK,
            PoolKind.NETWORK,
            "netty",
            "event loops",
            1,
            -1,
            CoverageLevel.L3,
            ResourceBudget.fixed(cores, 0)
        );
    }

    private Thread waiter(final ResourceControllerImpl controller, final PoolId pool, final Duration maxWait) {
        final Thread thread = new Thread(() -> {
            try {
                controller.acquire(new LeaseRequest(pool, this.deadline(), maxWait));
            } catch (RuntimeException _) {
                // the test owns the outcome; a shutdown closes the wait on purpose
            }
        }, "shedding-waiter-" + pool);
        thread.setDaemon(true);
        return thread;
    }

    private long deadline() {
        return this.clock.nanoTime() + Duration.ofMinutes(2).toNanos();
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
