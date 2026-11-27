package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves that the cores a critical pool is guaranteed cannot be leased by a non critical pool.
 *
 * <p>The guarantee is arithmetic, not a special case in the grant path: the planner takes the critical
 * minimum out of the remainder the shared pools divide, so an elastic compute pool with a cap of twenty
 * cores still only sees nine. These tests pin the arithmetic through the public controller contract,
 * because that is the shape a pool owner and a command actually see.
 */
class CriticalPoolReserveTest {

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
    void theReserveIsReportedAndSubtractedFromTheSharedBudget() {
        final ResourceControllerImpl controller = this.controller(
            this.compute(20),
            this.network(2, 2)
        );

        assertEquals(2, controller.criticalReserve());
        assertEquals(9, controller.usableCores(), "11 usable minus the 2 core critical reserve");
        assertEquals(9, controller.budget(PoolId.COMPUTE).orElseThrow().cores(),
            "a compute cap of twenty must be clamped to what is left after the reserve");
        assertEquals(2, controller.budget(PoolId.NETWORK).orElseThrow().cores());
        assertTrue(controller.budgetLedger().contains("+ critical 2"), controller.budgetLedger());
    }

    @Test
    void aComputeBurstCannotLeaseTheCriticalReserve() {
        final ResourceControllerImpl controller = this.controller(
            this.compute(20),
            this.network(2, 2)
        );
        final List<Lease> compute = new ArrayList<>();
        for (int core = 0; core < 9; core++) {
            compute.add(controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, this.deadline())));
        }

        assertThrows(
            LeaseTimeoutException.class,
            () -> controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, this.deadline())),
            "the burst must stop at nine cores, not at the twenty the pool asked for"
        );

        try (Lease _ = controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, this.deadline()));
             Lease _ = controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, this.deadline()))) {
            assertEquals(2, controller.inUse(PoolId.NETWORK),
                "the network keeps its two guaranteed cores while compute is maxed out");
        }
        for (final Lease lease : compute) {
            lease.release();
        }
        assertEquals(0, controller.inUse(PoolId.COMPUTE));
        assertEquals(0, controller.inUse(PoolId.NETWORK));
    }

    @Test
    void aCriticalPoolWithoutItsMinimumIsReportedAsPressure() {
        // the budget only allows two cores, the pool declares that it needs four
        final ResourceControllerImpl controller = this.controller(
            this.compute(1),
            this.network(4, 2)
        );

        assertTrue(controller.degradedReasons(PoolId.NETWORK).contains(DegradedReason.CRITICAL_POOL_PRESSURE),
            "an unsatisfied critical minimum is a degradation, not a silent overcommit");
        assertFalse(controller.degradedReasons(PoolId.of("ghost")).contains(DegradedReason.CRITICAL_POOL_PRESSURE),
            "an unknown pool answers with nothing");
    }

    @Test
    void aCriticalPoolWithAGrowingQueueIsReportedAsPressure() throws InterruptedException {
        final ResourceControllerImpl controller = this.controller(
            this.compute(1),
            this.network(1, 1)
        );
        final Lease held = controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, this.deadline()));
        final Thread waiter = this.waiter(controller, PoolId.NETWORK);
        waiter.start();
        awaitWaiting(controller, PoolId.NETWORK, 1);

        assertTrue(controller.degradedReasons(PoolId.NETWORK).contains(DegradedReason.CRITICAL_POOL_PRESSURE));
        assertFalse(controller.degradedReasons(PoolId.COMPUTE).contains(DegradedReason.CRITICAL_POOL_PRESSURE));

        controller.close();
        waiter.join(2_000);
        held.release();
        assertEquals(0, controller.waiting(PoolId.NETWORK));
    }

    private ResourceControllerImpl controller(final PoolSpec... specs) {
        final FakePoolRegistry registry = new FakePoolRegistry();
        for (final PoolSpec spec : specs) {
            registry.register(spec);
        }
        return new ResourceControllerImpl(
            new FakeTopology(16),
            registry,
            this.threads,
            new ThreadManagementSettings.Resources(true, 1, -1, false),
            BudgetDeductions.none(),
            this.clock,
            this.sink
        );
    }

    private PoolSpec compute(final int cores) {
        return new PoolSpec(
            PoolId.COMPUTE,
            PoolKind.COMPUTE,
            "async-compute",
            "background tasks",
            1,
            -1,
            CoverageLevel.L3,
            ResourceBudget.fixed(cores, 0)
        );
    }

    private PoolSpec network(final int minThreads, final int cores) {
        return new PoolSpec(
            PoolId.NETWORK,
            PoolKind.NETWORK,
            "netty",
            "event loops",
            minThreads,
            -1,
            CoverageLevel.L3,
            ResourceBudget.fixed(cores, 0)
        );
    }

    private Thread waiter(final ResourceControllerImpl controller, final PoolId pool) {
        final Thread thread = new Thread(() -> {
            try {
                controller.acquire(new LeaseRequest(pool, this.deadline(), Duration.ofSeconds(5)));
            } catch (RuntimeException _) {
                // the test only cares that it parked; close() releases it
            }
        }, "critical-reserve-waiter");
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
