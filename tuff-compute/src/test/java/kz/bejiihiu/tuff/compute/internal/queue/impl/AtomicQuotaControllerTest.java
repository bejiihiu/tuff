package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AtomicQuotaControllerTest {

    @Test
    void absoluteLimitIsNeverExceeded() {
        final AtomicQuotaController controller = new AtomicQuotaController(8);
        controller.updateConsumerPolicy("a", ConsumerPolicy.absolute(2));

        assertEquals(2, controller.limit("a"));
        assertTrue(controller.admits("a"));
        assertTrue(controller.tryAcquire("a"));
        assertTrue(controller.tryAcquire("a"));
        assertFalse(controller.tryAcquire("a"));
        assertFalse(controller.admits("a"));
        assertEquals(2, controller.activeTasks("a"));
        assertEquals(2, controller.activeTasks());
        assertEquals(8, controller.totalLimit());

        controller.release("a");
        assertTrue(controller.admits("a"));
        assertEquals(1, controller.activeTasks("a"));
        assertTrue(controller.tryAcquire("a"));
        assertEquals(2, controller.activeTasks("a"));
    }

    @Test
    void relativeLimitFollowsTheCapacity() {
        final AtomicQuotaController controller = new AtomicQuotaController(4);
        controller.updateConsumerPolicy("a", ConsumerPolicy.relative(0.5d));

        assertEquals(2, controller.limit("a"));
        controller.updateTotalLimit(10);
        assertEquals(5, controller.limit("a"));
        controller.updateTotalLimit(2);
        assertEquals(1, controller.limit("a"));
        assertEquals(2, controller.totalLimit());
    }

    @Test
    void loweringTheLimitDoesNotInterruptAnything() {
        final AtomicQuotaController controller = new AtomicQuotaController(4);
        // a relative limit so that lowering the capacity really lowers the resolved limit; an
        // absolute limit is deliberately not capped by the capacity
        controller.updateConsumerPolicy("a", ConsumerPolicy.relative(0.75d));
        assertEquals(3, controller.limit("a"));
        assertTrue(controller.tryAcquire("a"));
        assertTrue(controller.tryAcquire("a"));
        assertTrue(controller.tryAcquire("a"));

        controller.updateTotalLimit(2);

        assertEquals(2, controller.limit("a"));
        assertFalse(controller.admits("a"));
        assertFalse(controller.tryAcquire("a"));
        assertEquals(3, controller.activeTasks("a"));

        controller.release("a");
        assertFalse(controller.admits("a"));
        controller.release("a");
        assertTrue(controller.admits("a"));
    }

    @Test
    void absoluteLimitIsNotCappedByTheCapacity() {
        final AtomicQuotaController controller = new AtomicQuotaController(2);
        controller.updateConsumerPolicy("a", ConsumerPolicy.absolute(3));

        assertEquals(3, controller.limit("a"), "an absolute limit is not a share of the capacity");
        assertTrue(controller.tryAcquire("a"));
        assertTrue(controller.tryAcquire("a"));
        assertTrue(controller.tryAcquire("a"));
        assertFalse(controller.tryAcquire("a"), "an absolute limit is still a hard bound");
        assertEquals(3, controller.activeTasks("a"));
    }

    @Test
    void unknownConsumerHasNoLimitAndNoSlots() {
        final AtomicQuotaController controller = new AtomicQuotaController(4);

        assertEquals(0, controller.limit("ghost"));
        assertEquals(0, controller.activeTasks("ghost"));
        assertFalse(controller.admits("ghost"));
        assertFalse(controller.tryAcquire("ghost"));
        controller.release("ghost");
    }

    @Test
    void overReleaseIsClampedInsteadOfGoingNegative() {
        final AtomicQuotaController controller = new AtomicQuotaController(2);
        controller.updateConsumerPolicy("a", ConsumerPolicy.absolute(1));

        controller.release("a");
        assertEquals(0, controller.activeTasks("a"));
        assertTrue(controller.tryAcquire("a"));
        assertFalse(controller.tryAcquire("a"));
    }

    @Test
    void removeRequiresAnIdleConsumer() {
        final AtomicQuotaController controller = new AtomicQuotaController(2);
        controller.updateConsumerPolicy("a", ConsumerPolicy.absolute(1));
        assertTrue(controller.tryAcquire("a"));

        assertThrows(IllegalStateException.class, () -> controller.removeConsumer("a"));
        assertEquals(1, controller.activeTasks("a"));

        controller.release("a");
        controller.removeConsumer("a");
        assertEquals(0, controller.limit("a"));
        controller.removeConsumer("a");
    }

    @Test
    void resetClearsReservationsButKeepsPolicies() {
        final AtomicQuotaController controller = new AtomicQuotaController(4);
        controller.updateConsumerPolicy("a", ConsumerPolicy.absolute(2));
        assertTrue(controller.tryAcquire("a"));
        assertTrue(controller.tryAcquire("a"));

        controller.reset();

        assertEquals(0, controller.activeTasks("a"));
        assertEquals(2, controller.limit("a"));
        assertTrue(controller.tryAcquire("a"));
    }

    @Test
    void rejectsInvalidLimits() {
        assertThrows(IllegalArgumentException.class, () -> new AtomicQuotaController(0));
        final AtomicQuotaController controller = new AtomicQuotaController(1);
        assertThrows(IllegalArgumentException.class, () -> controller.updateTotalLimit(0));
    }

    @Test
    @Timeout(60)
    void concurrentAcquireNeverExceedsTheLimit() throws Exception {
        final AtomicQuotaController controller = new AtomicQuotaController(8);
        controller.updateConsumerPolicy("a", ConsumerPolicy.absolute(3));
        final int threads = 8;
        final int iterations = 2000;
        final CyclicBarrier start = new CyclicBarrier(threads);
        final CountDownLatch done = new CountDownLatch(threads);
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final AtomicInteger acquired = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();

        for (int index = 0; index < threads; index++) {
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    for (int iteration = 0; iteration < iterations; iteration++) {
                        if (!controller.tryAcquire("a")) {
                            failed.incrementAndGet();
                            continue;
                        }
                        final int active = concurrent.incrementAndGet();
                        peak.accumulateAndGet(active, Math::max);
                        acquired.incrementAndGet();
                        concurrent.decrementAndGet();
                        controller.release("a");
                    }
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                } catch (final BrokenBarrierException broken) {
                    throw new IllegalStateException(broken);
                } finally {
                    done.countDown();
                }
            }, "quota-test-" + index);
            thread.start();
        }

        assertTrue(done.await(50, TimeUnit.SECONDS));
        assertTrue(acquired.get() > 0);
        assertTrue(peak.get() <= 3, "peak concurrency was " + peak.get());
        assertEquals(threads * iterations, acquired.get() + failed.get());
        assertEquals(0, controller.activeTasks("a"));
    }
}
